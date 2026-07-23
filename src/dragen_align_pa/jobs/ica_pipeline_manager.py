"""
Generic ICA pipeline management loop.

This module contains the shared state-machine logic for managing ICA pipeline
runs (submission, monitoring, cancellation, and failure handling) for a cohort.
"""

import json
import time
from collections import Counter
from collections.abc import Callable, Sequence
from datetime import datetime
from enum import Enum, auto
from typing import TypeAlias

import cpg_utils
import google.cloud.exceptions as gcs_exceptions
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_utils.config import config_retrieve, try_get_ar_guid
from loguru import logger

from dragen_align_pa.batches import IcaBatch, PassfailStatusError
from dragen_align_pa.constants.batch_constants import MAX_CONSECUTIVE_ON_SUCCEEDED_FAILURES
from dragen_align_pa.jobs import cancel_ica_pipeline_run, monitor_dragen_pipeline
from dragen_align_pa.utils import delete_pipeline_id_file

ProcessingTarget: TypeAlias = Cohort | SequencingGroup | IcaBatch


class PipelineStatus(Enum):
    """Enumeration of possible ICA pipeline statuses."""

    PENDING = auto()
    INPROGRESS = auto()
    SUCCEEDED = auto()
    FAILED_RETRYING = auto()
    FAILED_FINAL = auto()
    CANCELLED = auto()


class MonitoredTarget:
    """Class to hold the state of a monitored target's ICA pipeline."""

    def __init__(self, target: ProcessingTarget, allow_retry: bool) -> None:
        self.target: ProcessingTarget = target
        self.status: PipelineStatus = PipelineStatus.PENDING
        self.pipeline_id: str | None = None
        self.ar_guid: str | None = None  # Initialise to None
        self.has_been_retried: bool = False
        self.allow_retry: bool = allow_retry
        # Counter for consecutive on_succeeded callback failures; reset to 0
        # on the first successful callback. See _process_succeeded_transition.
        self.on_succeeded_failure_count: int = 0

    @property
    def name(self) -> str:
        return self.target.name

    def set_status(self, new_status: PipelineStatus) -> None:
        if new_status != self.status:
            logger.info(f'Target {self.name}: {self.status.name} → {new_status.name}')
        self.status = new_status


def _process_succeeded_transition(
    target: MonitoredTarget,
    on_succeeded: Callable[[MonitoredTarget], None] | None,
    on_status_change: Callable[[MonitoredTarget, PipelineStatus], None] | None,
    max_failures: int = MAX_CONSECUTIVE_ON_SUCCEEDED_FAILURES,
) -> bool:
    """Run the transactional on_succeeded callback with a per-target failure cap.

    Returns True if the caller should mark the target SUCCEEDED (callback
    succeeded, or none configured). Returns False to leave the target as-is for
    the next poll cycle — the callback raised below the cap, or tripped the cap
    and was already transitioned to FAILED_FINAL. The cap stops a persistently
    broken callback spinning the loop forever.
    """
    if on_succeeded is None:
        return True
    try:
        on_succeeded(target)
    except PassfailStatusError:
        # A malformed passfail value is deterministic (re-reads re-raise); propagate
        # immediately instead of spinning to the cap and condemning the batch.
        raise
    except Exception as exc:  # noqa: BLE001
        target.on_succeeded_failure_count += 1
        if target.on_succeeded_failure_count >= max_failures:
            logger.error(
                f'on_succeeded callback failed {target.on_succeeded_failure_count} consecutive times '
                f'for {target.name} (pipeline {target.pipeline_id}): {exc}. '
                f'Escalating to FAILED_FINAL — the underlying callback error is persistent '
                f'and must be investigated before re-running.',
            )
            target.set_status(PipelineStatus.FAILED_FINAL)
            # Mirror _fire_status_change semantics: best-effort, swallow exceptions.
            if on_status_change is not None:
                try:
                    on_status_change(target, PipelineStatus.FAILED_FINAL)
                except Exception as cb_exc:  # noqa: BLE001
                    logger.error(
                        f'on_status_change callback failed for {target.name} during '
                        f'on_succeeded cap escalation: {cb_exc}. In-memory transition stands.',
                    )
            return False
        logger.error(
            f'on_succeeded callback failed ({target.on_succeeded_failure_count}/{max_failures}) for '
            f'{target.name} (pipeline {target.pipeline_id}): {exc}. '
            f'Leaving status INPROGRESS so the next poll re-fires.',
        )
        return False
    target.on_succeeded_failure_count = 0
    return True


def _failed_final_target_names(monitored_targets: Sequence['MonitoredTarget']) -> list[str]:
    """Names of targets that ended unrecoverably (FAILED_FINAL).

    Args:
        monitored_targets: The loop's monitored targets to inspect.

    Returns:
        The `.name` of each target in `FAILED_FINAL` status (empty if none).
    """
    return [t.name for t in monitored_targets if t.status == PipelineStatus.FAILED_FINAL]


def _log_status_counts(pipeline_name: str, status_counts: Counter[PipelineStatus]) -> None:
    """Log the target status breakdown for one poll cycle."""
    logger.info(
        f'{pipeline_name} pipeline status: '
        f'{status_counts[PipelineStatus.SUCCEEDED]} completed, '
        f'{status_counts[PipelineStatus.INPROGRESS]} in progress, '
        f'{status_counts[PipelineStatus.FAILED_FINAL]} failed, '
        f'{status_counts[PipelineStatus.CANCELLED]} cancelled.'
    )


def _flush_error_log(error_log_path: cpg_utils.Path, *, mode: str, on_error: str) -> None:
    """Copy tmp_errors.log into the GCS error log (best-effort; failures are logged)."""
    try:
        with open('tmp_errors.log') as tmp_log_handle:
            lines = tmp_log_handle.readlines()
            with error_log_path.open(mode) as gcp_error_log_file:
                gcp_error_log_file.write('\n'.join(lines))
    except (OSError, gcs_exceptions.GoogleCloudError) as e:
        logger.error(f'{on_error}: {e}')


def manage_ica_pipeline_loop(  # noqa: PLR0915
    targets_to_process: Sequence[ProcessingTarget],
    outputs: dict[str, cpg_utils.Path],
    pipeline_name: str,
    is_mlr_pipeline: bool,
    success_file_key_template: str,
    pipeline_id_file_key_template: str,
    error_log_key: str,
    submit_function_factory: Callable[[str], Callable[[], str]],
    allow_retry: bool,
    sleep_time_seconds: int,
    on_succeeded: Callable[[MonitoredTarget], None] | None = None,
    on_status_change: Callable[[MonitoredTarget, PipelineStatus], None] | None = None,
    raise_on_failed_final: bool = True,
) -> None:
    """Generic loop to manage ICA pipeline execution for a cohort.

    Args:
        targets_to_process: Targets (Cohort, SequencingGroup, or IcaBatch) to process.
        outputs: The outputs dictionary for the stage.
        pipeline_name: Pipeline name (e.g. "Dragen", "MLR") for logging.
        is_mlr_pipeline: Flag for the monitor/cancel functions.
        success_file_key_template: Template for the success file key (e.g. '{sg_name}_success').
        pipeline_id_file_key_template: Template for the pipeline ID file key.
        error_log_key: The `outputs` key for the final error log.
        submit_function_factory: Given a target name, returns a no-arg Callable
            that submits the job and returns the pipeline_id.
        allow_retry: Whether to retry a failed pipeline once.
        sleep_time_seconds: Time to sleep between polling loops.
        on_succeeded: Optional callback run when a target reaches SUCCEEDED. Runs
            FIRST and transactionally: only on clean return is the target marked
            SUCCEEDED and the success file written; if it raises, the target stays
            INPROGRESS and the next poll re-fires it. Receives the `MonitoredTarget`
            wrapper. MLR omits it.
        on_status_change: Optional notification callback for a TERMINAL non-success
            status (FAILED_FINAL / CANCELLED), fired AFTER `set_status` (exceptions
            logged, not rolled back). DRAGEN mirrors terminal status into
            `{cohort}_batches.json`; SUCCEEDED goes through `on_succeeded` instead.
            MLR omits it.
        raise_on_failed_final: When True (default), the loop raises as soon as any
            target reaches FAILED_FINAL (after its retry if `allow_retry`). When
            False, FAILED_FINAL targets are returned without raising.
    """
    if not targets_to_process:
        raise ValueError(f'Cannot run {pipeline_name} pipeline management loop with an empty list of targets.')
    # Both Cohort and SequencingGroup have a .name attribute
    run_context_name = targets_to_process[0].name
    logger.info(f'Starting {pipeline_name} management job for {run_context_name}')
    logger.add(sink='tmp_errors.log', format='{time} - {level} - {message}', level='ERROR')
    logger.error(
        f'Error logging for {pipeline_name} {run_context_name} run on {datetime.now()}'  # noqa: DTZ005
    )

    monitored_targets: list[MonitoredTarget] = [
        MonitoredTarget(target=target, allow_retry=allow_retry) for target in targets_to_process
    ]
    initial_ar_guid: str = try_get_ar_guid()  # Use a distinct name for the AR GUID from the environment

    # Get force_resubmit config
    force_resubmit = config_retrieve(['ica', 'management', 'force_resubmit'], default=False)

    # --- One-time setup loop for force_resubmit ---
    if force_resubmit:
        logger.warning("'force_resubmit' is true. Cleaning up previous runs before monitoring.")
        for target in monitored_targets:
            target_name: str = target.name
            pipeline_id_arguid_file: cpg_utils.Path = outputs[
                pipeline_id_file_key_template.format(target_name=target_name)
            ]
            pipeline_success_file: cpg_utils.Path = outputs[success_file_key_template.format(target_name=target_name)]

            preserved_ar_guid_for_target: str = initial_ar_guid

            if pipeline_id_arguid_file.exists():
                try:
                    with pipeline_id_arguid_file.open('r') as f:
                        data = json.load(f)
                        if 'ar_guid' in data:
                            preserved_ar_guid_for_target = data['ar_guid']
                            logger.info(
                                f"Preserved AR GUID '{preserved_ar_guid_for_target}' from existing file "
                                f'for {target.name}.'
                            )
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f'Could not read AR GUID from {pipeline_id_arguid_file}: {e}.')

                pipeline_id_arguid_file.unlink()
                logger.info(f'Deleted existing pipeline ID file: {pipeline_id_arguid_file}')

            if pipeline_success_file.exists():
                pipeline_success_file.unlink()
                logger.info(f'Deleted existing success file: {pipeline_success_file}')

            # Set the ar_guid on the target object to be used for the new submission
            target.ar_guid = preserved_ar_guid_for_target

    def is_finished(target: MonitoredTarget) -> bool:
        return target.status in {PipelineStatus.SUCCEEDED, PipelineStatus.CANCELLED, PipelineStatus.FAILED_FINAL}

    def _fire_status_change(target: MonitoredTarget, new_status: PipelineStatus) -> None:
        """Best-effort terminal-transition notification; exceptions are logged and
        swallowed (the in-memory transition stands). Distinct from transactional
        `on_succeeded`."""
        if on_status_change is None:
            return
        try:
            on_status_change(target, new_status)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                f'on_status_change callback failed for {target.name} '
                f'(new_status={new_status.name}, pipeline {target.pipeline_id}): {exc}. '
                f'Continuing — in-memory transition stands.',
            )

    while not all(is_finished(target) for target in monitored_targets):
        for target in monitored_targets:
            if is_finished(target):
                continue
            target_name = target.name
            pipeline_id_arguid_file = outputs[pipeline_id_file_key_template.format(target_name=target_name)]

            if pipeline_id_arguid_file.exists() and not target.pipeline_id:
                with pipeline_id_arguid_file.open('r') as pipeline_fid_handle:
                    pipeline_info = json.load(pipeline_fid_handle)
                    target.pipeline_id = pipeline_info['pipeline_id']
                    target.ar_guid = pipeline_info['ar_guid']
                    # If we are loading the pipeline ID from file, we assume it is at least in progress
                    if target.status == PipelineStatus.PENDING:
                        target.status = PipelineStatus.INPROGRESS

            # Cancel a pipeline if requested
            if config_retrieve(key=['ica', 'management', 'cancel_cohort_run'], default=False) and target.pipeline_id:
                logger.info(f'Cancelling {pipeline_name} pipeline run: {target.pipeline_id} for {target_name}')
                # If the ICA abort API fails, log but still mark CANCELLED locally:
                # user intent overrides the API result, and an uncaught blip here
                # would bypass run()'s CohortCancelled translation.
                try:
                    cancel_ica_pipeline_run.run(
                        ica_pipeline_id=target.pipeline_id,
                        is_mlr=is_mlr_pipeline,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.error(
                        f'ICA abort API call failed for {target_name} '
                        f'(pipeline {target.pipeline_id}): {e}. '
                        f'Marking CANCELLED locally anyway — user intent overrides the API result.',
                    )
                delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))
                target.set_status(PipelineStatus.CANCELLED)
                _fire_status_change(target, PipelineStatus.CANCELLED)
            else:
                submit_callable: Callable[[], str] = submit_function_factory(target_name)

                if not target.pipeline_id:
                    logger.info(f'Submitting new {pipeline_name} ICA pipeline for {target_name}')
                    target.pipeline_id = submit_callable()
                    # Use the preserved guid if it was set, otherwise use the initial one from the env
                    target.ar_guid = target.ar_guid if target.ar_guid else initial_ar_guid
                    target.set_status(PipelineStatus.INPROGRESS)
                    with pipeline_id_arguid_file.open('w') as f:
                        f.write(json.dumps({'pipeline_id': target.pipeline_id, 'ar_guid': target.ar_guid}))

                pipeline_status: str = monitor_dragen_pipeline.run(
                    ica_pipeline_id=target.pipeline_id,
                    is_mlr=is_mlr_pipeline,
                )

                if pipeline_status == 'INPROGRESS':
                    target.set_status(new_status=PipelineStatus.INPROGRESS)

                elif pipeline_status == 'SUCCEEDED':
                    # Transactional SUCCEEDED: run on_succeeded FIRST; only on clean
                    # return mark SUCCEEDED + write the marker, else stay INPROGRESS and
                    # re-fire next poll. Capped per target so a persistently failing
                    # callback escalates to FAILED_FINAL instead of spinning forever.
                    if not _process_succeeded_transition(target, on_succeeded, on_status_change):
                        continue
                    target.set_status(new_status=PipelineStatus.SUCCEEDED)
                    logger.info(f'{pipeline_name} pipeline {target.pipeline_id} has succeeded for {target_name}')
                    pipeline_success_file = outputs[success_file_key_template.format(target_name=target_name)]
                    with pipeline_success_file.open('w') as success_file:
                        success_file.write(
                            f'ICA {pipeline_name} pipeline {target.pipeline_id} has succeeded for {target_name}.'
                        )

                elif pipeline_status in ['ABORTING', 'ABORTED']:
                    # Treated as user-initiated cancellation: by policy an ICA analysis
                    # is only aborted via our cancel_cohort_run flow, never the ICA GUI.
                    logger.info(f'{pipeline_name} pipeline {target.pipeline_id} has been cancelled for {target_name}.')
                    target.set_status(new_status=PipelineStatus.CANCELLED)
                    target.pipeline_id = None
                    delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))
                    _fire_status_change(target, PipelineStatus.CANCELLED)

                elif pipeline_status in ['FAILED', 'FAILEDFINAL']:
                    logger.error(f'{pipeline_name} pipeline {target.pipeline_id} has failed for {target_name}.')
                    delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))
                    target.pipeline_id = None

                    # Determine if we should retry
                    if target.allow_retry and not target.has_been_retried:
                        logger.info(f'Retrying {pipeline_name} pipeline for {target.name}')
                        target.pipeline_id = submit_callable()
                        target.ar_guid = target.ar_guid if target.ar_guid else initial_ar_guid
                        target.has_been_retried = True
                        target.set_status(PipelineStatus.FAILED_RETRYING)
                        with pipeline_id_arguid_file.open('w') as f:
                            f.write(json.dumps({'pipeline_id': target.pipeline_id, 'ar_guid': target.ar_guid}))
                    else:
                        target.set_status(PipelineStatus.FAILED_FINAL)
                        _fire_status_change(target, PipelineStatus.FAILED_FINAL)
                        logger.error(
                            f'{target_name} failed {pipeline_name} pipeline {target.pipeline_id} and '
                            f'retry is not allowed or already attempted.'
                        )

        status_counts: Counter[PipelineStatus] = Counter(target.status for target in monitored_targets)
        if status_counts[PipelineStatus.CANCELLED] > 0:
            cancelled_pipelines: list[str] = [
                target.name for target in monitored_targets if target.status == PipelineStatus.CANCELLED
            ]
            logger.warning(f'Cancelled {pipeline_name} pipelines: {", ".join(cancelled_pipelines)}')
            if status_counts[PipelineStatus.FAILED_FINAL] > 0:
                _flush_error_log(outputs[error_log_key], mode='a', on_error='Error reading tmp_errors.log')
            _log_status_counts(pipeline_name, status_counts)
            raise Exception(
                f'The following {pipeline_name} pipelines have been cancelled: {", ".join(cancelled_pipelines)}'
            )

        # A target reaches FAILED_FINAL only after its retry is exhausted (or
        # immediately when allow_retry=False). Unless raise_on_failed_final=False,
        # any such failure aborts the run (no rate tolerance). DRAGEN opts out so
        # batch failures survive to its per-sample retry. status_counts is still
        # current (no status changes since; the CANCELLED branch raises).
        failed_pipelines = _failed_final_target_names(monitored_targets)
        if raise_on_failed_final and failed_pipelines:
            logger.error(
                f'{pipeline_name} pipelines failed after retries: {" ".join(failed_pipelines)}'
            )
            _flush_error_log(
                outputs[error_log_key],
                mode='w',
                on_error=f'Failed to persist tmp_errors.log to {error_log_key} before failure exit',
            )
            raise Exception(
                f'{pipeline_name} pipelines have failed (FAILED_FINAL): {" ".join(failed_pipelines)}'
            )
        _log_status_counts(pipeline_name, status_counts)
        if all(is_finished(target) for target in monitored_targets):
            break
        time.sleep(sleep_time_seconds)

    _flush_error_log(
        outputs[error_log_key],
        mode='w',
        on_error=f'Failed to persist tmp_errors.log to {error_log_key} on loop exit',
    )
