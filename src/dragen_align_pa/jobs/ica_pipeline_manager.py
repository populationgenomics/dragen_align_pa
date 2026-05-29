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

from dragen_align_pa.batches import IcaBatch
from dragen_align_pa.jobs import cancel_ica_pipeline_run
from dragen_align_pa.jobs.ica_status_provider import (
    ParallelPerIdStatusProvider,
    StatusProvider,
)
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


# Cap on consecutive `on_succeeded` callback failures for a single target.
# A persistently broken callback (e.g. permanent IAM error fetching passfail.json)
# would otherwise spin the polling loop forever, hammering ICA on every pass
# with no escalation. After the cap, the helper transitions the target to
# FAILED_FINAL and fires on_status_change so the orchestrator surfaces it as
# a real failure.
MAX_CONSECUTIVE_ON_SUCCEEDED_FAILURES = 5


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

    Returns True if the caller should proceed to mark the target SUCCEEDED
    (callback succeeded, or no callback configured). Returns False if the
    caller should leave the target as-is for the next poll cycle — either
    because the callback raised below the cap, or because it tripped the
    cap and has already been transitioned to FAILED_FINAL.

    Without a cap, a persistently broken callback (e.g. permanent IAM error
    on passfail.json fetch) would spin the polling loop forever, re-firing
    on every iteration and never escalating.
    """
    if on_succeeded is None:
        return True
    try:
        on_succeeded(target)
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
    status_provider: StatusProvider | None = None,
) -> None:
    """
    Generic loop to manage ICA pipeline execution for a cohort.

    Args:
        targets_to_process: The list of targets (Cohort, SequencingGroup, or IcaBatch) to process.
        outputs: The outputs dictionary for the stage.
        api_root: The ICA API root endpoint.
        pipeline_name: Name of the pipeline (e.g., "Dragen", "MLR") for logging.
        is_mlr_pipeline: Flag for monitor/cancel functions.
        success_file_key_template: String template for the success file key
                                   (e.g., '{sg_name}_success').
        pipeline_id_file_key_template: String template for the pipeline ID file key
                                        (e.g., '{sg_name}_pipeline_id').
        error_log_key: The key in 'outputs' for the final error log
                       (e.g., 'cohort_name_errors').
        submit_function_factory: A function that takes an sg_name (str) and
                                 returns a no-argument Callable which, when
                                 called, submits the job and returns the
                                 pipeline_id (str).
        allow_retry: Whether to retry a failed pipeline once.
        sleep_time_seconds: Time to sleep between polling loops.
        on_succeeded: Optional callback invoked when a target transitions to
                      SUCCEEDED. The callback runs FIRST; only after it returns
                      successfully is `target.set_status(SUCCEEDED)` and the
                      success marker file written. If the callback raises, the
                      target stays INPROGRESS, the next poll cycle re-fires the
                      callback. This makes the SUCCEEDED transition transactional:
                      the orchestrator never persists "the batch finished" if
                      post-success bookkeeping (e.g. downloading passfail.json)
                      failed.

                      Per-batch `error_strategy` is plumbed via the
                      `submit_function_factory` closure: the DRAGEN orchestrator
                      builds a factory that captures the per-batch error_strategy
                      recorded in `{cohort}_batches.json`, so retry batches with
                      `error_strategy='continue'` submit correctly without
                      changing the factory's `Callable[[str], Callable[[], str]]`
                      signature. MLR's factory ignores this dimension entirely.

                      Note: `on_succeeded` receives the `MonitoredTarget` wrapper,
                      not the wrapped `IcaBatch` / `SequencingGroup`. Access
                      `monitored.target.sg_names` (for `IcaBatch`) to reach the
                      underlying domain object.

                      MLR omits `on_succeeded` — its behaviour is unchanged.

        on_status_change: Optional notification callback invoked when a target
                      reaches a TERMINAL non-success status (`FAILED_FINAL` or
                      `CANCELLED`). Fires AFTER `target.set_status(new_status)`,
                      so it's pure notification — exceptions are caught and
                      logged but do not roll back the transition. DRAGEN uses
                      this to mirror the loop's in-memory terminal status into
                      `{cohort}_batches.json` (without this, the batches file
                      would forever say `INPROGRESS` for batches that ICA
                      reported as failed (mapped to `PipelineStatus.FAILED_FINAL`)
                      or that the operator cancelled via `cancel_cohort_run`,
                      breaking the per-sample retry
                      path's `elif b['status'] == 'FAILED':` branch).
                      `SUCCEEDED` is intentionally NOT routed through this
                      callback — that transition is transactional and goes
                      through `on_succeeded` instead.

                      MLR omits this callback (default `None`); its in-memory
                      target state is sufficient because MLR has no equivalent
                      cohort-level state file.
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
    total_targets: int = len(monitored_targets)
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
                    logger.warning(f"Could not read AR GUID from {pipeline_id_arguid_file}: {e}.")

                pipeline_id_arguid_file.unlink()
                logger.info(f"Deleted existing pipeline ID file: {pipeline_id_arguid_file}")

            if pipeline_success_file.exists():
                pipeline_success_file.unlink()
                logger.info(f"Deleted existing success file: {pipeline_success_file}")

            # Set the ar_guid on the target object to be used for the new submission
            target.ar_guid = preserved_ar_guid_for_target

    def is_finished(target: MonitoredTarget) -> bool:
        return target.status in {PipelineStatus.SUCCEEDED, PipelineStatus.CANCELLED, PipelineStatus.FAILED_FINAL}

    def _fire_status_change(target: MonitoredTarget, new_status: PipelineStatus) -> None:
        """Best-effort notification of a terminal transition. Exceptions are
        logged and swallowed — the in-memory transition has already happened
        and we do NOT want to roll it back. Distinct from `on_succeeded`
        (which is transactional for the SUCCEEDED transition)."""
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

    # The status_provider owns the parallel per-id fan-out across in-flight
    # targets. Pass-through allows the orchestrator (or tests) to inject a
    # fake; otherwise we instantiate one driven by [ica.polling] config.
    owns_provider = status_provider is None
    if status_provider is None:
        # MLR uses a different project than DRAGEN — caller can pass a
        # pre-configured provider with project_id set; the default path
        # reads from secrets in refresh() and falls back to the DRAGEN
        # project, which is correct for the DRAGEN call site.
        status_provider = ParallelPerIdStatusProvider(
            concurrency=config_retrieve(['ica', 'polling', 'concurrency'], default=16),
            refresh_timeout_seconds=config_retrieve(
                ['ica', 'polling', 'refresh_timeout_seconds'], default=180,
            ),
        )

    while not all(is_finished(target) for target in monitored_targets):
        # Pre-pass: hydrate pipeline IDs from disk for any target that doesn't
        # have one yet. This must happen before the in_flight computation so
        # that newly-resumed targets (whose pipeline_id file was written in a
        # prior run) are included in the refresh batch on the very first cycle.
        for t in monitored_targets:
            if not is_finished(t) and not t.pipeline_id:
                _pid_file = outputs[pipeline_id_file_key_template.format(target_name=t.name)]
                if _pid_file.exists():
                    with _pid_file.open('r') as _fh:
                        _info = json.load(_fh)
                        t.pipeline_id = _info['pipeline_id']
                        t.ar_guid = _info['ar_guid']
                        if t.status == PipelineStatus.PENDING:
                            t.status = PipelineStatus.INPROGRESS

        in_flight = {
            t.pipeline_id
            for t in monitored_targets
            if t.pipeline_id and not is_finished(t)
        }
        status_provider.refresh(in_flight)

        for target in monitored_targets:
            if is_finished(target):
                continue
            target_name = target.name
            pipeline_id_arguid_file = outputs[
                pipeline_id_file_key_template.format(target_name=target_name)
            ]

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
                # Match `_handle_management_flags`'s pre-loop cancel behaviour:
                # if the ICA abort API fails, log it but still mark the target
                # CANCELLED locally. User intent (cancel_cohort_run=true) takes
                # precedence over the API result — without this catch, an ICA
                # blip during cancel would propagate out of the loop as a
                # generic exception, bypassing the orchestrator's CohortCancelled
                # translation at the end of run().
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

                pipeline_status: str = status_provider.get_status(target.pipeline_id)

                if pipeline_status == 'UNKNOWN':
                    # No fresh status this cycle (worker failed or timed out);
                    # leave the target's in-memory state untouched and retry
                    # next cycle.
                    continue
                if pipeline_status == 'INPROGRESS':
                    target.set_status(new_status=PipelineStatus.INPROGRESS)

                elif pipeline_status == 'SUCCEEDED':
                    # Transactional SUCCEEDED transition: run the post-success callback
                    # FIRST (e.g. fetch passfail.json + persist into the cohort batches
                    # file). Only when it returns cleanly do we mark the target SUCCEEDED
                    # and write the success marker. If the callback raises, leave state
                    # as INPROGRESS so the next poll cycle re-fires it — this prevents
                    # divergent state where the loop has set SUCCEEDED but the caller's
                    # side-state (e.g. batches.json) was not updated.
                    #
                    # Capped at MAX_CONSECUTIVE_ON_SUCCEEDED_FAILURES per target: a
                    # persistently failing callback (e.g. permanent IAM error) escalates
                    # to FAILED_FINAL rather than spinning the loop forever.
                    if not _process_succeeded_transition(target, on_succeeded, on_status_change):
                        continue
                    target.set_status(new_status=PipelineStatus.SUCCEEDED)
                    logger.info(f'{pipeline_name} pipeline {target.pipeline_id} has succeeded for {target_name}')
                    pipeline_success_file = outputs[
                        success_file_key_template.format(target_name=target_name)
                    ]
                    with pipeline_success_file.open('w') as success_file:
                        success_file.write(
                            f'ICA {pipeline_name} pipeline {target.pipeline_id} has succeeded for {target_name}.'
                        )

                elif pipeline_status in ['ABORTING', 'ABORTED']:
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
                try:
                    with open('tmp_errors.log') as tmp_log_handle:
                        lines: list[str] = tmp_log_handle.readlines()
                        with outputs[error_log_key].open('a') as gcp_error_log_file:
                            gcp_error_log_file.write('\n'.join(lines))
                except (OSError, gcs_exceptions.GoogleCloudError) as e:
                    logger.error(f'Error reading tmp_errors.log: {e}')
            logger.info(
                f'{pipeline_name} pipeline status: '
                f'{status_counts[PipelineStatus.SUCCEEDED]} completed, '
                f'{status_counts[PipelineStatus.INPROGRESS]} in progress, '
                f'{status_counts[PipelineStatus.FAILED_FINAL]} failed, '
                f'{status_counts[PipelineStatus.CANCELLED]} cancelled. '
            )
            raise Exception(
                f'The following {pipeline_name} pipelines have been cancelled: {", ".join(cancelled_pipelines)}'
            )

        n_failed: int = status_counts[PipelineStatus.FAILED_FINAL]
        if n_failed > 0 and float(n_failed) / float(total_targets) > 0.05:  # noqa: PLR2004
            failed_pipelines: list[str] = [
                target.name for target in monitored_targets if target.status == PipelineStatus.FAILED_FINAL
            ]
            logger.error(
                f'More than 5% of {pipeline_name} pipelines have failed. '
                f'Failing pipelines: {" ".join(failed_pipelines)}'
            )
            try:
                with open('tmp_errors.log') as tmp_log_handle:
                    lines = tmp_log_handle.readlines()
                    with outputs[error_log_key].open('w') as gcp_error_log_file:
                        gcp_error_log_file.write('\n'.join(lines))
            except (OSError, gcs_exceptions.GoogleCloudError) as e:
                logger.error(f'Failed to persist tmp_errors.log to {error_log_key} before 5% failure exit: {e}')
            raise Exception(
                f'More than 5% of {pipeline_name} pipelines have failed. '
                f'Failing pipelines: {" ".join(failed_pipelines)}'
            )
        status_counts = Counter(target.status for target in monitored_targets)
        logger.info(
            f'{pipeline_name} pipeline status: '
            f'{status_counts[PipelineStatus.SUCCEEDED]} completed, '
            f'{status_counts[PipelineStatus.INPROGRESS]} in progress, '
            f'{status_counts[PipelineStatus.FAILED_FINAL]} failed, '
            f'{status_counts[PipelineStatus.CANCELLED]} cancelled.'
        )
        if all(is_finished(target) for target in monitored_targets):
            break
        time.sleep(sleep_time_seconds)

    if owns_provider:
        # close() shuts down the ThreadPoolExecutor; idempotent + safe.
        status_provider.close()  # type: ignore[union-attr,attr-defined]

    try:
        with open('tmp_errors.log') as tmp_log_handle:
            lines = tmp_log_handle.readlines()
            with outputs[error_log_key].open('w') as gcp_error_log_file:
                gcp_error_log_file.write('\n'.join(lines))
    except (OSError, gcs_exceptions.GoogleCloudError) as e:
        logger.error(f'Failed to persist tmp_errors.log to {error_log_key} before 5% failure exit: {e}')
