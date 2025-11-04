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

from dragen_align_pa.jobs import cancel_ica_pipeline_run, monitor_dragen_pipeline
from dragen_align_pa.utils import delete_pipeline_id_file

ProcessingTarget: TypeAlias = Cohort | SequencingGroup


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
        self.ar_guid: str | None = None
        self.has_been_retried: bool = False
        self.allow_retry: bool = allow_retry

    @property
    def name(self) -> str:
        return self.target.name

    def set_status(self, new_status: PipelineStatus) -> None:
        logger.info(f'Target {self.name} status moving from {self.status.name} to {new_status.name}')
        self.status = new_status


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
) -> None:
    """
    Generic loop to manage ICA pipeline execution for a cohort.

    Args:
        targets_to_process: The list of targets (Cohort or SequencingGroup) to process.
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
    ar_guid: str = try_get_ar_guid()

    def is_finished(target: MonitoredTarget) -> bool:
        return target.status in {PipelineStatus.SUCCEEDED, PipelineStatus.CANCELLED, PipelineStatus.FAILED_FINAL}

    while not all(is_finished(target) for target in monitored_targets):
        for target in monitored_targets:
            if is_finished(target):
                continue
            target_name: str = target.name
            pipeline_id_arguid_file: cpg_utils.Path = outputs[
                pipeline_id_file_key_template.format(target_name=target_name)
            ]
            pipeline_success_file: cpg_utils.Path = outputs[success_file_key_template.format(target_name=target_name)]

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
                cancel_ica_pipeline_run.run(
                    ica_pipeline_id=target.pipeline_id,
                    is_mlr=is_mlr_pipeline,
                )
                delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))
                target.set_status(PipelineStatus.CANCELLED)
            else:
                submit_callable: Callable[[], str] = submit_function_factory(target_name)

                if not target.pipeline_id:
                    logger.info(f'Submitting new {pipeline_name} ICA pipeline for {target_name}')
                    target.pipeline_id = submit_callable()
                    target.ar_guid = ar_guid
                    target.set_status(PipelineStatus.INPROGRESS)
                    with pipeline_id_arguid_file.open('w') as f:
                        f.write(json.dumps({'pipeline_id': target.pipeline_id, 'ar_guid': ar_guid}))
                else:
                    logger.info(f'Checking status of existing {pipeline_name} ICA pipeline for {target_name}')

                pipeline_status: str = monitor_dragen_pipeline.run(
                    ica_pipeline_id=target.pipeline_id,
                    is_mlr=is_mlr_pipeline,
                )

                if pipeline_status == 'INPROGRESS':
                    target.set_status(new_status=PipelineStatus.INPROGRESS)

                elif pipeline_status == 'SUCCEEDED':
                    target.set_status(new_status=PipelineStatus.SUCCEEDED)
                    logger.info(f'{pipeline_name} pipeline {target.pipeline_id} has succeeded for {target_name}')
                    with pipeline_success_file.open('w') as success_file:
                        success_file.write(
                            f'ICA {pipeline_name} pipeline {target.pipeline_id} has succeeded for {target_name}.'
                        )

                elif pipeline_status in ['ABORTING', 'ABORTED']:
                    logger.info(f'{pipeline_name} pipeline {target.pipeline_id} has been cancelled for {target_name}.')
                    target.set_status(new_status=PipelineStatus.CANCELLED)
                    target.pipeline_id = None
                    delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))

                elif pipeline_status in ['FAILED', 'FAILEDFINAL']:
                    logger.error(f'{pipeline_name} pipeline {target.pipeline_id} has failed for {target_name}.')
                    delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))
                    target.pipeline_id = None

                    # Determine if we should retry
                    if target.allow_retry and not target.has_been_retried:
                        logger.info(f'Retrying {pipeline_name} pipeline for {target_name}')
                        target.pipeline_id = submit_callable()
                        target.ar_guid = ar_guid
                        target.has_been_retried = True
                        target.set_status(PipelineStatus.FAILED_RETRYING)
                        with pipeline_id_arguid_file.open('w') as f:
                            f.write(json.dumps({'pipeline_id': target.pipeline_id, 'ar_guid': ar_guid}))
                    else:
                        target.set_status(PipelineStatus.FAILED_FINAL)
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
        logger.info(f'Waiting {sleep_time_seconds}s.')
        time.sleep(sleep_time_seconds)

    try:
        with open('tmp_errors.log') as tmp_log_handle:
            lines = tmp_log_handle.readlines()
            with outputs[error_log_key].open('w') as gcp_error_log_file:
                gcp_error_log_file.write('\n'.join(lines))
    except (OSError, gcs_exceptions.GoogleCloudError) as e:
        logger.error(f'Failed to persist tmp_errors.log to {error_log_key} before 5% failure exit: {e}')
