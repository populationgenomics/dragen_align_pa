"""
Generic ICA pipeline management loop.

This module contains the shared state-machine logic for managing ICA pipeline
runs (submission, monitoring, cancellation, and failure handling) for a cohort.
"""

import json
import time
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import TypeAlias

import cpg_utils
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_utils.config import config_retrieve, try_get_ar_guid
from loguru import logger

from dragen_align_pa.jobs import cancel_ica_pipeline_run, monitor_dragen_pipeline
from dragen_align_pa.utils import delete_pipeline_id_file

ProcessingTarget: TypeAlias = Cohort | SequencingGroup


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
    if targets_to_process:
        # Explicitly get the first target after checking the sequence is not empty
        first_target = targets_to_process[0]
        # Both Cohort and SequencingGroup have a .name attribute
        cohort_name = first_target.name
    else:
        cohort_name = 'unknown'
    logger.info(f'Starting {pipeline_name} management job for {cohort_name}')
    logger.add(sink='tmp_errors.log', format='{time} - {level} - {message}', level='ERROR')
    logger.error(
        f'Error logging for {pipeline_name} {cohort_name} run on {datetime.now()}'  # noqa: DTZ005
    )

    ar_guid: str = try_get_ar_guid()
    running_pipelines: list[str] = []
    cancelled_pipelines: list[str] = []
    failed_pipelines: list[str] = []
    retried_pipelines: list[str] = []
    completed_pipelines: list[str] = []

    total_targets: int = len(targets_to_process)

    while (len(completed_pipelines) + len(cancelled_pipelines) + len(failed_pipelines)) < total_targets:
        for target in targets_to_process:
            target_name: str = target.name
            pipeline_id_arguid_file: cpg_utils.Path = outputs[
                pipeline_id_file_key_template.format(target_name=target_name)
            ]
            pipeline_success_file: cpg_utils.Path = outputs[success_file_key_template.format(target_name=target_name)]

            if pipeline_success_file.exists() and target_name not in completed_pipelines:
                completed_pipelines.append(target_name)
                continue

            if (
                target_name in completed_pipelines
                or target_name in cancelled_pipelines
                or target_name in failed_pipelines
            ):
                continue

            ica_pipeline_id: str = ''
            pipeline_id_file_exists: bool = pipeline_id_arguid_file.exists()
            if pipeline_id_file_exists:
                with pipeline_id_arguid_file.open('r') as pipeline_fid_handle:
                    ica_pipeline_id = json.load(pipeline_fid_handle)['pipeline_id']

            if config_retrieve(key=['ica', 'management', 'cancel_cohort_run'], default=False) and ica_pipeline_id:
                logger.info(f'Cancelling {pipeline_name} pipeline run: {ica_pipeline_id} for {target_name}')
                cancel_ica_pipeline_run.run(
                    ica_pipeline_id=ica_pipeline_id,
                    is_mlr=is_mlr_pipeline,
                )
                delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))
                cancelled_pipelines.append(target_name)
            else:
                submit_callable: Callable[[], str] = submit_function_factory(target_name)

                if not pipeline_id_file_exists:
                    logger.info(f'Submitting new {pipeline_name} ICA pipeline for {target_name}')
                    ica_pipeline_id = submit_callable()
                    with pipeline_id_arguid_file.open('w') as f:
                        f.write(json.dumps({'pipeline_id': ica_pipeline_id, 'ar_guid': ar_guid}))
                else:
                    logger.info(f'Checking status of existing {pipeline_name} ICA pipeline for {target_name}')

                pipeline_status: str = monitor_dragen_pipeline.run(
                    ica_pipeline_id=ica_pipeline_id,
                    is_mlr=is_mlr_pipeline,
                )

                if pipeline_status == 'INPROGRESS':
                    if target_name not in running_pipelines:
                        running_pipelines.append(target_name)

                elif pipeline_status == 'SUCCEEDED':
                    logger.info(f'{pipeline_name} pipeline {ica_pipeline_id} has succeeded for {target_name}')
                    completed_pipelines.append(target_name)
                    if target_name in running_pipelines:
                        running_pipelines.remove(target_name)
                    with pipeline_success_file.open('w') as success_file:
                        success_file.write(
                            f'ICA {pipeline_name} pipeline {ica_pipeline_id} has succeeded for {target_name}.'
                        )

                elif pipeline_status in ['ABORTING', 'ABORTED']:
                    logger.info(f'{pipeline_name} pipeline {ica_pipeline_id} has been cancelled for {target_name}.')
                    cancelled_pipelines.append(target_name)
                    if target_name in running_pipelines:
                        running_pipelines.remove(target_name)
                    delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))

                elif pipeline_status in ['FAILED', 'FAILEDFINAL']:
                    logger.error(f'{pipeline_name} pipeline {ica_pipeline_id} has failed for {target_name}.')
                    if target_name in running_pipelines:
                        running_pipelines.remove(target_name)

                    delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))

                    if allow_retry and target_name not in retried_pipelines:
                        logger.info(f'Retrying {pipeline_name} pipeline for {target_name}')
                        ica_pipeline_id = submit_callable()
                        with pipeline_id_arguid_file.open('w') as f:
                            f.write(json.dumps({'pipeline_id': ica_pipeline_id, 'ar_guid': ar_guid}))
                        retried_pipelines.append(target_name)
                    else:
                        failed_pipelines.append(target_name)
                        logger.error(
                            f'{target_name} failed {pipeline_name} pipeline {ica_pipeline_id} and '
                            f'retry is not allowed or already attempted.'
                        )

        if cancelled_pipelines:
            logger.warning(f'Cancelled {pipeline_name} pipelines: {", ".join(cancelled_pipelines)}')
            if failed_pipelines:
                with open('tmp_errors.log') as tmp_log_handle:
                    lines: list[str] = tmp_log_handle.readlines()
                    with outputs[error_log_key].open('a') as gcp_error_log_file:
                        gcp_error_log_file.write('\n'.join(lines))
            logger.info(
                f'{pipeline_name} pipeline status: '
                f'{len(completed_pipelines)} completed, '
                f'{len(running_pipelines)} in progress, '
                f'{len(failed_pipelines)} failed, '
                f'{len(cancelled_pipelines)} cancelled. '
                f'Waiting {sleep_time_seconds}s.'
            )
            raise Exception(
                f'The following {pipeline_name} pipelines have been cancelled: {" ".join(cancelled_pipelines)}'
            )

        if failed_pipelines and float(len(failed_pipelines)) / float(total_targets) > 0.05:  # noqa: PLR2004
            logger.error(
                f'More than 5% of {pipeline_name} pipelines have failed. '
                f'Failing pipelines: {" ".join(failed_pipelines)}'
            )
            raise Exception(
                f'More than 5% of {pipeline_name} pipelines have failed. '
                f'Failing pipelines: {" ".join(failed_pipelines)}'
            )

        if (len(completed_pipelines) + len(cancelled_pipelines) + len(failed_pipelines)) == total_targets:
            break

        logger.info(
            f'{pipeline_name} pipeline status: '
            f'{len(completed_pipelines)} completed, '
            f'{len(running_pipelines)} in progress, '
            f'{len(failed_pipelines)} failed, '
            f'{len(cancelled_pipelines)} cancelled. '
            f'Waiting {sleep_time_seconds}s.'
        )
        time.sleep(sleep_time_seconds)

    with open('tmp_errors.log') as tmp_log_handle:
        lines = tmp_log_handle.readlines()
        with outputs[error_log_key].open('w') as gcp_error_log_file:
            gcp_error_log_file.write('\n'.join(lines))
