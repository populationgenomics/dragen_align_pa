import json
import time
from collections.abc import Callable
from functools import partial

import cpg_utils
import icasdk
from cpg_utils.config import config_retrieve, try_get_ar_guid
from icasdk import Configuration
from loguru import logger

from dragen_align_pa.jobs import (
    cancel_ica_pipeline_run,
    monitor_dragen_pipeline,
    run_intake_qc_pipeline,
)
from dragen_align_pa.utils import delete_pipeline_id_file


def _submit_md5_run(
    cohort_name: str,
    ica_fastq_ids: list[str],
    api_config: icasdk.Configuration,
    project_id: str,
    ar_guid: str,
    md5_outputs_folder_id: str,
) -> str:
    """
    Submits the MD5 intake QC pipeline to ICA.
    """
    logger.info(f'Submitting new MD5 ICA pipeline for {cohort_name}')
    md5_pipeline_id: str = run_intake_qc_pipeline.run_md5_pipeline(
        cohort_name=cohort_name,
        ica_fastq_ids=ica_fastq_ids,
        api_config=api_config,
        project_id=project_id,
        ar_guid=ar_guid,
        md5_outputs_folder_id=md5_outputs_folder_id,
    )
    return md5_pipeline_id


def _check_for_success(
    outputs: dict[str, cpg_utils.Path],
    cohort_name: str,
) -> cpg_utils.Path | None:
    """
    Checks for a pre-existing success file.
    Returns the pipeline ID file path if successful, else None.
    """
    pipeline_id_file = outputs['md5sum_pipeline_run']
    pipeline_success_file = outputs['md5sum_pipeline_success']

    if pipeline_success_file.exists():
        logger.info(f'MD5 pipeline for {cohort_name} already marked as SUCCEEDED.')
        if not pipeline_id_file.exists():
            # This case should not happen, but good to check
            raise FileNotFoundError(
                f'MD5 success file exists at {pipeline_success_file} but '
                f'pipeline ID file is missing at {pipeline_id_file}.',
            )
        return pipeline_id_file
    return None


def _get_existing_pipeline_id(pipeline_id_file: cpg_utils.Path) -> tuple[str, bool]:
    """
    Tries to read an existing pipeline ID from a file.
    Returns (pipeline_id, file_existed_and_was_valid)
    """
    ica_pipeline_id = ''
    if pipeline_id_file.exists():
        try:
            with pipeline_id_file.open('r') as f:
                ica_pipeline_id = json.load(f)['pipeline_id']
            return ica_pipeline_id, True  # File exists and is valid
        except (json.JSONDecodeError, KeyError):
            logger.warning(
                f'Could not parse pipeline ID from {pipeline_id_file}. Assuming invalid, will delete and resubmit.',
            )
            delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_file))

    return ica_pipeline_id, False  # File either didn't exist or was invalid


def _handle_cancellation(
    ica_pipeline_id: str,
    cohort_name: str,
    pipeline_id_file: cpg_utils.Path,
) -> None:
    """
    Checks for and executes pipeline cancellation.
    Raises an Exception if cancellation is triggered.
    """
    if config_retrieve(key=['ica', 'management', 'cancel_cohort_run'], default=False) and ica_pipeline_id:
        logger.info(
            f'Cancelling MD5 pipeline run: {ica_pipeline_id} for {cohort_name}',
        )
        cancel_ica_pipeline_run.run(ica_pipeline_id=ica_pipeline_id, is_mlr=False)
        delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_file))
        raise Exception(
            f'MD5 pipeline {ica_pipeline_id} for {cohort_name} has been cancelled.',
        )


def _handle_failed_status(
    ica_pipeline_id: str,
    cohort_name: str,
    pipeline_id_file: cpg_utils.Path,
    submit_callable: Callable[[], str],
    has_retried: bool,
    ar_guid: str,
) -> tuple[str, bool]:
    """
    Handles a FAILED/FAILEDFINAL status, including retry logic.
    Returns (new_pipeline_id, new_has_retried_state)
    Raises Exception if retry fails or is not allowed.
    """
    logger.error(f'MD5 pipeline {ica_pipeline_id} has FAILED for {cohort_name}.')
    delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_file))

    if not has_retried:
        logger.info(f'Retrying failed MD5 pipeline for {cohort_name}.')
        new_pipeline_id = submit_callable()  # Retry once
        with pipeline_id_file.open('w') as f:
            f.write(
                json.dumps({'pipeline_id': new_pipeline_id, 'ar_guid': ar_guid}),
            )
        return new_pipeline_id, True  # (new_id, has_retried=True)

    logger.error(f'MD5 pipeline {ica_pipeline_id} failed after one retry.')
    raise Exception(
        f'MD5 pipeline {ica_pipeline_id} for {cohort_name} failed after one retry.',
    )


def _run_monitoring_loop(
    ica_pipeline_id: str,
    cohort_name: str,
    pipeline_id_file: cpg_utils.Path,
    submit_callable: Callable[[], str],
    ar_guid: str,
) -> cpg_utils.Path:
    """
    Continuously monitors the ICA pipeline until a terminal state is reached.
    """
    has_retried = False
    while True:
        pipeline_status: str = monitor_dragen_pipeline.run(
            ica_pipeline_id=ica_pipeline_id,
            is_mlr=False,
        )

        if pipeline_status == 'SUCCEEDED':
            logger.info(
                f'MD5 pipeline {ica_pipeline_id} has SUCCEEDED for {cohort_name}',
            )
            return pipeline_id_file

        if pipeline_status in ['ABORTING', 'ABORTED']:
            logger.warning(
                f'MD5 pipeline {ica_pipeline_id} has been ABORTED for {cohort_name}.',
            )
            delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_file))
            raise Exception(
                f'MD5 pipeline {ica_pipeline_id} for {cohort_name} was aborted.',
            )

        if pipeline_status in ['FAILED', 'FAILEDFINAL']:
            ica_pipeline_id, has_retried = _handle_failed_status(
                ica_pipeline_id=ica_pipeline_id,
                cohort_name=cohort_name,
                pipeline_id_file=pipeline_id_file,
                submit_callable=submit_callable,
                has_retried=has_retried,
                ar_guid=ar_guid,
            )
            continue  # Continue to monitor the new pipeline_id

        # Status is INPROGRESS, REQUESTED, or AWAITINGINPUT
        logger.info(
            f'MD5 pipeline {ica_pipeline_id} status is {pipeline_status}. Waiting 300s.',
        )
        time.sleep(300)


def manage_md5_pipeline(
    cohort_name: str,
    ica_fastq_ids: list[str],
    outputs: dict[str, cpg_utils.Path],
    api_config: Configuration,
    project_id: str,
    md5_outputs_folder_id: str,
) -> cpg_utils.Path:
    """
    Manages a single cohort-level ICA pipeline by submitting,
    monitoring, and handling failures/retries.
    """
    # 1. Check for existing success
    if success_file_path := _check_for_success(outputs, cohort_name):
        return success_file_path

    pipeline_id_file = outputs['md5sum_pipeline_run']
    ar_guid: str = try_get_ar_guid()

    # 2. Check for existing pipeline ID
    ica_pipeline_id, pipeline_id_file_exists = _get_existing_pipeline_id(
        pipeline_id_file,
    )

    # 3. Handle cancellation
    _handle_cancellation(ica_pipeline_id, cohort_name, pipeline_id_file)

    # 4. Setup submit function
    submit_callable = partial(
        _submit_md5_run,
        cohort_name=cohort_name,
        ica_fastq_ids=ica_fastq_ids,
        api_config=api_config,
        project_id=project_id,
        ar_guid=ar_guid,
        md5_outputs_folder_id=md5_outputs_folder_id,
    )

    # 5. Submit if it doesn't exist
    if not pipeline_id_file_exists:
        ica_pipeline_id = submit_callable()
        with pipeline_id_file.open('w') as f:
            f.write(json.dumps({'pipeline_id': ica_pipeline_id, 'ar_guid': ar_guid}))
    else:
        logger.info(
            f'Checking status of existing MD5 ICA pipeline for {cohort_name}: {ica_pipeline_id}',
        )

    # 6. Monitor loop
    return _run_monitoring_loop(
        ica_pipeline_id=ica_pipeline_id,
        cohort_name=cohort_name,
        pipeline_id_file=pipeline_id_file,
        submit_callable=submit_callable,
        ar_guid=ar_guid,
    )
