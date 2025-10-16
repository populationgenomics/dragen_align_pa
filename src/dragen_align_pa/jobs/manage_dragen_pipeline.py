import json
import time
from datetime import datetime

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_driver_image, try_get_ar_guid
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger

from dragen_align_pa.jobs import cancel_ica_pipeline_run, monitor_dragen_pipeline, run_align_genotype_with_dragen
from dragen_align_pa.utils import delete_pipeline_id_file


def _initalise_management_job(cohort: Cohort) -> PythonJob:
    management_job: PythonJob = get_batch().new_python_job(
        name=f'Manage Dragen pipeline runs for cohort: {cohort.name}',
        attributes=cohort.get_job_attrs() or {} | {'tool': 'Dragen'},  # type: ignore[ReportUnknownVariableType]
    )
    management_job.image(image=get_driver_image())
    return management_job


def _submit_new_ica_pipeline(
    sg_name: str,
    cram_ica_fids_path: cpg_utils.Path | None,
    fastq_list_file_path: cpg_utils.Path | None,
    fastq_ids_path: cpg_utils.Path | None,
    analysis_output_fid_path: cpg_utils.Path,
    api_root: str,
) -> str:
    ica_pipeline_id: str = run_align_genotype_with_dragen.run(
        cram_ica_fids_path=cram_ica_fids_path,
        fastq_list_file_path=fastq_list_file_path,
        fastq_ids_path=fastq_ids_path,
        analysis_output_fid_path=analysis_output_fid_path,
        api_root=api_root,
        sg_name=sg_name,
    )
    return ica_pipeline_id


def manage_ica_pipeline(
    cohort: Cohort,
    outputs: dict[str, cpg_utils.Path],
    analysis_output_fids_path: dict[str, cpg_utils.Path],
    api_root: str,
    cram_ica_fids_path: dict[str, cpg_utils.Path] | None,
    fastq_list_file_path: dict[str, cpg_utils.Path] | None,
    fastq_ids_path: dict[str, cpg_utils.Path] | None,
) -> PythonJob:
    job: PythonJob = _initalise_management_job(cohort=cohort)

    job.call(
        _run,
        cohort=cohort,
        outputs=outputs,
        cram_ica_fids_path=cram_ica_fids_path,
        fastq_list_file_path=fastq_list_file_path,
        fastq_ids_path=fastq_ids_path,
        analysis_output_fids_path=analysis_output_fids_path,
        api_root=api_root,
    )

    return job


def _run(  # noqa: PLR0915
    cohort: Cohort,
    outputs: dict[str, cpg_utils.Path],
    cram_ica_fids_path: dict[str, cpg_utils.Path] | None,
    analysis_output_fids_path: dict[str, cpg_utils.Path],
    fastq_list_file_path: dict[str, cpg_utils.Path] | None,
    fastq_ids_path: dict[str, cpg_utils.Path] | None,
    api_root: str,
) -> None:
    logger.info(f'Starting management job for {cohort.name}')

    # Error sink needs to be configured here
    logger.add(sink='tmp_errors.log', format='{time} - {level} - {message}', level='ERROR')

    # Add a single entry to the error log file so that the pipeline doesn't incorrectly think outputs don't
    # exist if everything ran fine
    logger.error(f'Error logging for {cohort.name} run on {datetime.now()}')  # noqa: DTZ005

    ar_guid: str = try_get_ar_guid()

    running_pipelines: list[str] = []
    cancelled_pipelines: list[str] = []
    failed_pipelines: list[str] = []
    retried_pipelines: list[str] = []
    # If a sequencing group is in this list, we can skip checking its status
    completed_pipelines: list[str] = []
    while (len(completed_pipelines) + len(cancelled_pipelines) + len(failed_pipelines)) < len(
        cohort.get_sequencing_groups()
    ):
        for sequencing_group in cohort.get_sequencing_groups():
            sg_name: str = sequencing_group.name
            pipeline_id_arguid_file: cpg_utils.Path = outputs[f'{sg_name}_pipeline_id_and_arguid']
            pipeline_success_file: cpg_utils.Path = outputs[f'{sg_name}_success']

            # In case of Hail Batch crashes, find previous completed runs so we can skip trying to monitor them
            if pipeline_success_file.exists() and sg_name not in completed_pipelines:
                completed_pipelines.append(sg_name)
                continue

            if any(
                [
                    sg_name in completed_pipelines,
                    sg_name in cancelled_pipelines,
                    sg_name in failed_pipelines,
                ]
            ):
                continue

            # Get a pipeline ID for each sequencing group, if it exists.
            ica_pipeline_id: str = ''
            if pipeline_id_arguid_file_exists := pipeline_id_arguid_file.exists():
                with pipeline_id_arguid_file.open('r') as pipeline_fid_handle:
                    ica_pipeline_id = json.load(pipeline_fid_handle)['pipeline_id']

            # Handle cancelling a pipeline firrst
            # Cancel a running job in ICA
            if config_retrieve(key=['ica', 'management', 'cancel_cohort_run'], default=False) and ica_pipeline_id:
                logger.info(f'Cancelling pipeline run: {ica_pipeline_id} for sequencing group {sg_name}')
                cancel_ica_pipeline_run.run(ica_pipeline_id=ica_pipeline_id, api_root=api_root)
                delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))
            else:
                # If a pipeline ID file doesn't exist we have to submit a new run, regardless of other settings
                if not pipeline_id_arguid_file_exists:
                    ica_pipeline_id = _submit_new_ica_pipeline(
                        sg_name=sg_name,
                        cram_ica_fids_path=cram_ica_fids_path[sg_name] if cram_ica_fids_path else None,
                        fastq_list_file_path=fastq_list_file_path[sg_name] if fastq_list_file_path else None,
                        fastq_ids_path=fastq_ids_path[sg_name] if fastq_ids_path else None,
                        analysis_output_fid_path=analysis_output_fids_path[sg_name],
                        api_root=api_root,
                    )
                    with pipeline_id_arguid_file.open('w') as f:
                        f.write(json.dumps({'pipeline_id': ica_pipeline_id, 'ar_guid': ar_guid}))
                else:
                    # Get an existing pipeline ID
                    with pipeline_id_arguid_file.open('r') as pipeline_fid_handle:
                        ica_pipeline_id = json.load(pipeline_fid_handle)['pipeline_id']

                pipeline_status: str = monitor_dragen_pipeline.run(ica_pipeline_id=ica_pipeline_id, api_root=api_root)

                if pipeline_status == 'INPROGRESS':
                    running_pipelines.append(sg_name)

                elif pipeline_status == 'SUCCEEDED':
                    logger.info(f'Pipeline run {ica_pipeline_id} has succeeded for {sg_name}')
                    completed_pipelines.append(sg_name)
                    # Testing fix
                    if sg_name in running_pipelines:
                        running_pipelines.remove(sg_name)
                    # Write the success to GCP
                    with pipeline_success_file.open('w') as success_file:
                        success_file.write(
                            f'ICA pipeline {ica_pipeline_id} has succeeded for sequencing group {sg_name}.'
                        )

                elif pipeline_status in ['ABORTING', 'ABORTED']:
                    logger.info(f'The pipeline run {ica_pipeline_id} has been cancelled for sample {sg_name}.')
                    cancelled_pipelines.append(sg_name)
                    if sg_name in running_pipelines:
                        running_pipelines.remove(sg_name)
                    delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))

                elif pipeline_status in ['FAILED', 'FAILEDFINAL']:
                    # Log failed ICA pipeline to a file somewhere
                    if sg_name in running_pipelines:
                        running_pipelines.remove(sg_name)
                    failed_pipelines.append(sg_name)
                    delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))
                    logger.error(
                        f'The pipeline {ica_pipeline_id} has failed, deleting pipeline ID file {sg_name}_pipeline_id'
                    )
                    # Try again one time in case of transient Dragen errors
                    if sg_name not in retried_pipelines:
                        ica_pipeline_id = _submit_new_ica_pipeline(
                            sg_name=sg_name,
                            cram_ica_fids_path=cram_ica_fids_path[sg_name] if cram_ica_fids_path else None,
                            fastq_list_file_path=fastq_list_file_path[sg_name] if fastq_list_file_path else None,
                            fastq_ids_path=fastq_ids_path[sg_name] if fastq_ids_path else None,
                            analysis_output_fid_path=analysis_output_fids_path[sg_name],
                            api_root=api_root,
                        )
                        with pipeline_id_arguid_file.open('w') as f:
                            f.write(json.dumps({'pipeline_id': ica_pipeline_id, 'ar_guid': ar_guid}))
                        retried_pipelines.append(sg_name)
                        logger.info(f'Retrying Dragen pipeline for sequencing group: {sg_name}')

        # If some pipelines have been cancelled, abort this pipeline
        # This code will only trigger if a 'cancel pipeline' run is submitted before this master pipeline run is
        # cancelled in Hail Batch
        if cancelled_pipelines:
            # If there are both cancelled and failed pipelines,
            # write the failed pipelines to the error log before exiting.
            if failed_pipelines:
                with open('tmp_errors.log') as tmp_log_handle:
                    lines: list[str] = tmp_log_handle.readlines()
                    with outputs[f'{cohort.name}_errors'].open('a') as gcp_error_log_file:
                        gcp_error_log_file.write('\n'.join(lines))
            raise Exception(f'The following pipelines have been cancelled: {" ".join(cancelled_pipelines)}')

        # If more than 5% of pipelines are failing, exit now so we can investigate
        if failed_pipelines and float(len(failed_pipelines)) / float(len(cohort.get_sequencing_groups())) > 0.05:  # noqa: PLR2004
            raise Exception(f'More than 5% of pipelines have failed. Failing pipelines: {" ".join(failed_pipelines)}')
        # Catch just in case everything is finished on the first pass through the loop
        if (len(completed_pipelines) + len(cancelled_pipelines) + len(failed_pipelines)) == len(
            cohort.get_sequencing_groups()
        ):
            break
        # Wait 10 minutes before checking again
        time.sleep(600)
    with open('tmp_errors.log') as tmp_log_handle:
        lines = tmp_log_handle.readlines()
        with outputs[f'{cohort.name}_errors'].open('w') as gcp_error_log_file:
            gcp_error_log_file.write('\n'.join(lines))
