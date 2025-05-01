import subprocess
import sys
import time
from datetime import datetime

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils import to_path  # type: ignore  # noqa: PGH003
from cpg_utils.config import config_retrieve
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger

from dragen_align_pa.jobs import cancel_ica_pipeline_run, monitor_dragen_pipeline, run_align_genotype_with_dragen


def _initalise_management_job(cohort: Cohort) -> PythonJob:
    management_job: PythonJob = get_batch().new_python_job(
        name=f'Manage Dragen pipeline runs for cohort: {cohort.name}',
        attributes=cohort.get_job_attrs() or {} | {'tool': 'Dragen'},  # type: ignore  # noqa: PGH003
    )
    management_job.image(image=config_retrieve(['images', 'ica']))
    return management_job


def _delete_pipeline_id_file(pipeline_id_file: str) -> None:
    logger.info(f'Deleting the pipeline run ID file {pipeline_id_file}')
    subprocess.run(['gcloud', 'storage', 'rm', pipeline_id_file], check=True)  # noqa: S603, S607


def manage_ica_pipeline(
    cohort: Cohort,
    outputs: dict[str, cpg_utils.Path],
    ica_fids_path: dict[str, cpg_utils.Path],
    analysis_output_fids_path: dict[str, cpg_utils.Path],
    api_root: str,
) -> PythonJob:
    job: PythonJob = _initalise_management_job(cohort=cohort)

    job.call(
        _run,
        cohort=cohort,
        outputs=outputs,
        ica_fids_path=ica_fids_path,
        analysis_output_fids_path=analysis_output_fids_path,
        api_root=api_root,
    )

    return job


def _run(
    cohort: Cohort,
    outputs: dict[str, cpg_utils.Path],
    ica_fids_path: dict[str, cpg_utils.Path],
    analysis_output_fids_path: dict[str, cpg_utils.Path],
    api_root: str,
) -> None:
    logger.remove(0)
    logger.add(sink=sys.stdout, format='{time} - {level} - {message}')
    logger.add(sink='tmp_errors.log', format='{time} - {level} - {message}', level='ERROR')
    logger.info(f'Starting management job for {cohort.name}')

    # Add a single entry to the error log file so that the pipeline doesn't incorrectly think outputs don't
    # exist if everything ran fine
    logger.error(f'Error logging for {cohort.name} run on {datetime.now()}')  # noqa: DTZ005

    running_pipelines: list[str] = []
    cancelled_pipelines: list[str] = []
    failed_pipelines: list[str] = []
    # If a sequencing group is in this list, we can skip checking its status
    completed_pipelines: list[str] = []
    while (len(completed_pipelines) + len(cancelled_pipelines) + len(failed_pipelines)) < len(
        cohort.get_sequencing_groups()
    ):
        for sequencing_group in cohort.get_sequencing_groups():
            sg_name: str = sequencing_group.name
            pipeline_id_file: cpg_utils.Path = outputs[f'{sg_name}_pipeline_id']
            pipeline_success_file: cpg_utils.Path = outputs[f'{sg_name}_success']
            logger.info(
                f'sg_name: {sg_name}, pipeline ID file: {pipeline_id_file}, pipeline success file: {pipeline_success_file}'
            )

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

            # If a pipeline ID file doesn't exist we have to submit a new run, regardless of other settings
            if not pipeline_id_file.exists():
                ica_pipeline_id: str = _submit_new_ica_pipeline(
                    sg_name=sg_name,
                    ica_fids_path=str(ica_fids_path[sg_name]),
                    analysis_output_fid_path=str(analysis_output_fids_path[sg_name]),
                    api_root=api_root,
                )
                with pipeline_id_file.open('w') as f:
                    f.write(ica_pipeline_id)
            else:
                # Get an existing pipeline ID
                with pipeline_id_file.open('r') as pipeline_fid_handle:
                    ica_pipeline_id = pipeline_fid_handle.read().rstrip()
                # Cancel a running job in ICA
                if config_retrieve(key=['ica', 'management', 'cancel_cohort_run'], default=False):
                    logger.info(f'Cancelling pipeline run: {ica_pipeline_id} for sequencing group {sg_name}')
                    cancel_ica_pipeline_run.run(ica_pipeline_id=ica_pipeline_id, api_root=api_root)
                    _delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_file))

            pipeline_status: str = monitor_dragen_pipeline.run(ica_pipeline_id=ica_pipeline_id, api_root=api_root)

            if pipeline_status == 'INPROGRESS':
                running_pipelines.append(sg_name)

            elif pipeline_status == 'SUCCEEDED':
                logger.info(f'Pipeline run {ica_pipeline_id} has succeeded for {sg_name}')
                completed_pipelines.append(sg_name)
                running_pipelines.remove(sg_name)
                # Write the success to GCP
                with open(to_path(outputs[sg_name]), 'w') as success_file:
                    success_file.write(f'ICA pipeline {ica_pipeline_id} has succeeded for sequencing group {sg_name}.')

            elif pipeline_status in ['ABORTING', 'ABORTED']:
                logger.info(f'The pipeline run {ica_pipeline_id} has been cancelled for sample {sg_name}.')
                cancelled_pipelines.append(sg_name)
                running_pipelines.remove(sg_name)
                _delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_file))

            elif pipeline_status in ['FAILED', 'FAILEDFINAL']:
                # Log failed ICA pipeline to a file somewhere
                running_pipelines.remove(sg_name)
                failed_pipelines.append(sg_name)
                _delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_file))
                logger.error(
                    f'The pipeline {ica_pipeline_id} has failed, deleting pipeline ID file {sg_name}_pipeline_id'
                )

            # ICA has a max concurrent running pipeline limit of 20, but I have observed it as low as 16 before.
            # Once we reach this number of running pipelines, we don't need to check on the status of others.
            if len(running_pipelines) >= 16:  # noqa: PLR2004
                continue
        # If some pipelines have been cancelled, abort this pipeline
        # This code will only trigger if a 'cancel pipeline' run is submitted before this master pipeline run is
        # cancelled in Hail Batch
        if cancelled_pipelines:
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
        lines: list[str] = tmp_log_handle.readlines()
        with outputs[f'{cohort.name}_errors'].open('a') as gcp_error_log_file:
            gcp_error_log_file.write('\n'.join(lines))


def _submit_new_ica_pipeline(
    sg_name: str,
    ica_fids_path: str,
    analysis_output_fid_path: str,
    api_root: str,
) -> str:
    ica_pipeline_id: str = run_align_genotype_with_dragen.run(
        ica_fids_path=ica_fids_path,
        analysis_output_fid_path=analysis_output_fid_path,
        dragen_ht_id=config_retrieve(['ica', 'pipelines', 'dragen_ht_id']),
        cram_reference_id=config_retrieve(
            ['ica', 'cram_references', config_retrieve(['ica', 'cram_references', 'old_cram_reference'])]
        ),
        qc_cross_cont_vcf_id=config_retrieve(['ica', 'qc', 'cross_cont_vcf']),
        qc_cov_region_1_id=config_retrieve(['ica', 'qc', 'coverage_region_1']),
        qc_cov_region_2_id=config_retrieve(['ica', 'qc', 'coverage_region_2']),
        dragen_pipeline_id=config_retrieve(['ica', 'pipelines', 'dragen_3_7_8']),
        user_tags=config_retrieve(['ica', 'tags', 'user_tags']),
        technical_tags=config_retrieve(['ica', 'tags', 'technical_tags']),
        reference_tags=config_retrieve(['ica', 'tags', 'reference_tags']),
        user_reference=sg_name,
        api_root=api_root,
    )
    return ica_pipeline_id
