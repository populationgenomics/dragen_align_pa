import subprocess

from cpg_flow.targets import Cohort
from cpg_utils import to_path  # type: ignore  # noqa: PGH003
from cpg_utils.config import config_retrieve
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger

from dragen_align_pa.jobs import cancel_ica_pipeline_run, monitor_dragen_pipeline, run_align_genotype_with_dragen


def initalise_management_job(cohort: Cohort) -> PythonJob:
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
    pipeline_id_file: str,
    ica_fids_path: str,
    analysis_output_fid_path: str,
    api_root: str,
    success_file: str,
) -> PythonJob:
    logger.info(f'Starting management job for {cohort.name}')

    job: PythonJob = initalise_management_job(cohort=cohort)

    management_output = job.call(
        _run,
        cohort=cohort,
        pipeline_id_file=pipeline_id_file,
        ica_fids_path=ica_fids_path,
        analysis_output_fid_path=analysis_output_fid_path,
        api_root=api_root,
    )

    get_batch().write_output(management_output.as_json(), success_file)

    return job


def _run(
    cohort: Cohort,
    pipeline_id_file: str,
    ica_fids_path: str,
    analysis_output_fid_path: str,
    api_root: str,
) -> dict[str, str] | None:
    for sequencing_group in cohort.get_sequencing_groups():
        has_succeeded: bool = False
        try_counter = 1
        # Attempt one retry on pipeline failure
        while not has_succeeded and try_counter <= 2:  # noqa: PLR2004
            # If a pipeline ID file doesn't exist we have to submit a new run, regardless of other settings
            if not to_path(pipeline_id_file).exists():
                ica_pipeline_id: str = _submit_new_ica_pipeline(
                    sg_name=sequencing_group.name,
                    ica_fids_path=ica_fids_path,
                    analysis_output_fid_path=analysis_output_fid_path,
                    api_root=api_root,
                )
                with to_path(pipeline_id_file).open('w') as f:
                    f.write(ica_pipeline_id)
            else:
                # Get an existing pipeline ID
                with open(to_path(pipeline_id_file)) as pipeline_fid_handle:
                    ica_pipeline_id = pipeline_fid_handle.read().rstrip()
                # Cancel a running job in ICA
                if config_retrieve(key=['ica', 'management', 'cancel_cohort_run'], default=False):
                    logger.info(
                        f'Cancelling pipeline run: {ica_pipeline_id} for sequencing group {sequencing_group.name}'
                    )
                    cancel_ica_pipeline_run.run(ica_pipeline_id=ica_pipeline_id, api_root=api_root)
                    _delete_pipeline_id_file(pipeline_id_file=pipeline_id_file)
                    return {ica_pipeline_id: 'ABORTED'}

            # Monitor an existing ICA pipeline run
            pipeline_status = monitor_dragen_pipeline.run(ica_pipeline_id=ica_pipeline_id, api_root=api_root)

            if pipeline_status == 'SUCCEEDED':
                logger.info(f'Pipeline run {ica_pipeline_id} has succeeded')
                has_succeeded = True
                return {ica_pipeline_id: 'SUCCEEDED'}
            if pipeline_status in ['ABORTING', 'ABORTED']:
                logger.info(
                    f'The pipeline run {ica_pipeline_id} has been cancelled for sample {sequencing_group.name}.'
                )
                _delete_pipeline_id_file(pipeline_id_file=pipeline_id_file)
                raise Exception(f'The pipeline run for sequencing group {sequencing_group.name} has been cancelled.')
            # Log failed ICA pipeline to a file somewhere
            # Delete the pipeline ID file
            _delete_pipeline_id_file(pipeline_id_file=pipeline_id_file)
            try_counter += 1
            logger.info(
                f'The pipeline {ica_pipeline_id} has failed, deleting pipeline ID file {pipeline_id_file} and retrying once'
            )
            if try_counter > 2:  # noqa: PLR2004
                raise Exception(
                    f'The pipeline run for sequencing group {sequencing_group.name} has failed after 2 retries, please check ICA for more info.'  # noqa: E501
                )
    return None


time.sleep(600 + randint(-60, 60))


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
