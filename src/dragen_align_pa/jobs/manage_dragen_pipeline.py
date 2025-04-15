import logging
import subprocess

import coloredlogs
from cpg_flow.targets import SequencingGroup
from cpg_utils import to_path  # type: ignore  # noqa: PGH003
from cpg_utils.cloud import get_path_components_from_gcp_path
from cpg_utils.config import config_retrieve
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob

from dragen_align_pa.jobs import cancel_ica_pipeline_run, monitor_dragen_pipeline, run_align_genotype_with_dragen
from dragen_align_pa.utils import create_object_in_gcp


def initalise_management_job(sequencing_group: SequencingGroup, pipeline_id_file: str) -> PythonJob:
    if config_retrieve(key=['ica', 'management', 'cancel_cohort_run'], default=False):
        if to_path(pipeline_id_file).exists():
            name: str = 'CancelIcaPipeline'
        else:
            raise FileNotFoundError(
                f"Trying to cancel a pipeline run for {sequencing_group.name}, but the pipeline ID file {pipeline_id_file} doesn't exist."  # noqa: E501
            )
    elif config_retrieve(['ica', 'management', 'monitor_previous'], False) and to_path(pipeline_id_file).exists():
        name = 'MoniterIcaPipeline'
    else:
        name = 'AlignGenotypeWithDragen'
    management_job: PythonJob = get_batch().new_python_job(
        name=name,
        attributes=sequencing_group.get_job_attrs() or {} | {'tool': 'Dragen'},  # type: ignore  # noqa: PGH003
    )
    management_job.image(image=config_retrieve(['workflow', 'driver_image']))
    return management_job


def _delete_pipeline_id_file(pipeline_id_file: str) -> None:
    logging.info(f'Deleting the pipeline run ID file {pipeline_id_file}')
    subprocess.run(['gcloud', 'storage', 'rm', pipeline_id_file], check=True)  # noqa: S603, S607


def manage_ica_pipeline(
    job: PythonJob,
    sequencing_group: SequencingGroup,
    pipeline_id_file: str,
    ica_fids_path: str,
    analysis_output_fid_path: str,
    api_root: str,
    output: str,
) -> None:
    coloredlogs.install(level=logging.INFO)
    logging.info(f'Starting management job for {sequencing_group.name}')

    management_output = job.call(
        _run,
        sequencing_group=sequencing_group,
        pipeline_id_file=pipeline_id_file,
        ica_fids_path=ica_fids_path,
        analysis_output_fid_path=analysis_output_fid_path,
        api_root=api_root,
        output=output,
    )

    get_batch().write_output(management_output.as_str(), output)


def _run(
    sequencing_group: SequencingGroup,
    pipeline_id_file: str,
    ica_fids_path: str,
    analysis_output_fid_path: str,
    api_root: str,
    output: str,
) -> str | None:
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
                output=output,
            )
            # Create the pipeline ID in GCP
            bucket: str = get_path_components_from_gcp_path(output)['bucket']
            object_path: str = (
                get_path_components_from_gcp_path(pipeline_id_file)['suffix']
                + get_path_components_from_gcp_path(pipeline_id_file)['file']
            )
            logging.info(f'Pipeline ID file: {pipeline_id_file}')
            logging.info(f'bucket: {bucket}')
            logging.info(f'object_path: {object_path}')
            create_object_in_gcp(bucket=bucket, object_path=object_path, contents=ica_pipeline_id)
        else:
            # Get an existing pipeline ID
            with open(to_path(pipeline_id_file)) as pipeline_fid_handle:
                ica_pipeline_id = pipeline_fid_handle.read().rstrip()
            # Cancel a running job in ICA
            if config_retrieve(key=['ica', 'management', 'cancel_cohort_run'], default=False):
                logging.info(f'Cancelling pipeline run: {ica_pipeline_id} for sequencing group {sequencing_group.name}')
                cancel_ica_pipeline_run.run(ica_pipeline_id=ica_pipeline_id, api_root=api_root)
                _delete_pipeline_id_file(pipeline_id_file=pipeline_id_file)
                return 'ABORTED'

        # Monitor an existing ICA pipeline run
        pipeline_status = monitor_dragen_pipeline.run(ica_pipeline_id=ica_pipeline_id, api_root=api_root)

        if pipeline_status == 'SUCCEEDED':
            logging.info(f'Pipeline run {ica_pipeline_id} has succeeded')
            has_succeeded = True
            return 'SUCCEEDED'
        if pipeline_status in ['ABORTING', 'ABORTED']:
            logging.info(f'The pipeline run {ica_pipeline_id} has been cancelled for sample {sequencing_group.name}.')
            _delete_pipeline_id_file(pipeline_id_file=pipeline_id_file)
            raise Exception(f'The pipeline run for sequencing group {sequencing_group.name} has been cancelled.')
        # Log failed ICA pipeline to a file somewhere
        # Delete the pipeline ID file
        _delete_pipeline_id_file(pipeline_id_file=pipeline_id_file)
        try_counter += 1
        if try_counter == 2:  # noqa: PLR2004
            raise Exception(
                f'The pipeline run for sequencing group {sequencing_group.name} has failed after 2 retries, please check ICA for more info.'  # noqa: E501
            )
    return None


def _submit_new_ica_pipeline(
    sg_name: str,
    ica_fids_path: str,
    analysis_output_fid_path: str,
    api_root: str,
    output: str,
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
        output_path=output,
    )
    return ica_pipeline_id
