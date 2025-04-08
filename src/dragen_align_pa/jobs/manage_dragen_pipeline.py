import logging
import subprocess

import coloredlogs
from cpg_flow.targets import SequencingGroup
from cpg_utils import to_path
from cpg_utils.config import config_retrieve
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob

from src.dragen_align_pa.jobs import cancel_ica_pipeline_run, monitor_dragen_pipeline


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
        attributes=sequencing_group.get_job_attrs(sequencing_group) or {} | {'tool': 'Dragen'},  # type: ignore
    )
    management_job.image(image=config_retrieve(['workflow', 'driver_image']))
    return management_job


def _delete_pipeline_id_file(pipeline_id_file: str) -> None:
    logging.info(f'Deleting the pipeline run ID file {pipeline_id_file}')
    subprocess.run(['gcloud', 'storage', 'rm', pipeline_id_file], check=True)  # noqa: S603, S607


def _submit_ica_pipeline_job() -> None:
    pass


def manage_ica_pipeline(
    management_job: PythonJob,
    sequencing_group: SequencingGroup,
    pipeline_id_file: str,
    api_root: str,
    output: str,
) -> None:
    coloredlogs.install(level=logging.INFO)
    logging.info(f'Starting management job for {sequencing_group.name}')
    management_output = management_job.call(
        _run,
        sequencing_group=sequencing_group,
        pipeline_id_file=pipeline_id_file,
        api_root=api_root,
    ).as_json()

    get_batch().write_output(management_output, output)


def _run(sequencing_group: SequencingGroup, pipeline_id_file: str, api_root: str) -> dict[str, str]:
    # Get an existing pipeline ID
    with open(to_path(pipeline_id_file)) as pipeline_fid_handle:
        ica_pipeline_id: str = pipeline_fid_handle.read().rstrip()
    # Cancel a running job in ICA
    if (
        config_retrieve(key=['ica', 'management', 'cancel_cohort_run'], default=False)
        and to_path(pipeline_id_file).exists()
    ):
        logging.info(f'Cancelling pipeline run: {ica_pipeline_id} for sequencing group {sequencing_group.name}')
        cancel_ica_pipeline_run.run(ica_pipeline_id=ica_pipeline_id, api_root=api_root)
        _delete_pipeline_id_file(pipeline_id_file=pipeline_id_file)
    pipeline_status = monitor_dragen_pipeline.run(ica_pipeline_id=ica_pipeline_id, api_root=api_root)
    if pipeline_status == 'SUCCEEDED':
        logging.info(f'Pipeline run {ica_pipeline_id} has succeeded')
        return {'pipeline': ica_pipeline_id, 'status': 'success'}
    if pipeline_status in ['ABORTING', 'ABORTED']:
        logging.info(f'The pipeline run {ica_pipeline_id} has been cancelled for sample {sequencing_group.name}.')
        _delete_pipeline_id_file(pipeline_id_file=pipeline_id_file)
        raise Exception(f'Pipeline run {ica_pipeline_id} has been cancelled.')
    # Log failed ICA pipeline to a file somewhere
    # Delete the pipeline ID file
    _delete_pipeline_id_file(pipeline_id_file=pipeline_id_file)
    raise Exception(f'The pipeline run {ica_pipeline_id} has failed, please check ICA for more info.')

    _submit_ica_pipeline_job()
