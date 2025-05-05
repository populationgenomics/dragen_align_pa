from typing import Literal

from cpg_flow.targets import SequencingGroup
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger


def _initalise_ica_prep_job(sequencing_group: SequencingGroup) -> PythonJob:
    prepare_ica_job: PythonJob = get_batch().new_python_job(
        name='PrepareIcaForDragenAnalysis',
        attributes=sequencing_group.get_job_attrs() or {} | {'tool': 'ICA'},  # type: ignore[ReportUnknownVariableType]
    )
    prepare_ica_job.image(image=get_driver_image())

    return prepare_ica_job


def run_ica_prep_job(
    sequencing_group: SequencingGroup,
    output: str,
    api_root: str,
    sg_name: str,
    bucket_name: str,
) -> PythonJob:
    job: PythonJob = _initalise_ica_prep_job(sequencing_group=sequencing_group)

    output_fids = job.call(
        _run,
        ica_analysis_output_folder=config_retrieve(['ica', 'data_prep', 'output_folder']),
        api_root=api_root,
        sg_name=sg_name,
        bucket_name=bucket_name,
    ).as_json()

    get_batch().write_output(output_fids, output)

    return job


def _run(
    ica_analysis_output_folder: str,
    api_root: str,
    sg_name: str,
    bucket_name: str,
) -> dict[str, str]:
    """Prepare ICA pipeline runs by generating a folder ID for the
    outputs of the Dragen pipeline.

    Args:
        ica_analysis_output_folder (str): The folder that outputs from the pipeline run should be written to
        api_root (str): The ICA API endpoint
        sg_name (str): The name of the sequencing group
        bucket_name (str): The  name of the GCP bucket that the data reside in

    Returns:
        dict [str, str] noting the analysis ID.
    """
    import icasdk
    from icasdk.apis.tags import project_data_api

    from dragen_align_pa.utils import create_upload_object_id, get_ica_secrets

    secrets: dict[Literal['projectID', 'apiKey'], str] = get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']

    configuration = icasdk.Configuration(host=api_root)
    configuration.api_key['ApiKeyAuth'] = api_key
    path_parameters: dict[str, str] = {'projectId': project_id}

    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        folder_path = f'/{bucket_name}/{ica_analysis_output_folder}'
        object_id: str = create_upload_object_id(
            api_instance=api_instance,
            path_params=path_parameters,
            sg_name=sg_name,
            file_name=sg_name,
            folder_path=folder_path,
            object_type='FOLDER',
        )
        logger.info(f'Created folder ID {object_id} for analysis outputs')
    return {'analysis_output_fid': object_id}
