import json
from typing import Literal

import cpg_utils
import icasdk
from cpg_flow.targets import Cohort
from cpg_utils import Path
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa.utils import create_upload_object_id, get_ica_secrets


def _initalise_ica_prep_job(cohort: Cohort) -> PythonJob:
    prepare_ica_job: PythonJob = get_batch().new_python_job(
        name='PrepareIcaForDragenAnalysis',
        attributes=cohort.get_job_attrs() or {} | {'tool': 'ICA'},  # type: ignore[ReportUnknownVariableType]
    )
    prepare_ica_job.image(image=get_driver_image())

    return prepare_ica_job


def run_ica_prep_job(
    cohort: Cohort,
    output: dict[str, cpg_utils.Path],
    api_root: str,
    bucket_path: Path,
) -> PythonJob:
    job: PythonJob = _initalise_ica_prep_job(cohort=cohort)

    bucket_name: str = str(bucket_path).removeprefix('gs://')

    job.call(
        _run,
        api_root=api_root,
        cohort=cohort,
        bucket_name=bucket_name,
        output=output,
    )

    return job


def _run(
    api_root: str,
    cohort: Cohort,
    bucket_name: str,
    output: dict[str, cpg_utils.Path],
) -> None:
    """Prepare ICA pipeline runs by generating a folder ID for the
    outputs of the Dragen pipeline.

    Args:
        api_root (str): The ICA API endpoint
        sg_name (str): The name of the sequencing group
        bucket_name (str): The  name of the GCP bucket that the data reside in

    Returns:
        dict [str, str] noting the analysis ID.
    """
    secrets: dict[Literal['projectID', 'apiKey'], str] = get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']

    configuration = icasdk.Configuration(host=api_root)
    configuration.api_key['ApiKeyAuth'] = api_key
    path_parameters: dict[str, str] = {'projectId': project_id}

    ica_analysis_output_folder: str = config_retrieve(['ica', 'data_prep', 'output_folder'])

    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        folder_path: str = f'/{bucket_name}/{ica_analysis_output_folder}'
        for sg_name in cohort.get_sequencing_group_ids():
            object_id: str = create_upload_object_id(
                api_instance=api_instance,
                path_params=path_parameters,
                sg_name=sg_name,
                file_name=sg_name,
                folder_path=folder_path,
                object_type='FOLDER',
            )
            logger.info(f'Created folder ID {object_id} for analysis outputs for sequencing group {sg_name}')
            with output[sg_name].open('w') as opath:
                opath.write(json.dumps({'analysis_output_fid': object_id}))
