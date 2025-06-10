from typing import TYPE_CHECKING, Literal

import icasdk
from cpg_flow.targets import Cohort
from cpg_utils.config import get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import utils

if TYPE_CHECKING:
    from collections.abc import Sequence


def _initalise_delete_job(cohort: Cohort) -> PythonJob:
    delete_job: PythonJob = get_batch().new_python_job(
        name='DeleteDataInIca',
        attributes=(cohort.get_job_attrs() or {} | {'tool': 'Dragen'}),  # type: ignore[ReportUnknownVariableType]
    )
    delete_job.image(image=get_driver_image())
    return delete_job


def delete_data_in_ica(cohort: Cohort, bucket: str, api_root: str) -> PythonJob:
    delete_job: PythonJob = _initalise_delete_job(cohort=cohort)
    delete_job.call(
        _run,
        bucket=bucket,
        api_root=api_root,
    )
    return delete_job


def _run(bucket: str, api_root: str) -> None:
    secrets: dict[Literal['projectID', 'apiKey'], str] = utils.get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']

    path_params: dict[str, str] = {'projectId': project_id}
    query_params: dict[str, Sequence[str] | list[str] | str] = {
        'filePath': [bucket],
        'filePathMatchMode': 'STARTS_WITH_CASE_INSENSITIVE',
        'type': 'FOLDER',
    }

    configuration = icasdk.Configuration(host=api_root)
    configuration.api_key['ApiKeyAuth'] = api_key
    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        api_response = api_instance.get_project_data_list(  # type: ignore[ReportUnknownVariableType]
            query_params=query_params,  # type: ignore[ReportUnknownVariableType]
            path_params=path_params,  # type: ignore[ReportUnknownVariableType]
        )
        if folder_id := api_response.body['items'][0]['data']['id']:
            path_params = path_params | {'dataId': folder_id}
            deletion_response = api_instance.delete_data(  # type: ignore[ReportUnknownVariableType]
                path_params=path_params  # type: ignore[ReportUnknownVariableType]
            )
        else:
            logger.info(f"The folder {bucket} with folder ID {folder_id} doesn't exist. Has it already been deleted?")
