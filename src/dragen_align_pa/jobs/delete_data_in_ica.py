import contextlib
import json
from typing import Literal

import cpg_utils
import icasdk
from cpg_flow.targets import SequencingGroup
from cpg_utils.config import get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api
from icasdk.exceptions import ApiException, ApiValueError
from loguru import logger

from dragen_align_pa import utils


def _initalise_delete_job(sequencing_group: SequencingGroup) -> PythonJob:
    delete_job: PythonJob = get_batch().new_python_job(
        name='DeleteDataInIca',
        attributes=(sequencing_group.get_job_attrs() or {} | {'tool': 'Dragen'}),  # type: ignore[ReportUnknownVariableType]
    )
    delete_job.image(image=get_driver_image())
    return delete_job


def delete_data_in_ica(
    sequencing_group: SequencingGroup, bucket: str, ica_fid_path: cpg_utils.Path, api_root: str
) -> PythonJob:
    delete_job: PythonJob = _initalise_delete_job(sequencing_group=sequencing_group)
    delete_job.call(
        _run,
        bucket=bucket,
        ica_fid_path=ica_fid_path,
        api_root=api_root,
    )
    return delete_job


def _run(bucket: str, ica_fid_path: cpg_utils.Path, api_root: str) -> None:
    secrets: dict[Literal['projectID', 'apiKey'], str] = utils.get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']

    path_params: dict[str, str] = {'projectId': project_id}

    configuration = icasdk.Configuration(host=api_root)
    configuration.api_key['ApiKeyAuth'] = api_key
    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        with ica_fid_path.open() as fid_handle:
            folder_id: str = json.load(fid_handle)['analysis_output_fid']
            path_params = path_params | {'dataId': folder_id}
            try:
                # The API returns None (invalid as defined by the sdk) but deletes the data anyway.
                with contextlib.suppress(ApiValueError):
                    api_instance.delete_data(  # type: ignore[ReportUnknownVariableType]
                        path_params=path_params  # type: ignore[ReportUnknownVariableType]
                    )
            # Used to catch instances where the data has been deleted already
            except ApiException:
                logger.info(
                    f"The folder {bucket} with folder ID {folder_id} doesn't exist. Has it already been deleted?"
                )
