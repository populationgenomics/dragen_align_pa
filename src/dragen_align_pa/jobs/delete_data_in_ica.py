import json
from typing import Literal

import cpg_utils
import icasdk
from cpg_flow.targets import SequencingGroup
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api
from icasdk.exceptions import ApiException, ApiValueError
from loguru import logger

from dragen_align_pa import ica_utils, utils
from dragen_align_pa.constants import ICA_REST_ENDPOINT


def delete_data_in_ica(
    sequencing_group: SequencingGroup,
    ica_fid_path: cpg_utils.Path,
    alignment_fid_paths: cpg_utils.Path,
) -> PythonJob:
    delete_job: PythonJob = utils.initialise_python_job(
        job_name='DeleteDataInIca',
        target=sequencing_group,
        tool_name='ICA',
    )
    delete_job.call(
        _run,
        ica_fid_path=ica_fid_path,
        alignment_fid_paths=alignment_fid_paths,
    )
    return delete_job


def _run(ica_fid_path: cpg_utils.Path, alignment_fid_paths: cpg_utils.Path) -> None:
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_utils.get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']

    path_params: dict[str, str] = {'projectId': project_id}
    fids: list[str] = []

    configuration = icasdk.Configuration(host=ICA_REST_ENDPOINT)
    configuration.api_key['ApiKeyAuth'] = api_key
    with ica_fid_path.open() as fid_handle:
        fids.append(json.load(fid_handle)['analysis_output_fid'])
    with alignment_fid_paths.open() as alignment_fid_handle:
        fids = fids + list(json.load(alignment_fid_handle).values())
    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        for f_id in fids:
            request_path_params = path_params | {'dataId': f_id}
            try:
                # The API returns None (invalid as defined by the sdk) but deletes
                # the data anyway.
                # We replace contextlib.suppress with an explicit try/except
                # to log the suppressed error.
                api_instance.delete_data(  # type: ignore[ReportUnknownVariableType]
                    path_params=request_path_params,  # type: ignore[ReportUnknownVariableType]
                )
            except ApiValueError as e:
                logger.warning(
                    f'Suppressed spurious ApiValueError for f_id {f_id}. '
                    f'Deletion is expected to have proceeded. Error: {e}',
                )
            except ApiException as e:
                logger.warning(
                    f'API exception for {f_id}. Has it already been deleted? Error: {e}',
                )
