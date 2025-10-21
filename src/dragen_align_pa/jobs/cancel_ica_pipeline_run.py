from typing import Literal

import icasdk
from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_analysis_api
from loguru import logger

from dragen_align_pa import utils
from dragen_align_pa.constants import ICA_REST_ENDPOINT


def run(ica_pipeline_id: str, is_mlr: bool = False) -> dict[str, str]:
    """Cancel a running ICA pipeline via the API

    Args:
        ica_pipeline_id_path (str): The path to the JSON file holding the pipeline ID
        api_root (str): The root for the ICA API

    Raises:
        icasdk.ApiException: Any API error

    Returns:
        dict[str, str]: A cancelled dict to be recorded in GCP noting that the pipeline was cancelled.
                        Includes a timestamp so that a single cancelled pipeline isn't blocking.
    """
    secrets: dict[Literal['projectID', 'apiKey'], str] = utils.get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']

    configuration = icasdk.Configuration(host=ICA_REST_ENDPOINT)
    configuration.api_key['ApiKeyAuth'] = api_key

    if not is_mlr:
        path_parameters: dict[str, str] = {'projectId': project_id}
    else:
        path_parameters = {'projectId': config_retrieve(['ica', 'projects', 'dragen_mlr_project_id'])}
    path_parameters = path_parameters | {'analysisId': ica_pipeline_id}

    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)
        try:
            api_instance.abort_analysis(
                path_params=path_parameters,  # type: ignore[ReportUnknownVariableType]
                skip_deserialization=True,
            )  # type: ignore[ReportUnknownVariableType]
            logger.info(f'Sent cancellation request for ICA analysis: {ica_pipeline_id}')
            return {'cancelled': ica_pipeline_id}
        except icasdk.ApiException as e:
            raise icasdk.ApiException(f'Exception when calling ProjectAnalysisApi->abort_analysis: {e}') from e
