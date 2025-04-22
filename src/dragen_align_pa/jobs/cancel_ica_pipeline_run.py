from typing import Literal

import icasdk
from icasdk.apis.tags import project_analysis_api
from loguru import logger

from dragen_align_pa import utils


def run(ica_pipeline_id: str, api_root: str) -> dict[str, str]:
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

    configuration = icasdk.Configuration(host=api_root)
    configuration.api_key['ApiKeyAuth'] = api_key

    path_parameters: dict[str, str] = {'projectId': project_id} | {'analysisId': ica_pipeline_id}

    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)
        try:
            api_instance.abort_analysis(
                path_params=path_parameters,  # type: ignore  # noqa: PGH003
                skip_deserialization=True,
            )  # type: ignore  # noqa: PGH003
            logger.info(f'Sent cancellation request for ICA analysis: {ica_pipeline_id}')
            return {'cancelled': ica_pipeline_id}
        except icasdk.ApiException as e:
            raise icasdk.ApiException(f'Exception when calling ProjectAnalysisApi->abort_analysis: {e}') from e
