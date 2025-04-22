import time
from random import randint
from typing import Literal

import icasdk
from icasdk.apis.tags import project_analysis_api
from loguru import logger

from dragen_align_pa import utils


def run(
    ica_pipeline_id: str | dict[str, str],
    api_root: str,
) -> str:
    """Monitor a pipeline running in ICA

    Args:
        ica_pipeline_id (str): The path to the file holding the pipeline ID
        api_root (str): The root API endpoint for ICA

    Raises:
        Exception: An exception if the pipeline is cancelled
        Exception: Any other exception if the pipeline gets into a FAILED state

    Returns:
        str: The pipeline status
    """
    secrets: dict[Literal['projectID', 'apiKey'], str] = utils.get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']

    configuration = icasdk.Configuration(host=api_root)
    configuration.api_key['ApiKeyAuth'] = api_key
    pipeline_id: str = ica_pipeline_id['pipeline_id'] if isinstance(ica_pipeline_id, dict) else ica_pipeline_id

    logger.info(f'Monitoring pipeline run {pipeline_id} which of type {type(pipeline_id)}')
    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)
        path_params: dict[str, str] = {'projectId': project_id}

        pipeline_status: str = utils.check_ica_pipeline_status(
            api_instance=api_instance,
            path_params=path_params | {'analysisId': pipeline_id},
        )
        # Other running statuses are REQUESTED AWAITINGINPUT INPROGRESS
        while pipeline_status not in ['SUCCEEDED', 'FAILED', 'FAILEDFINAL', 'ABORTED']:
            time.sleep(600 + randint(-60, 60))  # noqa: S311
            pipeline_status = utils.check_ica_pipeline_status(
                api_instance=api_instance,
                path_params=path_params | {'analysisId': pipeline_id},
            )
        return pipeline_status
