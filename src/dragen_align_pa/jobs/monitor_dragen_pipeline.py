from typing import Literal

from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_analysis_api
from loguru import logger

from dragen_align_pa import ica_api_utils


def run(ica_pipeline_id: str | dict[str, str], is_mlr: bool = False) -> str:
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
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    project_id: str = secrets['projectID']

    pipeline_id: str = ica_pipeline_id['pipeline_id'] if isinstance(ica_pipeline_id, dict) else ica_pipeline_id

    logger.info(
        f'Monitoring pipeline run {pipeline_id}',
    )
    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)
        if not is_mlr:
            path_params: dict[str, str] = {'projectId': project_id}
        else:
            path_params = {
                'projectId': config_retrieve(
                    ['ica', 'projects', 'dragen_mlr_project_id'],
                ),
            }

        return ica_api_utils.check_ica_pipeline_status(
            api_instance=api_instance,
            path_params=path_params | {'analysisId': pipeline_id},
        )
