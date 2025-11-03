from typing import Literal

from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_analysis_api
from loguru import logger

from dragen_align_pa import ica_api_utils


def run(ica_pipeline_id: str | dict[str, str], is_mlr: bool = False) -> str:
    """Monitor the status of an ICA pipeline run.
    Args:
        ica_pipeline_id: The ICA pipeline ID or a dict containing it.
        is_mlr: Whether the pipeline is a Machine Learning Recalibration (MLR) run.
    Returns:
        The status of the ICA pipeline run.
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
