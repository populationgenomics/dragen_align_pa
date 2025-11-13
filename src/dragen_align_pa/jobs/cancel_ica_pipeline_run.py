from typing import Literal

import icasdk
from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_analysis_api
from loguru import logger

from dragen_align_pa import ica_api_utils


def run(ica_pipeline_id: str, is_mlr: bool = False) -> dict[str, str]:
    """Cancel an ongoing ICA pipeline run.

    Args:
        ica_pipeline_id: The ICA pipeline ID or a dict containing it.
        is_mlr: Whether the pipeline is a Machine Learning Recalibration (MLR) run.

    Returns:
        A dict indicating the cancelled pipeline ID.
    """
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    project_id: str = secrets['projectID']

    if not is_mlr:
        path_parameters: dict[str, str] = {'projectId': project_id}
    else:
        path_parameters = {
            'projectId': config_retrieve(['ica', 'projects', 'dragen_mlr_project_id']),
        }
    path_parameters = path_parameters | {'analysisId': ica_pipeline_id}

    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)
        try:
            api_instance.abort_analysis(
                path_params=path_parameters,  # type: ignore[ReportUnknownVariableType]
                skip_deserialization=True,
            )  # type: ignore[ReportUnknownVariableType]
            logger.info(
                f'Sent cancellation request for ICA analysis: {ica_pipeline_id}',
            )
            return {'cancelled': ica_pipeline_id}
        except icasdk.ApiException as e:
            raise icasdk.ApiException(
                f'Exception when calling ProjectAnalysisApi->abort_analysis: {e}',
            ) from e
