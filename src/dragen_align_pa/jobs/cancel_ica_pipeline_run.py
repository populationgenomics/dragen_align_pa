import icasdk
from icasdk.apis.tags import project_analysis_api
from loguru import logger

from dragen_align_pa import ica_api_utils
from dragen_align_pa.constants_registry import ica_project_name, resolve_ica_project_id


def run(ica_pipeline_id: str, is_mlr: bool = False) -> dict[str, str]:
    """Cancel an ongoing ICA pipeline run.

    Args:
        ica_pipeline_id: The ICA pipeline ID or a dict containing it.
        is_mlr: Whether the pipeline is a Machine Learning Recalibration (MLR) run.

    Returns:
        A dict indicating the cancelled pipeline ID.
    """

    if is_mlr:
        project_name: str = ica_project_name('dragen_mlr')
    else:
        project_name = ica_project_name('dragen_align')
    project_id: str = resolve_ica_project_id(project_name)
    path_parameters: dict[str, str] = {'projectId': project_id}
    path_parameters = path_parameters | {'analysisId': ica_pipeline_id}

    with ica_api_utils.get_ica_api_client(project_name) as api_client:
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
