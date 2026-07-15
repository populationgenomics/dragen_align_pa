from icasdk.apis.tags import project_analysis_api

from dragen_align_pa import ica_api_utils
from dragen_align_pa.constants_registry import ica_project_name, resolve_ica_project_id


def run(ica_pipeline_id: str | dict[str, str], is_mlr: bool = False) -> str:
    """Monitor the status of an ICA pipeline run.
    Args:
        ica_pipeline_id: The ICA pipeline ID or a dict containing it.
        is_mlr: Whether the pipeline is a Machine Learning Recalibration (MLR) run.
    Returns:
        The status of the ICA pipeline run.
    """
    if is_mlr:
        project_name: str = ica_project_name('dragen_mlr')
    else:
        project_name = ica_project_name('dragen_align')
    project_id: str = resolve_ica_project_id(project_name)

    pipeline_id: str = ica_pipeline_id['pipeline_id'] if isinstance(ica_pipeline_id, dict) else ica_pipeline_id

    with ica_api_utils.get_ica_api_client(project_name) as api_client:
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)
        path_params: dict[str, str] = {'projectId': project_id}

        return ica_api_utils.check_ica_pipeline_status(
            api_instance=api_instance,
            path_params=path_params | {'analysisId': pipeline_id},
        )
