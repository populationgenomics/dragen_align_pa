from icasdk.apis.tags import project_analysis_api

from dragen_align_pa import ica_api_utils
from dragen_align_pa.constants_registry import ROLE_DRAGEN_ALIGN, ROLE_DRAGEN_MLR


def run(ica_pipeline_id: str | dict[str, str], is_mlr: bool = False) -> str:
    """Monitor the status of an ICA pipeline run.
    Args:
        ica_pipeline_id: The ICA pipeline ID or a dict containing it.
        is_mlr: Whether the pipeline is a Machine Learning Recalibration (MLR) run.
    Returns:
        The status of the ICA pipeline run.
    """
    role = ROLE_DRAGEN_MLR if is_mlr else ROLE_DRAGEN_ALIGN
    pipeline_id: str = ica_pipeline_id['pipeline_id'] if isinstance(ica_pipeline_id, dict) else ica_pipeline_id

    with ica_api_utils.ica_project_session(role) as (api_client, path_params):
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)
        return ica_api_utils.check_ica_pipeline_status(
            api_instance=api_instance,
            path_params=path_params | {'analysisId': pipeline_id},
        )
