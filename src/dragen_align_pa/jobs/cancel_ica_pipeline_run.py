import icasdk
from loguru import logger

from dragen_align_pa import ica_api_utils
from dragen_align_pa.constants.constants_registry import ROLE_DRAGEN_ALIGN, ROLE_DRAGEN_MLR


def run(ica_pipeline_id: str, is_mlr: bool = False) -> dict[str, str]:
    """Cancel an ongoing ICA pipeline run.

    Args:
        ica_pipeline_id: The ICA pipeline ID or a dict containing it.
        is_mlr: Whether the pipeline is a Machine Learning Recalibration (MLR) run.

    Returns:
        A dict indicating the cancelled pipeline ID.
    """

    role = ROLE_DRAGEN_MLR if is_mlr else ROLE_DRAGEN_ALIGN

    with ica_api_utils.ica_project_analysis_api(role) as (api_instance, path_parameters):
        abort_path_params = path_parameters | {'analysisId': ica_pipeline_id}
        try:
            api_instance.abort_analysis(
                path_params=abort_path_params,  # type: ignore[ReportUnknownVariableType]
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
