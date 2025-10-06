from typing import Any

import icasdk
from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_analysis_api
from icasdk.model.analysis_data_input import AnalysisDataInput
from icasdk.model.analysis_tag import AnalysisTag
from icasdk.model.create_nextflow_analysis import CreateNextflowAnalysis
from icasdk.model.nextflow_analysis_input import NextflowAnalysisInput


def run_md5_pipeline(
    cohort_name: str, ica_fastq_ids: list[str], api_config: icasdk.Configuration, project_id: str
) -> str:
    header_params: dict[Any, Any] = {}
    body = CreateNextflowAnalysis(
        userReference=cohort_name,
        pipelineId=config_retrieve(['ica', 'pipelines', 'md5_pipeline_id']),
        tags=AnalysisTag(
            technicalTags=[*config_retrieve(['ica', 'tags', 'technical_tags']), 'md5sum'],
            userTags=config_retrieve(['ica', 'tags', 'user_tags']),
            referenceTags=config_retrieve(['ica', 'tags', 'reference_tags']),
        ),
        outputParentFolderId='fol.4975ee93b6694319d41e08ddfb9bf5a9',
        analysisInput=NextflowAnalysisInput(inputs=[AnalysisDataInput(parameterCode='in', dataIds=ica_fastq_ids)]),
    )

    with icasdk.ApiClient(configuration=api_config) as api_client:
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)
        path_params: dict[str, str] = {'projectId': project_id}
        try:
            api_response = api_instance.create_nextflow_analysis(  # pyright: ignore[reportUnknownVariableType]
                path_params=path_params, header_params=header_params, body=body
            )
            return api_response.body['id']  # pyright: ignore[reportUnknownVariableType]
        except icasdk.ApiException as e:
            raise icasdk.ApiException(
                f'Exception when calling ProjectAnalysisApi->create_nextflow_analysis: {e}'
            ) from e
