from typing import Any

from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_analysis_api
from icasdk.model.analysis_data_input import AnalysisDataInput
from icasdk.model.analysis_tag import AnalysisTag
from icasdk.model.create_nextflow_analysis import CreateNextflowAnalysis
from icasdk.model.nextflow_analysis_input import NextflowAnalysisInput

from dragen_align_pa import ica_api_utils


def run_md5_pipeline(
    cohort_name: str,
    ica_fastq_ids: list[str],
    api_instance: project_analysis_api.ProjectAnalysisApi,
    project_id: str,
    ar_guid: str,
    md5_outputs_folder_id: str,
) -> str:
    header_params: dict[Any, Any] = {}
    body = CreateNextflowAnalysis(
        userReference=f'{cohort_name}_{ar_guid}',
        pipelineId=config_retrieve(['ica', 'pipelines', 'md5_pipeline_id']),
        tags=AnalysisTag(
            technicalTags=[*config_retrieve(['ica', 'tags', 'technical_tags']), 'md5sum'],
            userTags=config_retrieve(['ica', 'tags', 'user_tags']),
            referenceTags=config_retrieve(['ica', 'tags', 'reference_tags']),
        ),
        outputParentFolderId=md5_outputs_folder_id,
        analysisInput=NextflowAnalysisInput(inputs=[AnalysisDataInput(parameterCode='in', dataIds=ica_fastq_ids)]),
    )

    path_params: dict[str, str] = {'projectId': project_id}
    return ica_api_utils.submit_nextflow_analysis(
        api_instance=api_instance,
        path_params=path_params,
        body=body,
        header_params=header_params,
    )
