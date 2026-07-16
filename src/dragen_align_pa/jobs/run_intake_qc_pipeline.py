from typing import Any

import cpg_utils.config
from icasdk.apis.tags import project_analysis_api
from icasdk.model.analysis_data_input import AnalysisDataInput
from icasdk.model.analysis_parameter_input import AnalysisParameterInput
from icasdk.model.analysis_tag import AnalysisTag
from icasdk.model.create_nextflow_analysis import CreateNextflowAnalysis
from icasdk.model.nextflow_analysis_input import NextflowAnalysisInput

from dragen_align_pa import ica_api_utils


def run_md5_pipeline(
    cohort_name: str,
    fastq_list_file_id: str,
    api_instance: project_analysis_api.ProjectAnalysisApi,
    path_parameters: dict[str, str],
    ar_guid: str,
    md5_outputs_folder_id: str,
) -> str:
    header_params: dict[Any, Any] = {}
    chunk_size = str(cpg_utils.config.config_retrieve(['ica', 'pipelines', 'md5', 'chunk_size'], default='100'))
    # The projectId comes from the caller's session (one resolution per submission); the DRAGEN
    # project id is also passed to the pipeline itself as the `ica_project_id` parameter value.
    project_id: str = path_parameters['projectId']
    api_key: str = ica_api_utils.get_ica_api_key()

    body = CreateNextflowAnalysis(
        userReference=f'{cohort_name}_{ar_guid}',
        pipelineId=cpg_utils.config.config_retrieve(['ica', 'pipelines', 'md5_pipeline_id']),
        tags=AnalysisTag(
            technicalTags=[*cpg_utils.config.config_retrieve(['ica', 'tags', 'technical_tags']), 'md5sum'],
            userTags=cpg_utils.config.config_retrieve(['ica', 'tags', 'user_tags']),
            referenceTags=cpg_utils.config.config_retrieve(['ica', 'tags', 'reference_tags']),
        ),
        outputParentFolderId=md5_outputs_folder_id,
        analysisInput=NextflowAnalysisInput(
            inputs=[
                AnalysisDataInput(parameterCode='in', dataIds=[fastq_list_file_id]),
            ],
            parameters=[
                AnalysisParameterInput(code='ica_project_id', value=project_id),
                AnalysisParameterInput(code='ica_api_key', value=api_key),
                AnalysisParameterInput(code='chunk_size', value=chunk_size),
            ],
        ),
    )

    return ica_api_utils.submit_nextflow_analysis(
        api_instance=api_instance,
        path_params=path_parameters,
        body=body,
        header_params=header_params,
    )
