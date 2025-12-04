from typing import Any, Literal

from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_analysis_api

from dragen_align_pa import ica_api_utils


def run_md5_pipeline(
    cohort_name: str,
    fastq_list_file_id: str,
    api_instance: project_analysis_api.ProjectAnalysisApi,
    ar_guid: str,
    md5_outputs_folder_id: str,
) -> str:
    header_params: dict[Any, Any] = {}
    chunk_size = str(config_retrieve(['ica', 'pipelines', 'md5', 'chunk_size'], default='100'))
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']

    body: dict[str, Any] = {
        'userReference': f'{cohort_name}_{ar_guid}',
        'pipelineId': config_retrieve(['ica', 'pipelines', 'md5_pipeline_id']),
        'tags': {
            'technicalTags': [*config_retrieve(['ica', 'tags', 'technical_tags']), 'md5sum'],
            'userTags': config_retrieve(['ica', 'tags', 'user_tags']),
            'referenceTags': config_retrieve(['ica', 'tags', 'reference_tags']),
        },
        'outputParentFolderId': md5_outputs_folder_id,
        'analysisInput': {
            'inputs': [
                {'parameterCode': 'in', 'dataIds': [fastq_list_file_id]},
            ],
            'parameters': [
                {'parameterCode': 'ica_project_id', 'value': project_id},
                {'parameterCode': 'ica_api_key', 'value': api_key},
                {'parameterCode': 'chunk_size', 'value': chunk_size},
            ],
        },
        'analysisStorageId': config_retrieve(['ica', 'pipelines', 'analysis_storage_id']),
    }

    path_params: dict[str, str] = {'projectId': project_id}
    return ica_api_utils.submit_nextflow_analysis(
        api_instance=api_instance,
        path_params=path_params,
        body=body,
        header_params=header_params,
    )
