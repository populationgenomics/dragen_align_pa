import json
from typing import Any, Literal

import cpg_utils
import icasdk
import pandas as pd
from cpg_utils.config import config_retrieve, try_get_ar_guid
from icasdk.apis.tags import project_analysis_api
from icasdk.model.analysis_data_input import AnalysisDataInput
from icasdk.model.analysis_parameter_input import AnalysisParameterInput
from icasdk.model.analysis_tag import AnalysisTag
from icasdk.model.create_nextflow_analysis import CreateNextflowAnalysis
from icasdk.model.nextflow_analysis_input import NextflowAnalysisInput
from icecream import ic
from loguru import logger

from dragen_align_pa import ica_utils
from dragen_align_pa.constants import ICA_REST_ENDPOINT


def submit_dragen_run(
    cram_ica_fids_path: cpg_utils.Path | None,
    fastq_csv_list_file_path: cpg_utils.Path | None,
    fastq_ids_path: cpg_utils.Path | None,
    individual_fastq_file_list_paths: cpg_utils.Path | None,
    project_id: dict[str, str],
    ica_output_folder_id: str,
    api_instance: project_analysis_api.ProjectAnalysisApi,
    sg_name: str,
) -> str:
    """Submit a Dragen alignment and genotyping run to ICA"""
    dragen_ht_id: str = config_retrieve(['ica', 'pipelines', 'dragen_ht_id'])
    qc_cross_cont_vcf_id: str = config_retrieve(['ica', 'qc', 'cross_cont_vcf'])
    qc_cov_region_1_id: str = config_retrieve(['ica', 'qc', 'coverage_region_1'])
    qc_cov_region_2_id: str = config_retrieve(['ica', 'qc', 'coverage_region_2'])
    dragen_pipeline_id: str = config_retrieve(
        ['ica', 'pipelines', config_retrieve(['workflow', 'reads_type'])],
    )
    user_tags: list[str] = config_retrieve(['ica', 'tags', 'user_tags'])
    technical_tags: list[str] = config_retrieve(['ica', 'tags', 'technical_tags'])
    reference_tags: list[str] = config_retrieve(['ica', 'tags', 'reference_tags'])
    user_reference: str = f'{sg_name}_{try_get_ar_guid()}_'

    logger.info(
        f'Loaded Dragen ICA configuration values, user reference: {user_reference}',
    )

    cram_input: list[AnalysisDataInput] | None = []
    cram_parameters: list[AnalysisParameterInput] | None = []
    fastq_input: list[AnalysisDataInput] | None = []
    fastq_parameters: list[AnalysisParameterInput] | None = []

    if cram_ica_fids_path:
        logger.info(f'Using CRAM input for sequencing group {sg_name}')
        with cram_ica_fids_path.open() as cram_ica_fids_handle:
            cram_ica_fids: dict[str, str] = json.load(cram_ica_fids_handle)
            cram_reference_id: str = config_retrieve(
                [
                    'ica',
                    'cram_references',
                    config_retrieve(['ica', 'cram_references', 'old_cram_reference']),
                ],
            )
            cram_input = [
                AnalysisDataInput(
                    parameterCode='crams',
                    dataIds=[cram_ica_fids['cram_fid']],
                ),
                AnalysisDataInput(
                    parameterCode='cram_reference',
                    dataIds=[cram_reference_id],
                ),
            ]
            cram_parameters = [
                AnalysisParameterInput(
                    code='additional_args',
                    value=(
                        '--read-trimmers polyg '
                        '--soft-read-trimmers none '
                        "--vc-hard-filter 'DRAGENHardQUAL:all:QUAL<5.0;LowDepth:all:DP<=1' "
                        '--vc-frd-max-effective-depth 40 '
                        '--vc-enable-joint-detection true '
                        '--qc-coverage-ignore-overlaps true '
                        '--qc-coverage-count-soft-clipped-bases true '
                        '--qc-coverage-reports-1 cov_report,cov_report '
                        "--qc-coverage-filters-1 'mapq<1,bq<0,mapq<1,bq<0' "
                        '--vc-gvcf-gq-bands 13 20 30 40'
                    ),
                ),
            ]
    # Need the gcs path to the fastq list file to extract the fastq names from.
    elif fastq_csv_list_file_path and fastq_ids_path and individual_fastq_file_list_paths:
        logger.info(f'Using FASTQ input for sequencing group {sg_name}')
        fastq_file_list_id: str | None = None
        with fastq_csv_list_file_path.open() as fastq_list_file_handle:
            for line in fastq_list_file_handle:
                if sg_name not in line:
                    continue
                fastq_file_list_id = line.split(':')[1].strip()
        with individual_fastq_file_list_paths.open() as individual_fastq_file_list_handle:
            # Load the csv into a dataframe and filter the fastq_list_file for the fastqs that match the csv
            individual_fastq_csv_df: pd.DataFrame = pd.read_csv(
                individual_fastq_file_list_handle,
                sep=',',
            )
        with fastq_ids_path.open() as fastq_ids_handle:
            fastq_ica_ids_df: pd.DataFrame = pd.read_csv(
                fastq_ids_handle,
                sep=r'\s+',
                names=['ica_id', 'fastq_name'],
            )
            fastq_ica_ids: list[str] = fastq_ica_ids_df[
                fastq_ica_ids_df['fastq_name'].isin(
                    individual_fastq_csv_df['Read1File'].tolist(),
                )
                | fastq_ica_ids_df['fastq_name'].isin(
                    individual_fastq_csv_df['Read2File'].tolist(),
                )
            ]['ica_id'].tolist()
            fastq_input = [
                AnalysisDataInput(parameterCode='fastqs', dataIds=fastq_ica_ids),
                AnalysisDataInput(
                    parameterCode='fastq_list',
                    dataIds=[fastq_file_list_id],
                ),
            ]
            fastq_parameters = [
                AnalysisParameterInput(code='vc-hard-filter', value='false'),
                AnalysisParameterInput(
                    code='additional_args',
                    value=(
                        '--qc-coverage-reports-1 cov_report,cov_report '
                        "--qc-coverage-filters-1 'mapq<1,bq<0,mapq<1,bq<0' "
                        '--vc-gvcf-gq-bands 13 20 30 40 '
                        "--vc-hard-filter 'DRAGENHardQUAL:all:QUAL<5.0;LowDepth:all:DP<=1' "
                    ),
                ),
            ]
            ic()
            logger.info(f'Fastq input is {fastq_input}')
    else:
        raise ValueError('No valid input provided for either CRAM or FASTQ files.')

    header_params: dict[Any, Any] = {}
    body = CreateNextflowAnalysis(
        userReference=user_reference,
        pipelineId=dragen_pipeline_id,
        tags=AnalysisTag(
            technicalTags=technical_tags,
            userTags=user_tags,
            referenceTags=reference_tags,
        ),
        outputParentFolderId=ica_output_folder_id,
        analysisInput=NextflowAnalysisInput(
            inputs=[
                AnalysisDataInput(parameterCode='ref_tar', dataIds=[dragen_ht_id]),
                AnalysisDataInput(
                    parameterCode='qc_cross_cont_vcf',
                    dataIds=[qc_cross_cont_vcf_id],
                ),
                AnalysisDataInput(
                    parameterCode='qc_coverage_region_1',
                    dataIds=[qc_cov_region_1_id],
                ),
                AnalysisDataInput(
                    parameterCode='qc_coverage_region_2',
                    dataIds=[qc_cov_region_2_id],
                ),
                *cram_input,
                *fastq_input,
            ],
            parameters=[
                AnalysisParameterInput(code='enable_map_align', value='true'),
                AnalysisParameterInput(code='enable_map_align_output', value='true'),
                AnalysisParameterInput(code='output_format', value='CRAM'),
                AnalysisParameterInput(code='enable_duplicate_marking', value='true'),
                AnalysisParameterInput(code='enable_variant_caller', value='true'),
                AnalysisParameterInput(code='vc_emit_ref_confidence', value='GVCF'),
                AnalysisParameterInput(code='vc_enable_vcf_output', value='false'),
                AnalysisParameterInput(code='enable_cnv', value='true'),
                AnalysisParameterInput(code='cnv_segmentation_mode', value='SLM'),
                AnalysisParameterInput(code='enable_sv', value='true'),
                AnalysisParameterInput(code='enable_cyp2d6', value='true'),
                AnalysisParameterInput(code='repeat_genotype_enable', value='true'),
                AnalysisParameterInput(code='dragen_reports', value='false'),
                *cram_parameters,
                *fastq_parameters,
            ],
        ),
    )
    try:
        api_response = api_instance.create_nextflow_analysis(  # type: ignore[ReportUnknownVariableType]
            path_params=project_id,
            header_params=header_params,
            body=body,
        )
        return api_response.body['id']  # type: ignore[ReportUnknownVariableType]
    except icasdk.ApiException as e:
        raise icasdk.ApiException(
            f'Exception when calling ProjectAnalysisApi->create_nextflow_analysis: {e}',
        ) from e


def run(
    cram_ica_fids_path: cpg_utils.Path | None,
    fastq_csv_list_file_path: cpg_utils.Path | None,
    fastq_ids_path: cpg_utils.Path | None,
    individual_fastq_file_list_paths: cpg_utils.Path | None,
    analysis_output_fid_path: cpg_utils.Path,
    sg_name: str,
) -> str:
    """_summary_

    Args:
        ica_fids_path (str): Path to the JSON in GCP holding the file IDs for the input CRAM and CRAI
        analysis_output_fid_path (str): Path to the JSON in GCP holding the folder ID to store the analysis outputs in ICA
        dragen_ht_id (str): The ICA file ID for the Dragen hash table used for mapping
        cram_reference_id (str): The ICA file ID for the FASTA reference that was used to align the CRAM file
        dragen_pipeline_id (str): The ICA pipeline ID for the Dragen pipeline that is going to be run
        user_tags (list[str]): List of user tags for the analysis (optional, can be empty)
        technical_tags (list[str]): List of technical tags for the analysis (optional, can be empty)
        reference_tags (list[str]): List of reference tags for the analysis (optional, can be empty)
        user_reference (str): A reference name for the pipeline run
        api_root (str): The ICA API root
        output_path (str): The path to write the pipeline ID to
    """  # noqa: E501

    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_utils.get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']

    configuration = icasdk.Configuration(host=ICA_REST_ENDPOINT)
    configuration.api_key['ApiKeyAuth'] = api_key

    with analysis_output_fid_path.open() as analysis_outputs_fid_handle:
        analysis_output_fid: dict[str, str] = json.load(analysis_outputs_fid_handle)

    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)
        path_params: dict[str, str] = {'projectId': project_id}
        analysis_run_id: str = submit_dragen_run(
            cram_ica_fids_path=cram_ica_fids_path,
            fastq_csv_list_file_path=fastq_csv_list_file_path,
            fastq_ids_path=fastq_ids_path,
            individual_fastq_file_list_paths=individual_fastq_file_list_paths,
            ica_output_folder_id=analysis_output_fid['analysis_output_fid'],
            project_id=path_params,
            api_instance=api_instance,
            sg_name=sg_name,
        )

        logger.info(f'Submitted ICA run with pipeline ID: {analysis_run_id}')

    return analysis_run_id
