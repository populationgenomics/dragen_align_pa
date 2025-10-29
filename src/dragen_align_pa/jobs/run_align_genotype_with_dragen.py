import json
from typing import Any, Final, Literal

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

# Input Data Parameter Codes
PARAM_CODE_CRAMS: Final = 'crams'
PARAM_CODE_CRAM_REFERENCE: Final = 'cram_reference'
PARAM_CODE_FASTQS: Final = 'fastqs'
PARAM_CODE_FASTQ_LIST: Final = 'fastq_list'
PARAM_CODE_REF_TAR: Final = 'ref_tar'
PARAM_CODE_QC_CROSS_CONT_VCF: Final = 'qc_cross_cont_vcf'
PARAM_CODE_QC_COVERAGE_REGION_1: Final = 'qc_coverage_region_1'
PARAM_CODE_QC_COVERAGE_REGION_2: Final = 'qc_coverage_region_2'

# Input Parameter Codes (Settings)
PARAM_CODE_ADDITIONAL_ARGS: Final = 'additional_args'
PARAM_CODE_ENABLE_MAP_ALIGN: Final = 'enable_map_align'
PARAM_CODE_ENABLE_MAP_ALIGN_OUTPUT: Final = 'enable_map_align_output'
PARAM_CODE_OUTPUT_FORMAT: Final = 'output_format'
PARAM_CODE_ENABLE_DUPLICATE_MARKING: Final = 'enable_duplicate_marking'
PARAM_CODE_ENABLE_VARIANT_CALLER: Final = 'enable_variant_caller'
PARAM_CODE_VC_EMIT_REF_CONFIDENCE: Final = 'vc_emit_ref_confidence'
PARAM_CODE_VC_ENABLE_VCF_OUTPUT: Final = 'vc_enable_vcf_output'
PARAM_CODE_ENABLE_CNV: Final = 'enable_cnv'
PARAM_CODE_CNV_SEGMENTATION_MODE: Final = 'cnv_segmentation_mode'
PARAM_CODE_ENABLE_SV: Final = 'enable_sv'
PARAM_CODE_ENABLE_CYP2D6: Final = 'enable_cyp2d6'
PARAM_CODE_REPEAT_GENOTYPE_ENABLE: Final = 'repeat_genotype_enable'
PARAM_CODE_DRAGEN_REPORTS: Final = 'dragen_reports'
PARAM_CODE_VC_GVCF_GQ_BANDS: Final = 'vc-gvcf-gq-bands'


def _prepare_fastq_inputs(
    sg_name: str,
    fastq_csv_list_file_path: cpg_utils.Path,
    fastq_ids_path: cpg_utils.Path,
    individual_fastq_file_list_paths: cpg_utils.Path,
) -> tuple[list[AnalysisDataInput], list[AnalysisParameterInput]]:
    """
    Reads input files to find ICA FASTQ file IDs and the FASTQ list file ID,
    then constructs the specific ICA input objects for FASTQ mode.
    """
    logger.info(f'Preparing FASTQ inputs for sequencing group {sg_name}')
    fastq_file_list_id: str | None = None

    # Find the FASTQ List CSV file ID for this SG
    with fastq_csv_list_file_path.open() as fastq_list_file_handle:
        for line in fastq_list_file_handle:
            if sg_name in line:
                try:
                    fastq_file_list_id = line.split(':')[1].strip()
                    break  # Found the ID for this SG
                except IndexError:
                    logger.warning(f'Malformed line in {fastq_csv_list_file_path}: {line.strip()}')
                    continue  # Skip malformed lines

    if not fastq_file_list_id:
        # This was the original error cause
        raise ValueError(f'Could not find FASTQ List file ID for {sg_name} in {fastq_csv_list_file_path}')

    # Find the individual FASTQ file IDs for this SG
    fastq_ica_ids: list[str] = []
    try:
        with individual_fastq_file_list_paths.open() as individual_fastq_file_list_handle:
            individual_fastq_csv_df: pd.DataFrame = pd.read_csv(
                individual_fastq_file_list_handle,
                sep=',',
            )
        with fastq_ids_path.open() as fastq_ids_handle:
            fastq_ica_ids_df: pd.DataFrame = pd.read_csv(
                fastq_ids_handle,
                sep=r'\s+',  # Assumes tab or space separated ID<whitespace>Name
                header=None,  # Explicitly state no header
                names=['ica_id', 'fastq_name'],
                dtype={'ica_id': str, 'fastq_name': str},  # Ensure string type
            )

            # Filter the ICA ID dataframe based on filenames present in the SG's FASTQ list CSV
            relevant_filenames = set(individual_fastq_csv_df['Read1File'].tolist()) | set(
                individual_fastq_csv_df['Read2File'].tolist()
            )

            fastq_ica_ids = fastq_ica_ids_df[fastq_ica_ids_df['fastq_name'].isin(relevant_filenames)]['ica_id'].tolist()

            # Debugging and validation
            ic(sg_name, fastq_file_list_id, len(fastq_ica_ids))
            if not fastq_ica_ids:
                logger.warning(
                    f'No matching FASTQ file IDs found for {sg_name} using files in {individual_fastq_file_list_paths}'
                )
                # Depending on requirements, you might want to raise an error here instead

    except FileNotFoundError as e:
        logger.error(f'Required input file not found: {e}')
        raise
    except pd.errors.EmptyDataError as e:
        logger.error(f'Input CSV file is empty or invalid: {e}')
        raise
    except Exception as e:
        logger.error(f'Error processing FASTQ input files for {sg_name}: {e}')
        raise

    # Construct the ICA input objects
    fastq_data_inputs = [
        AnalysisDataInput(parameterCode=PARAM_CODE_FASTQS, dataIds=fastq_ica_ids),
        AnalysisDataInput(
            parameterCode=PARAM_CODE_FASTQ_LIST,
            dataIds=[fastq_file_list_id],  # Must be a list containing the ID
        ),
    ]
    fastq_parameter_inputs = [
        AnalysisParameterInput(
            code=PARAM_CODE_ADDITIONAL_ARGS,
            value=("--qc-coverage-reports-1 cov_report,cov_report --qc-coverage-filters-1 'mapq<1,bq<0,mapq<1,bq<0' "),
        ),
    ]

    return fastq_data_inputs, fastq_parameter_inputs


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

    specific_data_inputs: list[AnalysisDataInput] = []
    specific_parameter_inputs: list[AnalysisParameterInput] = []

    if cram_ica_fids_path:
        logger.info(f'Using CRAM input for sequencing group {sg_name}')
        with cram_ica_fids_path.open() as cram_ica_fids_handle:
            cram_ica_fids: dict[str, str] = json.load(cram_ica_fids_handle)
            if 'cram_fid' not in cram_ica_fids:
                raise ValueError(f"Missing 'cram_fid' in {cram_ica_fids_path}")

            cram_reference_id: str = config_retrieve(
                [
                    'ica',
                    'cram_references',
                    config_retrieve(['ica', 'cram_references', 'old_cram_reference']),
                ],
            )
            specific_data_inputs = [
                AnalysisDataInput(
                    parameterCode=PARAM_CODE_CRAMS,
                    dataIds=[cram_ica_fids['cram_fid']],
                ),
                AnalysisDataInput(
                    parameterCode=PARAM_CODE_CRAM_REFERENCE,
                    dataIds=[cram_reference_id],
                ),
            ]
            specific_parameter_inputs = [
                AnalysisParameterInput(
                    code=PARAM_CODE_ADDITIONAL_ARGS,
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
                    ),
                ),
            ]
    elif fastq_csv_list_file_path and fastq_ids_path and individual_fastq_file_list_paths:
        logger.info(f'Using FASTQ input for sequencing group {sg_name}')
        specific_data_inputs, specific_parameter_inputs = _prepare_fastq_inputs(
            sg_name=sg_name,
            fastq_csv_list_file_path=fastq_csv_list_file_path,
            fastq_ids_path=fastq_ids_path,
            individual_fastq_file_list_paths=individual_fastq_file_list_paths,
        )
    else:
        raise ValueError('No valid input files provided for either CRAM or FASTQ mode.')

    # Construct the common parts of the analysis body
    common_data_inputs: list[AnalysisDataInput] = [
        AnalysisDataInput(parameterCode=PARAM_CODE_REF_TAR, dataIds=[dragen_ht_id]),
        AnalysisDataInput(
            parameterCode=PARAM_CODE_QC_CROSS_CONT_VCF,
            dataIds=[qc_cross_cont_vcf_id],
        ),
        AnalysisDataInput(
            parameterCode=PARAM_CODE_QC_COVERAGE_REGION_1,
            dataIds=[qc_cov_region_1_id],
        ),
        AnalysisDataInput(
            parameterCode=PARAM_CODE_QC_COVERAGE_REGION_2,
            dataIds=[qc_cov_region_2_id],
        ),
    ]

    common_parameter_inputs: list[AnalysisParameterInput] = [
        AnalysisParameterInput(code=PARAM_CODE_ENABLE_MAP_ALIGN, value='true'),
        AnalysisParameterInput(code=PARAM_CODE_ENABLE_MAP_ALIGN_OUTPUT, value='true'),
        AnalysisParameterInput(code=PARAM_CODE_OUTPUT_FORMAT, value='CRAM'),
        AnalysisParameterInput(code=PARAM_CODE_ENABLE_DUPLICATE_MARKING, value='true'),
        AnalysisParameterInput(code=PARAM_CODE_ENABLE_VARIANT_CALLER, value='true'),
        AnalysisParameterInput(code=PARAM_CODE_VC_EMIT_REF_CONFIDENCE, value='GVCF'),
        AnalysisParameterInput(code=PARAM_CODE_VC_ENABLE_VCF_OUTPUT, value='false'),
        AnalysisParameterInput(code=PARAM_CODE_ENABLE_CNV, value='true'),
        AnalysisParameterInput(code=PARAM_CODE_CNV_SEGMENTATION_MODE, value='SLM'),
        AnalysisParameterInput(code=PARAM_CODE_ENABLE_SV, value='true'),
        AnalysisParameterInput(code=PARAM_CODE_ENABLE_CYP2D6, value='true'),
        AnalysisParameterInput(code=PARAM_CODE_REPEAT_GENOTYPE_ENABLE, value='true'),
        AnalysisParameterInput(code=PARAM_CODE_DRAGEN_REPORTS, value='false'),
        AnalysisParameterInput(code=PARAM_CODE_VC_GVCF_GQ_BANDS, value='13 20 30 40'),
    ]

    # Combine common and specific inputs/parameters
    all_data_inputs: list[AnalysisDataInput] = common_data_inputs + specific_data_inputs
    all_parameter_inputs: list[AnalysisParameterInput] = common_parameter_inputs + specific_parameter_inputs

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
            inputs=all_data_inputs,
            parameters=all_parameter_inputs,
        ),
    )
    header_params: dict[Any, Any] = {}
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
