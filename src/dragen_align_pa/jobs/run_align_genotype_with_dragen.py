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
from loguru import logger

from dragen_align_pa import ica_api_utils


def _prepare_fastq_inputs(
    sg_name: str,
    fastq_csv_list_file_path: cpg_utils.Path,
    fastq_ids_path: cpg_utils.Path,
    individual_fastq_file_list_paths: cpg_utils.Path,
    qc_cov_region_1_id: str,
    qc_cov_region_2_id: str,
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
    fastq_data_inputs: list[AnalysisDataInput] = [
        AnalysisDataInput(parameterCode='fastqs', dataIds=fastq_ica_ids),
        AnalysisDataInput(
            parameterCode='fastq_list',
            dataIds=[fastq_file_list_id],  # Must be a list containing the ID
        ),
        AnalysisDataInput(
            parameterCode='qc_coverage_region_beds',
            dataIds=[qc_cov_region_1_id, qc_cov_region_2_id],
        ),
    ]
    fastq_parameter_inputs: list[AnalysisParameterInput] = [
        AnalysisParameterInput(
            code='additional_args',
            value=("--qc-coverage-reports-1 cov_report,cov_report --qc-coverage-filters-1 'mapq<1,bq<0,mapq<1,bq<0' "),
        ),
    ]

    return fastq_data_inputs, fastq_parameter_inputs


def _build_cram_specific_inputs(
    cram_ica_fids_path: cpg_utils.Path,
    qc_cov_region_1_id: str,
    qc_cov_region_2_id: str,
) -> tuple[list[AnalysisDataInput], list[AnalysisParameterInput]]:
    """Builds the specific ICA input objects for CRAM mode."""
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
        specific_data_inputs: list[AnalysisDataInput] = [
            AnalysisDataInput(
                parameterCode='crams',
                dataIds=[cram_ica_fids['cram_fid']],
            ),
            AnalysisDataInput(
                parameterCode='cram_reference',
                dataIds=[cram_reference_id],
            ),
            AnalysisDataInput(
                parameterCode='qc_coverage_region_1',
                dataIds=[qc_cov_region_1_id],
            ),
            AnalysisDataInput(
                parameterCode='qc_coverage_region_2',
                dataIds=[qc_cov_region_2_id],
            ),
        ]
        specific_parameter_inputs: list[AnalysisParameterInput] = [
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
                ),
            ),
        ]
    return specific_data_inputs, specific_parameter_inputs


def _build_common_parameters() -> list[AnalysisParameterInput]:
    """Builds the common ICA parameter inputs."""
    return [
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
        AnalysisParameterInput(code='vc_gvcf_gq_bands', value='13 20 30 40'),
    ]


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

    common_data_inputs: list[AnalysisDataInput] = [
        AnalysisDataInput(parameterCode='ref_tar', dataIds=[dragen_ht_id]),
        AnalysisDataInput(
            parameterCode='qc_cross_cont_vcf',
            dataIds=[qc_cross_cont_vcf_id],
        ),
    ]
    common_parameter_inputs: list[AnalysisParameterInput] = _build_common_parameters()

    specific_data_inputs: list[AnalysisDataInput]
    specific_parameter_inputs: list[AnalysisParameterInput]

    if cram_ica_fids_path:
        logger.info(f'Using CRAM input for sequencing group {sg_name}')
        specific_data_inputs, specific_parameter_inputs = _build_cram_specific_inputs(
            cram_ica_fids_path=cram_ica_fids_path,
            qc_cov_region_1_id=qc_cov_region_1_id,
            qc_cov_region_2_id=qc_cov_region_2_id,
        )
    elif fastq_csv_list_file_path and fastq_ids_path and individual_fastq_file_list_paths:
        logger.info(f'Using FASTQ input for sequencing group {sg_name}')
        specific_data_inputs, specific_parameter_inputs = _prepare_fastq_inputs(
            sg_name=sg_name,
            fastq_csv_list_file_path=fastq_csv_list_file_path,
            fastq_ids_path=fastq_ids_path,
            individual_fastq_file_list_paths=individual_fastq_file_list_paths,
            qc_cov_region_1_id=qc_cov_region_1_id,
            qc_cov_region_2_id=qc_cov_region_2_id,
        )
    else:
        raise ValueError('No valid input files provided for either CRAM or FASTQ mode.')

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
    """
    Main entrypoint for the PythonJob.
    This function authenticates with ICA, reads necessary file IDs from GCS,
    and calls `submit_dragen_run` to launch the pipeline in ICA.

    Args:
        cram_ica_fids_path: Path to GCS JSON holding CRAM file ID (CRAM mode).
        fastq_csv_list_file_path: Path to GCS file holding FASTQ list CSV
                                  file IDs (FASTQ mode).
        fastq_ids_path: Path to GCS file holding all FASTQ file IDs (FASTQ mode).
        individual_fastq_file_list_paths: Path to GCS JSON holding paths to
                                          per-SG FASTQ lists (FASTQ mode).
        analysis_output_fid_path: Path to GCS JSON holding the ICA *folder ID*
                                  for pipeline outputs.
        sg_name: The name of the sequencing group being processed.

    Returns:
        The ICA analysis run ID (a string).
    """

    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    project_id: str = secrets['projectID']

    with analysis_output_fid_path.open() as analysis_outputs_fid_handle:
        analysis_output_fid: dict[str, str] = json.load(analysis_outputs_fid_handle)

    with ica_api_utils.get_ica_api_client() as api_client:
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
