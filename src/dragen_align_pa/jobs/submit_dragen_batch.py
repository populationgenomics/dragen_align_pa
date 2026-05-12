"""Submit one batch of SGs to the unified DRAGEN ICA pipeline.

This replaces the per-SG submission logic in the old `run_align_genotype_with_dragen.py`.
The new pipeline (`DRAGEN378-custom-unified-F2-v1`, id 18a4baab-…) accepts a list of
samples per analysis and is configured via top-level parameters + an `additional_args`
string. Per-sample retry is orchestrated by the caller, not here.
"""

import io
import json
import re

import cpg_utils
import pandas as pd
import requests
from cpg_utils.config import config_retrieve, try_get_ar_guid
from icasdk.apis.tags import project_analysis_api, project_data_api
from icasdk.model.analysis_data_input import AnalysisDataInput
from icasdk.model.analysis_parameter_input import AnalysisParameterInput
from icasdk.model.analysis_tag import AnalysisTag
from icasdk.model.create_nextflow_analysis import CreateNextflowAnalysis
from icasdk.model.nextflow_analysis_input import NextflowAnalysisInput
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.batches import Batch
from dragen_align_pa.constants import BUCKET_NAME

# DRAGEN flags that don't depend on input type (CRAM vs FASTQ) or sequencing type (WGS vs WES).
# Sourced from the production CRAM-mode preset in the legacy submitter — anything WGS/WES-divergent
# is instead carried in [dragen_align_pa.manage_dragen_pipeline.presets.{genome,exome}] in config.
_COMMON_ADDITIONAL_ARGS = (
    "--read-trimmers polyg "
    "--soft-read-trimmers none "
    "--vc-hard-filter 'DRAGENHardQUAL:all:QUAL<5.0;LowDepth:all:DP<=1' "
    '--vc-frd-max-effective-depth 40 '
    '--vc-enable-joint-detection true '
    '--qc-coverage-ignore-overlaps true '
    '--qc-coverage-count-soft-clipped-bases true '
    '--qc-coverage-reports-1 cov_report,cov_report '
    "--qc-coverage-filters-1 'mapq<1,bq<0,mapq<1,bq<0' "
    '--vc-gvcf-gq-bands 10 20 30 40 '
    '--vc-emit-ref-confidence GVCF '
    '--vc-enable-vcf-output false '
    '--enable-map-align-output true '
    '--enable-duplicate-marking true '
    '--enable-cyp2d6 true '
    '--repeat-genotype-enable true '
)


_PRESET_PLACEHOLDER_RE = re.compile(r'<[a-zA-Z][a-zA-Z0-9_-]*>')


def _build_additional_args() -> str:
    """Concatenate common + sequencing-type preset + user override into one args string.

    Raises if any `<placeholder>` sentinel survives in the final string (e.g. the
    WES preset shipping `<bed-name>` defaults that weren't filled in for this run).
    """
    sequencing_type = config_retrieve(['workflow', 'sequencing_type'])
    if sequencing_type not in {'genome', 'exome'}:
        raise ValueError(
            f"workflow.sequencing_type must be 'genome' or 'exome', got {sequencing_type!r}",
        )
    preset = config_retrieve(['dragen_align_pa', 'manage_dragen_pipeline', 'presets', sequencing_type], default=None)
    if preset is None:
        raise ValueError(
            f'Missing config section [dragen_align_pa.manage_dragen_pipeline.presets.{sequencing_type}]; add it to your TOML.',
        )
    user = config_retrieve(['dragen_align_pa', 'manage_dragen_pipeline', 'user'], default={'additional_args': '', 'additional_files': []})

    # Join with explicit spaces. Empty parts are filtered out before the join so
    # we don't end up with double spaces when a preset or user override is "".
    parts = [
        _COMMON_ADDITIONAL_ARGS.strip(),
        f"--cnv-segmentation-mode {preset['cnv_segmentation_mode']}",
        preset.get('additional_args', '').strip(),
        user.get('additional_args', '').strip(),
    ]
    assembled = ' '.join(part for part in parts if part)

    placeholders = _PRESET_PLACEHOLDER_RE.findall(assembled)
    if placeholders:
        raise ValueError(
            f"DRAGEN additional_args contains unfilled placeholders {placeholders} from "
            f"[dragen_align_pa.manage_dragen_pipeline.presets.{sequencing_type}]. Fill them in your config before running.",
        )
    return assembled


def _build_top_level_parameters(error_strategy: str = 'auto') -> list[AnalysisParameterInput]:
    """Top-level pipeline parameters sent on every run.

    `error_strategy` is overridable so the orchestrator can pass `continue` for
    single-sample retry batches (where the default `auto` would terminate).
    """
    return [
        AnalysisParameterInput(code='enable_map_align', value='true'),
        AnalysisParameterInput(code='output_format', value='CRAM'),
        AnalysisParameterInput(code='enable_variant_caller', value='true'),
        AnalysisParameterInput(code='enable_sv', value='true'),
        AnalysisParameterInput(code='enable_cnv', value='true'),
        AnalysisParameterInput(code='dragen_reports', value='false'),
        AnalysisParameterInput(code='error_strategy', value=error_strategy),
        AnalysisParameterInput(code='additional_args', value=_build_additional_args()),
    ]


def _build_cram_data_inputs(
    batch: Batch,
    per_sg_state_paths: dict[str, cpg_utils.Path],
) -> tuple[list[AnalysisDataInput], list[str]]:
    """Construct ICA data inputs for a CRAM-mode batch.

    `per_sg_state_paths[sg_name]` points at `{sg}_fids.json` (output of UploadDataToIca),
    each containing `{'cram_fid': 'fil.…'}`. We pass the list of CRAM file IDs in one batch.

    Returns `(data_inputs, cram_fids)` so the caller can persist `cram_fids` into the
    BatchesFile entry for audit / future cleanup.
    """
    cram_fids: list[str] = []
    for sg_name in batch.sg_names:
        state_path = per_sg_state_paths[sg_name]
        with state_path.open('r') as fh:
            sg_state = json.load(fh)
        if 'cram_fid' not in sg_state:
            raise ValueError(f"Missing 'cram_fid' in {state_path}")
        cram_fids.append(sg_state['cram_fid'])

    # Resolve the configured CRAM-reference folder ID. Two-step lookup matches today's
    # convention: `ica.cram_references.old_cram_reference` points at a key in
    # `[ica.cram_references]` (e.g. "dragmap" or "gatk") whose value is the folder ID.
    selected_ref: str | None = config_retrieve(
        ['ica', 'cram_references', 'old_cram_reference'], default=None,
    )
    if not selected_ref:
        raise ValueError(
            'Config missing ica.cram_references.old_cram_reference — cannot select a CRAM '
            'reference folder for batch submission. Set it to the name of an entry under '
            '[ica.cram_references] (e.g. "dragmap" or "gatk").',
        )
    cram_reference_id: str | None = config_retrieve(
        ['ica', 'cram_references', selected_ref], default=None,
    )
    if not cram_reference_id:
        raise ValueError(
            f'Config ica.cram_references.{selected_ref} is unset — '
            f'add the ICA folder ID for the {selected_ref!r} CRAM reference.',
        )

    return (
        [
            AnalysisDataInput(parameterCode='crams', dataIds=cram_fids),
            AnalysisDataInput(parameterCode='cram_reference', dataIds=[cram_reference_id]),
        ],
        cram_fids,
    )


def _read_fastq_ids(fastq_ids_path: cpg_utils.Path) -> pd.DataFrame:
    """Reads `{cohort}_fastq_ids.txt` (two whitespace-separated columns: ICA id, FASTQ name)."""
    with fastq_ids_path.open() as fh:
        return pd.read_csv(
            fh,
            sep=r'\s+',
            header=None,
            names=['ica_id', 'fastq_name'],
            dtype={'ica_id': str, 'fastq_name': str},
        )


def _load_per_sg_fastq_lists(
    sg_names: list[str],
    per_sg_fastq_list_paths: dict[str, cpg_utils.Path],
) -> tuple[list[str], pd.DataFrame]:
    """Loads per-SG FASTQ list CSVs (output of MakeFastqFileList) and returns:
    - the concatenated DataFrame (one CSV per batch),
    - the union of all FASTQ filenames referenced across those CSVs.

    Raises:
        ValueError: if any per-SG CSV has a different column header than the
                    first one (`pd.concat` would otherwise silently fill the
                    missing columns with NaN and produce a malformed batch CSV).
        ValueError: if a SG's CSV has zero data rows — the batch would silently
                    omit that SG's reads and surface only as a passfail Fail
                    much later.
    """
    fastq_filenames: set[str] = set()
    frames: list[pd.DataFrame] = []
    expected_columns: list[str] | None = None
    required_columns = {'Read1File', 'Read2File'}
    for sg_name in sg_names:
        path = per_sg_fastq_list_paths[sg_name]
        with path.open() as fh:
            df = pd.read_csv(fh)
        if expected_columns is None:
            expected_columns = list(df.columns)
            # Schema sanity-check on the first CSV: if MakeFastqFileList ever
            # renames its required columns, this surfaces immediately rather
            # than as a `KeyError` on `df['Read1File']` later.
            missing_required = required_columns - set(expected_columns)
            if missing_required:
                raise ValueError(
                    f'FASTQ list for SG {sg_name} at {path} is missing required '
                    f'columns {missing_required} (got {expected_columns}). '
                    f'MakeFastqFileList must emit Read1File / Read2File.',
                )
        elif list(df.columns) != expected_columns:
            raise ValueError(
                f'FASTQ list header mismatch for SG {sg_name} in {path}: '
                f'expected {expected_columns}, got {list(df.columns)}. '
                f'All per-SG CSVs must share the same column shape.',
            )
        if df.empty:
            raise ValueError(
                f'FASTQ list for SG {sg_name} at {path} has zero data rows; '
                f'the combined batch CSV would silently omit this SG.',
            )
        frames.append(df)
        fastq_filenames.update(df['Read1File'].tolist())
        fastq_filenames.update(df['Read2File'].tolist())
    combined = pd.concat(frames, ignore_index=True)
    return sorted(fastq_filenames), combined


def _upload_per_batch_fastq_list(
    api_instance: project_data_api.ProjectDataApi,
    project_id: str,
    cohort_name: str,
    batch_index: int,
    combined_csv: pd.DataFrame,
) -> str:
    """Materialise the per-batch FASTQ list CSV in-memory and upload it to ICA.

    Filename pattern: `{cohort}_batch{NN}_fastq_list.csv`.
    Returns the ICA file ID of the uploaded CSV.
    """
    file_name = f'{cohort_name}_batch{batch_index:04d}_fastq_list.csv'
    folder_path = f'/{BUCKET_NAME}/{config_retrieve(["ica", "data_prep", "output_folder"])}/{cohort_name}'

    file_id, file_status = ica_utils.create_upload_object_id(
        api_instance=api_instance,
        path_params={'projectId': project_id},
        sg_name=cohort_name,
        file_name=file_name,
        folder_path=folder_path,
        object_type='FILE',
    )

    if file_status == 'AVAILABLE':
        logger.info(f"FASTQ list {file_name} is 'AVAILABLE'. Skipping upload.")
        return file_id

    upload_url: str = api_instance.create_upload_url_for_data(
        path_params={'projectId': project_id, 'dataId': file_id},
    ).body['url']

    buffer = io.BytesIO()
    combined_csv.to_csv(buffer, index=False)
    buffer.seek(0)

    response = requests.put(url=upload_url, data=buffer, timeout=300)
    response.raise_for_status()
    logger.info(f'Uploaded per-batch FASTQ list {file_name} (file ID {file_id})')
    return file_id


def _build_fastq_data_inputs(
    api_instance: project_data_api.ProjectDataApi,
    project_id: str,
    batch: Batch,
    fastq_ids_path: cpg_utils.Path,
    per_sg_fastq_list_paths: dict[str, cpg_utils.Path],
) -> tuple[list[AnalysisDataInput], str]:
    """Construct ICA data inputs for a FASTQ-mode batch.

    Returns (data_inputs, fastq_list_fid). The per-batch combined CSV is uploaded inline.
    """
    sg_fastq_names, combined_csv = _load_per_sg_fastq_lists(batch.sg_names, per_sg_fastq_list_paths)
    fastq_ids_df = _read_fastq_ids(fastq_ids_path)
    matched = fastq_ids_df[fastq_ids_df['fastq_name'].isin(sg_fastq_names)]
    fastq_ica_ids = matched['ica_id'].tolist()
    if len(fastq_ica_ids) != len(sg_fastq_names):
        missing = set(sg_fastq_names) - set(matched['fastq_name'])
        raise ValueError(
            f'Mismatch in FASTQ IDs for batch {batch.name}: expected {len(sg_fastq_names)}, '
            f'found {len(fastq_ica_ids)}. Missing: {missing}',
        )

    fastq_list_fid = _upload_per_batch_fastq_list(
        api_instance=api_instance,
        project_id=project_id,
        cohort_name=batch.cohort_name,
        batch_index=batch.batch_index,
        combined_csv=combined_csv,
    )

    return (
        [
            AnalysisDataInput(parameterCode='fastqs', dataIds=fastq_ica_ids),
            AnalysisDataInput(parameterCode='fastq_list', dataIds=[fastq_list_fid]),
        ],
        fastq_list_fid,
    )
