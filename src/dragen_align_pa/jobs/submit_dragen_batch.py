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
