import os
from collections.abc import Callable, Sequence
from functools import partial
from typing import NamedTuple

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve
from loguru import logger

from dragen_align_pa import ica_cli_utils, popgen_mlr
from dragen_align_pa.constants.constants_registry import (
    ROLE_DRAGEN_ALIGN,
    ROLE_DRAGEN_MLR,
    ica_mlr_config_file_id,
)
from dragen_align_pa.constants.ica_constants import (
    ANALYSIS_INSTANCE_TIER,
    MLR_HASH_TABLE_RELPATH,
)
from dragen_align_pa.ica_utils import ica_run_path
from dragen_align_pa.jobs.ica_pipeline_manager import manage_ica_pipeline_loop
from dragen_align_pa.paths import IcaPath
from dragen_align_pa.utils import load_per_sg_state


class MlrInputs(NamedTuple):
    """Resolved ICA URLs one SG's MLR submission needs."""

    cram_url: str
    gvcf_url: str
    output_folder_url: str


# Shared with the manage_ica_pipeline_loop call in run(): the pending-set check and
# the loop's own submit condition must read the same marker file.
_MLR_PIPELINE_ID_KEY_TEMPLATE = '{target_name}_mlr_pipeline_id'


# Mirrors the loop's own condition (no pipeline-id file, or force_resubmit
# deleting them all) so prefetch covers the SGs that get submitted; the on-demand
# fallback in run() handles any divergence, so this is an optimisation hint, not
# a correctness invariant. Cancellation submits nothing, so nothing is prefetched.
def _pending_sg_names(sg_names: Sequence[str], outputs: dict[str, cpg_utils.Path]) -> list[str]:
    """Names of SGs the loop is expected to submit this run."""
    if config_retrieve(['ica', 'management', 'cancel_cohort_run'], default=False):
        return []
    if config_retrieve(['ica', 'management', 'force_resubmit'], default=False):
        return list(sg_names)
    return [
        name
        for name in sg_names
        if not outputs[_MLR_PIPELINE_ID_KEY_TEMPLATE.format(target_name=name)].exists()
    ]


def _resolve_mlr_inputs(
    sg_name: str,
    cohort_name: str,
    pipeline_id_arguid_path_dict: dict[str, cpg_utils.Path],
) -> MlrInputs:
    """Resolve one SG's CRAM/gVCF/output URLs with a single ICA list call.

    Assumes the icav2 CLI has entered the DRAGEN align project.
    """
    state = load_per_sg_state(
        pipeline_id_arguid_path_dict[f'{sg_name}_pipeline_id_and_arguid'],
        required_keys=('pipeline_id', 'user_reference'),
        expected_cohort_name=cohort_name,
    )
    # One IcaPath for this SG's folder feeds both the REST folder form (input
    # lookup) and the ica:// URL form (pipeline output).
    sample_path = ica_run_path(cohort_name, state['user_reference'], state['pipeline_id']) / sg_name
    found = ica_cli_utils.find_ica_file_paths_by_names(
        sample_path.as_folder(),
        [f'{sg_name}.cram', f'{sg_name}.hard-filtered.gvcf.gz'],
    )
    # The CRAM and gVCF live in the dragen_align project, resolved via [ica.projects].
    return MlrInputs(
        cram_url=IcaPath.from_relpath(found[f'{sg_name}.cram']).as_url(ROLE_DRAGEN_ALIGN),
        gvcf_url=IcaPath.from_relpath(found[f'{sg_name}.hard-filtered.gvcf.gz']).as_url(ROLE_DRAGEN_ALIGN),
        output_folder_url=sample_path.as_url(ROLE_DRAGEN_ALIGN),
    )


# Runs before anything is submitted, so bad inputs fail the cohort with zero
# analyses launched — and all failing SGs are reported in one pass rather than one
# fix-and-rerun cycle each. (A submission failure inside the loop can still abort
# mid-cohort; this guarantee covers input resolution only.)
def _prefetch_mlr_inputs(
    pending: Sequence[str],
    cohort_name: str,
    pipeline_id_arguid_path_dict: dict[str, cpg_utils.Path],
) -> dict[str, MlrInputs]:
    """Resolve every pending SG's inputs with one auth and one list call per SG.

    Raises:
        ValueError: If any SG's inputs cannot be resolved, naming every affected SG.
    """
    ica_cli_utils.authenticate_ica_cli(ROLE_DRAGEN_ALIGN)

    inputs_by_sg: dict[str, MlrInputs] = {}
    failures: dict[str, str] = {}
    for sg_name in pending:
        try:
            inputs_by_sg[sg_name] = _resolve_mlr_inputs(sg_name, cohort_name, pipeline_id_arguid_path_dict)
        except (ValueError, KeyError) as exc:
            # Per-SG data problems (missing files, malformed state) are collected;
            # infrastructure errors (CLI failures) propagate immediately.
            failures[sg_name] = str(exc)
    if failures:
        summary = '; '.join(f'{sg_name}: {message}' for sg_name, message in failures.items())
        raise ValueError(
            f'MLR input resolution failed for {len(failures)}/{len(pending)} SGs '
            f'(nothing submitted): {summary}',
        )
    return inputs_by_sg


def _mlr_download_config(mlr_config_json_fid: str, local_tmp_dir: str) -> str:
    """Downloads the MLR config JSON to a local temp path."""
    local_config_path = os.path.join(local_tmp_dir, 'mlr_config.json')
    if not os.path.exists(local_config_path):
        ica_cli_utils.download_file_by_id(mlr_config_json_fid, local_config_path)
    return local_config_path


def _mlr_submit_argv(
    local_config_path: str,
    analysis_json_dir: str,
    run_id: str,
    sample_id: str,
    mlr_hash_table: str,
    output_folder_url: str,
    cram_url: str,
    gvcf_url: str,
) -> list[str]:
    """Builds the argv for `popgen_mlr.submit_analysis` (the flags `dragen-mlr submit` takes)."""
    return [
        '--input-project-config-file-path',
        local_config_path,
        # Required by the wheel's parser and mkdir'd by its check_args, but nothing
        # writes to it since the analysis id is returned directly — point it at the
        # batch tmp dir so it doesn't litter the CWD with one empty dir per SG.
        '--output-analysis-json-folder-path',
        analysis_json_dir,
        '--run-id',
        run_id,
        '--sample-id',
        sample_id,
        '--input-ht-folder-url',
        mlr_hash_table,
        '--output-folder-url',
        output_folder_url,
        '--input-align-file-url',
        cram_url,
        '--input-gvcf-file-url',
        gvcf_url,
        '--analysis-instance-tier',
        ANALYSIS_INSTANCE_TIER,
    ]


# Tags exist purely for GUI filtering — ICA deliberately exposes no API to read
# them back. The ar_guid is NOT tagged; it is already part of the ICA output path.
def _mlr_analysis_tags() -> dict[str, list[str]]:
    """ICA analysis tags for MLR submissions: `[ica.tags]` plus an `mlr` technical marker."""
    return {
        'technicalTags': [*config_retrieve(['ica', 'tags', 'technical_tags']), 'mlr'],
        'userTags': config_retrieve(['ica', 'tags', 'user_tags']),
        'referenceTags': config_retrieve(['ica', 'tags', 'reference_tags']),
    }


def _submit_mlr_run(
    sg_name: str,
    resolve_inputs: Callable[[str], MlrInputs],
    mlr_config_path: Callable[[], str],
    mlr_hash_table: str,
    tags: dict[str, list[str]],
) -> str:
    """Submits one SG's DRAGEN MLR analysis from its resolved inputs."""
    inputs = resolve_inputs(sg_name)
    local_config_path = mlr_config_path()
    argv = _mlr_submit_argv(
        local_config_path=local_config_path,
        analysis_json_dir=os.path.join(os.path.dirname(local_config_path), 'mlr_analysis_json'),
        run_id=f'{sg_name}-mlr',
        sample_id=sg_name,
        mlr_hash_table=mlr_hash_table,
        output_folder_url=inputs.output_folder_url,
        cram_url=inputs.cram_url,
        gvcf_url=inputs.gvcf_url,
    )
    mlr_analysis_id = popgen_mlr.submit_analysis(argv, tags=tags)
    logger.info(f'MLR pipeline ID for {sg_name} is {mlr_analysis_id}')
    return mlr_analysis_id


# All once-per-run ICA work happens up front: one align-project auth covering
# every input lookup, one MLR-project auth covering the config download, and
# the config download itself. The per-SG submit callables then only build an
# argv and POST the analysis (popgen_cli authenticates from the config JSON,
# not the icav2 CLI session).
def run(
    cohort: Cohort,
    pipeline_id_arguid_path_dict: dict[str, cpg_utils.Path],
    outputs: dict[str, cpg_utils.Path],
) -> None:
    """Calls the generic pipeline manager with settings for the MLR pipeline."""
    mlr_hash_table: str = IcaPath.from_relpath(MLR_HASH_TABLE_RELPATH).as_url(ROLE_DRAGEN_MLR)
    mlr_tags = _mlr_analysis_tags()
    sg_names = [sg.name for sg in cohort.get_sequencing_groups()]

    inputs_by_sg: dict[str, MlrInputs] = {}
    mlr_config_cache: dict[str, str] = {}

    def _resolve_with_fallback(sg_name: str) -> MlrInputs:
        # The pending set is an optimisation hint, not a correctness invariant: if
        # the loop submits an SG the prefetch didn't cover (a divergent marker-file
        # state, or a future allow_retry=True resubmission), resolve it on demand
        # instead of dying on a bare KeyError.
        if sg_name not in inputs_by_sg:
            logger.warning(f'{sg_name} was not prefetched; resolving its MLR inputs on demand.')
            ica_cli_utils.authenticate_ica_cli(ROLE_DRAGEN_ALIGN)
            inputs_by_sg[sg_name] = _resolve_mlr_inputs(sg_name, cohort.name, pipeline_id_arguid_path_dict)
        return inputs_by_sg[sg_name]

    def _mlr_config_path() -> str:
        # Lazy so the on-demand fallback works even when nothing was prefetched;
        # cached so the MLR-project auth (only needed for this icav2 download) and
        # the download itself still happen once per run.
        if 'path' not in mlr_config_cache:
            batch_tmpdir = os.environ.get('BATCH_TMPDIR')
            if not batch_tmpdir:
                raise ValueError(
                    'BATCH_TMPDIR is not set — the MLR manager only runs as a Hail '
                    'Batch job and needs it for the config download.',
                )
            ica_cli_utils.authenticate_ica_cli(ROLE_DRAGEN_MLR)
            mlr_config_cache['path'] = _mlr_download_config(ica_mlr_config_file_id(), batch_tmpdir)
        return mlr_config_cache['path']

    pending = _pending_sg_names(sg_names, outputs)
    if pending:
        inputs_by_sg.update(_prefetch_mlr_inputs(pending, cohort.name, pipeline_id_arguid_path_dict))
        _mlr_config_path()

    def _create_submit_callable(sg_name: str) -> Callable[[], str]:
        """Creates a zero-argument callable for pipeline submission."""
        return partial(
            _submit_mlr_run,
            sg_name=sg_name,
            resolve_inputs=_resolve_with_fallback,
            mlr_config_path=_mlr_config_path,
            mlr_hash_table=mlr_hash_table,
            tags=mlr_tags,
        )

    manage_ica_pipeline_loop(
        targets_to_process=cohort.get_sequencing_groups(),
        outputs=outputs,
        pipeline_name='MLR',
        is_mlr_pipeline=True,
        success_file_key_template='{target_name}_mlr_success',
        pipeline_id_file_key_template=_MLR_PIPELINE_ID_KEY_TEMPLATE,
        error_log_key=f'{cohort.name}_mlr_errors',
        submit_function_factory=_create_submit_callable,
        allow_retry=False,
        sleep_time_seconds=330,
        # Zero-tolerance (loop default): any FAILED_FINAL aborts the cohort (the 5%-rate gate was removed branch-wide).
        raise_on_failed_final=True,
    )
