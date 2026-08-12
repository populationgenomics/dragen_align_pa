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


def _pending_sg_names(sg_names: Sequence[str], outputs: dict[str, cpg_utils.Path]) -> list[str]:
    """Names of SGs the loop will actually submit this run.

    Mirrors the loop's own condition (no pipeline-id file, or force_resubmit
    deleting them all) so prefetch covers exactly the SGs that get submitted.
    Cancellation submits nothing, so nothing is prefetched.
    """
    if config_retrieve(['ica', 'management', 'cancel_cohort_run'], default=False):
        return []
    if config_retrieve(['ica', 'management', 'force_resubmit'], default=False):
        return list(sg_names)
    return [name for name in sg_names if not outputs[f'{name}_mlr_pipeline_id'].exists()]


def _prefetch_mlr_inputs(
    pending: Sequence[str],
    cohort_name: str,
    pipeline_id_arguid_path_dict: dict[str, cpg_utils.Path],
) -> dict[str, MlrInputs]:
    """Resolve every pending SG's CRAM/gVCF/output URLs with one auth and one list call per SG.

    Runs before anything is submitted, so a missing input fails the cohort with
    zero analyses launched (previously a mid-cohort lookup failure aborted with
    earlier SGs already running).
    """
    ica_cli_utils.authenticate_ica_cli(ROLE_DRAGEN_ALIGN)

    inputs_by_sg: dict[str, MlrInputs] = {}
    for sg_name in pending:
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
        inputs_by_sg[sg_name] = MlrInputs(
            cram_url=IcaPath.from_relpath(found[f'{sg_name}.cram']).as_url(ROLE_DRAGEN_ALIGN),
            gvcf_url=IcaPath.from_relpath(found[f'{sg_name}.hard-filtered.gvcf.gz']).as_url(ROLE_DRAGEN_ALIGN),
            output_folder_url=sample_path.as_url(ROLE_DRAGEN_ALIGN),
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
        '--output-analysis-json-folder-path',
        sample_id,
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


def _submit_mlr_run(
    sg_name: str,
    inputs_by_sg: dict[str, MlrInputs],
    local_config_path: str,
    mlr_hash_table: str,
) -> str:
    """Submits one SG's DRAGEN MLR analysis from its prefetched inputs."""
    # KeyError here means the loop tried to submit an SG the prefetch didn't
    # cover — a bug in the pending-set mirror, worth failing loudly on.
    inputs = inputs_by_sg[sg_name]
    argv = _mlr_submit_argv(
        local_config_path=local_config_path,
        run_id=f'{sg_name}-mlr',
        sample_id=sg_name,
        mlr_hash_table=mlr_hash_table,
        output_folder_url=inputs.output_folder_url,
        cram_url=inputs.cram_url,
        gvcf_url=inputs.gvcf_url,
    )
    mlr_analysis_id = popgen_mlr.submit_analysis(argv)
    logger.info(f'MLR pipeline ID for {sg_name} is {mlr_analysis_id}')
    return mlr_analysis_id


def run(
    cohort: Cohort,
    pipeline_id_arguid_path_dict: dict[str, cpg_utils.Path],
    outputs: dict[str, cpg_utils.Path],
) -> None:
    """
    Calls the generic pipeline manager with settings for the MLR pipeline.

    All once-per-run ICA work happens up front: one align-project auth covering
    every input lookup, one MLR-project auth covering the config download, and
    the config download itself. The per-SG submit callables then only build an
    argv and POST the analysis (popgen_cli authenticates from the config JSON,
    not the icav2 CLI session).
    """
    mlr_hash_table: str = IcaPath.from_relpath(MLR_HASH_TABLE_RELPATH).as_url(ROLE_DRAGEN_MLR)
    sg_names = [sg.name for sg in cohort.get_sequencing_groups()]

    pending = _pending_sg_names(sg_names, outputs)
    inputs_by_sg: dict[str, MlrInputs] = {}
    local_config_path = ''
    if pending:
        inputs_by_sg = _prefetch_mlr_inputs(pending, cohort.name, pipeline_id_arguid_path_dict)
        # The MLR project context is only needed by the icav2 config download.
        ica_cli_utils.authenticate_ica_cli(ROLE_DRAGEN_MLR)
        local_config_path = _mlr_download_config(
            ica_mlr_config_file_id(),
            os.environ.get('BATCH_TMPDIR', '/io'),
        )

    def _create_submit_callable(sg_name: str) -> Callable[[], str]:
        """Creates a zero-argument callable for pipeline submission."""
        # Bind the dict, not inputs_by_sg[sg_name]: the loop calls this factory
        # for EVERY unfinished target each poll cycle, including already-submitted
        # SGs that were never prefetched.
        return partial(
            _submit_mlr_run,
            sg_name=sg_name,
            inputs_by_sg=inputs_by_sg,
            local_config_path=local_config_path,
            mlr_hash_table=mlr_hash_table,
        )

    manage_ica_pipeline_loop(
        targets_to_process=cohort.get_sequencing_groups(),
        outputs=outputs,
        pipeline_name='MLR',
        is_mlr_pipeline=True,
        success_file_key_template='{target_name}_mlr_success',
        pipeline_id_file_key_template='{target_name}_mlr_pipeline_id',
        error_log_key=f'{cohort.name}_mlr_errors',
        submit_function_factory=_create_submit_callable,
        allow_retry=False,
        sleep_time_seconds=330,
        # Zero-tolerance (loop default): any FAILED_FINAL aborts the cohort (the 5%-rate gate was removed branch-wide).
        raise_on_failed_final=True,
    )
