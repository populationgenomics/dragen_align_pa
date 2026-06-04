import json
import re
import subprocess
from collections.abc import Callable
from math import ceil
from typing import TYPE_CHECKING, Any

import cpg_utils
from cloudpathlib.exceptions import NoStatError
from cpg_flow.inputs import get_multicohort
from cpg_flow.stage import Stage, StageInput
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_utils.config import config_retrieve, get_access_level, get_driver_image, output_path
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger
from metamist.graphql import gql, query

from dragen_align_pa.constants import (
    BUCKET_NAME,
    DESIGN_TO_BEDS,
    DESIGN_TO_CANONICAL,
    DRAGEN_VERSION,
)

PER_SG_STATE_SCHEMA_VERSION = 1

if TYPE_CHECKING:
    from graphql import DocumentNode


def validate_cli_path_input(path: str, arg_name: str) -> None:
    """
    Validates that a path string does not contain shell metacharacters
    to prevent potential injection vulnerabilities.
    """
    # Regex for common shell metacharacters and whitespace,
    # excluding GCS 'gs://' prefix, path slashes '/', and underscores '_'
    if re.search(r'[;&|$`(){}[\]<>*?!#\s]', path):
        logger.error(f'Invalid characters found in {arg_name}: {path}')
        raise ValueError(f'Potential unsafe characters in {arg_name}')


def delete_pipeline_id_file(pipeline_id_file: str) -> None:
    logger.info(f'Deleting the pipeline run ID file {pipeline_id_file}')
    subprocess.run(  # noqa: S603
        ['gcloud', 'storage', 'rm', pipeline_id_file],  # noqa: S607
        check=True,
    )


def calculate_needed_storage(
    cram_path: cpg_utils.Path,
) -> str:
    try:
        storage_size: int = cram_path.stat().st_size
        # Added a buffer (3GB) and increased multiplier slightly (1.2 -> 1.3)
        # Ceil ensures we get whole GiB, adding buffer helps avoid edge cases
        calculated_gb = ceil((storage_size / (1024**3)) + 3) * 1.3
        # Ensure a minimum storage request (e.g., 10GiB)
        final_storage_gb: int = max(10, ceil(calculated_gb))
        logger.info(f'Calculated storage need: {final_storage_gb}GiB for {cram_path}')
    except NoStatError as e:
        logger.error(f'Failed to get size for {cram_path}: {e}')
        final_storage_gb = 50
    return f'{final_storage_gb}Gi'


def run_subprocess_with_log(
    cmd: str | list[str],
    step_name: str,
    stdin_input: str | None = None,
    shell: bool = False,
) -> subprocess.CompletedProcess[Any]:
    """
    Runs a subprocess command with robust logging.
    Logs the command, its output, and errors if any occur.
    """
    cmd_str = cmd if isinstance(cmd, str) else ' '.join(cmd)
    executable = '/bin/bash' if shell else None
    logger.info(f'Running {step_name} command: {cmd_str}')
    try:
        process: subprocess.CompletedProcess[str] = subprocess.run(  # noqa: S603
            cmd,
            check=True,
            capture_output=True,
            text=True,
            input=stdin_input,
            shell=shell,
            executable=executable,
        )
        logger.info(f'{step_name} completed successfully.')
        if process.stdout:
            logger.info(f'{step_name} STDOUT:\n{process.stdout.strip()}')
        if process.stderr:
            logger.info(f'{step_name} STDERR:\n{process.stderr.strip()}')
        return process
    except subprocess.CalledProcessError as e:
        logger.error(f'{step_name} failed with return code {e.returncode}')
        logger.error(f'CMD: {cmd_str}')
        logger.error(f'STDOUT: {e.stdout}')
        logger.error(f'STDERR: {e.stderr}')
        raise


def _resolve_sg_canonical_design(sg: SequencingGroup) -> str:
    """Resolve one SG's canonical exome design from its metadata."""

    sequencing_library: str | None = sg.meta.get('sequencing_library')
    if not sequencing_library:
        raise RuntimeError(
            f"Sequencing group {sg.id} has no meta['sequencing_library']; cannot resolve exome design.",
        )
    if sequencing_library not in DESIGN_TO_CANONICAL:
        raise RuntimeError(
            f'Sequencing group {sg.id} has sequencing_library {sequencing_library!r} that '
            f"doesn't map to a canonical design. Add it to DESIGN_TO_CANONICAL in "
            f'dragen_align_pa.constants.',
        )
    return DESIGN_TO_CANONICAL[sequencing_library]

    # raw_values: set[str] = set()
    # for assay in sg.assays or ():
    #     sequencing_library = assay.meta.get('sequencing_library')
    #     if sequencing_library:
    #         raw_values.add(str(sequencing_library))
    # if not raw_values:
    #     raise RuntimeError(
    #         f"Sequencing group {sg.id} has no assay.meta['sequencing_library']; "
    #         f'cannot resolve exome design.',
    #     )

    # canonical: set[str] = set()
    # unmapped: set[str] = set()
    # for raw in raw_values:
    #     match = DESIGN_TO_CANONICAL.get(raw)
    #     if match is None:
    #         unmapped.add(raw)
    #     else:
    #         canonical.add(match)
    # if unmapped:
    #     raise RuntimeError(
    #         f'Sequencing group {sg.id} has unmapped sequencing_library value(s): '
    #         f'{sorted(unmapped)}. Add these to DESIGN_TO_CANONICAL in '
    #         f'dragen_align_pa.constants.',
    #     )
    # if len(canonical) != 1:
    #     raise RuntimeError(
    #         f'Sequencing group {sg.id} maps to multiple canonical designs: '
    #         f'{sorted(canonical)}.',
    #     )
    # return canonical.pop()


def get_bed_names_for_seqtype() -> dict[str, str]:
    """Read `[presets.<seqtype>.bed_names]` and return its `{key: basename}` map.

    Empty-string values raise so an un-overridden run halts before ICA
    submission. Genome runs have no bed_names block by design and return
    an empty dict.
    """
    sequencing_type = config_retrieve(['workflow', 'sequencing_type'])
    bed_names = config_retrieve(
        ['dragen_align_pa', 'manage_dragen_pipeline', 'presets', sequencing_type, 'bed_names'],
        default={},
    )

    if not bed_names:
        if sequencing_type == 'exome':
            raise ValueError(
                '[dragen_align_pa.manage_dragen_pipeline.presets.exome.bed_names] '
                'is missing or empty. Set vc_target, cnv_target, and sv_call_regions '
                'in your run config to BED basenames registered in ICA_FILE_IDS.',
            )
        return {}

    unset_entries = sorted(key for key, name in bed_names.items() if not str(name).strip())
    if unset_entries:
        raise ValueError(
            f'[dragen_align_pa.manage_dragen_pipeline.presets.{sequencing_type}.bed_names] '
            f'is missing values for {unset_entries}. Set each to a BED basename '
            f'registered in ICA_FILE_IDS.',
        )
    return {key: str(name) for key, name in bed_names.items()}


def assert_cohort_design_matches_configured_bed(cohort: Cohort) -> None:
    """Hard-fail at stage queuing if the cohort isn't a single exome design
    or the configured bed_names aren't valid for that design.

    Only runs when `workflow.sequencing_type == 'exome'`. Catches both
    design-mixed cohorts and a config TOML pointing at the wrong design's
    BEDs before any ICA submission.
    """
    if config_retrieve(['workflow', 'sequencing_type']) != 'exome':
        return

    sgs = cohort.get_sequencing_groups()
    if not sgs:
        raise RuntimeError(f'Cohort {cohort.id} has no sequencing groups.')

    designs: dict[str, str] = {sg.id: _resolve_sg_canonical_design(sg) for sg in sgs}
    unique_designs = set(designs.values())
    if len(unique_designs) != 1:
        by_design: dict[str, list[str]] = {}
        for sg_id, d in designs.items():
            by_design.setdefault(d, []).append(sg_id)
        raise RuntimeError(
            f'Cohort {cohort.id} has mixed exome designs {sorted(unique_designs)}. '
            f'Split into one cohort per design. Breakdown: {by_design}',
        )
    cohort_design = unique_designs.pop()

    valid_beds = DESIGN_TO_BEDS.get(cohort_design)
    if valid_beds is None:
        raise RuntimeError(
            f'No DESIGN_TO_BEDS entry for design {cohort_design!r}; update dragen_align_pa.constants.',
        )

    # get_bed_names_for_seqtype raises if exome bed_names is missing or has
    # any unset entries, so by the time we get here the dict is complete.
    bed_names = get_bed_names_for_seqtype()
    outside_design = sorted(set(bed_names.values()) - valid_beds)
    if outside_design:
        raise RuntimeError(
            f'Cohort {cohort.id} resolves to design {cohort_design!r}, but '
            f'[presets.exome.bed_names] uses basename(s) {outside_design} that '
            f"aren't in DESIGN_TO_BEDS[{cohort_design!r}] = "
            f'{sorted(valid_beds)}. Check the config against the cohort design.',
        )
    logger.info(
        f'Exome design check passed: cohort {cohort.id} -> {cohort_design}, beds {sorted(set(bed_names.values()))}.',
    )


def initialise_python_job(
    job_name: str,
    target: Cohort | SequencingGroup,
    tool_name: str,
) -> PythonJob:
    """
    Initialises a standard PythonJob with common attributes.
    """
    py_job: PythonJob = get_batch().new_python_job(
        name=job_name,
        attributes=(target.get_job_attrs() or {}) | {'tool': tool_name},  # pyright: ignore[reportUnknownArgumentType]
    )
    py_job.image(get_driver_image())
    return py_job


def get_prep_path(filename: str) -> cpg_utils.Path:
    """Gets a path in the 'prepare' directory."""
    return cpg_utils.to_path(output_path(f'ica/{DRAGEN_VERSION}/prepare/{filename}'))


def get_pipeline_path(filename: str) -> cpg_utils.Path:
    """Gets a path in the 'pipelines' (state) directory."""
    return cpg_utils.to_path(output_path(f'ica/{DRAGEN_VERSION}/pipelines/{filename}'))


def get_output_path(filename: str, category: str | None = None) -> cpg_utils.Path:
    """Gets a path in the final 'output' directory."""
    return cpg_utils.to_path(output_path(f'ica/{DRAGEN_VERSION}/output/{filename}', category=category))


def get_batch_artefacts_root() -> cpg_utils.Path:
    """Per-cohort artefacts root under GCS (siblings: passfail/summary/reports per batch).

    All cohorts share a `dragen_batch_metrics/` root under the cohort-scoped
    output prefix; per-cohort scoping is applied at the leaf by
    `get_batch_artefacts_path`, which prefixes the batch name with cohort_name.
    """
    return get_output_path('dragen_batch_metrics')


def get_batch_artefacts_path(cohort_name: str, batch_index: int) -> cpg_utils.Path:
    """GCS folder for this cohort's outputs. Each batch gets a subfolder containing
    `passfail.json`, `summary.json`, and a `reports/` directory.

    Note: the GCS subdirectory uses **underscore** (`{cohort}_batch{NN}`) — distinct
    from `IcaBatch.name`, which uses **hyphen** (`{cohort}-batch{NN}`) as the
    cpg-flow target identifier. The two are deliberately split: hyphen for the
    in-process target name (cpg-flow Stage identifier convention), underscore
    for the GCS path (filesystem-friendly, won't be confused with the cohort name).
    Both forms zero-pad `batch_index` to width 4.
    """
    return get_batch_artefacts_root() / f'{cohort_name}_batch{batch_index:04d}'


def get_per_sg_state_path(
    inputs: StageInput,
    sequencing_group: SequencingGroup,
    state_stage: Callable[..., Stage],
) -> tuple[Cohort, cpg_utils.Path]:
    """Look up an SG's per-SG state file via the given upstream stage's outputs.

    Returns `(cohort, state_path)` because both are needed at every call site:
    the cohort to read `inputs.as_dict`, and `cohort.name` to thread into
    downstream `resolve_and_run` / `get_ica_sample_folder` calls.
    """
    matching = [c for c in get_multicohort().get_cohorts() if sequencing_group in c.get_sequencing_groups()]
    if len(matching) != 1:
        raise ValueError(
            f'Expected sequencing group {sequencing_group.name} to belong to exactly one '
            f'cohort in the MultiCohort, found {len(matching)}.',
        )
    cohort = matching[0]
    state_path = inputs.as_dict(target=cohort, stage=state_stage)[f'{sequencing_group.name}_pipeline_id_and_arguid']
    return cohort, state_path


def load_per_sg_state(
    pipeline_id_arguid_path: cpg_utils.Path,
    required_keys: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Read + validate a per-SG state file, returning the parsed JSON dict.

    Single source of truth for the schema_version check and required-key
    validation. Callers that need specific fields pass them in `required_keys`
    so a malformed file raises `KeyError` here rather than several frames deeper.
    """
    with pipeline_id_arguid_path.open('r') as fh:
        state: dict[str, Any] = json.load(fh)
    version = state.get('schema_version', 0)
    if version != PER_SG_STATE_SCHEMA_VERSION:
        raise ValueError(
            f'Per-SG state file {pipeline_id_arguid_path} has schema_version {version}; '
            f'code expects {PER_SG_STATE_SCHEMA_VERSION}. Rerun the cohort with '
            f'force_resubmit=true (or manually delete the file) to rewrite it under '
            f'the new schema.',
        )
    missing = [key for key in required_keys if key not in state]
    if missing:
        raise KeyError(
            f'Per-SG state file {pipeline_id_arguid_path} missing required key(s): '
            f'{", ".join(repr(k) for k in missing)}.',
        )
    return state


def get_ica_sample_folder(
    pipeline_id_arguid_path: cpg_utils.Path,
    sg_name: str,
    cohort_name: str,
) -> str:
    """Resolve the ICA folder for a single SG's batch output.

    Returns `/{bucket}/{output_folder}/{cohort_name}/{user_reference}-{pipeline_id}/{sg_name}/`.

    A schema-mismatched or missing-key state file raises here rather than
    downstream — operators can recover by rerunning with `force_resubmit=true`
    or deleting the offending per-SG file so the next resume reads from
    `{cohort}_batches.json` (the authoritative source). The helper has no
    awareness of CANCELLED batches; the orchestrator's resume-after-cancel
    guard halts the cohort before any Download stage runs.
    """
    state = load_per_sg_state(
        pipeline_id_arguid_path,
        required_keys=('user_reference', 'pipeline_id', 'batch_index'),
    )
    user_reference = state['user_reference']
    pipeline_id = state['pipeline_id']
    output_folder = config_retrieve(['ica', 'data_prep', 'output_folder'])
    return f'/{BUCKET_NAME}/{output_folder}/{cohort_name}/{user_reference}-{pipeline_id}/{sg_name}/'


def get_manifest_path_for_cohort(cohort: Cohort) -> cpg_utils.Path:
    """
    Queries Metamist for the 'manifest' analysis for a given cohort
    and returns the GCS path to the manifest file.

    If access_level is 'test', it fetches the 'control' manifest.
    Otherwise, it fetches the 'production' manifest.
    """
    logger.info(f'Querying Metamist for manifest path for cohort {cohort.id}')
    access_level: str = get_access_level()
    logger.info(f'Using access level: {access_level}')

    if access_level == 'test':
        manifest_type: str = 'control'
        required_basename_str: str = 'control_manifest'
        required_dirname_str: str = 'control_manifests'
    else:
        manifest_type = 'production'
        required_basename_str = 'production_manifest'
        required_dirname_str = 'production_manifests'

    logger.info(f'Searching for {manifest_type} manifest analysis in Metamist')

    manifest_query: DocumentNode = gql(
        request_string="""
        query GetCohortManifest($cohortId: String!) {
          cohorts(id: {eq: $cohortId}) {
            id
            name
            analyses {
              id
              type
              outputs
            }
          }
        }
    """
    )

    try:
        result = query(manifest_query, variables={'cohortId': cohort.id})

        if not result.get('cohorts'):
            raise ValueError(f'No cohort found in Metamist with ID {cohort.id}')

        analyses = result['cohorts'][0].get('analyses', [])
        if not analyses:
            raise ValueError(f'No analyses found for cohort {cohort.id}')

        # 1. Filter all analyses based on all criteria
        matching_manifests = []
        for a in analyses:
            outputs = a.get('outputs')
            if (
                a.get('type') == 'manifest'
                and outputs
                and required_basename_str in outputs.get('basename', '')
                and required_dirname_str in outputs.get('dirname', '')
            ):
                matching_manifests.append(a)

        # 2. Check the number of matches
        if not matching_manifests:
            raise ValueError(
                f"No 'manifest' analysis found for cohort {cohort.id} "
                f"matching type '{manifest_type}' "
                f"(basename: '{required_basename_str}', dirname: '{required_dirname_str}')."
            )

        if len(matching_manifests) > 1:
            all_ids = [m.get('id') for m in matching_manifests]
            logger.warning(
                f"Found {len(matching_manifests)} matching '{manifest_type}' analyses for "
                f'cohort {cohort.id} (IDs: {all_ids}). '
                f'Using the first one found (ID: {matching_manifests[0].get("id")}).'
            )

        manifest_entry = matching_manifests[0]
        manifest_path = manifest_entry.get('outputs', {}).get('path')

        if not manifest_path:
            raise ValueError(
                f"Matching '{manifest_type}' analysis (ID: {manifest_entry.get('id')}) found for {cohort.id}, "
                f"but 'outputs.path' is missing or null."
                f'{manifest_entry}'
            )

        logger.info(f"Found '{manifest_type}' manifest path: {manifest_path} (Analysis ID: {manifest_entry.get('id')})")
        return cpg_utils.to_path(manifest_path)

    except Exception as e:
        logger.error(f'Failed to query/parse manifest path for cohort {cohort.id}: {e}')
        raise
