"""Submit-time configuration guards.

Every check here runs on the submitter machine, from `run_workflow.cli_main` before
`run_workflow` hands the stage graph to the executor. A violation aborts the submission
before any job is queued — so no data is created in, or shipped to, the wrong ICA project
under a misconfiguration. Keep per-job / executor-side logic out; this module is only for
preconditions that must fail fast at submit time.
"""

from cpg_flow.inputs import get_multicohort
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_utils.config import config_retrieve
from loguru import logger

from dragen_align_pa.constants.ica_constants import DESIGN_TO_BEDS, DESIGN_TO_CANONICAL
from dragen_align_pa.constants.constants_registry import (
    REQUIRED_ICA_ROLES,
    ROLE_DRAGEN_ALIGN,
    ROLE_DRAGEN_MLR,
    ROLE_FASTQ_UPLOAD,
    configured_family,
    resolve_ica_api_key_field,
    resolve_ica_can_delete_fastq,
    resolve_ica_file_id,
    resolve_ica_project_id,
    resolve_ica_project_name,
    resolve_mlr_config_file_id,
)
from dragen_align_pa.utils import get_bed_names_for_seqtype


def validate_configuration() -> None:
    """Run every submit-time guard, raising on the first violation.

    Called from `run_workflow.cli_main` before `run_workflow`, so all guards execute on the
    submitter machine before anything is sent to the executor.

    Raises:
        KeyError / ValueError: If `[ica.projects].project_root` isn't a registered family whose
            projects cover every required role, or if more than one `[ica.management]` flag is set.
        RuntimeError: If `[workflow].input_cohorts` doesn't name exactly one cohort, or if any
            cohort's exome design doesn't match the configured BEDs.
    """
    assert_single_input_cohort()
    assert_management_flags_exclusive()
    assert_ica_project_root_resolves()
    for cohort in get_multicohort().get_cohorts():
        assert_cohort_design_matches_configured_bed(cohort)


# Pure config validation (no I/O), so it belongs on the submitter: a flag conflict
# aborts before any PythonJob boots, and one check covers the DRAGEN, MLR, and MD5
# managers. `manage_dragen_pipeline._handle_management_flags` and the shared loop's
# force_resubmit+cancel guard remain as executor-side defence for direct callers.
def assert_management_flags_exclusive() -> None:
    """Fail loud at submit if more than one `[ica.management]` flag is set.

    Raises:
        ValueError: If more than one of `force_resubmit` / `monitor_previous` /
            `cancel_cohort_run` / `force_retry` is true.
    """
    active_flags = [
        name
        for name in ('force_resubmit', 'monitor_previous', 'cancel_cohort_run', 'force_retry')
        if config_retrieve(['ica', 'management', name], default=False)
    ]
    if len(active_flags) > 1:
        raise ValueError(
            f'[ica.management] flags {active_flags} are mutually exclusive — set at most one of '
            f'force_resubmit / monitor_previous / cancel_cohort_run / force_retry. '
            f'force_resubmit starts a fresh submission; monitor_previous resumes monitoring; '
            f'cancel_cohort_run aborts in-flight runs; force_retry reconciles against ICA and '
            f'reruns genuine failures.',
        )
    logger.info(f'Management-flag check passed: {active_flags or "none set"}.')


# Every GCS state file is written under a prefix keyed on the single configured cohort id
# (`utils.single_input_cohort_id`), which `get_prep_path` / `get_pipeline_path` read without
# re-checking. A second cohort in one run would write both cohorts' state into the first
# cohort's prefix, so the two would clobber each other's per-SG pointers.
def assert_single_input_cohort() -> None:
    """Fail loud at submit unless `[workflow].input_cohorts` names exactly one cohort.

    Raises:
        RuntimeError: If `[workflow].input_cohorts` is missing, empty, or holds more than one id.
    """
    input_cohorts: list[str] = config_retrieve(['workflow', 'input_cohorts'], default=[])
    if len(input_cohorts) != 1:
        raise RuntimeError(
            f'[workflow].input_cohorts must name exactly one cohort, got {input_cohorts!r}. '
            f'This pipeline scopes its GCS state by cohort id and submits one cohort per run; '
            f'launch a separate run per cohort.',
        )
    logger.info(f'Single-cohort check passed: {input_cohorts[0]}.')


def assert_ica_project_root_resolves() -> None:
    """Fail loud at submit if `[ica.projects].project_root` is misconfigured.

    Confirms, purely from the registry tables (no ICA calls), that the configured
    family is registered and complete for every required role — so a family mistake
    aborts on the submitter rather than at the first ICA call deep in a job.

    Raises:
        KeyError: If `project_root` isn't a registered family, or a required role /
            project name / project id / API-key field / `can_delete_fastq` flag is missing.
        ValueError: If the MLR config JSON is still the not-yet-minted placeholder, or the
            family sets `can_delete_fastq = True` but its FASTQ-upload project has no id.
    """
    project_root = configured_family()
    for role in REQUIRED_ICA_ROLES:
        resolve_ica_project_name(project_root, role)
    # DRAGEN-align/MLR are addressed by id at runtime, so their ids must resolve now;
    # FASTQ-upload's id is legitimately absent for a collaborator-managed family and is
    # only required when the family claims deletion authority (below).
    resolve_ica_project_id(project_root, ROLE_DRAGEN_ALIGN)
    resolve_ica_project_id(project_root, ROLE_DRAGEN_MLR)
    resolve_ica_api_key_field(project_root)
    resolve_mlr_config_file_id(project_root)
    if resolve_ica_can_delete_fastq(project_root):
        resolve_ica_project_id(project_root, ROLE_FASTQ_UPLOAD)


def _resolve_sg_canonical_design(sg: SequencingGroup) -> str:
    """Resolve one SG's canonical exome design from its metadata.

    Args:
        sg: The sequencing group whose `meta['sequencing_library']` names its capture design.

    Returns:
        The canonical design (a `CANONICAL_DESIGN_*` value) the SG's library maps to.

    Raises:
        RuntimeError: If the SG has no `sequencing_library` metadata, or its value isn't
            registered in `DESIGN_TO_CANONICAL`.
    """
    sequencing_library: str | None = sg.meta.get('sequencing_library')
    if not sequencing_library:
        raise RuntimeError(
            f"Sequencing group {sg.id} has no meta['sequencing_library']; cannot resolve exome design.",
        )
    if sequencing_library not in DESIGN_TO_CANONICAL:
        raise RuntimeError(
            f'Sequencing group {sg.id} has sequencing_library {sequencing_library!r} that '
            f"doesn't map to a canonical design. Add it to DESIGN_TO_CANONICAL in "
            f'dragen_align_pa.constants.ica_constants.',
        )
    return DESIGN_TO_CANONICAL[sequencing_library]


def assert_cohort_design_matches_configured_bed(cohort: Cohort) -> None:
    """Hard-fail at submit if an exome cohort mixes designs or its configured BEDs
    don't match the resolved design. Genome runs (non-exome) return immediately.

    Raises:
        RuntimeError: If the cohort has no sequencing groups, mixes exome designs, resolves
            to a design absent from `DESIGN_TO_BEDS`, or the configured BEDs fall outside the
            resolved design's valid set.
        ValueError: If `get_bed_names_for_seqtype` rejects a missing or empty exome
            bed_names block, or a configured BED's registered ID is still a placeholder.
        KeyError: If a configured BED isn't registered in the running family's
            `FAMILY_FILE_IDS` table.
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
            f'No DESIGN_TO_BEDS entry for design {cohort_design!r}; update dragen_align_pa.constants.ica_constants.',
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

    # A design-valid BED must also be minted in the running family's ICA domain. tenk10k runs
    # WGS only and registers no exome BEDs, so a tenk10k exome config fails here at submit
    # rather than per-batch inside the submitter.
    project_root = configured_family()
    for bed_name in sorted(set(bed_names.values())):
        resolve_ica_file_id(project_root, bed_name)
    logger.info(
        f'Exome design check passed: cohort {cohort.id} -> {cohort_design}, beds {sorted(set(bed_names.values()))}.',
    )
