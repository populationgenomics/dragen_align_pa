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

from dragen_align_pa.constants import DESIGN_TO_BEDS, DESIGN_TO_CANONICAL
from dragen_align_pa.constants_registry import (
    REQUIRED_ICA_ROLES,
    resolve_ica_api_key_field,
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
            projects cover every required role.
        RuntimeError: If any cohort's exome design doesn't match the configured BEDs.
    """
    assert_ica_project_root_resolves()
    for cohort in get_multicohort().get_cohorts():
        assert_cohort_design_matches_configured_bed(cohort)


def assert_ica_project_root_resolves() -> None:
    """Fail loud at submit if `[ica.projects].project_root` is misconfigured.

    Config names only the dataset family; this confirms, purely from the registry tables (no
    ICA calls), that the family is registered and complete — so a family mistake aborts on the
    submitter rather than surfacing at the first ICA call deep in a job. It checks that:

    - every required role (DRAGEN-align, DRAGEN-MLR, FASTQ-upload) resolves to a project;
    - the family has a registered `illumina_cpg_workbench_api` API-key field (else every ICA
      client build would fail at job runtime with a bare `KeyError`);
    - the family's MLR project has a minted MLR config JSON (not the `fil.TODO_` placeholder),
      which the MLR submission job would otherwise only discover mid-run.

    Raises:
        KeyError: If `project_root` isn't a registered family, a required role is missing, or
            the family has no registered API-key field.
        ValueError: If a role entry is missing its project name, or the MLR config JSON is still
            the not-yet-minted placeholder.
    """
    project_root = config_retrieve(['ica', 'projects', 'project_root'])
    for role in REQUIRED_ICA_ROLES:
        resolve_ica_project_name(project_root, role)
    resolve_ica_api_key_field(project_root)
    resolve_mlr_config_file_id(project_root)


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
            f'dragen_align_pa.constants.',
        )
    return DESIGN_TO_CANONICAL[sequencing_library]


def assert_cohort_design_matches_configured_bed(cohort: Cohort) -> None:
    """Hard-fail at submit time if the cohort isn't a single exome design or the configured
    BEDs aren't valid for that design.

    Only acts when `workflow.sequencing_type == 'exome'`; genome runs return immediately.
    Catches both design-mixed cohorts and a config TOML pointing at the wrong design's BEDs
    before any ICA submission.

    Args:
        cohort: The cohort whose sequencing groups' designs are checked against
            `[presets.exome.bed_names]`.

    Raises:
        RuntimeError: If the cohort has no sequencing groups, mixes exome designs, resolves
            to a design absent from `DESIGN_TO_BEDS`, or the configured BEDs fall outside the
            resolved design's valid set.
        ValueError: If `get_bed_names_for_seqtype` rejects a missing or empty exome
            bed_names block.
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
