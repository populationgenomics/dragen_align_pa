"""Resolvers over the ICA registry tables in `dragen_align_pa.constants`.

`constants` holds only data (the `ICA_PROJECT_SETUP` and `ICA_FILE_IDS` tables and the
placeholder marker). The functions here are the logic that reads those tables: project /
API-key / MLR-config lookups and reference-file-ID resolution. They reference the tables via
the `constants` module (not `from constants import …`) so a test that patches
`dragen_align_pa.constants.<TABLE>` is seen here at call time.

A run operates within one dataset *family*, named by `[ica.projects].project_root`.
`ICA_PROJECT_SETUP[family]` holds everything that family needs: its per-role projects
(`projects[role]` → name + id), its API-key secret field, and its MLR config JSON file id. A
role resolves by direct indexing — there is no inference from project names.
"""

from typing import Final

from cpg_utils.config import config_retrieve

from dragen_align_pa import constants

# The three roles every family registers under `ICA_PROJECT_SETUP[family]['projects']`. Call
# sites pass these instead of literal strings so a typo is a NameError, not a silent lookup miss.
ROLE_DRAGEN_ALIGN: Final = 'dragen_align'
ROLE_DRAGEN_MLR: Final = 'dragen_mlr'
ROLE_FASTQ_UPLOAD: Final = 'fastq_upload'

REQUIRED_ICA_ROLES: Final = (ROLE_DRAGEN_ALIGN, ROLE_DRAGEN_MLR, ROLE_FASTQ_UPLOAD)


def configured_family() -> str:
    """Return the dataset family named by `[ica.projects].project_root`.

    Raises:
        cpg_utils.config.ConfigError: If `[ica.projects].project_root` is unset.
    """
    return config_retrieve(['ica', 'projects', 'project_root'])


def _family_setup(project_root: str) -> constants.IcaFamilySetup:
    """Return the full `ICA_PROJECT_SETUP` block for family `project_root`.

    Args:
        project_root: The dataset family — a top-level key of `ICA_PROJECT_SETUP`.

    Raises:
        KeyError: If `project_root` is not a registered family. The message lists the registered
            families so a `[ica.projects].project_root` typo is immediately actionable.
    """
    try:
        return constants.ICA_PROJECT_SETUP[project_root]
    except KeyError:
        raise KeyError(
            f'Unknown ICA project family {project_root!r}. Registered families: {sorted(constants.ICA_PROJECT_SETUP)}.',
        ) from None


def _project_entry(project_root: str, role: str) -> constants.IcaProject:
    """Return the `{project_name, project_id}` entry for `role` in family `project_root`.

    Args:
        project_root: The dataset family.
        role: One of `REQUIRED_ICA_ROLES`.

    Raises:
        KeyError: If `project_root` is not a registered family, or the family has no entry for
            `role`. The message lists what is registered so a config/table typo is actionable.
    """
    projects = _family_setup(project_root)['projects']
    try:
        return projects[role]
    except KeyError:
        raise KeyError(
            f'ICA project family {project_root!r} has no {role!r} project. Registered roles: {sorted(projects)}.',
        ) from None


def resolve_ica_project_name(project_root: str, role: str) -> str:
    """Return the ICA project name registered for `role` in family `project_root`.

    Args:
        project_root: The dataset family (e.g. `ourdna`).
        role: One of `REQUIRED_ICA_ROLES`.

    Returns:
        The ICA project name.

    Raises:
        KeyError: If the family or role is not registered.
    """
    return _project_entry(project_root, role)['project_name']


def resolve_ica_project_id_or_none(project_root: str, role: str) -> str | None:
    """Return the ICA project ID for `role`, or `None` if registered without one.

    A `None` id is deliberate: it marks a project whose data we can't address by ID here (a
    collaborator-managed FASTQ upload area). Callers that can act on the absence decide what
    `None` means for them; `resolve_ica_project_id` treats it as an error.

    Args:
        project_root: The dataset family.
        role: One of `REQUIRED_ICA_ROLES`.

    Raises:
        KeyError: If the family or role is not registered.
    """
    return _project_entry(project_root, role)['project_id']


def resolve_ica_project_id(project_root: str, role: str) -> str:
    """Return the ICA project ID for `role` in family `project_root`, requiring one.

    Args:
        project_root: The dataset family.
        role: One of `REQUIRED_ICA_ROLES`.

    Returns:
        The ICA project ID (UUID).

    Raises:
        KeyError: If the family or role is not registered.
        ValueError: If the role is registered with an explicit `None` id (collaborator-managed;
            use `resolve_ica_project_id_or_none`). This mirrors the placeholder-file-id path:
            both mean "registered but unusable", distinct from "not registered" (`KeyError`).
    """
    project_id = resolve_ica_project_id_or_none(project_root, role)
    if project_id is None:
        raise ValueError(
            f'{role!r} project in family {project_root!r} is registered with no ICA project ID '
            f'(an explicit None in ICA_PROJECT_SETUP); it cannot be addressed by project ID. Use '
            f'resolve_ica_project_id_or_none if a missing ID is expected here.',
        )
    return project_id


def resolve_ica_api_key_field(project_root: str) -> str:
    """Return the `illumina_cpg_workbench_api` secret field holding `project_root`'s API key.

    The secret carries one API-key field per dataset family (OurDNA's `apiKey`, tenk10k's
    `tenk10k_apiKey`, …). Selecting per family keeps one dataset's key from being used against
    another's project.

    Args:
        project_root: The dataset family.

    Returns:
        The secret field name (e.g. `apiKey`).

    Raises:
        KeyError: If `project_root` is not a registered family.
    """
    return _family_setup(project_root)['api_key']['name']


def resolve_mlr_config_file_id(project_root: str) -> str:
    """Return the ICA file ID of family `project_root`'s MLR config JSON.

    The config JSON lives in the family's MLR project; its file id is registered under
    `ICA_PROJECT_SETUP[family]['mlr_config_json']`.

    Args:
        project_root: The dataset family.

    Returns:
        The ICA file ID of that family's MLR config JSON.

    Raises:
        KeyError: If `project_root` is not a registered family.
        ValueError: If the registered value is still the `fil.TODO_…` placeholder (not yet uploaded).
    """
    file_id = _family_setup(project_root)['mlr_config_json']['ica_file_id']
    return _reject_placeholder_file_id(f'{project_root} MLR config JSON', file_id)


def resolve_ica_can_delete_fastq(project_root: str) -> bool:
    """Return whether we may delete uploaded FASTQ data for family `project_root`.

    This is the authoritative in-repo call on FASTQ-deletion authority (ICA enforces the same
    permission independently, so a `True` here still can't delete from a collaborator project).
    A family we don't control the upload area for registers `can_delete_fastq = False`, and
    `DeleteDataInIca` skips its FASTQ deletion.

    Args:
        project_root: The dataset family.

    Returns:
        `True` if the pipeline may attempt FASTQ deletion for this family, else `False`.

    Raises:
        KeyError: If `project_root` is not a registered family.
    """
    return _family_setup(project_root)['can_delete_fastq']


def ica_project_name(role: str) -> str:
    """Resolve the ICA project name for `role` from the configured dataset family."""
    return resolve_ica_project_name(configured_family(), role)


def ica_project_id(role: str) -> str:
    """Resolve the ICA project ID for `role` from the configured dataset family (requires one)."""
    return resolve_ica_project_id(configured_family(), role)


def ica_api_key_field() -> str:
    """Return the secret API-key field for the configured dataset family."""
    return resolve_ica_api_key_field(configured_family())


def ica_mlr_config_file_id() -> str:
    """Return the MLR config JSON file id for the configured dataset family."""
    return resolve_mlr_config_file_id(configured_family())


def ica_can_delete_fastq() -> bool:
    """Return whether the configured dataset family permits FASTQ deletion."""
    return resolve_ica_can_delete_fastq(configured_family())


def _reject_placeholder_file_id(name: str, file_id: str) -> str:
    """Return `file_id`, unless it's still the not-yet-minted `_TODO_FID` sentinel.

    A placeholder means the entry is registered but the file hasn't been uploaded to ICA yet,
    which would otherwise surface as an opaque ICA "no such file" failure mid-run.

    Raises:
        ValueError: If `file_id` starts with the `fil.TODO_…` placeholder prefix.
    """
    if file_id.startswith(constants.TODO_FID_PREFIX):
        raise ValueError(
            f'ICA file ID for {name!r} has not been registered yet — the entry still holds the '
            f'placeholder {file_id!r}. Upload the file to ICA and replace the placeholder with '
            f'the real fil.… ID before running.',
        )
    return file_id


def resolve_ica_file_id(name: str) -> str:
    """Look up the ICA file ID for a registered reference-asset basename (BED / VCF / genome).

    Args:
        name: The registered basename to resolve (from `ICA_FILE_IDS`).

    Returns:
        The ICA `fil.…`/`fol.…` ID for `name`.

    Raises:
        KeyError: If `name` is not registered. The message lists the registered basenames, so a
            config typo surfaces at submitter startup with an immediately actionable message.
        ValueError: If the registered value is still the `fil.TODO_…` placeholder (not yet uploaded).
    """
    try:
        file_id = constants.ICA_FILE_IDS[name]
    except KeyError:
        raise KeyError(
            f'{name!r} is not a registered ICA file basename. '
            f'Add it to ICA_FILE_IDS in dragen_align_pa.constants, or check for typos. '
            f'Registered names: {sorted(constants.ICA_FILE_IDS)}',
        ) from None
    return _reject_placeholder_file_id(name, file_id)


# Reserved key in an ICA_PON_FILE_IDS panel entry holding the `fil.…` ID of the
# `<panel>.normals.txt` list file (all other keys are per-SG count basenames).
_PON_LIST_KEY = 'pon_list_file'


def resolve_cnv_normals_panel(panel_name: str) -> tuple[str, list[str]]:
    """Resolve a registered CNV panel-of-normals to its list basename and file IDs.

    Args:
        panel_name: The panel key registered in `ICA_PON_FILE_IDS`.

    Returns:
        A `(normals_list_basename, file_ids)` tuple: the basename DRAGEN reads via
        `--cnv-normals-list` (`<panel>.normals.txt`, derived from `panel_name`
        because the builder always names it that way), and every ICA file ID in
        the panel (the per-SG count files plus the list file) to pass as analysis
        data inputs.

    Raises:
        KeyError: If `panel_name` is not registered. The message lists the
            registered panels so a config typo surfaces at submitter startup.
        ValueError: If the panel has no `pon_list_file` entry, or if any registered
            file ID is still a `fil.TODO_…` placeholder.
    """
    try:
        panel = constants.ICA_PON_FILE_IDS[panel_name]
    except KeyError:
        raise KeyError(
            f'{panel_name!r} is not a registered CNV panel of normals. '
            f'Add it to ICA_PON_FILE_IDS in dragen_align_pa.constants (via '
            f'scripts/build_cnv_panel_of_normals.py), or check for typos. '
            f'Registered panels: {sorted(constants.ICA_PON_FILE_IDS)}',
        ) from None

    if _PON_LIST_KEY not in panel:
        raise ValueError(
            f'CNV panel {panel_name!r} has no {_PON_LIST_KEY!r} entry naming its normals-list '
            f'file ID. Rebuild it with scripts/build_cnv_panel_of_normals.py.',
        )

    file_ids = [_reject_placeholder_file_id(name, file_id) for name, file_id in panel.items()]
    return f'{panel_name}.normals.txt', file_ids
