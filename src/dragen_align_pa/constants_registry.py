"""Resolvers over the ICA registry tables in `dragen_align_pa.constants`.

`constants` holds only data (the `ICA_PROJECT_IDS` / `ICA_FILE_IDS` /
`MLR_CONFIG_FILE_ID_BY_PROJECT` / `ICA_API_KEY_FIELD_BY_FAMILY` tables and the
placeholder markers). The functions here are the logic that reads those tables:
project/family/API-key lookups and file-ID resolution. They reference the tables
via the `constants` module (not `from constants import …`) so a test that patches
`dragen_align_pa.constants.<TABLE>` is seen here at call time.
"""

from cpg_utils.config import config_retrieve

from dragen_align_pa import constants


def _registered_project_names() -> list[str]:
    """Flat, sorted list of every registered ICA project name across all families."""
    return sorted(name for projects in constants.ICA_PROJECT_IDS.values() for name in projects)


def ica_project_id_or_none(name: str) -> str | None:
    """Return a registered ICA project's ID, or `None` if it was registered without one.

    A `None` entry is deliberate: it marks a project whose data we can't address by ID here
    (e.g. a collaborator-managed FASTQ upload area). Callers that can act on the absence
    decide what `None` means for them; `resolve_ica_project_id` treats it as an error.

    Args:
        name: A registered ICA project name (a `[ica.projects]` value).

    Returns:
        The ICA project ID, or `None` for a project registered without one.

    Raises:
        KeyError: If `name` is not registered under any family. The message lists the
            registered names, so a config typo surfaces with an immediately actionable message.
    """
    for projects in constants.ICA_PROJECT_IDS.values():
        if name in projects:
            return projects[name]
    raise KeyError(f'Unknown ICA project name: {name}. Registered names: {_registered_project_names()}')


def resolve_ica_project_id(name: str) -> str:
    """Look up the ICA project ID for a registered project name that has one.

    Args:
        name: The registered ICA project name to resolve.

    Returns:
        The ICA project ID (UUID) registered for `name`.

    Raises:
        KeyError: If `name` is not registered, or is registered with an explicit `None` ID (a
            collaborator-managed project that can't be addressed by ID). Callers that tolerate
            a missing ID should use `ica_project_id_or_none` instead.
    """
    project_id = ica_project_id_or_none(name)
    if project_id is None:
        raise KeyError(
            f'ICA project {name!r} is registered with no project ID (an explicit None in '
            f'ICA_PROJECT_IDS); it cannot be addressed by project ID. Use ica_project_id_or_none '
            f'if a missing ID is expected here.',
        )
    return project_id


def project_family(project_name: str) -> str:
    """Return the dataset family an ICA project belongs to.

    The family is the top-level key under which `project_name` is registered in
    `ICA_PROJECT_IDS` — an explicit grouping, not inferred from the project name. It keys
    both the DRAGEN/MLR project-match guard and API-key selection, so making it explicit
    keeps a project whose name merely shares a prefix with another dataset from being
    misclassified into that dataset's key.

    Args:
        project_name: A registered ICA project name (a `[ica.projects]` value).

    Returns:
        The dataset family (e.g. `ourdna`, `tenk10k`).

    Raises:
        KeyError: If `project_name` is not registered under any family in ICA_PROJECT_IDS.
    """
    for family, projects in constants.ICA_PROJECT_IDS.items():
        if project_name in projects:
            return family
    raise KeyError(f'Unknown ICA project name: {project_name}. Registered names: {_registered_project_names()}')


def resolve_ica_api_key_field(project_name: str) -> str:
    """Return the secret field holding the ICA API key for a project's dataset.

    The `illumina_cpg_workbench_api` secret carries one API key field per dataset (OurDNA's
    `apiKey`, tenk10k's `tenk10k_apiKey`, …); a project's dataset family is looked up in
    `ICA_PROJECT_IDS` (see `project_family`). Choosing per project keeps one dataset's key
    from being used against another dataset's project.

    Args:
        project_name: The ICA project name to authenticate against.

    Returns:
        The secret field name (e.g. `apiKey`) to read the API key from.

    Raises:
        KeyError: If the project's family has no registered key field. The message names the
            family and lists the registered ones, so onboarding a dataset fails loud with an
            actionable message rather than silently reusing another dataset's key.
    """
    family = project_family(project_name)
    try:
        return constants.ICA_API_KEY_FIELD_BY_FAMILY[family]
    except KeyError as e:
        raise KeyError(
            f'No ICA API key field registered for dataset family {family!r} (from project '
            f'{project_name!r}). Add it to ICA_API_KEY_FIELD_BY_FAMILY in dragen_align_pa.constants. '
            f'Registered families: {sorted(constants.ICA_API_KEY_FIELD_BY_FAMILY)}.',
        ) from e


def _project_role(project_name: str) -> str | None:
    """Classify an ICA project name into its `[ica.projects]` role, or `None` if it fits none.

    Follows the ICA_PROJECT_IDS naming convention within a family: the MLR project's name
    contains `mlr`; the FASTQ upload project's contains `upload`; the remaining DRAGEN
    alignment project contains `dragen` (e.g. `…-dragen-378`). Matched on a case- and
    separator-normalised name, so `Tenk10K_Dragen_MLR_Jobs` and `ourdna-dragen-mlr-jobs`
    both classify as `dragen_mlr`.
    """
    normalised = project_name.lower().replace('_', '-')
    if 'mlr' in normalised:
        return 'dragen_mlr'
    if 'upload' in normalised:
        return 'fastq_source_project'
    if 'dragen' in normalised:
        return 'dragen_align'
    return None


def resolve_ica_project_name(project_root: str, role: str) -> str:
    """Return the ICA project name for `role` within dataset family `project_root`.

    Config names only the family (`[ica.projects].project_root`); the specific project for a
    role (`dragen_align` / `dragen_mlr` / `fastq_source_project`) is derived by classifying the
    family's registered project names (see `_project_role`).

    Args:
        project_root: The dataset family — a top-level key of `ICA_PROJECT_IDS` (e.g. `ourdna`).
        role: The `[ica.projects]` role to resolve.

    Returns:
        The ICA project name registered for `role` in `project_root`.

    Raises:
        KeyError: If `project_root` is not a registered family.
        ValueError: If the family's names don't yield exactly one project for `role` (a naming
            convention violation in ICA_PROJECT_IDS).
    """
    try:
        projects = constants.ICA_PROJECT_IDS[project_root]
    except KeyError as e:
        raise KeyError(
            f'Unknown ICA project family {project_root!r}. Registered families: {sorted(constants.ICA_PROJECT_IDS)}.',
        ) from e
    matches = [name for name in projects if _project_role(name) == role]
    if len(matches) != 1:
        raise ValueError(
            f'Expected exactly one {role!r} project in family {project_root!r}, found {matches}. '
            f'Check ICA_PROJECT_IDS naming (MLR name contains "mlr", upload contains "upload", '
            f'DRAGEN align contains "dragen").',
        )
    return matches[0]


def ica_project_name(role: str) -> str:
    """Resolve the ICA project name for `role` from the configured dataset family.

    Reads `[ica.projects].project_root` and derives the role's project via
    `resolve_ica_project_name`. This is the single entry point call sites use in place of the
    old per-role `[ica.projects][role]` config keys.

    Args:
        role: The `[ica.projects]` role (`dragen_align` / `dragen_mlr` / `fastq_source_project`).

    Returns:
        The ICA project name for `role`.

    Raises:
        cpg_utils.config.ConfigError: If `[ica.projects].project_root` is unset.
        KeyError / ValueError: As raised by `resolve_ica_project_name`.
    """
    return resolve_ica_project_name(config_retrieve(['ica', 'projects', 'project_root']), role)


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


def resolve_mlr_config_file_id(project_name: str) -> str:
    """Look up the MLR config-JSON ICA file ID for the MLR project MLR runs in.

    The config JSON lives in the MLR project, so this is keyed by the `[ica.projects].dragen_mlr`
    project name (via `MLR_CONFIG_FILE_ID_BY_PROJECT`) — separate from the reference-asset
    basenames in `ICA_FILE_IDS`.

    Args:
        project_name: The MLR ICA project name (an `[ica.projects].dragen_mlr` value).

    Returns:
        The ICA file ID of that project's MLR config JSON.

    Raises:
        KeyError: If no config JSON is registered for `project_name`. The message lists the
            registered MLR projects.
        ValueError: If the registered value is still the `fil.TODO_…` placeholder (not yet uploaded).
    """
    try:
        file_id = constants.MLR_CONFIG_FILE_ID_BY_PROJECT[project_name]
    except KeyError:
        raise KeyError(
            f'No MLR config JSON registered for MLR project {project_name!r}. '
            f'Add it to MLR_CONFIG_FILE_ID_BY_PROJECT in dragen_align_pa.constants. '
            f'Registered MLR projects: {sorted(constants.MLR_CONFIG_FILE_ID_BY_PROJECT)}',
        ) from None
    return _reject_placeholder_file_id(project_name, file_id)
