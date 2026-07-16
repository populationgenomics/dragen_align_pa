"""Tests for the resolvers in constants_registry over the constants tables.

The registries map human-readable names (BED basenames, ICA project names) to
ICA IDs / fields so config can be written in terms the user understands, and the
submitter does one indirection to recover the ICA value. The data tables live in
`constants`; the resolvers here read them. Tests that need to substitute a table
patch it on `dragen_align_pa.constants` (where the data lives) — the resolvers
read it off that module at call time.
"""

import pytest

from dragen_align_pa import constants_registry


def test_resolve_ica_file_id_returns_registered_id(monkeypatch):
    """Known basename mapped to a real fil.… ID → returns that ID."""
    monkeypatch.setattr(
        'dragen_align_pa.constants.ICA_FILE_IDS',
        {'real.bed': 'fil.0123456789abcdef'},
    )
    assert constants_registry.resolve_ica_file_id('real.bed') == 'fil.0123456789abcdef'


def test_resolve_ica_file_id_raises_on_unknown_name():
    """Unknown basename → clear error naming the unknown entry."""
    unknown = 'this_bed_is_not_registered.bed'
    with pytest.raises(KeyError, match=r'this_bed_is_not_registered\.bed'):
        constants_registry.resolve_ica_file_id(unknown)


def test_resolve_ica_file_id_rejects_placeholder_id(monkeypatch):
    """A registered basename whose value still starts with `fil.TODO_` is a
    sentinel for "no ICA upload yet" — submitting it to ICA produces an
    opaque "no such file" failure mid-run. resolve_ica_file_id must instead
    raise at submitter startup with a clear actionable message."""
    monkeypatch.setattr(
        'dragen_align_pa.constants.ICA_FILE_IDS',
        {'staged_but_not_uploaded.bed': 'fil.TODO_REPLACE_AFTER_ICA_UPLOAD'},
    )
    with pytest.raises(ValueError, match=r'staged_but_not_uploaded\.bed'):
        constants_registry.resolve_ica_file_id('staged_but_not_uploaded.bed')


def test_resolve_mlr_config_file_id_returns_registered_id():
    """The MLR config JSON is registered per family in ICA_PROJECT_SETUP, separate from the
    reference-asset basenames in ICA_FILE_IDS."""
    assert constants_registry.resolve_mlr_config_file_id('ourdna') == 'fil.91c3e63114fc43dc31ed08dde927d6b4'


def test_resolve_mlr_config_file_id_raises_on_unknown_family():
    """Unknown family → error naming the family (not 'not a registered BED basename')."""
    with pytest.raises(KeyError, match=r'no-such-family'):
        constants_registry.resolve_mlr_config_file_id('no-such-family')


def test_resolve_mlr_config_file_id_rejects_placeholder():
    """tenk10k's MLR config JSON is the _TODO_FID sentinel until minted — fail loud."""
    with pytest.raises(ValueError, match=r'tenk10k'):
        constants_registry.resolve_mlr_config_file_id('tenk10k')


def test_resolve_ica_project_name_by_role():
    """A role resolves to its registered project name by direct (family, role) indexing."""
    assert (
        constants_registry.resolve_ica_project_name('ourdna', constants_registry.ROLE_DRAGEN_ALIGN)
        == 'OurDNA-DRAGEN-378'
    )
    assert (
        constants_registry.resolve_ica_project_name('tenk10k', constants_registry.ROLE_DRAGEN_MLR)
        == 'Tenk10K_Dragen_MLR_Jobs'
    )
    assert (
        constants_registry.resolve_ica_project_name('tenk10k', constants_registry.ROLE_FASTQ_UPLOAD)
        == 'tenk10k_fastq_upload'
    )


def test_resolve_ica_project_name_unknown_family_raises():
    with pytest.raises(KeyError, match=r'no-such-family'):
        constants_registry.resolve_ica_project_name('no-such-family', constants_registry.ROLE_DRAGEN_ALIGN)


def test_resolve_ica_project_name_unknown_role_raises():
    """A role the family doesn't register fails loud (no name inference from project strings)."""
    with pytest.raises(KeyError, match=r'no-such-role'):
        constants_registry.resolve_ica_project_name('ourdna', 'no-such-role')


def test_resolve_ica_project_id_by_role():
    assert (
        constants_registry.resolve_ica_project_id('ourdna', constants_registry.ROLE_DRAGEN_ALIGN)
        == '5c3a60b0-1458-4e37-8877-ec6b25dc4003'
    )
    assert (
        constants_registry.resolve_ica_project_id('tenk10k', constants_registry.ROLE_DRAGEN_MLR)
        == '16bb091c-5866-4e39-929f-2b678457b772'
    )


def test_resolve_ica_project_id_raises_on_none_id():
    """tenk10k's fastq-upload is registered with an explicit None id (collaborator-managed data);
    resolve_ica_project_id fails loud rather than returning None into its `str` contract."""
    with pytest.raises(KeyError, match=r'fastq-upload'):
        constants_registry.resolve_ica_project_id('tenk10k', constants_registry.ROLE_FASTQ_UPLOAD)


def test_resolve_ica_project_id_or_none_returns_none_for_collaborator_project():
    """The tolerant accessor returns the explicit None (the delete path relies on this to
    distinguish 'skip, collaborator-managed' from a misconfiguration)."""
    assert constants_registry.resolve_ica_project_id_or_none('tenk10k', constants_registry.ROLE_FASTQ_UPLOAD) is None
    assert (
        constants_registry.resolve_ica_project_id_or_none('ourdna', constants_registry.ROLE_DRAGEN_ALIGN)
        == '5c3a60b0-1458-4e37-8877-ec6b25dc4003'
    )


def test_resolve_ica_api_key_field_selects_by_family():
    """OurDNA uses the base `apiKey`; tenk10k its own field. Selection is by dataset family."""
    assert constants_registry.resolve_ica_api_key_field('ourdna') == 'apiKey'
    assert constants_registry.resolve_ica_api_key_field('tenk10k') == 'tenk10k_apiKey'


def test_resolve_ica_api_key_field_raises_on_unregistered_family():
    """An unregistered family fails loud rather than silently reusing another dataset's key."""
    with pytest.raises(KeyError, match=r'newdataset'):
        constants_registry.resolve_ica_api_key_field('newdataset')


def test_resolve_ica_can_delete_fastq_by_family():
    """ourdna owns its upload area (True); tenk10k is collaborator-managed (False)."""
    assert constants_registry.resolve_ica_can_delete_fastq('ourdna') is True
    assert constants_registry.resolve_ica_can_delete_fastq('tenk10k') is False


def test_config_reading_entry_points_use_configured_family():
    """The bare `ica_*` entry points read `[ica.projects].project_root` (conftest: 'ourdna')."""
    assert constants_registry.ica_project_name(constants_registry.ROLE_DRAGEN_ALIGN) == 'OurDNA-DRAGEN-378'
    mlr_id = constants_registry.ica_project_id(constants_registry.ROLE_DRAGEN_MLR)
    assert mlr_id == 'f2f55709-f8d4-4364-bb04-c41975d4c0ed'
    assert constants_registry.ica_api_key_field() == 'apiKey'
    assert constants_registry.ica_mlr_config_file_id() == 'fil.91c3e63114fc43dc31ed08dde927d6b4'
    assert constants_registry.ica_can_delete_fastq() is True
