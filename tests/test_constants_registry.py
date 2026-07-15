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
    """The MLR config JSON is keyed by MLR project name in its own map, separate from the
    reference-asset basenames in ICA_FILE_IDS."""
    resolved = constants_registry.resolve_mlr_config_file_id('ourdna-dragen-mlr-jobs')
    assert resolved == 'fil.91c3e63114fc43dc31ed08dde927d6b4'


def test_resolve_mlr_config_file_id_raises_on_unknown_project():
    """Unknown MLR project → error naming the project (not 'not a registered BED basename')."""
    with pytest.raises(KeyError, match=r'no-such-mlr-project'):
        constants_registry.resolve_mlr_config_file_id('no-such-mlr-project')


def test_resolve_mlr_config_file_id_rejects_placeholder():
    """tenk10k's MLR config JSON is the _TODO_FID sentinel until minted — fail loud."""
    with pytest.raises(ValueError, match=r'Tenk10K_Dragen_MLR_Jobs'):
        constants_registry.resolve_mlr_config_file_id('Tenk10K_Dragen_MLR_Jobs')


def test_project_family_looks_up_registered_family():
    """Family is the explicit ICA_PROJECT_IDS grouping, not a name prefix — every project
    of a dataset (DRAGEN, MLR, upload) resolves to the same family regardless of casing."""
    assert constants_registry.project_family('OurDNA-DRAGEN-378') == 'ourdna'
    assert constants_registry.project_family('ourdna-dragen-mlr-jobs') == 'ourdna'
    assert constants_registry.project_family('Tenk10k_Dragen_378') == 'tenk10k'
    assert constants_registry.project_family('Tenk10K_Dragen_MLR_Jobs') == 'tenk10k'


def test_project_family_raises_on_unregistered_project():
    """An unregistered project name fails loud rather than being silently classified by
    prefix into some dataset's family (and thus its API key)."""
    with pytest.raises(KeyError, match=r'not-a-real-project'):
        constants_registry.project_family('not-a-real-project')


def test_resolve_ica_project_id_resolves_across_families():
    """Names are registered under a family; the resolver searches all families."""
    assert constants_registry.resolve_ica_project_id('OurDNA-DRAGEN-378') == '5c3a60b0-1458-4e37-8877-ec6b25dc4003'
    mlr_id = constants_registry.resolve_ica_project_id('Tenk10K_Dragen_MLR_Jobs')
    assert mlr_id == '16bb091c-5866-4e39-929f-2b678457b772'


def test_resolve_ica_project_id_raises_on_unknown_name():
    with pytest.raises(KeyError, match=r'nope-not-registered'):
        constants_registry.resolve_ica_project_id('nope-not-registered')


def test_resolve_ica_project_id_raises_on_none_id():
    """A project registered with an explicit None ID (collaborator-managed data, e.g.
    tenk10k_fastq_upload) can't be addressed by ID here — resolve fails loud rather than
    returning None into its `str` contract."""
    with pytest.raises(KeyError, match=r'tenk10k_fastq_upload'):
        constants_registry.resolve_ica_project_id('tenk10k_fastq_upload')


def test_ica_project_id_or_none_returns_none_for_collaborator_project():
    """The tolerant accessor returns the explicit None (the delete path relies on this to
    distinguish 'skip, collaborator-managed' from 'unregistered typo')."""
    assert constants_registry.ica_project_id_or_none('tenk10k_fastq_upload') is None
    assert constants_registry.ica_project_id_or_none('OurDNA-DRAGEN-378') == '5c3a60b0-1458-4e37-8877-ec6b25dc4003'


def test_ica_project_id_or_none_raises_on_unregistered():
    with pytest.raises(KeyError, match=r'nope-not-registered'):
        constants_registry.ica_project_id_or_none('nope-not-registered')


def test_resolve_ica_api_key_field_selects_by_family():
    """OurDNA uses the base `apiKey`; tenk10k its own field. Selection is by the project's
    dataset family, so DRAGEN and MLR projects of the same dataset resolve to the same field."""
    assert constants_registry.resolve_ica_api_key_field('OurDNA-DRAGEN-378') == 'apiKey'
    assert constants_registry.resolve_ica_api_key_field('ourdna-dragen-mlr-jobs') == 'apiKey'
    assert constants_registry.resolve_ica_api_key_field('Tenk10k_Dragen_378') == 'tenk10k_apiKey'
    assert constants_registry.resolve_ica_api_key_field('Tenk10K_Dragen_MLR_Jobs') == 'tenk10k_apiKey'


def test_resolve_ica_api_key_field_raises_on_unregistered_project():
    """An unregistered project fails loud rather than silently reusing another dataset's key."""
    with pytest.raises(KeyError, match=r'newdataset'):
        constants_registry.resolve_ica_api_key_field('newdataset-dragen-378')
