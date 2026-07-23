"""Unit tests for `dragen_align_pa.paths.IcaPath`."""

import pytest
from cpg_utils.config import ConfigError

from dragen_align_pa import paths
from dragen_align_pa.constants import ica_constants, constants_registry

_SENTINEL = object()

_CONFIG: dict[tuple[str, ...], str] = {
    ('ica', 'data_prep', 'output_folder'): 'test-dragen-378',
}


def _patch_config(monkeypatch) -> None:
    """Patch `paths.config_retrieve` / `BUCKET_NAME` to test values.

    `IcaPath` lives in `paths` and reads these names as bound there, so the patch
    targets `paths`. The fake raises on an unknown key when no default is given,
    mirroring the real `cpg_utils.config.config_retrieve` — so the `as_url` fail-loud
    contract is exercised.
    """

    def fake_config_retrieve(key, default=_SENTINEL):
        try:
            return _CONFIG[tuple(key)]
        except KeyError:
            if default is _SENTINEL:
                raise ConfigError(f'Key not found: {key}') from None
            return default

    monkeypatch.setattr('dragen_align_pa.paths.config_retrieve', fake_config_retrieve)
    monkeypatch.setattr('dragen_align_pa.paths.BUCKET_NAME', 'cpg-test-dataset-test')


def test_output_root_as_folder(monkeypatch):
    _patch_config(monkeypatch)
    assert paths.IcaPath.output_root().as_folder() == '/cpg-test-dataset-test/test-dragen-378/'


def test_truediv_joins_and_folder_has_trailing_slash(monkeypatch):
    _patch_config(monkeypatch)
    path = paths.IcaPath.output_root() / 'COH0001' / 'ref-abc'
    assert path.as_folder() == '/cpg-test-dataset-test/test-dragen-378/COH0001/ref-abc/'


def test_as_file_has_leading_no_trailing_slash(monkeypatch):
    _patch_config(monkeypatch)
    path = paths.IcaPath.output_root() / 'COH0001'
    assert path.as_file('SYN00001.cram') == '/cpg-test-dataset-test/test-dragen-378/COH0001/SYN00001.cram'


def test_as_url_uses_project_name_and_no_trailing_slash(monkeypatch):
    _patch_config(monkeypatch)
    url = paths.IcaPath.from_relpath('data/ref/hashtable/hg38/DRAGEN/9').as_url(constants_registry.ROLE_DRAGEN_MLR)
    assert url == 'ica://ourdna-dragen-mlr-jobs/data/ref/hashtable/hg38/DRAGEN/9'


def test_segments_collapse_stray_slashes(monkeypatch):
    _patch_config(monkeypatch)
    path = paths.IcaPath.from_relpath('/a//b/') / 'c/d'
    assert path.as_folder() == '/a/b/c/d/'


def test_as_url_missing_role_raises(monkeypatch):
    _patch_config(monkeypatch)
    # `does_not_exist` is not a registered role in the configured family → resolution fails loud.
    with pytest.raises(KeyError, match=r'does_not_exist'):
        paths.IcaPath.from_relpath('data/ref').as_url('does_not_exist')


def test_as_file_empty_filename_raises(monkeypatch):
    _patch_config(monkeypatch)
    with pytest.raises(ValueError, match=r'non-empty filename'):
        paths.IcaPath.from_relpath('a').as_file('/')


def test_str_raises_to_force_explicit_form(monkeypatch):
    _patch_config(monkeypatch)
    with pytest.raises(TypeError, match=r'no default string form'):
        str(paths.IcaPath.from_relpath('a/b'))


def test_empty_path_folder_is_root_slash(monkeypatch):
    _patch_config(monkeypatch)
    assert paths.IcaPath(()).as_folder() == '/'


def test_mlr_hash_table_relpath_composes_expected_url(monkeypatch):
    _patch_config(monkeypatch)
    url = paths.IcaPath.from_relpath(ica_constants.MLR_HASH_TABLE_RELPATH).as_url(constants_registry.ROLE_DRAGEN_MLR)
    assert url == 'ica://ourdna-dragen-mlr-jobs/data/ref/hashtable/hg38_alt_masked_graph_v2/DRAGEN/9'


def test_mlr_hash_table_relpath_carries_no_scheme_or_project():
    # The ICA project must be resolved from [ica.projects], never baked into the constant.
    assert not ica_constants.MLR_HASH_TABLE_RELPATH.startswith('ica://')
    assert 'ourdna' not in ica_constants.MLR_HASH_TABLE_RELPATH
