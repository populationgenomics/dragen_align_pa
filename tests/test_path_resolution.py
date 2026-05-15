import json
from pathlib import Path

import cpg_utils.config
import pytest

from dragen_align_pa.utils import PER_SG_STATE_SCHEMA_VERSION, get_batch_artefacts_path, get_ica_sample_folder


def _write_state(path: Path, **fields) -> None:
    payload = {
        'schema_version': PER_SG_STATE_SCHEMA_VERSION,
        'pipeline_id': '00000000-1111-2222-3333-444444444444',
        'ar_guid': 'test-guid',
        'user_reference': 'COH0001-batch0000_test-guid_',
        'batch_index': 0,
    }
    payload.update(fields)
    path.write_text(json.dumps(payload))


def test_get_ica_sample_folder_renders_expected_path(tmp_path: Path, monkeypatch):
    state_path = tmp_path / 'SYN00001_pipeline_id_and_arguid.json'
    _write_state(state_path)

    def fake_config_retrieve(key, default=None):
        if key == ['ica', 'data_prep', 'output_folder']:
            return 'test-dragen-378'
        return default

    monkeypatch.setattr('dragen_align_pa.utils.config_retrieve', fake_config_retrieve)
    monkeypatch.setattr('dragen_align_pa.utils.BUCKET_NAME', 'cpg-test-dataset-test')

    result = get_ica_sample_folder(state_path, sg_name='SYN00001')
    expected = (
        '/cpg-test-dataset-test/test-dragen-378/'
        'COH0001-batch0000_test-guid_-00000000-1111-2222-3333-444444444444/SYN00001/'
    )
    assert result == expected


def test_get_ica_sample_folder_rejects_old_schema(tmp_path: Path):
    """Per-SG state files written by older code (no `schema_version`) must
    raise a friendly error so a resume after a deploy doesn't silently
    misresolve paths."""
    state_path = tmp_path / 'SYN00001_pipeline_id_and_arguid.json'
    state_path.write_text(json.dumps({
        'pipeline_id': 'abc',
        'ar_guid': 'xyz',
        # missing schema_version, user_reference, batch_index
    }))
    with pytest.raises(ValueError, match='schema_version'):
        get_ica_sample_folder(state_path, sg_name='SYN00001')


def test_get_ica_sample_folder_raises_on_missing_state(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        get_ica_sample_folder(tmp_path / 'does-not-exist.json', sg_name='SYN00001')


def test_get_ica_sample_folder_raises_keyerror_on_missing_batch_index(tmp_path: Path):
    """Per-SG state schema v1 requires batch_index — downstream resume /
    reconciliation paths read it. A v1 file missing that key should raise
    KeyError naming the missing field, matching the validation contract
    the docstring promises."""
    state_path = tmp_path / 'SYN00001_pipeline_id_and_arguid.json'
    state_path.write_text(json.dumps({
        'schema_version': PER_SG_STATE_SCHEMA_VERSION,
        'pipeline_id': '00000000-1111-2222-3333-444444444444',
        'ar_guid': 'test-guid',
        'user_reference': 'COH0001-batch0000_test-guid_',
        # missing batch_index
    }))
    with pytest.raises(KeyError, match='batch_index'):
        get_ica_sample_folder(state_path, sg_name='SYN00001')


def test_conftest_output_path_stub_accepts_category_kwarg():
    """The real `cpg_utils.config.output_path` accepts a `category` kwarg
    (and `dragen_align_pa.utils.get_output_path` passes it through). The
    conftest in-memory stub must accept the same kwarg, or any future test
    that triggers `output_path(..., category=...)` against the stub will
    blow up with TypeError instead of producing a stable path."""
    result = cpg_utils.config.output_path('foo/bar', category='analysis')
    assert isinstance(result, str)


def test_get_batch_artefacts_path(monkeypatch):
    monkeypatch.setattr('dragen_align_pa.utils.DRAGEN_VERSION', 'dragen_3_7_8')

    captured = {}

    def fake_output_path(suffix, category=None):
        captured['suffix'] = suffix
        captured['category'] = category
        return f'gs://test-bucket/{suffix}'

    monkeypatch.setattr('dragen_align_pa.utils.output_path', fake_output_path)

    result = get_batch_artefacts_path(cohort_name='COH0001', batch_index=3)
    assert str(result) == 'gs://test-bucket/ica/dragen_3_7_8/output/dragen_batch_metrics/COH0001_batch0003'
    # `get_batch_artefacts_root` builds the prefix; the per-batch path is
    # constructed by trailing `/{batch_name}` from the cpg_utils.Path operator.
    assert captured['suffix'] == 'ica/dragen_3_7_8/output/dragen_batch_metrics'
