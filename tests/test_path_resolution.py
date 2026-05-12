import json
from pathlib import Path

from dragen_align_pa.utils import PER_SG_STATE_SCHEMA_VERSION, get_ica_sample_folder


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
    state_path = tmp_path / 'CPG00001_pipeline_id_and_arguid.json'
    _write_state(state_path)

    def fake_config_retrieve(key, default=None):
        if key == ['ica', 'data_prep', 'output_folder']:
            return 'test-dragen-378'
        return default

    monkeypatch.setattr('dragen_align_pa.utils.config_retrieve', fake_config_retrieve)
    monkeypatch.setattr('dragen_align_pa.utils.BUCKET_NAME', 'cpg-test-dataset-test')

    result = get_ica_sample_folder(state_path, sg_name='CPG00001')
    assert result == '/cpg-test-dataset-test/test-dragen-378/COH0001-batch0000_test-guid_-00000000-1111-2222-3333-444444444444/CPG00001/'


def test_get_ica_sample_folder_rejects_old_schema(tmp_path: Path):
    """Per-SG state files written by older code (no `schema_version`) must
    raise a friendly error so a resume after a deploy doesn't silently
    misresolve paths."""
    state_path = tmp_path / 'CPG00001_pipeline_id_and_arguid.json'
    state_path.write_text(json.dumps({
        'pipeline_id': 'abc',
        'ar_guid': 'xyz',
        # missing schema_version, user_reference, batch_index
    }))
    import pytest
    with pytest.raises(ValueError, match='schema_version'):
        get_ica_sample_folder(state_path, sg_name='CPG00001')


def test_get_ica_sample_folder_raises_on_missing_state(tmp_path: Path):
    import pytest
    with pytest.raises(FileNotFoundError):
        get_ica_sample_folder(tmp_path / 'does-not-exist.json', sg_name='CPG00001')
