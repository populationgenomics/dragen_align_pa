"""Tests for the MLR submission helper.

Focus: the AR-GUID-stamping branch added in PR 2. The popgen-cli
invocation itself is shelled out and not exercised here — the helpers
that touch ICA / the filesystem / subprocess are stubbed so the focus
stays on the post-submit tag write and the per-SG state-file contract.
"""

import json

import pytest

from dragen_align_pa.jobs import manage_dragen_mlr


def _write_state(tmp_path, **overrides):
    state = {
        'schema_version': 1,
        'pipeline_id': 'dragen-id',
        'user_reference': 'COH0001-batch0000',
        'ar_guid': 'AR-GUID-12345',
        'batch_index': 0,
    }
    state.update(overrides)
    path = tmp_path / 'pid.json'
    path.write_text(json.dumps(state))
    return path


def _stub_submission_helpers(monkeypatch):
    """Stub everything _submit_mlr_run shells out to, leaving the
    state-file read and the tag-write under test."""
    monkeypatch.setattr(manage_dragen_mlr.ica_cli_utils, 'authenticate_ica_cli', lambda: None)
    monkeypatch.setattr(
        manage_dragen_mlr, '_mlr_find_input_urls',
        lambda *_a, **_k: ('ica://a/cram', 'ica://a/gvcf'),
    )
    monkeypatch.setattr(manage_dragen_mlr, '_mlr_enter_project', lambda _p: None)
    monkeypatch.setattr(
        manage_dragen_mlr, '_mlr_download_config', lambda *_a, **_k: 'local-mlr-config.json',
    )
    monkeypatch.setattr(
        manage_dragen_mlr, '_mlr_build_popgen_cli_command', lambda *_a, **_k: ['popgen', 'submit'],
    )
    monkeypatch.setattr(manage_dragen_mlr.utils, 'run_subprocess_with_log', lambda *_a, **_k: None)
    monkeypatch.setattr(
        manage_dragen_mlr, '_mlr_parse_submission_output', lambda *_a, **_k: 'mlr-analysis-id',
    )


def _call_submit(state_path):
    return manage_dragen_mlr._submit_mlr_run(
        pipeline_id_arguid_path=state_path,
        ica_analysis_output_folder='out',
        sg_name='CPG_A',
        cohort_name='COH0001',
        mlr_project='mlr-project-name',
        mlr_project_id='mlr-project-uuid',
        mlr_config_json='cfg-id',
        mlr_hash_table='ht-id',
        output_prefix='ica://x',
    )


def test_submit_mlr_run_stamps_ar_guid_with_project_id(tmp_path, monkeypatch):
    """The post-submit tag write must use the MLR *project id* (the UUID
    update_analysis needs), the returned analysis id, and the raw AR-GUID
    from the per-SG state file."""
    _stub_submission_helpers(monkeypatch)
    state_path = _write_state(tmp_path)

    calls: list[tuple[str, str, str]] = []
    monkeypatch.setattr(
        'dragen_align_pa.ica_api_utils.add_technical_tag',
        lambda project_id, analysis_id, tag: calls.append((project_id, analysis_id, tag)),
    )

    result = _call_submit(state_path)

    assert result == 'mlr-analysis-id'
    assert calls == [('mlr-project-uuid', 'mlr-analysis-id', 'AR-GUID-12345')]


def test_submit_mlr_run_requires_ar_guid_in_state(tmp_path, monkeypatch):
    """ar_guid is now a required key, so a state file missing it fails fast
    at load time rather than as a KeyError mid-submission."""
    _stub_submission_helpers(monkeypatch)
    state_path = tmp_path / 'pid.json'
    state_path.write_text(json.dumps({
        'schema_version': 1,
        'pipeline_id': 'dragen-id',
        'user_reference': 'COH0001-batch0000',
    }))

    with pytest.raises(KeyError, match='ar_guid'):
        _call_submit(state_path)
