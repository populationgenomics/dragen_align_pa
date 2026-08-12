"""Unit tests for the MLR submission prefetch phase.

The efficiency contract: for N pending SGs the module authenticates once per ICA
project and makes one list call per SG (both filenames in one call), and the
per-SG submit callable performs no auth/lookup/download work of its own.
"""

import json
from collections.abc import Collection
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import pytest

from dragen_align_pa.constants.constants_registry import ROLE_DRAGEN_ALIGN, ROLE_DRAGEN_MLR
from dragen_align_pa.jobs import manage_dragen_mlr
from tests.conftest import DEMO_COHORT_NAME, DEMO_PIPELINE_ID, DEMO_USER_REFERENCE

_SGS = ('SYN00001', 'SYN00002')


def _outputs(tmp_path: Path, existing: Collection[str] = frozenset()) -> dict[str, Path]:
    outputs: dict[str, Path] = {f'{DEMO_COHORT_NAME}_mlr_errors': tmp_path / 'errors.log'}
    for name in _SGS:
        marker = tmp_path / f'{name}_mlr_pipeline_id.json'
        if name in existing:
            marker.write_text('{}')
        outputs[f'{name}_mlr_pipeline_id'] = marker
        outputs[f'{name}_mlr_success'] = tmp_path / f'{name}_mlr_pipeline_success.json'
    return outputs


def _state_files(tmp_path: Path) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for name in _SGS:
        p = tmp_path / f'{name}_pipeline_id_and_arguid.json'
        p.write_text(
            json.dumps(
                {
                    'schema_version': 2,
                    'cohort_name': DEMO_COHORT_NAME,
                    'pipeline_id': DEMO_PIPELINE_ID,
                    'user_reference': DEMO_USER_REFERENCE,
                    'ar_guid': 'test-guid',
                },
            ),
        )
        paths[f'{name}_pipeline_id_and_arguid'] = p
    return paths


# --- _pending_sg_names ---


def test_pending_skips_sgs_with_existing_pipeline_id_file(tmp_path):
    outputs = _outputs(tmp_path, existing={'SYN00001'})
    assert manage_dragen_mlr._pending_sg_names(_SGS, outputs) == ['SYN00002']


def test_pending_includes_all_sgs_under_force_resubmit(tmp_path, monkeypatch):
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.config_retrieve',
        lambda key, default=None: True if key == ['ica', 'management', 'force_resubmit'] else default,
    )
    outputs = _outputs(tmp_path, existing={'SYN00001'})
    assert manage_dragen_mlr._pending_sg_names(_SGS, outputs) == list(_SGS)


def test_pending_empty_when_cancelling(tmp_path, monkeypatch):
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.config_retrieve',
        lambda key, default=None: True if key == ['ica', 'management', 'cancel_cohort_run'] else default,
    )
    outputs = _outputs(tmp_path)
    assert manage_dragen_mlr._pending_sg_names(_SGS, outputs) == []


# --- _prefetch_mlr_inputs ---


def test_prefetch_authenticates_once_and_lists_once_per_sg(tmp_path, monkeypatch):
    auth_calls: list[str] = []
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.ica_cli_utils.authenticate_ica_cli',
        auth_calls.append,
    )
    find_calls: list[tuple[str, tuple[str, ...]]] = []

    def _fake_find(parent_folder, file_names):
        find_calls.append((parent_folder, tuple(file_names)))
        return {name: f'{parent_folder}{name}' for name in file_names}

    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.ica_cli_utils.find_ica_file_paths_by_names',
        _fake_find,
    )

    inputs = manage_dragen_mlr._prefetch_mlr_inputs(
        pending=list(_SGS),
        cohort_name=DEMO_COHORT_NAME,
        pipeline_id_arguid_path_dict=_state_files(tmp_path),
    )

    assert auth_calls == [ROLE_DRAGEN_ALIGN]
    assert len(find_calls) == 2
    assert find_calls[0][1] == ('SYN00001.cram', 'SYN00001.hard-filtered.gvcf.gz')
    assert set(inputs) == set(_SGS)
    assert inputs['SYN00001'].cram_url.startswith('ica://')
    assert inputs['SYN00001'].cram_url.endswith('SYN00001.cram')
    assert inputs['SYN00001'].gvcf_url.endswith('SYN00001.hard-filtered.gvcf.gz')
    assert inputs['SYN00001'].output_folder_url.startswith('ica://')


# --- _submit_mlr_run ---


def test_submit_mlr_run_builds_argv_from_prefetched_inputs(monkeypatch):
    captured: dict[str, list[str]] = {}

    def _fake_submit(argv: list[str]) -> str:
        captured['argv'] = argv
        return 'analysis-42'

    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.popgen_mlr.submit_analysis',
        _fake_submit,
    )
    inputs_by_sg = {
        'SYN00001': manage_dragen_mlr.MlrInputs(
            cram_url='ica://proj/run/SYN00001/SYN00001.cram',
            gvcf_url='ica://proj/run/SYN00001/SYN00001.hard-filtered.gvcf.gz',
            output_folder_url='ica://proj/run/SYN00001/',
        ),
    }

    analysis_id = manage_dragen_mlr._submit_mlr_run(
        sg_name='SYN00001',
        inputs_by_sg=inputs_by_sg,
        local_config_path='/io/mlr_config.json',
        mlr_hash_table='ica://mlrproj/ref/hashtable/hg38_alt_masked_graph_v2/DRAGEN/9',
    )

    assert analysis_id == 'analysis-42'
    argv = captured['argv']
    assert 'popgen-cli' not in argv  # flags only; the subprocess prefix is gone
    assert argv[argv.index('--run-id') + 1] == 'SYN00001-mlr'
    assert argv[argv.index('--sample-id') + 1] == 'SYN00001'
    assert argv[argv.index('--input-align-file-url') + 1] == 'ica://proj/run/SYN00001/SYN00001.cram'
    assert argv[argv.index('--input-project-config-file-path') + 1] == '/io/mlr_config.json'


def test_submit_callable_for_unprefetched_sg_fails_only_when_called():
    """The loop's factory runs for every unfinished target each poll cycle, so the
    callable must bind the dict and fail only if a submission is actually attempted."""
    submit = partial(
        manage_dragen_mlr._submit_mlr_run,
        sg_name='SYN00009',
        inputs_by_sg={},
        local_config_path='',
        mlr_hash_table='ht',
    )  # creation must not raise
    with pytest.raises(KeyError):
        submit()


# --- run() wiring ---


def _fake_cohort():
    return SimpleNamespace(
        name=DEMO_COHORT_NAME,
        get_sequencing_groups=lambda: [SimpleNamespace(name=n) for n in _SGS],
    )


def test_run_prefetches_then_prepares_mlr_project_then_starts_loop(tmp_path, monkeypatch):
    order: list[str] = []
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr._prefetch_mlr_inputs',
        lambda pending, cohort_name, pipeline_id_arguid_path_dict: order.append('prefetch') or {},
    )
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.ica_cli_utils.authenticate_ica_cli',
        lambda role: order.append(f'auth:{role}'),
    )
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr._mlr_download_config',
        lambda fid, tmp_dir: order.append('download') or '/io/mlr_config.json',
    )
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.ica_mlr_config_file_id',
        lambda: 'fil.mlrconfig',
    )
    loop_kwargs: dict = {}
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.manage_ica_pipeline_loop',
        lambda **kwargs: loop_kwargs.update(kwargs),
    )

    manage_dragen_mlr.run(_fake_cohort(), _state_files(tmp_path), _outputs(tmp_path))

    assert order == ['prefetch', f'auth:{ROLE_DRAGEN_MLR}', 'download']
    assert loop_kwargs['pipeline_name'] == 'MLR'
    assert loop_kwargs['allow_retry'] is False


def test_run_skips_all_cli_setup_when_nothing_pending(tmp_path, monkeypatch):
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr._prefetch_mlr_inputs',
        lambda *a, **k: pytest.fail('prefetch must not run when nothing is pending'),
    )
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.ica_cli_utils.authenticate_ica_cli',
        lambda role: pytest.fail('no CLI auth is needed when nothing is pending'),
    )
    loop_kwargs: dict = {}
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.manage_ica_pipeline_loop',
        lambda **kwargs: loop_kwargs.update(kwargs),
    )

    manage_dragen_mlr.run(
        _fake_cohort(),
        _state_files(tmp_path),
        _outputs(tmp_path, existing=set(_SGS)),
    )

    assert loop_kwargs['pipeline_name'] == 'MLR'
