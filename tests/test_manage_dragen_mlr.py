"""Unit tests for the MLR submission prefetch phase.

The efficiency contract: for N pending SGs the module authenticates once per ICA
project and makes one list call per SG (both filenames in one call), and the
per-SG submit callable performs no auth/lookup/download work of its own.
"""

import json
from collections.abc import Collection
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


def test_prefetch_raises_once_naming_every_failed_sg(tmp_path, monkeypatch):
    """k SGs with missing inputs must cost one report naming all k, not k
    fix-and-rerun cycles; nothing is submitted either way."""
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.ica_cli_utils.authenticate_ica_cli',
        lambda role: None,
    )
    find_calls: list[str] = []

    def _fake_find(parent_folder, file_names):
        find_calls.append(parent_folder)
        raise ValueError(f'No file(s) named {", ".join(file_names)} found in folder "{parent_folder}"')

    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.ica_cli_utils.find_ica_file_paths_by_names',
        _fake_find,
    )

    with pytest.raises(ValueError, match=r'SYN00001(.|\n)*SYN00002') as exc_info:
        manage_dragen_mlr._prefetch_mlr_inputs(
            pending=list(_SGS),
            cohort_name=DEMO_COHORT_NAME,
            pipeline_id_arguid_path_dict=_state_files(tmp_path),
        )

    assert len(find_calls) == 2  # did not stop at the first failing SG
    assert 'nothing submitted' in str(exc_info.value)


# --- _mlr_analysis_tags ---


def test_mlr_analysis_tags_appends_mlr_marker_to_config_tags():
    """Tags come from [ica.tags] (same convention as the align/md5 submissions)
    plus an 'mlr' technical marker for GUI filtering; ICA offers no API to read
    tags back, so this is their only consumer-visible effect."""
    assert manage_dragen_mlr._mlr_analysis_tags() == {
        'technicalTags': ['test_technical_tag', 'mlr'],
        'userTags': ['test_user_tags'],
        'referenceTags': ['test_reference_tags'],
    }


# --- _submit_mlr_run ---

_TEST_TAGS = {'technicalTags': ['mlr'], 'userTags': [], 'referenceTags': []}


def test_submit_mlr_run_builds_argv_from_resolved_inputs(monkeypatch):
    captured: dict = {}

    def _fake_submit(argv: list[str], tags: dict[str, list[str]]) -> str:
        captured['argv'] = argv
        captured['tags'] = tags
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
        resolve_inputs=lambda sg_name: inputs_by_sg[sg_name],
        mlr_config_path=lambda: '/io/mlr_config.json',
        mlr_hash_table='ica://mlrproj/ref/hashtable/hg38_alt_masked_graph_v2/DRAGEN/9',
        tags=_TEST_TAGS,
    )

    assert analysis_id == 'analysis-42'
    assert captured['tags'] == _TEST_TAGS
    argv = captured['argv']
    assert 'popgen-cli' not in argv  # flags only; the subprocess prefix is gone
    assert argv[argv.index('--run-id') + 1] == 'SYN00001-mlr'
    assert argv[argv.index('--sample-id') + 1] == 'SYN00001'
    assert argv[argv.index('--input-align-file-url') + 1] == 'ica://proj/run/SYN00001/SYN00001.cram'
    assert argv[argv.index('--input-project-config-file-path') + 1] == '/io/mlr_config.json'
    # The wheel's check_args mkdirs this folder, so it must live under the batch
    # tmp dir, not litter the process CWD with one empty dir per SG.
    assert argv[argv.index('--output-analysis-json-folder-path') + 1] == '/io/mlr_analysis_json'


# --- run() wiring ---


def _fake_cohort():
    return SimpleNamespace(
        name=DEMO_COHORT_NAME,
        get_sequencing_groups=lambda: [SimpleNamespace(name=n) for n in _SGS],
    )


def test_run_prefetches_then_prepares_mlr_project_then_starts_loop(tmp_path, monkeypatch):
    monkeypatch.setenv('BATCH_TMPDIR', str(tmp_path))
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
    # The loop and the prefetch pending-set check must read the same marker file.
    assert loop_kwargs['pipeline_id_file_key_template'] == manage_dragen_mlr._MLR_PIPELINE_ID_KEY_TEMPLATE


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


def test_run_factory_callable_resolves_unprefetched_sg_on_demand(tmp_path, monkeypatch):
    """The pending set is an optimisation hint, not a correctness invariant: if the
    loop submits an SG the prefetch didn't cover (divergent marker-file state, or a
    future allow_retry=True), the callable resolves it on demand instead of dying
    on a bare KeyError."""
    monkeypatch.setenv('BATCH_TMPDIR', str(tmp_path))
    auths: list[str] = []
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.ica_cli_utils.authenticate_ica_cli',
        auths.append,
    )
    resolved: list[str] = []

    def _fake_resolve(sg_name, _cohort_name, _pipeline_id_arguid_path_dict):
        resolved.append(sg_name)
        return manage_dragen_mlr.MlrInputs(
            cram_url='ica://proj/run/x.cram',
            gvcf_url='ica://proj/run/x.hard-filtered.gvcf.gz',
            output_folder_url='ica://proj/run/',
        )

    monkeypatch.setattr('dragen_align_pa.jobs.manage_dragen_mlr._resolve_mlr_inputs', _fake_resolve)
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr._mlr_download_config',
        lambda fid, tmp_dir: str(tmp_path / 'mlr_config.json'),
    )
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.ica_mlr_config_file_id',
        lambda: 'fil.mlrconfig',
    )
    submitted: list[tuple[list[str], dict]] = []

    def _fake_submit(argv: list[str], tags: dict[str, list[str]]) -> str:
        submitted.append((argv, tags))
        return 'analysis-77'

    monkeypatch.setattr('dragen_align_pa.jobs.manage_dragen_mlr.popgen_mlr.submit_analysis', _fake_submit)
    loop_kwargs: dict = {}
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.manage_ica_pipeline_loop',
        lambda **kwargs: loop_kwargs.update(kwargs),
    )

    # Both SGs have pipeline-id files → nothing pending, nothing prefetched.
    manage_dragen_mlr.run(_fake_cohort(), _state_files(tmp_path), _outputs(tmp_path, existing=set(_SGS)))
    assert resolved == []

    submit = loop_kwargs['submit_function_factory']('SYN00001')
    assert submit() == 'analysis-77'
    assert resolved == ['SYN00001']
    assert ROLE_DRAGEN_ALIGN in auths  # the fallback authenticated for its lookup
    assert ROLE_DRAGEN_MLR in auths  # and the lazy config download authenticated too
    assert submitted and submitted[0][1] == manage_dragen_mlr._mlr_analysis_tags()


def test_run_fails_loudly_without_batch_tmpdir(tmp_path, monkeypatch):
    """BATCH_TMPDIR is a hard precondition of the batch VM; a missing variable is a
    misconfiguration to surface, not to paper over with a default path."""
    monkeypatch.delenv('BATCH_TMPDIR', raising=False)
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr._prefetch_mlr_inputs',
        lambda pending, cohort_name, pipeline_id_arguid_path_dict: {},
    )
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_mlr.ica_cli_utils.authenticate_ica_cli',
        lambda role: None,
    )

    with pytest.raises(ValueError, match='BATCH_TMPDIR'):
        manage_dragen_mlr.run(_fake_cohort(), _state_files(tmp_path), _outputs(tmp_path))
