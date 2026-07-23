"""Tests for the DeleteDataInIca cleanup job.

The job has two ICA project-scoped passes (DRAGEN runs project +
supplier project for FASTQ mode). Tests mock the icasdk ProjectDataApi
to control delete/verify outcomes per FID. settle_seconds=0 is threaded
through every test so the 60s production sleep doesn't run.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import icasdk
import pytest

from dragen_align_pa.jobs import delete_data_in_ica

# Real ourdna project ids (conftest sets project_root='ourdna', so the job resolves these from
# the ICA_PROJECT_SETUP table via the DRAGEN-align / FASTQ-upload roles).
RUNS_PROJECT_ID = '5c3a60b0-1458-4e37-8877-ec6b25dc4003'
FASTQ_PROJECT_ID = 'e7a1d085-f12e-4cff-acda-2334338585a8'


def _write_cohort_fid(tmp_path: Path, fid: str = 'fol.cohort_xyz') -> Path:
    path = tmp_path / 'cohort_analysis_output_fid.json'
    path.write_text(json.dumps({'analysis_output_fid': fid}))
    return path


def _write_cram_fids(tmp_path: Path, sg_to_fid: dict[str, str]) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for sg_name, fid in sg_to_fid.items():
        p = tmp_path / f'{sg_name}_cram_fid.json'
        p.write_text(json.dumps({'cram_fid': fid}))
        paths[sg_name] = p
    return paths


def _write_fastq_fid_list(tmp_path: Path, fids: dict[str, str]) -> Path:
    # Write JSON document with {fid: filename}
    p: Path = tmp_path / 'fastq_ids.txt'
    with p.open('w') as f:
        json.dump(fids, f)
    return p


def _make_api_instance(
    verify_deleting_fids: set[str] | None = None,
    verify_available_fids: set[str] | None = None,
    delete_raises: dict[str, Exception] | None = None,
) -> MagicMock:
    """Build a MagicMock `ProjectDataApi` whose delete_data + get_project_data
    behave per the maps above. Unmapped FIDs default to delete-OK + verify-404."""
    verify_deleting_fids = verify_deleting_fids or set()
    verify_available_fids = verify_available_fids or set()
    delete_raises = delete_raises or {}

    api = MagicMock()

    def fake_delete(path_params):
        fid = path_params['dataId']
        if fid in delete_raises:
            raise delete_raises[fid]
        return MagicMock()

    api.delete_data.side_effect = fake_delete

    def fake_get(path_params):
        fid = path_params['dataId']
        if fid in verify_deleting_fids:
            resp = MagicMock()
            resp.body = {'data': {'details': {'status': 'DELETING'}}}
            return resp
        if fid in verify_available_fids:
            resp = MagicMock()
            resp.body = {'data': {'details': {'status': 'AVAILABLE'}}}
            return resp
        # default = 404 not found
        exc = icasdk.ApiException(status=404, reason='Not Found')
        raise exc

    api.get_project_data.side_effect = fake_get
    return api


@pytest.fixture
def patched_env(monkeypatch):
    """Stub the ICA client + ProjectDataApi constructor. Project ids/names resolve from the real
    ICA_PROJECT_SETUP table via the configured family (conftest sets `project_root='ourdna'`), so
    `ica_project_session` yields the real ourdna project ids the assertions expect. The
    api_instance MagicMock is returned via a closure so each test can swap it per scenario."""
    state: dict[str, MagicMock] = {'api_instance': MagicMock()}

    fake_client = MagicMock()
    fake_client.__enter__ = MagicMock(return_value=fake_client)
    fake_client.__exit__ = MagicMock(return_value=False)
    monkeypatch.setattr(
        'dragen_align_pa.jobs.delete_data_in_ica.ica_api_utils.get_ica_api_client',
        lambda: fake_client,
    )

    # Patch the ProjectDataApi constructor so it returns our mock api_instance.
    monkeypatch.setattr(
        'dragen_align_pa.jobs.delete_data_in_ica.project_data_api.ProjectDataApi',
        lambda _client: state['api_instance'],
    )
    return state


def test_cram_mode_happy_path_writes_marker_no_log(tmp_path: Path, patched_env):
    """All deletes succeed, all verifies return 404. Marker is written.
    No error log exists. Supplier project is never queried (CRAM-only)."""
    cohort_fid = 'fol.cohort_001'
    cram_fids = {'SYN00001': 'fil.cram_001', 'SYN00002': 'fil.cram_002'}

    api = _make_api_instance()  # default: all 404 on verify
    patched_env['api_instance'] = api

    cohort_path = _write_cohort_fid(tmp_path, fid=cohort_fid)
    cram_paths = _write_cram_fids(tmp_path, cram_fids)
    marker_path = tmp_path / 'COH0001_delete_complete.json'

    delete_data_in_ica.run(
        cohort_name='COH0001',
        output_path=marker_path,
        cohort_analysis_output_fid_path=cohort_path,
        cram_fid_paths_dict=cram_paths,
        fastq_ids_list_path=None,
        settle_seconds=0,
    )

    assert marker_path.exists(), 'success marker should be written'
    payload = json.loads(marker_path.read_text())
    assert payload['cohort_name'] == 'COH0001'
    assert payload['runs_project']['cohort_folder'] == cohort_fid
    assert payload['runs_project']['cram_count'] == 2
    assert 'fastq_source_project' not in payload

    # No error log file in the same directory
    error_log = tmp_path / 'COH0001_delete_errors.log'
    assert not error_log.exists()

    # Every delete_data and get_project_data call used the RUNS project
    for call in api.delete_data.call_args_list + api.get_project_data.call_args_list:
        assert call.kwargs['path_params']['projectId'] == RUNS_PROJECT_ID


def test_fastq_mode_uses_supplier_project_and_writes_marker(tmp_path: Path, patched_env):
    """FASTQ-mode happy path: runs project handles cohort folder (no CRAMs);
    supplier project handles all FASTQ FIDs. Marker includes fastq_count."""
    cohort_fid = 'fol.cohort_002'
    fastq_fids: dict[str, str] = {
        'fil.fastq_001': 'fil.fastq_001',
        'fil.fastq_002': 'fil.fastq_002',
        'fil.fastq_003': 'fil.fastq_003',
    }

    # Verify returns DELETING for everything — delete-in-flight = success.
    api = _make_api_instance(verify_deleting_fids={cohort_fid, *fastq_fids})
    patched_env['api_instance'] = api

    cohort_path = _write_cohort_fid(tmp_path, fid=cohort_fid)
    fastq_path = _write_fastq_fid_list(tmp_path, fastq_fids)
    marker_path = tmp_path / 'COH0002_delete_complete.json'

    delete_data_in_ica.run(
        cohort_name='COH0002',
        output_path=marker_path,
        cohort_analysis_output_fid_path=cohort_path,
        cram_fid_paths_dict=None,
        fastq_ids_list_path=fastq_path,
        settle_seconds=0,
    )

    assert marker_path.exists()
    payload = json.loads(marker_path.read_text())
    assert payload['runs_project']['cram_count'] == 0
    assert payload['fastq_source_project']['fastq_count'] == 3

    runs_calls = [c for c in api.delete_data.call_args_list if c.kwargs['path_params']['projectId'] == RUNS_PROJECT_ID]
    fastq_calls = [
        c for c in api.delete_data.call_args_list if c.kwargs['path_params']['projectId'] == FASTQ_PROJECT_ID
    ]
    assert len(runs_calls) == 1, 'one cohort folder delete in runs project'
    assert len(fastq_calls) == 3, 'one delete per FASTQ FID in supplier project'


def test_fastq_mode_skips_collaborator_managed_project(tmp_path: Path, patched_env, monkeypatch):
    """A family with `can_delete_fastq=false` (collaborator-managed, e.g. tenk10k) is skipped: no
    FASTQ deletes attempted, no error, marker still written with `skipped=true`. We ask
    collaborators to delete that data rather than doing it ourselves."""
    cohort_fid = 'fol.cohort_003'
    fastq_fids = {'fil.fastq_001': 'fil.fastq_001'}
    api = _make_api_instance(verify_deleting_fids={cohort_fid})
    patched_env['api_instance'] = api
    # project_root=tenk10k → can_delete_fastq is False. `ica_can_delete_fastq` reads config in the
    # constants_registry binding, so patch it there.
    monkeypatch.setattr(
        'dragen_align_pa.constants.constants_registry.config_retrieve',
        lambda key, default=None: 'tenk10k',  # noqa: ARG005
    )

    cohort_path = _write_cohort_fid(tmp_path, fid=cohort_fid)
    fastq_path = _write_fastq_fid_list(tmp_path, fastq_fids)
    marker_path = tmp_path / 'COH0003_delete_complete.json'

    delete_data_in_ica.run(
        cohort_name='COH0003',
        output_path=marker_path,
        cohort_analysis_output_fid_path=cohort_path,
        cram_fid_paths_dict=None,
        fastq_ids_list_path=fastq_path,
        settle_seconds=0,
    )

    assert marker_path.exists()
    fastq_calls = [
        c for c in api.delete_data.call_args_list if c.kwargs['path_params']['projectId'] == FASTQ_PROJECT_ID
    ]
    assert not fastq_calls, 'collaborator-managed project must not be deleted from'
    # The marker records the skip so a downstream reader doesn't mistake the count for deletions.
    payload = json.loads(marker_path.read_text())
    assert payload['fastq_source_project'] == {'fastq_count': 1, 'skipped': True}


def test_spurious_apivalueerror_with_404_verify_is_success(tmp_path: Path, patched_env):
    """User-reported scenario: delete_data raises ApiValueError but actually
    deletes. Verify returns 404 → counted as success, no log."""
    cohort_fid = 'fol.cohort_003'
    api = _make_api_instance(
        delete_raises={cohort_fid: icasdk.ApiValueError('spurious')},
    )
    patched_env['api_instance'] = api

    cohort_path = _write_cohort_fid(tmp_path, fid=cohort_fid)
    marker_path = tmp_path / 'COH0003_delete_complete.json'

    delete_data_in_ica.run(
        cohort_name='COH0003',
        output_path=marker_path,
        cohort_analysis_output_fid_path=cohort_path,
        cram_fid_paths_dict=_write_cram_fids(tmp_path, {'SYN1': 'fil.cram_good'}),
        fastq_ids_list_path=None,
        settle_seconds=0,
    )

    assert marker_path.exists()


def test_real_failure_writes_log_and_raises(tmp_path: Path, patched_env, monkeypatch):
    """One CRAM FID still has status=AVAILABLE after delete + settle. The
    failure is logged to {cohort}_delete_errors.log and RuntimeError raises."""
    cohort_fid = 'fol.cohort_004'
    bad_cram = 'fil.cram_bad'
    good_cram = 'fil.cram_good'

    api = _make_api_instance(verify_available_fids={bad_cram})
    patched_env['api_instance'] = api

    cohort_path = _write_cohort_fid(tmp_path, fid=cohort_fid)
    cram_paths = _write_cram_fids(tmp_path, {'SYN1': bad_cram, 'SYN2': good_cram})
    marker_path = tmp_path / 'COH0004_delete_complete.json'

    # Redirect get_pipeline_path so the error log lands under tmp_path.
    error_log_path = tmp_path / 'COH0004_delete_errors.log'
    monkeypatch.setattr(
        'dragen_align_pa.jobs.delete_data_in_ica.get_pipeline_path',
        lambda filename: error_log_path,
    )

    with pytest.raises(RuntimeError, match=r'1 FIDs failed cleanup'):
        delete_data_in_ica.run(
            cohort_name='COH0004',
            output_path=marker_path,
            cohort_analysis_output_fid_path=cohort_path,
            cram_fid_paths_dict=cram_paths,
            fastq_ids_list_path=None,
            settle_seconds=0,
        )

    assert error_log_path.exists()
    lines = error_log_path.read_text().splitlines()
    assert lines[0] == 'project_id\tfid\tkind\tcontext'
    assert any(bad_cram in line and 'still_present' in line for line in lines[1:])
    assert not any(good_cram in line for line in lines[1:])
    assert not marker_path.exists(), 'marker NOT written on failure'


def test_mixed_outcomes_logs_only_failures(tmp_path: Path, patched_env, monkeypatch):
    """Five FIDs: cohort folder (DELETING=ok), two CRAMs (404=ok),
    one CRAM still AVAILABLE (fail), one FASTQ also AVAILABLE (fail).
    Log contains exactly the two failures; raise mentions count=2."""
    cohort_fid = 'fol.cohort_005'
    crams: dict[str, str] = {'SYN1': 'fil.cram_ok', 'SYN2': 'fil.cram_stuck'}
    fastqs: dict[str, str] = {'fil.fastq_ok': 'fil.fastq_ok', 'fil.fastq_stuck': 'fil.fastq_stuck'}

    api = _make_api_instance(
        verify_deleting_fids={cohort_fid},
        verify_available_fids={'fil.cram_stuck', 'fil.fastq_stuck'},
    )
    patched_env['api_instance'] = api

    cohort_path = _write_cohort_fid(tmp_path, fid=cohort_fid)
    cram_paths = _write_cram_fids(tmp_path, crams)
    fastq_path = _write_fastq_fid_list(tmp_path, fastqs)
    marker_path = tmp_path / 'COH0005_delete_complete.json'

    error_log_path = tmp_path / 'COH0005_delete_errors.log'
    monkeypatch.setattr(
        'dragen_align_pa.jobs.delete_data_in_ica.get_pipeline_path',
        lambda filename: error_log_path,
    )

    with pytest.raises(RuntimeError, match=r'2 FIDs failed cleanup'):
        delete_data_in_ica.run(
            cohort_name='COH0005',
            output_path=marker_path,
            cohort_analysis_output_fid_path=cohort_path,
            cram_fid_paths_dict=cram_paths,
            fastq_ids_list_path=fastq_path,
            settle_seconds=0,
        )

    lines = error_log_path.read_text().splitlines()[1:]
    assert len(lines) == 2
    assert any('fil.cram_stuck' in line for line in lines)
    assert any('fil.fastq_stuck' in line for line in lines)


def test_idempotent_rerun_after_full_deletion(tmp_path: Path, patched_env):
    """Re-running after a clean prior run: every delete_data call raises
    ApiException(status=404), every get_project_data also raises 404.
    No failures, marker written normally."""
    cohort_fid = 'fol.cohort_006'
    crams = {'SYN1': 'fil.cram_001'}

    not_found = icasdk.ApiException(status=404, reason='Not Found')
    api = _make_api_instance(
        delete_raises={cohort_fid: not_found, 'fil.cram_001': not_found},
    )
    # default get_project_data behavior is 404 — matches re-run scenario.
    patched_env['api_instance'] = api

    cohort_path = _write_cohort_fid(tmp_path, fid=cohort_fid)
    cram_paths = _write_cram_fids(tmp_path, crams)
    marker_path = tmp_path / 'COH0006_delete_complete.json'

    delete_data_in_ica.run(
        cohort_name='COH0006',
        output_path=marker_path,
        cohort_analysis_output_fid_path=cohort_path,
        cram_fid_paths_dict=cram_paths,
        fastq_ids_list_path=None,
        settle_seconds=0,
    )

    assert marker_path.exists()
