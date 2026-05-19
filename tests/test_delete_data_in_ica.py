"""Tests for the DeleteDataInIca cleanup job.

The job has two ICA project-scoped passes (DRAGEN runs project +
supplier project for FASTQ mode). Tests mock the icasdk ProjectDataApi
to control delete/verify outcomes per FID. settle_seconds=0 is threaded
through every test so the 60s production sleep doesn't run.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from icasdk import ApiException

from dragen_align_pa.jobs import delete_data_in_ica


RUNS_PROJECT_ID = 'runs-project-id'
FASTQ_PROJECT_ID = 'fastq-supplier-project-id'


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


def _write_fastq_fid_list(tmp_path: Path, fids: list[str]) -> Path:
    p = tmp_path / 'fastq_ids.txt'
    p.write_text('\n'.join(fids))
    return p


def _make_api_instance(
    verify_404_fids: set[str] | None = None,
    verify_deleting_fids: set[str] | None = None,
    verify_available_fids: set[str] | None = None,
    delete_raises: dict[str, Exception] | None = None,
) -> MagicMock:
    """Build a MagicMock `ProjectDataApi` whose delete_data + get_project_data
    behave per the maps above. Unmapped FIDs default to delete-OK + verify-404."""
    verify_404_fids = verify_404_fids or set()
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
        exc = ApiException(status=404, reason='Not Found')
        raise exc

    api.get_project_data.side_effect = fake_get
    return api


@pytest.fixture
def patched_env(monkeypatch):
    """Stub get_ica_secrets + get_ica_api_client + config_retrieve for the
    fastq source project lookup. The api_instance MagicMock is returned via
    a closure so each test can swap it per scenario."""
    state: dict[str, MagicMock] = {'api_instance': MagicMock()}

    monkeypatch.setattr(
        'dragen_align_pa.jobs.delete_data_in_ica.ica_api_utils.get_ica_secrets',
        lambda: {'projectID': RUNS_PROJECT_ID, 'apiKey': 'stub'},
    )
    fake_client = MagicMock()
    fake_client.__enter__ = MagicMock(return_value=fake_client)
    fake_client.__exit__ = MagicMock(return_value=False)
    monkeypatch.setattr(
        'dragen_align_pa.jobs.delete_data_in_ica.ica_api_utils.get_ica_api_client',
        lambda: fake_client,
    )

    def fake_config_retrieve(key, default=None):
        if tuple(key) == ('ica', 'projects', 'fastq_source_project_id'):
            return FASTQ_PROJECT_ID
        return default

    monkeypatch.setattr(
        'dragen_align_pa.jobs.delete_data_in_ica.config_retrieve',
        fake_config_retrieve,
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
