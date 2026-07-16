"""Unit tests for `prepare_ica_for_analysis.run`.

Focus: the RAISE-vs-WARN split on the folder status returned by
`ica_utils.create_upload_object_id`. ARCHIVED / DELETING / UNARCHIVING are
terminal-bad and must raise; ARCHIVING / PARTIAL are in-flight transitions
that warn but don't block; AVAILABLE is the silent happy path.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import cpg_utils
import pytest

from dragen_align_pa.jobs import prepare_ica_for_analysis


def _fake_cohort(name: str = 'COH0001') -> MagicMock:
    cohort = MagicMock()
    cohort.name = name
    return cohort


@pytest.fixture
def patched_environment(monkeypatch):
    """Stub the ICA client context manager.

    Project id/name resolution goes through the real `ICA_PROJECT_SETUP` table for the configured
    family (conftest sets `project_root='ourdna'`); only the client (and thus the Secret Manager
    fetch) needs stubbing.
    """
    fake_client = MagicMock()
    fake_client.__enter__ = MagicMock(return_value=fake_client)
    fake_client.__exit__ = MagicMock(return_value=False)
    monkeypatch.setattr(
        'dragen_align_pa.jobs.prepare_ica_for_analysis.ica_api_utils.get_ica_api_client',
        lambda: fake_client,
    )


def _patch_create_object(monkeypatch, status: str, folder_id: str = 'fid-123') -> None:
    monkeypatch.setattr(
        'dragen_align_pa.jobs.prepare_ica_for_analysis.ica_utils.create_upload_object_id',
        MagicMock(return_value=(folder_id, status)),
    )


@pytest.mark.parametrize('terminal_status', ['ARCHIVED', 'DELETING', 'UNARCHIVING'])
def test_run_raises_on_terminal_bad_status(
    patched_environment,  # noqa: ARG001
    monkeypatch,
    tmp_path: Path,
    terminal_status,  # noqa: ARG001
):
    """ARCHIVED / DELETING / UNARCHIVING are terminal-bad: subsequent
    pipeline submissions WILL fail with opaque ICA errors, so fail-fast
    at the prep step. The output file is NOT written."""
    _patch_create_object(monkeypatch, terminal_status)
    output_path = tmp_path / 'analysis_output_fid.json'

    with pytest.raises(RuntimeError, match='terminal-bad status'):
        prepare_ica_for_analysis.run(
            cohort=_fake_cohort(),
            output=cpg_utils.to_path(output_path),
        )

    assert not output_path.exists()


@pytest.mark.parametrize('transient_status', ['ARCHIVING', 'PARTIAL'])
def test_run_warns_on_transient_status_but_writes_output(
    patched_environment,  # noqa: ARG001
    monkeypatch,
    tmp_path: Path,
    transient_status,
    caplog,  # noqa: ARG001
):
    """ARCHIVING / PARTIAL are in-flight transitions — they may resolve
    by the time the orchestrator submits. Warn but continue; the output
    file is written so downstream stages can proceed."""
    _patch_create_object(monkeypatch, transient_status, folder_id='fid-XYZ')
    output_path = tmp_path / 'analysis_output_fid.json'

    prepare_ica_for_analysis.run(
        cohort=_fake_cohort(),
        output=cpg_utils.to_path(output_path),
    )

    assert output_path.exists()
    payload = json.loads(output_path.read_text())
    assert payload == {'analysis_output_fid': 'fid-XYZ'}


def test_run_silent_path_on_available_status(
    patched_environment,  # noqa: ARG001
    monkeypatch,
    tmp_path: Path,  # noqa: ARG001
):
    """AVAILABLE is the happy path: no raise, output written. We don't
    assert on log level here (the docstring just says 'log info'), only
    that the function completes and the output is correct."""
    _patch_create_object(monkeypatch, 'AVAILABLE', folder_id='fid-OK')
    output_path = tmp_path / 'analysis_output_fid.json'

    prepare_ica_for_analysis.run(
        cohort=_fake_cohort(),
        output=cpg_utils.to_path(output_path),
    )

    payload = json.loads(output_path.read_text())
    assert payload == {'analysis_output_fid': 'fid-OK'}
