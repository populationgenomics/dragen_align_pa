"""Unit tests for `ica_cli_utils`.

Auth writes `~/.icav2/config.yaml` in Python (reusing the `get_ica_api_key` guard) and then
runs `icav2 projects enter` — no `gcloud`/`jq` shell step, and the API key never enters a
command string (so it can't leak into the command `run_subprocess_with_log` logs).

Every icav2 invocation goes through the shared transient-error retry: the CLI surfaces an
ICA rate-limit only as exit code 1 with `ICA_API_429` in its output, so one 429 on the
JWT fetch must not kill an upload job while the same 429 on an SDK call is survivable.
"""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from loguru import logger as loguru_logger

from dragen_align_pa import ica_cli_utils
from dragen_align_pa.constants.constants_registry import ROLE_DRAGEN_ALIGN

# The observed production failure: the CLI's JWT fetch is rate-limited, it proceeds
# without a token, and exits 1 with both messages on stdout.
_RATE_LIMITED_JWT_OUTPUT = (
    'Error when fetching JWT :  429 Too Many Requests : ICA_API_429 : '
    'Too many requests. Please try again later! (ref. b59769f6)\n'
    '401 Unauthorized : ICA_SEC_002 : Unauthorized (ref. 52c1f587)\n'
)


def _rate_limited_error(cmd: list[str]) -> subprocess.CalledProcessError:
    return subprocess.CalledProcessError(1, cmd, output=_RATE_LIMITED_JWT_OUTPUT, stderr='')


def test_authenticate_writes_config_in_python_and_enters_project(monkeypatch, tmp_path: Path):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(
        'dragen_align_pa.ica_cli_utils.ica_api_utils.get_ica_api_key',
        lambda: 'SECRET-KEY',
    )
    captured: list[list[str]] = []
    monkeypatch.setattr(
        'dragen_align_pa.ica_cli_utils.utils.run_subprocess_with_log',
        lambda cmd, step_name, log_failure=True: captured.append(cmd),  # noqa: ARG005
    )

    # Default family (conftest: project_root='ourdna') → dragen-align id below.
    ica_cli_utils.authenticate_ica_cli(ROLE_DRAGEN_ALIGN)

    config = (tmp_path / '.icav2' / 'config.yaml').read_text()
    assert 'server-url: ica.illumina.com' in config
    assert 'x-api-key: SECRET-KEY' in config
    # Only the project-enter command is shell-executed; the key is never in a command string.
    assert captured == [['icav2', 'projects', 'enter', '5c3a60b0-1458-4e37-8877-ec6b25dc4003']]


def test_authenticate_propagates_missing_secret_guard(monkeypatch, tmp_path: Path):
    """The get_ica_api_key guard's failure surfaces here rather than writing a broken config."""
    monkeypatch.setenv('HOME', str(tmp_path))

    def _raise():
        raise KeyError("secret has no non-empty 'tenk10k_apiKey' field")

    monkeypatch.setattr('dragen_align_pa.ica_cli_utils.ica_api_utils.get_ica_api_key', _raise)

    with pytest.raises(KeyError, match=r'tenk10k_apiKey'):
        ica_cli_utils.authenticate_ica_cli(ROLE_DRAGEN_ALIGN)

    assert not (tmp_path / '.icav2' / 'config.yaml').exists()


# --- icav2 transient-error retry ---


def test_enter_project_retries_rate_limited_cli_then_succeeds(monkeypatch, tmp_path: Path):
    """A 429 on the CLI's JWT fetch must be retried, not kill the job (the
    originating production failure in upload_data_to_ica)."""
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(
        'dragen_align_pa.ica_cli_utils.ica_api_utils.get_ica_api_key',
        lambda: 'SECRET-KEY',
    )
    enter_cmd = ['icav2', 'projects', 'enter', '5c3a60b0-1458-4e37-8877-ec6b25dc4003']
    run = MagicMock(side_effect=[_rate_limited_error(enter_cmd), subprocess.CompletedProcess(enter_cmd, 0)])
    monkeypatch.setattr('dragen_align_pa.ica_cli_utils.utils.run_subprocess_with_log', run)

    ica_cli_utils.authenticate_ica_cli(ROLE_DRAGEN_ALIGN)

    assert run.call_count == 2


def test_upload_retries_rate_limited_cli_then_succeeds(monkeypatch):
    """`icav2 projectdata upload` fetches a JWT too, so it dies the same way
    without retry. A retried upload takes the same recovery path as a stage
    re-run over a PARTIAL file (see the comment in `upload_local_file`)."""
    upload_cmd = ['icav2', 'projectdata', 'upload', '/io/x.cram', '/upload/x/']
    run = MagicMock(side_effect=[_rate_limited_error(upload_cmd), subprocess.CompletedProcess(upload_cmd, 0)])
    monkeypatch.setattr('dragen_align_pa.ica_cli_utils.utils.run_subprocess_with_log', run)

    ica_cli_utils.upload_local_file('/io/x.cram', '/upload/x/')

    assert run.call_count == 2


def test_cli_retry_does_not_retry_permanent_failures(monkeypatch):
    """Exit 1 without a transient marker (bad args, missing file, auth denied)
    is a real error — surface it on the first attempt."""
    upload_cmd = ['icav2', 'projectdata', 'upload', '/io/x.cram', '/upload/x/']
    run = MagicMock(
        side_effect=subprocess.CalledProcessError(1, upload_cmd, output='', stderr='no such local file'),
    )
    monkeypatch.setattr('dragen_align_pa.ica_cli_utils.utils.run_subprocess_with_log', run)

    with pytest.raises(subprocess.CalledProcessError):
        ica_cli_utils.upload_local_file('/io/x.cram', '/upload/x/')

    assert run.call_count == 1


def test_cli_retry_gives_up_after_persistent_rate_limit(monkeypatch):
    """If every attempt is rate-limited, the original CalledProcessError
    eventually surfaces (default 10 retries => 11 total attempts)."""
    # Relies on conftest's autouse `_instant_retry_sleeps` (else this really sleeps for
    # minutes) and on `_TEST_CONFIG` having no [ica.retry] max_retries key, so the
    # asserted 11 is the production default.
    upload_cmd = ['icav2', 'projectdata', 'upload', '/io/x.cram', '/upload/x/']
    run = MagicMock(side_effect=_rate_limited_error(upload_cmd))
    monkeypatch.setattr('dragen_align_pa.ica_cli_utils.utils.run_subprocess_with_log', run)

    with pytest.raises(subprocess.CalledProcessError):
        ica_cli_utils.upload_local_file('/io/x.cram', '/upload/x/')

    assert run.call_count == 11


def test_find_ica_file_retries_rate_limited_cli_then_succeeds(monkeypatch):
    """The read-only `projectdata list` lookup is idempotent and goes through
    the same retry."""
    list_result = subprocess.CompletedProcess(
        ['icav2'],
        0,
        stdout='{"items": [{"details": {"path": "/found/x.cram"}}]}',
    )
    run = MagicMock(side_effect=[_rate_limited_error(['icav2']), list_result])
    monkeypatch.setattr('dragen_align_pa.ica_cli_utils.utils.run_subprocess_with_log', run)

    path = ica_cli_utils.find_ica_file_path_by_name('/found', 'x.cram')

    assert path == '/found/x.cram'
    assert run.call_count == 2


def test_cli_retry_matches_503_backend_unavailable(monkeypatch):
    """503 (ICA backend unavailable) is the other transient class the SDK path
    retries; the CLI path must treat it the same."""
    upload_cmd = ['icav2', 'projectdata', 'upload', '/io/x.cram', '/upload/x/']
    error = subprocess.CalledProcessError(
        1,
        upload_cmd,
        output='Error when fetching JWT :  503 Service Unavailable : ICA_API_503 (ref. abc)\n',
        stderr='',
    )
    run = MagicMock(side_effect=[error, subprocess.CompletedProcess(upload_cmd, 0)])
    monkeypatch.setattr('dragen_align_pa.ica_cli_utils.utils.run_subprocess_with_log', run)

    ica_cli_utils.upload_local_file('/io/x.cram', '/upload/x/')

    assert run.call_count == 2


def test_download_file_by_id_retries_rate_limited_cli_then_succeeds(monkeypatch):
    """`icav2 projectdata download` (used by the MLR job for its config JSON)
    fetches a JWT like every icav2 call, so it needs the same retry."""
    run = MagicMock(
        side_effect=[_rate_limited_error(['icav2']), subprocess.CompletedProcess(['icav2'], 0)],
    )
    monkeypatch.setattr('dragen_align_pa.ica_cli_utils.utils.run_subprocess_with_log', run)

    ica_cli_utils.download_file_by_id('fid-123', '/io/mlr_config.json')

    assert run.call_count == 2
    cmd = run.call_args.args[0]
    assert cmd[:3] == ['icav2', 'projectdata', 'download']
    assert 'fid-123' in cmd
    assert '/io/mlr_config.json' in cmd
    assert '--exclude-source-path' in cmd


# --- Retry logging contract ---
#
# Log monitoring alerts on ERROR lines: every retried attempt must log its full
# failure detail at ERROR with a 'RETRYING' marker (so monitoring can note it
# without acting), and only the final, propagating failure logs ERROR without
# the marker.


def _capture_error_logs():
    """Attach a loguru sink collecting formatted ERROR-level messages; returns (records, sink_id)."""
    records: list[str] = []
    sink_id = loguru_logger.add(lambda message: records.append(str(message)), level='ERROR')
    return records, sink_id


def test_retried_attempts_log_error_with_retrying_marker(monkeypatch):
    """Two rate-limited attempts before success => two ERROR logs marked
    RETRYING (with the failure detail), and no unmarked failure ERROR at all —
    an unmarked ERROR must mean action is needed."""
    upload_cmd = ['icav2', 'projectdata', 'upload', '/io/x.cram', '/upload/x/']
    run = MagicMock(
        side_effect=[
            _rate_limited_error(upload_cmd),
            _rate_limited_error(upload_cmd),
            subprocess.CompletedProcess(upload_cmd, 0),
        ],
    )
    monkeypatch.setattr('dragen_align_pa.ica_cli_utils.utils.run_subprocess_with_log', run)
    records, sink_id = _capture_error_logs()
    try:
        ica_cli_utils.upload_local_file('/io/x.cram', '/upload/x/')
    finally:
        loguru_logger.remove(sink_id)

    retrying = [r for r in records if 'RETRYING' in r]
    assert len(retrying) == 2
    # The marked log must carry the failure detail monitoring/debugging needs.
    assert any('ICA_API_429' in r for r in retrying)
    assert all('RETRYING' in r for r in records)


def test_final_failure_logs_single_error_without_retrying_marker(monkeypatch):
    """When every attempt is rate-limited, attempts 1..10 log RETRYING ERRORs
    and the final propagating failure logs exactly one unmarked ERROR block."""
    upload_cmd = ['icav2', 'projectdata', 'upload', '/io/x.cram', '/upload/x/']
    run = MagicMock(side_effect=_rate_limited_error(upload_cmd))
    monkeypatch.setattr('dragen_align_pa.ica_cli_utils.utils.run_subprocess_with_log', run)
    records, sink_id = _capture_error_logs()
    try:
        with pytest.raises(subprocess.CalledProcessError):
            ica_cli_utils.upload_local_file('/io/x.cram', '/upload/x/')
    finally:
        loguru_logger.remove(sink_id)

    assert len([r for r in records if 'RETRYING' in r]) == 10
    unmarked_failures = [r for r in records if 'RETRYING' not in r and 'failed with return code' in r]
    assert len(unmarked_failures) == 1
