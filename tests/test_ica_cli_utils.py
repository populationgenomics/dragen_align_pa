"""Unit tests for `ica_cli_utils.authenticate_ica_cli`.

Auth writes `~/.icav2/config.yaml` in Python (reusing the `get_ica_secrets` guard) and then
runs `icav2 projects enter` — no `gcloud`/`jq` shell step, and the API key never enters a
command string (so it can't leak into the command `run_subprocess_with_log` logs).
"""

from pathlib import Path

import pytest

from dragen_align_pa import ica_cli_utils


def test_authenticate_writes_config_in_python_and_enters_project(monkeypatch, tmp_path: Path):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(
        'dragen_align_pa.ica_cli_utils.ica_api_utils.get_ica_secrets',
        lambda project_name: {'projectID': 'x', 'apiKey': 'SECRET-KEY'},  # noqa: ARG005
    )
    captured: list[list[str]] = []
    monkeypatch.setattr(
        'dragen_align_pa.ica_cli_utils.utils.run_subprocess_with_log',
        lambda cmd, step_name: captured.append(cmd),  # noqa: ARG005
    )

    ica_cli_utils.authenticate_ica_cli('OurDNA-DRAGEN-378')

    config = (tmp_path / '.icav2' / 'config.yaml').read_text()
    assert 'server-url: ica.illumina.com' in config
    assert 'x-api-key: SECRET-KEY' in config
    # Only the project-enter command is shell-executed; the key is never in a command string.
    assert captured == [['icav2', 'projects', 'enter', '5c3a60b0-1458-4e37-8877-ec6b25dc4003']]


def test_authenticate_propagates_missing_secret_guard(monkeypatch, tmp_path: Path):
    """The get_ica_secrets guard's failure surfaces here rather than writing a broken config."""
    monkeypatch.setenv('HOME', str(tmp_path))

    def _raise(project_name):  # noqa: ARG001
        raise KeyError("secret has no non-empty 'tenk10k_apiKey' field")

    monkeypatch.setattr('dragen_align_pa.ica_cli_utils.ica_api_utils.get_ica_secrets', _raise)

    with pytest.raises(KeyError, match=r'tenk10k_apiKey'):
        ica_cli_utils.authenticate_ica_cli('Tenk10k_Dragen_378')

    assert not (tmp_path / '.icav2' / 'config.yaml').exists()
