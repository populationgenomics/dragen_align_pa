"""
This module centralizes all interactions with the Illumina Connected Analytics
(ICA) command-line interface (CLI), `icav2`. It provides helper functions for
authentication and running CLI commands via subprocess.
"""

import json
import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger

from dragen_align_pa import ica_api_utils, utils
from dragen_align_pa.constants.constants_registry import ica_project_id

if TYPE_CHECKING:
    from tenacity import RetryCallState

# --- Transient-error retry ---

# icav2 surfaces a transient ICA error only as exit code 1 with the HTTP reason phrase
# and ICA error code in its output (e.g. the JWT fetch prints
# "429 Too Many Requests : ICA_API_429 : Too many requests..."), so retryability is
# matched on the shared textual markers — there is no structured status to inspect.


def _is_transient_cli_error(exc: BaseException) -> bool:
    """Tenacity predicate: True for an icav2 failure whose output shows a transient ICA error."""
    if not isinstance(exc, subprocess.CalledProcessError):
        return False
    output = f'{exc.stdout or ""}\n{exc.stderr or ""}'
    return any(marker in output for marker in ica_api_utils.TRANSIENT_ICA_ERROR_MARKERS)


def _run_icav2_with_retry(cmd: list[str], step_name: str) -> subprocess.CompletedProcess[Any]:
    """Run an icav2 command, retrying transient ICA errors (429/503) with the shared backoff.

    Every failed attempt logs its full detail at ERROR: intermediate (retried) attempts
    carry a RETRYING marker so log monitoring can note them without acting, and only the
    final, propagating failure is unmarked.

    Args:
        cmd: The full icav2 command line.
        step_name: Human-readable step name for logging.
    """

    def run_icav2() -> subprocess.CompletedProcess[Any]:
        # Failure logging is handled here per-attempt (with the RETRYING marker),
        # not inside run_subprocess_with_log, which can't know whether a retry follows.
        return utils.run_subprocess_with_log(cmd, step_name, log_failure=False)

    def log_retrying(retry_state: 'RetryCallState') -> None:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        if not isinstance(exc, subprocess.CalledProcessError):
            return
        sleep = retry_state.next_action.sleep if retry_state.next_action else 0.0
        utils.log_subprocess_failure(
            step_name,
            exc,
            note=f'; RETRYING (attempt {retry_state.attempt_number}) after {sleep:.1f}s',
        )

    try:
        return ica_api_utils.ica_retrying(_is_transient_cli_error, before_sleep=log_retrying)(run_icav2)
    except subprocess.CalledProcessError as exc:
        utils.log_subprocess_failure(step_name, exc)
        raise


# --- CLI Wrappers ---


def _write_icav2_config() -> None:
    """Write the icav2 CLI config (`~/.icav2/config.yaml`) for the configured dataset family."""
    # Fetch/validate the key in Python (get_ica_api_key raises on a missing/blank field) and
    # write it straight to the file — the key never enters a shell command string, so it can't
    # leak into the command that `run_subprocess_with_log` logs.
    api_key = ica_api_utils.get_ica_api_key()
    config_dir = Path.home() / '.icav2'
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / 'config.yaml').write_text(f'server-url: ica.illumina.com\nx-api-key: {api_key}\n')


def authenticate_ica_cli(role: str) -> None:
    """Configure the icav2 CLI for the configured family and enter `role`'s ICA project.

    Args:
        role: The ICA role to enter (one of `constants_registry.REQUIRED_ICA_ROLES`).
    """
    _write_icav2_config()
    _run_icav2_with_retry(
        ['icav2', 'projects', 'enter', ica_project_id(role)],
        f'Enter ICA {role} project',
    )


def upload_local_file(local_file_path: str, ica_folder_path: str) -> None:
    """
    Uploads a local file to ICA using the icav2 CLI.
    Assumes the CLI is already authenticated.
    """
    # Retrying a mid-transfer failure re-runs the upload over whatever state the dead
    # attempt left (possibly a PARTIAL record) — the same recovery path a stage re-run
    # takes through `perform_upload_if_needed`, which re-uploads unless the file is
    # AVAILABLE. Whether icav2 then overwrites the record or creates a duplicate is not
    # documented by Illumina; `ica_utils.finalise_upload`'s existence check is the
    # downstream arbiter either way.
    _run_icav2_with_retry(
        [
            'icav2',
            'projectdata',
            'upload',
            local_file_path,
            ica_folder_path,
        ],
        f'Upload {os.path.basename(local_file_path)} to ICA',
    )


def download_file_by_id(file_id: str, local_file_path: str) -> None:
    """Downloads a single ICA file (by data ID) to a local path using the icav2 CLI.

    Args:
        file_id: The ICA data ID of the file (e.g. `fil.xxxx`).
        local_file_path: The local destination path.
    """
    _run_icav2_with_retry(
        [
            'icav2',
            'projectdata',
            'download',
            file_id,
            local_file_path,
            '--exclude-source-path',
        ],
        f'Download ICA file {file_id}',
    )


def find_ica_file_path_by_name(parent_folder: str, file_name: str) -> str:
    """
    Finds a file in ICA using the CLI and returns its full `details.path`.
    """
    command = [
        'icav2',
        'projectdata',
        'list',
        '--parent-folder',
        parent_folder,
        '--data-type',
        'FILE',
        '--file-name',
        file_name,
        '--match-mode',
        'EXACT',
        '-o',
        'json',
    ]
    result: subprocess.CompletedProcess[Any] = _run_icav2_with_retry(command, f'Find ICA file {file_name}')
    try:
        data = json.loads(result.stdout)
        if not data.get('items'):
            raise ValueError(
                f'No file found with name "{file_name}" in folder "{parent_folder}"',
            )

        file_path = data['items'][0].get('details', {}).get('path')
        if not file_path:
            raise ValueError(
                f'File "{file_name}" found, but it has no "details.path" in API response.',
            )

        return file_path

    except json.JSONDecodeError:
        logger.error(f'Failed to decode JSON from icav2 list command: {result.stdout}')
        raise
    except (ValueError, IndexError) as e:
        logger.error(f'Error parsing icav2 list output for {file_name}: {e}')
        raise


def perform_upload_if_needed(cram_status: str | None, paths: dict[str, str], role: str) -> None:
    """Download a CRAM from GCS and upload it to ICA using the CLIs (used by upload_data_to_ica.py).

    Args:
        cram_status: The CRAM's current ICA status; `AVAILABLE` skips the upload.
        paths: The GCS/local/ICA paths for the CRAM (keys `cram_name`, `local_cram_path`,
            `gcs_cram_path`, `ica_folder_path`).
        role: The ICA role to upload into (one of `constants_registry.REQUIRED_ICA_ROLES`).
    """
    if cram_status == 'AVAILABLE':
        logger.info(f'{paths["cram_name"]} already AVAILABLE in ICA. Skipping.')
        return

    # Authenticate ICA CLI
    authenticate_ica_cli(role)

    local_dir = os.path.dirname(paths['local_cram_path'])
    if not os.path.exists(local_dir):
        os.makedirs(local_dir, exist_ok=True)

    # Download from GCS to local disk
    utils.run_subprocess_with_log(
        ['gcloud', 'storage', 'cp', paths['gcs_cram_path'], paths['local_cram_path']],
        f'Download {paths["cram_name"]}',
    )

    upload_local_file(paths['local_cram_path'], paths['ica_folder_path'])

    # Clean up the large local file
    try:
        os.remove(paths['local_cram_path'])
    except OSError as e:
        logger.warning(
            f'Could not remove local file {paths["local_cram_path"]}: {e}',
        )
