"""
This module centralizes all interactions with the Illumina Connected Analytics
(ICA) command-line interface (CLI), `icav2`. It provides helper functions for
authentication and running CLI commands via subprocess.
"""

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger

from dragen_align_pa import ica_api_utils, utils
from dragen_align_pa.constants.constants_registry import ica_project_id

if TYPE_CHECKING:
    from subprocess import CompletedProcess

# --- CLI Wrappers ---


def _write_icav2_config() -> None:
    """Write the icav2 CLI config (`~/.icav2/config.yaml`) for the configured dataset family.

    Fetches and validates the key with the shared Python guard (`get_ica_api_key`, which raises
    if the family's API-key field is missing or blank), then writes the key straight to the
    config file. The key never enters a shell command string, so it cannot leak into the command
    that `run_subprocess_with_log` logs — unlike fetching it via `gcloud | jq` inside the shell
    (and `set +x` would not have hidden it from that Python-side log line).
    """
    api_key = ica_api_utils.get_ica_api_key()
    config_dir = Path.home() / '.icav2'
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / 'config.yaml').write_text(f'server-url: ica.illumina.com\nx-api-key: {api_key}\n')


def authenticate_ica_cli(role: str) -> None:
    """Configure the icav2 CLI for the configured family and enter `role`'s ICA project.

    Writes the CLI config with the family's API key in Python (no `gcloud`/`jq` shell step, and
    the key never touches a logged command), then enters the project registered for `role`.

    Args:
        role: The ICA role to enter (one of `constants_registry.REQUIRED_ICA_ROLES`).
    """
    _write_icav2_config()
    utils.run_subprocess_with_log(
        ['icav2', 'projects', 'enter', ica_project_id(role)],
        f'Enter ICA {role} project',
    )


def upload_local_file(local_file_path: str, ica_folder_path: str) -> None:
    """
    Uploads a local file to ICA using the icav2 CLI.
    Assumes the CLI is already authenticated.
    """
    utils.run_subprocess_with_log(
        [
            'icav2',
            'projectdata',
            'upload',
            local_file_path,
            ica_folder_path,
        ],
        f'Upload {os.path.basename(local_file_path)} to ICA',
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
    result: CompletedProcess[Any] = utils.run_subprocess_with_log(command, f'Find ICA file {file_name}')
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
        cram_status: The CRAM's current ICA status; `AVAILABLE` means already uploaded, so the
            upload is skipped.
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
