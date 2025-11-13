"""
This module centralizes all interactions with the Illumina Connected Analytics
(ICA) command-line interface (CLI), `icav2`. It provides helper functions for
authentication and running CLI commands via subprocess.
"""

import json
import os
from typing import TYPE_CHECKING, Any, Final

from loguru import logger

from dragen_align_pa import utils

if TYPE_CHECKING:
    from subprocess import CompletedProcess

# --- Constants ---

ICA_CLI_SETUP: Final = """
mkdir -p $HOME/.icav2
echo "server-url: ica.illumina.com" > /root/.icav2/config.yaml

set +x
gcloud secrets versions access latest --secret=illumina_cpg_workbench_api --project=cpg-common | jq -r .apiKey > key
gcloud secrets versions access latest --secret=illumina_cpg_workbench_api --project=cpg-common | jq -r .projectID > projectID
echo "x-api-key: $(cat key)" >> $HOME/.icav2/config.yaml
icav2 projects enter $(cat projectID)
set -x
"""  # noqa: E501

# --- CLI Wrappers ---


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

        logger.info(f'Found {file_name} at path: {file_path}')
        return file_path

    except json.JSONDecodeError:
        logger.error(f'Failed to decode JSON from icav2 list command: {result.stdout}')
        raise
    except (ValueError, IndexError) as e:
        logger.error(f'Error parsing icav2 list output for {file_name}: {e}')
        raise


def perform_upload_if_needed(cram_status: str | None, paths: dict[str, str]) -> None:
    """
    Handles the actual download from GCS and upload to ICA using CLIs.
    (Used by upload_data_to_ica.py)
    """
    if cram_status == 'AVAILABLE':
        logger.info(f'{paths["cram_name"]} already AVAILABLE in ICA. Skipping.')
        return

    # Authenticate ICA CLI
    logger.info('Authenticating ICA CLI...')
    # This command uses shell=True, but ICA_CLI_SETUP is a trusted constant
    utils.run_subprocess_with_log(ICA_CLI_SETUP, 'Authenticate ICA CLI', shell=True)  # noqa: S604

    local_dir = os.path.dirname(paths['local_cram_path'])
    if not os.path.exists(local_dir):
        os.makedirs(local_dir, exist_ok=True)
        logger.info(f'Created local directory: {local_dir}')

    # Download from GCS to local disk
    logger.info(
        f'Downloading {paths["cram_name"]} from GCS to {paths["local_cram_path"]}...',
    )
    utils.run_subprocess_with_log(
        ['gcloud', 'storage', 'cp', paths['gcs_cram_path'], paths['local_cram_path']],
        f'Download {paths["cram_name"]}',
    )

    logger.info(
        f'Uploading {paths["local_cram_path"]} to ICA (using CLI for large file)...',
    )
    utils.run_subprocess_with_log(
        [
            'icav2',
            'projectdata',
            'upload',
            paths['local_cram_path'],
            paths['ica_folder_path'],
        ],
        f'Upload {paths["cram_name"]}',
    )

    # Clean up the large local file
    try:
        os.remove(paths['local_cram_path'])
        logger.info(f'Removed local file: {paths["local_cram_path"]}')
    except OSError as e:
        logger.warning(
            f'Could not remove local file {paths["local_cram_path"]}: {e}',
        )
