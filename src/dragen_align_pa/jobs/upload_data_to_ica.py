"""
Uploads large CRAM files from GCS to ICA.

Uses a hybrid PythonJob approach:
- icasdk (Python) is used for API calls (checking existence, getting IDs).
- gcloud CLI (subprocess) is used to download the large file from GCS.
- icav2 CLI (subprocess) is used to upload the large local file to ICA,
  bypassing the Python SDK's 10MB file size limit.
"""

import os
from typing import Literal

from cpg_flow.targets import SequencingGroup
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_cli_utils, ica_utils
from dragen_align_pa.constants import BUCKET_NAME
from dragen_align_pa.utils import validate_cli_path_input


def _setup_paths(
    sequencing_group: SequencingGroup,
    upload_folder: str,
) -> dict[str, str]:
    """
    Resolves and returns all necessary paths and names for the job.
    """
    sg_name: str = sequencing_group.name
    cram_name = f'{sg_name}.cram'

    # Ensure we get the .cram path, not .crai
    gcs_base_path = str(sequencing_group.cram)
    if gcs_base_path.endswith('.cram.crai'):
        gcs_cram_path = gcs_base_path.removesuffix('.crai')
    elif not gcs_base_path.endswith('.cram'):
        raise ValueError(
            f'Unexpected path for sequencing_group.cram: {gcs_base_path}',
        )
    else:
        gcs_cram_path = gcs_base_path

    logger.info(f'Resolved CRAM path to upload: {gcs_cram_path}')

    batch_tmpdir = os.environ.get('BATCH_TMPDIR', '/io')
    local_cram_path = os.path.join(batch_tmpdir, sg_name, cram_name)

    return {
        'sg_name': sg_name,
        'cram_name': cram_name,
        'gcs_cram_path': gcs_cram_path,
        'local_cram_path': local_cram_path,
        'ica_folder_path': f'/{BUCKET_NAME}/{upload_folder}/{sg_name}/',
    }


def run(
    sequencing_group: SequencingGroup,
    output_path_str: str,
    upload_folder: str,
) -> None:
    """
    Main function for the PythonJob.
    Orchestrates SDK checks and CLI uploads for the CRAM file.
    """

    # 1. --- Setup Names and Paths ---
    paths = _setup_paths(sequencing_group, upload_folder)
    validate_cli_path_input(paths['gcs_cram_path'], 'gcs_cram_path')
    validate_cli_path_input(paths['ica_folder_path'], 'ica_folder_path')

    # 2. --- Authenticate Python SDK ---
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    project_id: str = secrets['projectID']
    path_params: dict[str, str] = {'projectId': project_id}

    # 3. --- Check File Existence ---
    cram_status: str | None = None
    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        cram_status = ica_utils.check_file_existence(
            api_instance=api_instance,
            path_params=path_params,
            ica_folder_path=paths['ica_folder_path'],
            cram_name=paths['cram_name'],
        )

        # 4. --- Perform Upload (if needed) ---
        ica_cli_utils.perform_upload_if_needed(cram_status, paths)

        # 5. --- Get Final File ID and Write Output ---
        ica_utils.finalize_upload(
            api_instance=api_instance,
            path_params=path_params,
            paths=paths,
            output_path_str=output_path_str,
        )
