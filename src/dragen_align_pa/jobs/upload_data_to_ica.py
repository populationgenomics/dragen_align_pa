"""
Uploads large CRAM files from GCS to ICA.

Uses a hybrid PythonJob approach:
- icasdk (Python) is used for API calls (checking existence, getting IDs).
- gcloud CLI (subprocess) is used to download the large file from GCS.
- icav2 CLI (subprocess) is used to upload the large local file to ICA,
  bypassing the Python SDK's 10MB file size limit.
"""

import os

import cpg_utils.config
from cpg_flow.targets import SequencingGroup
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_cli_utils, ica_utils
from dragen_align_pa.constants import BUCKET_NAME, DRAGEN_VERSION, resolve_ica_project_id
from dragen_align_pa.utils import validate_cli_path_input


def _setup_paths(
    sequencing_group: SequencingGroup,
    upload_folder: str,
) -> dict[str, str]:
    """Resolve the GCS source, local scratch, and ICA destination paths for the upload.

    Allows for using Dragen generated CRAMs if the config setting
    'dragen_align_pa.upload_data_to_ica.use_dragen_crams' is true.

    Args:
        sequencing_group: The sequencing group whose CRAM is being uploaded;
            supplies the SG name and, in non-DRAGEN mode, the source CRAM path.
        upload_folder: ICA folder name under which the CRAM is staged.

    Returns:
        A dict with keys 'sg_name', 'cram_name', 'gcs_cram_path',
        'local_cram_path', and 'ica_folder_path'.

    Raises:
        ValueError: In non-DRAGEN mode, if 'sequencing_group.cram.path' is
            neither a '.cram' nor a '.cram.crai' path.
    """
    sg_name: str = sequencing_group.name
    cram_name = f'{sg_name}.cram'

    if cpg_utils.config.config_retrieve(['dragen_align_pa', 'upload_data_to_ica', 'use_dragen_crams'], False):
        gcs_cram_path: cpg_utils.Path | str = (
            sequencing_group.dataset.prefix()
            / 'ica'
            / DRAGEN_VERSION
            / 'output'
            / 'cram'
            / f'{sequencing_group.name}.cram'
        )
    else:
        # Ensure we get the .cram path, not .crai
        gcs_base_path: str = str(sequencing_group.cram.path)
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
        'gcs_cram_path': str(gcs_cram_path),
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
    path_parameters: dict[str, str] = {
        'projectId': resolve_ica_project_id(cpg_utils.config.config_retrieve(['ica', 'projects', 'dragen_align']))
    }

    # 3. --- Check File Existence ---
    cram_status: str | None = None
    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        cram_status = ica_utils.check_file_existence(
            api_instance=api_instance,
            path_params=path_parameters,
            ica_folder_path=paths['ica_folder_path'],
            file_name=paths['cram_name'],
        )

        # 4. --- Perform Upload (if needed) ---
        ica_cli_utils.perform_upload_if_needed(cram_status, paths)

        # 5. --- Get Final File ID and Write Output ---
        ica_utils.finalise_upload(
            api_instance=api_instance,
            path_params=path_parameters,
            paths=paths,
            output_path_str=output_path_str,
        )
