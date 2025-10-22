"""
Uploads large CRAM files from GCS to ICA.

Uses a hybrid PythonJob approach:
- icasdk (Python) is used for API calls (checking existence, getting IDs).
- gcloud CLI (subprocess) is used to download the large file from GCS.
- icav2 CLI (subprocess) is used to upload the large local file to ICA,
  bypassing the Python SDK's 10MB file size limit.
"""

import json
import os
import subprocess
from typing import Any, Literal

import cpg_utils
import icasdk
from cpg_flow.targets import SequencingGroup
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import utils
from dragen_align_pa.constants import BUCKET, ICA_CLI_SETUP, ICA_REST_ENDPOINT
from dragen_align_pa.utils import validate_cli_path_input


def _initalise_upload_job(sequencing_group: SequencingGroup) -> PythonJob:
    """
    Initialise a PythonJob for uploading data to ICA.
    """
    upload_job: PythonJob = get_batch().new_python_job(
        name='UploadDataToIca',
        attributes=sequencing_group.get_job_attrs() or {} | {'tool': 'ICA-Python'},  # type: ignore[ReportUnknownVariableType]
    )

    upload_job.image(image=get_driver_image())
    # Storage is requested to hold the CRAM file locally during transfer
    upload_job.storage(utils.calculate_needed_storage(cram=str(sequencing_group.cram)))
    upload_job.memory('8Gi')
    upload_job.spot(is_spot=False)

    return upload_job


def upload_data_to_ica(sequencing_group: SequencingGroup, output: str) -> PythonJob:
    """
    Creates a PythonJob to upload a CRAM file to ICA.
    """
    upload_folder = config_retrieve(['ica', 'data_prep', 'upload_folder'])

    job: PythonJob = _initalise_upload_job(sequencing_group=sequencing_group)

    job.call(
        _run,
        sequencing_group=sequencing_group,
        output_path_str=output,
        upload_folder=upload_folder,
    )

    return job


def _get_file_details_from_ica(
    api_instance: project_data_api.ProjectDataApi,
    path_params: dict[str, str],
    ica_folder_path: str,
    file_name: str,
) -> dict[str, Any] | None:
    """
    Checks if a file exists in ICA and returns its 'data' block if found.
    """
    try:
        query_params: dict[str, Any] = {
            'parentFolderPath': ica_folder_path,
            'filename': [file_name],
            'filenameMatchMode': 'EXACT',
            'pageSize': '2',
        }

        api_response = api_instance.get_project_data_list(
            path_params=path_params,
            query_params=query_params,
        )
        items = api_response.body.get('items', [])
        if len(items) > 0:
            return items[0]['data']  # pyright: ignore[reportUnknownVariableType]

    except icasdk.ApiException as e:
        logger.error(f'API error checking for file {file_name}: {e}')
        # Don't raise, just return None
    return None


def _setup_paths(sequencing_group: SequencingGroup, upload_folder: str) -> dict[str, str]:
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
        raise ValueError(f'Unexpected path for sequencing_group.cram: {gcs_base_path}')
    else:
        gcs_cram_path = gcs_base_path

    logger.info(f'Resolved CRAM path to upload: {gcs_cram_path}')

    batch_tmpdir = os.environ.get('BATCH_TMPDIR', '/batch')
    local_cram_path = os.path.join(batch_tmpdir, sg_name, cram_name)

    return {
        'sg_name': sg_name,
        'cram_name': cram_name,
        'gcs_cram_path': gcs_cram_path,
        'local_cram_path': local_cram_path,
        'ica_folder_path': f'/{BUCKET}/{upload_folder}/{sg_name}/',
    }


def _check_file_existence(
    api_instance: project_data_api.ProjectDataApi,
    path_params: dict[str, str],
    ica_folder_path: str,
    cram_name: str,
) -> str | None:
    """
    Checks if the CRAM file already exists in ICA and returns its status.
    """
    logger.info(f'Checking existence of {cram_name}...')
    cram_data = _get_file_details_from_ica(api_instance, path_params, ica_folder_path, cram_name)
    if cram_data:
        return cram_data['details']['status']  # pyright: ignore[reportUnknownVariableType]
    return None


def _perform_upload_if_needed(cram_status: str | None, paths: dict[str, str]) -> None:
    """
    Handles the actual download from GCS and upload to ICA using CLIs.
    """
    if cram_status == 'AVAILABLE':
        logger.info(f'{paths["cram_name"]} already AVAILABLE in ICA. Skipping.')
        return

    # Authenticate ICA CLI
    logger.info('Authenticating ICA CLI...')
    # This command uses shell=True, but ICA_CLI_SETUP is a trusted constant
    subprocess.run(ICA_CLI_SETUP, shell=True, check=True, executable='/bin/bash')

    local_dir = os.path.dirname(paths['local_cram_path'])
    if not os.path.exists(local_dir):
        os.makedirs(local_dir, exist_ok=True)
        logger.info(f'Created local directory: {local_dir}')

    # Download from GCS to local disk
    logger.info(f'Downloading {paths["cram_name"]} from GCS to {paths["local_cram_path"]}...')
    subprocess.run(
        ['gcloud', 'storage', 'cp', paths['gcs_cram_path'], paths['local_cram_path']],
        check=True,
    )

    logger.info(f'Uploading {paths["local_cram_path"]} to ICA (using CLI for large file)...')
    subprocess.run(
        ['icav2', 'projectdata', 'upload', paths['local_cram_path'], paths['ica_folder_path']],
        check=True,
    )

    # Clean up the large local file
    try:
        os.remove(paths['local_cram_path'])
        logger.info(f'Removed local file: {paths["local_cram_path"]}')
    except OSError as e:
        logger.warning(f'Could not remove local file {paths["local_cram_path"]}: {e}')


def _finalize_upload(
    api_instance: project_data_api.ProjectDataApi,
    path_params: dict[str, str],
    paths: dict[str, str],
    output_path_str: str,
) -> None:
    """
    Re-fetches the file ID from ICA and writes the output JSON file.
    """
    logger.info(f'Re-fetching file ID for {paths["sg_name"]}...')
    cram_data = _get_file_details_from_ica(api_instance, path_params, paths['ica_folder_path'], paths['cram_name'])

    cram_fid = cram_data['id'] if cram_data else None  # pyright: ignore[reportUnknownVariableType]

    if not cram_fid:
        raise ValueError(f'Failed to find file ID in ICA after upload for {paths["sg_name"]}.')

    logger.info(f'CRAM FID: {cram_fid}')

    # Write only the CRAM FID to the output JSON
    output_data = {'cram_fid': cram_fid}
    with cpg_utils.to_path(output_path_str).open('w') as f:
        json.dump(output_data, f)

    logger.info(f'Successfully uploaded {paths["cram_name"]} for {paths["sg_name"]}.')


def _run(
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
    secrets: dict[Literal['projectID', 'apiKey'], str] = utils.get_ica_secrets()
    project_id: str = secrets['projectID']
    path_params: dict[str, str] = {'projectId': project_id}

    configuration = icasdk.Configuration(host=ICA_REST_ENDPOINT)
    configuration.api_key['ApiKeyAuth'] = secrets['apiKey']

    # 3. --- Check File Existence ---
    cram_status: str | None = None
    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        cram_status = _check_file_existence(api_instance, path_params, paths['ica_folder_path'], paths['cram_name'])

    # 4. --- Perform Upload (if needed) ---
    _perform_upload_if_needed(cram_status, paths)

    # 5. --- Get Final File ID and Write Output ---
    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        _finalize_upload(api_instance, path_params, paths, output_path_str)
