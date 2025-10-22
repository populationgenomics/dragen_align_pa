"""
Uploads large CRAM files from GCS to ICA.

Uses a hybrid PythonJob approach:
- icasdk (Python) is used for API calls (checking existence, getting IDs).
- icav2 (CLI) is called via subprocess for the large file uploads,
  bypassing the Python SDK's 10MB file size limit.
"""

import json
import subprocess
from typing import Any, Literal

import cpg_utils
import icasdk
from cpg_flow.targets import SequencingGroup
from cpg_utils.cloud import get_path_components_from_gcp_path
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import utils
from dragen_align_pa.constants import ICA_CLI_SETUP, ICA_REST_ENDPOINT

# Import the new validator
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
    # Storage and memory requests from the original BashJob
    upload_job.storage(utils.calculate_needed_storage(cram=str(sequencing_group.cram)))
    upload_job.memory('8Gi')
    upload_job.spot(is_spot=False)

    return upload_job


def upload_data_to_ica(sequencing_group: SequencingGroup, output: str) -> PythonJob:
    """
    Creates a PythonJob to upload a CRAM file to ICA.
    """
    upload_folder = config_retrieve(['ica', 'data_prep', 'upload_folder'])
    bucket: str = get_path_components_from_gcp_path(str(sequencing_group.cram))['bucket']

    job: PythonJob = _initalise_upload_job(sequencing_group=sequencing_group)

    job.call(
        _run,
        sequencing_group=sequencing_group,
        output_path_str=output,
        upload_folder=upload_folder,
        bucket=bucket,
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


def _run(
    sequencing_group: SequencingGroup,
    output_path_str: str,
    upload_folder: str,
    bucket: str,
) -> None:
    """
    Main function for the PythonJob.
    Coordinates SDK checks and CLI uploads for the CRAM file.
    """

    # 1. --- Setup Names and Paths ---
    sg_name: str = sequencing_group.name
    cram_name = f'{sg_name}.cram'

    # The sequencing_group.cram attribute might point to the .crai
    # We must manually ensure we are using the .cram path.
    gcs_base_path = str(sequencing_group.cram)
    if gcs_base_path.endswith('.cram.crai'):
        gcs_cram_path = gcs_base_path.removesuffix('.crai')
    elif not gcs_base_path.endswith('.cram'):
        # This is a safety check in case the path is something unexpected
        raise ValueError(f'Unexpected path for sequencing_group.cram: {gcs_base_path}')
    else:
        # It already ends with .cram, so it's correct
        gcs_cram_path = gcs_base_path

    logger.info(f'Resolved CRAM path to upload: {gcs_cram_path}')
    # -------------------------------------------

    ica_folder_path = f'/{bucket}/{upload_folder}/{sg_name}/'

    logger.info(f'Starting upload process for {sg_name}')
    logger.info(f'Target ICA folder: {ica_folder_path}')

    # --- Validate inputs before use ---
    validate_cli_path_input(gcs_cram_path, 'gcs_cram_path')
    validate_cli_path_input(ica_folder_path, 'ica_folder_path')
    # ----------------------------------

    # 2. --- Authenticate Python SDK ---
    secrets: dict[Literal['projectID', 'apiKey'], str] = utils.get_ica_secrets()
    project_id: str = secrets['projectID']
    path_params: dict[str, str] = {'projectId': project_id}

    configuration = icasdk.Configuration(host=ICA_REST_ENDPOINT)
    configuration.api_key['ApiKeyAuth'] = secrets['apiKey']

    cram_status: str | None = None

    # 3. --- Check File Existence with SDK ---
    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)

        logger.info(f'Checking existence of {cram_name}...')
        cram_data = _get_file_details_from_ica(api_instance, path_params, ica_folder_path, cram_name)
        if cram_data:
            cram_status = cram_data['details']['status']  # pyright: ignore[reportUnknownVariableType]

    # 4. --- Authenticate ICA CLI ---
    # This is required for the subsequent subprocess.run calls
    logger.info('Authenticating ICA CLI...')
    # This command uses shell=True, but ICA_CLI_SETUP is a trusted constant
    subprocess.run(ICA_CLI_SETUP, shell=True, check=True, executable='/bin/bash')

    # 5. --- Upload to ICA (CRAM) ---
    if cram_status == 'AVAILABLE':
        logger.info(f'{cram_name} already AVAILABLE in ICA. Skipping.')
    else:
        logger.info(f'Streaming {cram_name} from GCS to ICA (using CLI for large file)...')

        # This call is safe: shell=False and inputs are validated
        subprocess.run(['icav2', 'projectdata', 'upload', gcs_cram_path, ica_folder_path], check=True)

    # 6. --- Get File ID and Write Output ---
    cram_fid: str | None = None
    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)

        logger.info(f'Re-fetching file ID for {sg_name}...')
        cram_data = _get_file_details_from_ica(api_instance, path_params, ica_folder_path, cram_name)

        if cram_data:
            cram_fid = cram_data['id']  # pyright: ignore[reportUnknownVariableType]

    if not cram_fid:
        raise ValueError(f'Failed to find file ID in ICA after upload for {sg_name}.')

    logger.info(f'CRAM FID: {cram_fid}')

    # Write only the CRAM FID to the output JSON
    output_data = {'cram_fid': cram_fid}
    with cpg_utils.to_path(output_path_str).open('w') as f:
        json.dump(output_data, f)

    logger.info(f'Successfully uploaded {cram_name} for {sg_name}.')
