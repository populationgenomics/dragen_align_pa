"""
This module centralizes all interactions with the Illumina Connected Analytics (ICA)
API and CLI. It provides helper functions for authentication, file/folder operations,
pipeline status checking, and data streaming.
"""

import hashlib
import json
import os
import subprocess
from typing import TYPE_CHECKING, Any, Final, Literal

if TYPE_CHECKING:
    from collections.abc import Sequence

import cpg_utils
import icasdk
import requests
from google.cloud import exceptions as gcs_exceptions
from google.cloud import secretmanager
from google.cloud.storage.bucket import Bucket
from icasdk.apis.tags import project_analysis_api, project_data_api
from icasdk.model.create_data import CreateData
from loguru import logger

from dragen_align_pa.constants import ICA_CLI_SETUP

# --- Secret Management ---

SECRET_CLIENT: Final = secretmanager.SecretManagerServiceClient()
SECRET_PROJECT: Final = 'cpg-common'
SECRET_NAME: Final = 'illumina_cpg_workbench_api'
SECRET_VERSION: Final = 'latest'


def get_ica_secrets() -> dict[Literal['projectID', 'apiKey'], str]:
    """Gets the project ID and API key used to interact with ICA

    Returns:
        dict[str, str]: A dictionary with the keys projectId and apiKey
    """
    secret_path: str = SECRET_CLIENT.secret_version_path(
        project=SECRET_PROJECT,
        secret=SECRET_NAME,
        secret_version=SECRET_VERSION,
    )
    response: secretmanager.AccessSecretVersionResponse = SECRET_CLIENT.access_secret_version(
        request={'name': secret_path},
    )
    return json.loads(response.payload.data.decode('UTF-8'))


# --- CLI Interaction ---


def run_cli_command(
    command: str | list[str],
    capture_output: bool = False,
    shell: bool = False,
) -> subprocess.CompletedProcess:
    """
    Runs a subprocess command with robust error logging.
    """
    executable = '/bin/bash' if shell else None
    cmd_str = command if isinstance(command, str) else ' '.join(command)

    try:
        logger.info(f'Running command: {cmd_str}')
        return subprocess.run(
            command,
            check=True,
            text=True,
            capture_output=capture_output,
            shell=shell,
            executable=executable,
        )
    except subprocess.CalledProcessError as e:
        logger.error(f'Command failed with return code {e.returncode}: {cmd_str}')
        if e.stdout:
            logger.error(f'STDOUT: {e.stdout.strip()}')
        if e.stderr:
            logger.error(f'STDERR: {e.stderr.strip()}')
        # Re-raise as a generic exception to fail the job
        raise ValueError('A subprocess command failed. See logs.') from e


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
    result = run_cli_command(command, capture_output=True)
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


# --- Pipeline Management (SDK) ---


def check_ica_pipeline_status(
    api_instance: project_analysis_api.ProjectAnalysisApi,
    path_params: dict[str, str],
) -> str:
    """Check the status of an ICA pipeline via a pipeline ID

    Args:
        api_instance (project_analysis_api.ProjectAnalysisApi): An instance of the ProjectAnalysisApi
        path_params (dict[str, str]): Dict with projectId and analysisId

    Raises:
        icasdk.ApiException: Any exception if the API call is incorrect

    Returns:
        str: The status of the pipeline. Can be one of ['REQUESTED', 'AWAITINGINPUT', 'INPROGRESS', 'SUCCEEDED', 'FAILED', 'FAILEDFINAL', 'ABORTED']
    """  # noqa: E501
    try:
        api_response = api_instance.get_analysis(path_params=path_params)  # type: ignore[ReportUnknownVariableType]
        pipeline_status: str = api_response.body['status']  # type: ignore[ReportUnknownVariableType]
        return pipeline_status  # type: ignore[ReportUnknownVariableType]
    except icasdk.ApiException as e:
        raise icasdk.ApiException(
            f'Exception when calling ProjectAnalysisApi -> get_analysis: {e}',
        ) from e


# --- Data Operations (SDK) ---


def check_object_already_exists(
    api_instance: project_data_api.ProjectDataApi,
    path_params: dict[str, str],
    file_name: str,
    folder_path: str,
    object_type: str,
) -> str | None:
    """Check if an object already exists in ICA, as trying to create another object at
    the same path causes an error

    Args:
        api_instance (project_data_api.ProjectDataApi): An instance of the ProjectDataApi
        path_params (dict[str, str]): A dict with the projectId
        file_name (str): The name of the object that you want to check in ICA e.g.
        folder_path (str): The path to the object that you want to create in ICA.
        object_type (str): The type of hte object to create in ICA. Must be one of ['FILE', 'FOLDER']

    Raises:
        NotImplementedError: Only checks for files with the status 'PARTIAL'
        icasdk.ApiException: Other API errors

    Returns:
        str | None: The object ID, if it exists, or else None
    """
    query_params: dict[str, Sequence[str] | list[str] | str] = {
        'filePath': [f'{folder_path}/{file_name}'],
        'filePathMatchMode': 'STARTS_WITH_CASE_INSENSITIVE',
        'type': object_type,
    }
    if object_type == 'FILE':
        query_params = {  # pyright: ignore[reportUnknownVariableType]
            'filename': [file_name],
            'filenameMatchMode': 'EXACT',
        } | query_params
    logger.info(
        f'Checking to see if the {object_type} object already exists at {folder_path}/{file_name}',
    )
    try:
        api_response = api_instance.get_project_data_list(  # type: ignore[ReportUnknownVariableType]
            path_params=path_params,  # type: ignore[ReportUnknownVariableType]
            query_params=query_params,  # type: ignore[ReportUnknownVariableType]
        )  # type: ignore[ReportUnknownVariableType]
        if len(api_response.body['items']) == 0:  # type: ignore[ReportUnknownVariableType]
            return None
        status: str | None = api_response.body['items'][0]['data']['details']['status']  # pyright: ignore[reportUnknownVariableType]
        if object_type == 'FOLDER' or status == 'PARTIAL':
            return api_response.body['items'][0]['data']['id']  # pyright: ignore[reportUnknownVariableType]
        if status == 'AVAILABLE':  # pyright: ignore[reportPossiblyUnboundVariable]
            return status
        # Statuses are ["PARTIAL", "AVAILABLE", "ARCHIVING", "ARCHIVED", "UNARCHIVING", "DELETING", ]
        raise NotImplementedError('Checking for other status is not implemented yet.')
    except icasdk.ApiException as e:
        raise icasdk.ApiException(
            f'Exception when calling ProjectDataApi -> get_project_data_list: {e}',
        ) from e


def create_upload_object_id(
    api_instance: project_data_api.ProjectDataApi,
    path_params: dict[str, str],
    sg_name: str,
    file_name: str,
    folder_path: str,
    object_type: str,
) -> str:
    """Create an object in ICA that can be used to upload data to,
    or to write analysis outputs into

    Args:
        api_instance (project_data_api.ProjectDataApi): An instance of the ProjectDataApi
        path_params (dict[str, str]): A dict with the projectId
        sg_name (str): The name of the sequencing group
        file_name (str): The name of the file to upload e.g. CPGxxxx.CRAM
        folder_path (str): The base path to the object in ICA to create
        object_type (str): The type of the object to create. Must be one of ['FILE', 'FOLDER']

    Raises:
        icasdk.ApiException: Any API error

    Returns:
        str: The ID of the object that was created, or the existing ID if it was already present.
    """
    existing_object_id: str | None = check_object_already_exists(
        api_instance=api_instance,
        path_params=path_params,
        file_name=file_name,
        folder_path=folder_path,
        object_type=object_type,
    )
    logger.info(f'{existing_object_id}')
    if existing_object_id:
        return existing_object_id
    logger.info(f'Creating a new {object_type} object at {folder_path}/{file_name}')
    try:
        if object_type == 'FILE':
            body = CreateData(
                name=file_name,
                folderPath=f'{folder_path}/',
                dataType=object_type,
            )
        else:
            body = CreateData(
                name=sg_name,
                folderPath=f'{folder_path}/',
                dataType=object_type,
            )
        api_response = api_instance.create_data_in_project(  # type: ignore[ReportUnknownVariableType]
            path_params=path_params,  # type: ignore[ReportUnknownVariableType]
            body=body,
        )
        return api_response.body['data']['id']  # type: ignore[ReportUnknownVariableType]
    except icasdk.ApiException as e:
        raise icasdk.ApiException(
            f'Exception when calling ProjectDataApi -> create_data_in_project: {e}',
        ) from e


def find_file_id_by_name(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    parent_folder_path: str,
    file_name: str,
) -> str:
    """
    Finds a specific file ID in an ICA folder by its exact name.
    (Used by download_specific_files_from_ica.py)
    """
    logger.info(f"Searching for file '{file_name}' in '{parent_folder_path}'...")
    try:
        api_response = api_instance.get_project_data_list(  # pyright: ignore[reportUnknownVariableType]
            path_params=path_parameters,
            query_params={  # pyright: ignore[reportUnknownVariableType]
                'parentFolderPath': parent_folder_path,
                'filename': [file_name],
                'filenameMatchMode': 'EXACT',
                'pageSize': '2',
            },
        )

        items = api_response.body.get('items', [])  # pyright: ignore[reportUnknownVariableType]
        if len(items) == 0:  # pyright: ignore[reportUnknownArgumentType]
            raise FileNotFoundError(
                f'File not found in ICA: {parent_folder_path}{file_name}',
            )
        if len(items) > 1:  # pyright: ignore[reportUnknownArgumentType]
            logger.warning(
                f"Found multiple files named '{file_name}'; using the first one.",
            )

        file_id = items[0]['data'].get('id')  # pyright: ignore[reportUnknownVariableType]
        if not file_id:
            raise ValueError(f"Found file item for '{file_name}' but it has no ID.")

        logger.info(f'Found file ID: {file_id}')
        return file_id  # pyright: ignore[reportUnknownVariableType]

    except icasdk.ApiException as e:
        logger.error(f"API Error finding file '{file_name}': {e}")
        raise
    except Exception as e:
        logger.error(f"Error finding file '{file_name}': {e}")
        raise


def get_md5_from_ica(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    md5_file_id: str,
) -> tuple[str, str]:
    """
    Downloads the content of the MD5 file from ICA.
    (Used by download_specific_files_from_ica.py)
    Returns (expected_hash, file_content).
    """
    try:
        url_response = api_instance.create_download_url_for_data(  # pyright: ignore[reportUnknownVariableType]
            path_params=path_parameters | {'dataId': md5_file_id},
        )
        download_url = url_response.body['url']  # pyright: ignore[reportUnknownVariableType]

        response = requests.get(
            download_url,
            timeout=300,
        )  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
        response.raise_for_status()

        content = response.text  # pyright: ignore[reportUnknownVariableType]
        # Handle both md5sum (hash filename) and md5 (hash only) formats
        expected_hash = content.split()[0]  # pyright: ignore[reportUnknownVariableType]
        return expected_hash, content  # pyright: ignore[reportUnknownVariableType]

    except icasdk.ApiException as e:
        logger.error(
            f'Failed to get download URL for MD5 file (ID: {md5_file_id}): {e}',
        )
        raise
    except requests.RequestException as e:
        logger.error(f'Failed to download MD5 content (ID: {md5_file_id}): {e}')
        raise


def stream_file_to_gcs_and_verify(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    file_id: str,
    file_name: str,
    gcs_bucket: Bucket,
    gcs_prefix: str,
    expected_md5_hash: str | None = None,
) -> None:
    """
    Streams a file from ICA to GCS and optionally verifies its MD5.
    (Used by download_specific_files_from_ica.py)
    """
    gcs_blob_path = f'{gcs_prefix}/{file_name}'
    blob = gcs_bucket.blob(gcs_blob_path)
    bucket_name = gcs_bucket.name  # pyright: ignore[reportUnknownVariableType]

    logger.info(
        f'Streaming {file_name} (ID: {file_id}) to gs://{bucket_name}/{gcs_blob_path}',
    )

    try:
        # 1. Get pre-signed URL
        url_response = api_instance.create_download_url_for_data(  # pyright: ignore[reportUnknownVariableType]
            path_params=path_parameters | {'dataId': file_id},  # pyright: ignore[reportArgumentType]
        )
        download_url = url_response.body['url']  # pyright: ignore[reportUnknownVariableType]

        # 2. Download and upload as a stream
        md5_hasher = hashlib.md5()
        with requests.get(
            download_url,
            stream=True,
            timeout=600,
        ) as r:  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
            r.raise_for_status()

            # Stream directly to GCS
            with blob.open('wb', timeout=600) as gcs_file:  # pyright: ignore[reportUnknownArgumentType]
                for chunk in r.iter_content(
                    chunk_size=1024 * 1024 * 8,
                ):  # pyright: ignore[reportUnknownVariableType] # 8MB chunks
                    gcs_file.write(chunk)  # pyright: ignore[reportUnknownArgumentType]
                    md5_hasher.update(chunk)  # pyright: ignore[reportUnknownArgumentType]

        actual_md5_hash = md5_hasher.hexdigest()
        logger.info(
            f'Finished streaming {file_name}. Actual MD5: {actual_md5_hash}',
        )

        # 3. Verify MD5 if provided
        if expected_md5_hash:
            if actual_md5_hash != expected_md5_hash:
                logger.error(f'MD5 MISMATCH for {file_name}!')
                logger.error(f'  Expected: {expected_md5_hash}')
                logger.error(f'  Actual:   {actual_md5_hash}')
                # Delete the corrupted file from GCS
                try:
                    blob.delete()
                except gcs_exceptions.GoogleCloudError as del_e:
                    logger.error(
                        f'Failed to delete corrupted file {gcs_blob_path}: {del_e}',
                    )
                raise ValueError(f'MD5 mismatch for {file_name}')
            logger.info(f'MD5 checksum OK for {file_name}.')

    except icasdk.ApiException as e:
        logger.error(
            f'Failed to get download URL for {file_name} (ID: {file_id}): {e}',
        )
        raise
    except requests.RequestException as e:
        logger.error(f'Failed to stream/download {file_name} (ID: {file_id}): {e}')
        raise
    except gcs_exceptions.GoogleCloudError as e:
        logger.error(f'An error occurred uploading to GCS for {file_name}: {e}')
        raise


def list_and_filter_ica_files(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    base_ica_folder_path: str,
) -> list[tuple[str, str]]:
    """
    Lists all files in the ICA folder, handles pagination,
    and filters out CRAMs/GVCFs.
    (Used by download_ica_pipeline_outputs.py)
    """
    files_to_download: list[tuple[str, str]] = []
    page_token: str | None = None
    logger.info('Listing files in ICA folder...')

    while True:
        query_params = {  # pyright: ignore[reportUnknownVariableType]
            'parentFolderPath': base_ica_folder_path,
            'pageSize': '1000',
        }
        if page_token:
            query_params['pageToken'] = page_token

        try:
            api_response = api_instance.get_project_data_list(  # pyrisght: ignore[reportUnknownVariableType] # pyright: ignore[reportUnknownVariableType]
                path_params=path_parameters,  # pyright: ignore[reportArgumentType]
                query_params=query_params,  # type: ignore[reportArgumentType]
            )
        except icasdk.ApiException as e:
            logger.error(
                f'Exception when calling ProjectDataApi->get_project_data_list: {e}',
            )
            raise

        # --- Filter files ---
        for item in api_response.body['items']:  # pyright: ignore[reportUnknownVariableType]
            details = item['data'].get('details', {})  # pyright: ignore[reportUnknownVariableType]
            file_name = details.get('name')  # pyright: ignore[reportUnknownVariableType]
            file_id = item['data'].get('id')  # pyright: ignore[reportUnknownVariableType]

            if not file_name or not file_id:
                logger.warning(f'Skipping item with missing name or id: {item}')
                continue

            # Exclude CRAMs, GVCFs, and their indices
            if not file_name.endswith(
                ('.cram', '.cram.crai', '.gvcf.gz', '.gvcf.gz.tbi'),
            ):
                files_to_download.append(
                    (file_name, file_id),
                )  # pyright: ignore[reportUnknownArgumentType]

        page_token = api_response.body.get(
            'nextPageToken',
        )  # pyright: ignore[reportUnknownVariableType]
        if not page_token:
            break  # Exit loop if no more pages

    logger.info(f'Found {len(files_to_download)} files to download.')
    return files_to_download


def stream_files_to_gcs(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    files_to_download: list[tuple[str, str]],
    gcs_bucket: Bucket,
    gcs_output_path_prefix: str,
) -> None:
    """
    Streams files from ICA pre-signed URLs directly to GCS blobs.
    (Used by download_ica_pipeline_outputs.py)
    """
    bucket_name = gcs_bucket.name  # pyright: ignore[reportUnknownVariableType]
    for file_name, file_id in files_to_download:
        # Define the full GCS path for the file
        gcs_blob_path = f'{gcs_output_path_prefix}/{file_name}'
        blob = gcs_bucket.blob(gcs_blob_path)

        logger.info(
            f'Streaming {file_name} (ID: {file_id}) to gs://{bucket_name}/{gcs_blob_path}',
        )

        try:
            # Get a pre-signed URL
            url_response = api_instance.create_download_url_for_data(  # pyright: ignore[reportUnknownVariableType]
                path_params=path_parameters | {'dataId': file_id},  # type: ignore[reportArgumentType]
            )
            download_url = url_response.body['url']  # pyright: ignore[reportUnknownVariableType]

            # Download and upload as a stream
            with requests.get(
                download_url,
                stream=True,
                timeout=300,
            ) as r:  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
                r.raise_for_status()
                # r.raw is the raw byte stream (a file-like object)
                blob.upload_from_file(
                    r.raw,
                    timeout=300,
                )  # pyright: ignore[reportUnknownArgumentType]

        except icasdk.ApiException as e:
            logger.error(
                f'Failed to get download URL for {file_name} (ID: {file_id}): {e}',
            )
        except requests.RequestException as e:
            logger.error(
                f'Failed to stream/download {file_name} (ID: {file_id}): {e}',
            )
        except gcs_exceptions.GoogleCloudError as e:  # <-- This is the more specific exception
            logger.error(f'An error occurred uploading to GCS for {file_name}: {e}')


def get_file_details_from_ica(
    api_instance: project_data_api.ProjectDataApi,
    path_params: dict[str, str],
    ica_folder_path: str,
    file_name: str,
) -> dict[str, Any] | None:
    """
    Checks if a file exists in ICA and returns its 'data' block if found.
    (Used by upload_data_to_ica.py)
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


def check_file_existence(
    api_instance: project_data_api.ProjectDataApi,
    path_params: dict[str, str],
    ica_folder_path: str,
    cram_name: str,
) -> str | None:
    """
    Checks if the CRAM file already exists in ICA and returns its status.
    (Used by upload_data_to_ica.py)
    """
    logger.info(f'Checking existence of {cram_name}...')
    cram_data = get_file_details_from_ica(
        api_instance,
        path_params,
        ica_folder_path,
        cram_name,
    )
    if cram_data:
        return cram_data['details']['status']  # pyright: ignore[reportUnknownVariableType]
    return None


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
    run_cli_command(ICA_CLI_SETUP, shell=True)

    local_dir = os.path.dirname(paths['local_cram_path'])
    if not os.path.exists(local_dir):
        os.makedirs(local_dir, exist_ok=True)
        logger.info(f'Created local directory: {local_dir}')

    # Download from GCS to local disk
    logger.info(
        f'Downloading {paths["cram_name"]} from GCS to {paths["local_cram_path"]}...',
    )
    run_cli_command(
        ['gcloud', 'storage', 'cp', paths['gcs_cram_path'], paths['local_cram_path']],
    )

    logger.info(
        f'Uploading {paths["local_cram_path"]} to ICA (using CLI for large file)...',
    )
    run_cli_command(
        [
            'icav2',
            'projectdata',
            'upload',
            paths['local_cram_path'],
            paths['ica_folder_path'],
        ],
    )

    # Clean up the large local file
    try:
        os.remove(paths['local_cram_path'])
        logger.info(f'Removed local file: {paths["local_cram_path"]}')
    except OSError as e:
        logger.warning(
            f'Could not remove local file {paths["local_cram_path"]}: {e}',
        )


def finalize_upload(
    api_instance: project_data_api.ProjectDataApi,
    path_params: dict[str, str],
    paths: dict[str, str],
    output_path_str: str,
) -> None:
    """
    Re-fetches the file ID from ICA and writes the output JSON file.
    (Used by upload_data_to_ica.py)
    """
    logger.info(f'Re-fetching file ID for {paths["sg_name"]}...')
    cram_data = get_file_details_from_ica(
        api_instance,
        path_params,
        paths['ica_folder_path'],
        paths['cram_name'],
    )

    cram_fid = cram_data['id'] if cram_data else None  # pyright: ignore[reportUnknownVariableType]

    if not cram_fid:
        raise ValueError(
            f'Failed to find file ID in ICA after upload for {paths["sg_name"]}.',
        )

    logger.info(f'CRAM FID: {cram_fid}')

    # Write only the CRAM FID to the output JSON
    output_data = {'cram_fid': cram_fid}
    with cpg_utils.to_path(output_path_str).open('w') as f:
        json.dump(output_data, f)

    logger.info(
        f'Successfully uploaded {paths["cram_name"]} for {paths["sg_name"]}.',
    )
