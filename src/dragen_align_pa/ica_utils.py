"""
This module provides high-level helper functions and business logic for
interacting with ICA. It orchestrates calls to the low-level ica_api
and ica_cli modules.
"""

import hashlib
import json
from typing import TYPE_CHECKING

import cpg_utils
import icasdk
import requests
from google.cloud import exceptions as gcs_exceptions
from icasdk.model.create_data import CreateData
from loguru import logger

from dragen_align_pa import ica_api_utils

if TYPE_CHECKING:
    from google.cloud.storage.bucket import Bucket
    from icasdk.apis.tags import project_data_api


def create_upload_object_id(
    api_instance: 'project_data_api.ProjectDataApi',
    path_params: dict[str, str],
    sg_name: str,
    file_name: str,
    folder_path: str,
    object_type: str,
) -> tuple[str, str]:
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
        tuple[str, str]: (object_ID, status)
        Status will be from ICA, e.g. 'AVAILABLE', 'PARTIAL'.
    """
    existing_object_details: tuple[str, str] | None = ica_api_utils.check_object_already_exists(
        api_instance=api_instance,
        path_params=path_params,
        file_name=file_name,
        folder_path=folder_path,
        object_type=object_type,
    )

    if existing_object_details:
        object_id, status = existing_object_details
        logger.info(f'Found existing {object_type} with ID {object_id} and status {status}')
        return object_id, status

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
        new_object_id = api_response.body['data']['id']  # type: ignore[ReportUnknownVariableType]
        new_status = api_response.body['data']['details']['status']  # type: ignore[ReportUnknownVariableType]
        logger.info(f'Created new {object_type} with ID {new_object_id} and status {new_status}')
        return new_object_id, new_status
    except icasdk.ApiException as e:
        raise icasdk.ApiException(
            f'Exception when calling ProjectDataApi -> create_data_in_project: {e}',
        ) from e


def get_md5_from_ica(
    api_instance: 'project_data_api.ProjectDataApi',
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


def stream_ica_file_to_gcs(
    api_instance: 'project_data_api.ProjectDataApi',
    path_parameters: dict[str, str],
    file_id: str,
    file_name: str,
    gcs_bucket: 'Bucket',
    gcs_prefix: str,
    expected_md5_hash: str | None = None,
) -> None:
    """
    Streams a file from ICA to GCS and optionally verifies its MD5.
    (Used by download_specific_files_from_ica.py and download_ica_pipeline_outputs.py)
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
        md5_hasher = hashlib.md5()  # noqa: S324
        with requests.get(
            download_url,  # pyright: ignore[reportUnknownArgumentType]
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
    api_instance: 'project_data_api.ProjectDataApi',
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
            'type': 'FILE',
            'pageSize': '1000',
        }
        if page_token:
            query_params['pageToken'] = page_token

        try:
            api_response = api_instance.get_project_data_list(  # pyright: ignore[reportUnknownVariableType]
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


def check_file_existence(
    api_instance: 'project_data_api.ProjectDataApi',
    path_params: dict[str, str],
    ica_folder_path: str,
    cram_name: str,
) -> str | None:
    """
    Checks if the CRAM file already exists in ICA and returns its status.
    (Used by upload_data_to_ica.py)
    """
    logger.info(f'Checking existence of {cram_name}...')
    cram_data = ica_api_utils.get_file_details_from_ica(
        api_instance,
        path_params,
        ica_folder_path,
        cram_name,
    )
    if cram_data:
        return cram_data['details']['status']  # pyright: ignore[reportUnknownVariableType]
    return None


def finalize_upload(
    api_instance: 'project_data_api.ProjectDataApi',
    path_params: dict[str, str],
    paths: dict[str, str],
    output_path_str: str,
) -> None:
    """
    Re-fetches the file ID from ICA and writes the output JSON file.
    (Used by upload_data_to_ica.py)
    """
    logger.info(f'Re-fetching file ID for {paths["sg_name"]}...')
    cram_data = ica_api_utils.get_file_details_from_ica(
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


def get_pipeline_details(
    pipeline_id_arguid_path: cpg_utils.Path,
) -> tuple[str, str]:
    """
    Loads pipeline ID and AR GUID from the provided path.
    """
    with pipeline_id_arguid_path.open('r') as f:
        data = json.load(f)
        pipeline_id = data['pipeline_id']
        ar_guid = f'_{data["ar_guid"]}_'
    return pipeline_id, ar_guid
