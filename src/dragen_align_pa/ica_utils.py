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
    folder_name: str,
    file_name: str,
    folder_path: str,
    object_type: str,
) -> tuple[str, str]:
    """Create an object in ICA that can be used to upload data to,
    or to write analysis outputs into

    Args:
        api_instance (project_data_api.ProjectDataApi): An instance of the ProjectDataApi
        path_params (dict[str, str]): A dict with the projectId
        folder_name (str): Name used when creating a FOLDER object (ignored for FILE)
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

    try:
        if object_type == 'FILE':
            body = CreateData(
                name=file_name,
                folderPath=f'{folder_path}/',
                dataType=object_type,
            )
        else:
            body = CreateData(
                name=folder_name,
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


def list_ica_files(
    api_instance: 'project_data_api.ProjectDataApi',
    path_parameters: dict[str, str],
    base_ica_folder_path: str,
    *,
    recursive: bool = False,
) -> list[tuple[str, str]]:
    """List files under an ICA folder. Pagination is handled internally.

    Returns ``(name_or_relative_path, file_id)`` tuples.

    With ``recursive=False`` (default), lists only files directly inside
    ``base_ica_folder_path``; the first tuple element is the leaf file
    name. With ``recursive=True``, walks subfolders and returns relative
    paths (e.g. ``'report_files/samples/foo.csv'``) — pass the relative
    path directly to ``stream_ica_file_to_gcs`` as ``file_name`` and the
    GCS object key preserves the nested layout.

    No extension filtering — callers compose any filter they need (e.g.
    ``[(n, f) for n, f in list_ica_files(...) if not n.endswith(...)]``).

    Folder traversal uses separate ``type=FOLDER`` queries — the ICA SDK's
    ``get_project_data_list`` does not expose a recursive flag. The walk
    is not transactional: if a subfolder query fails after some files
    have been collected, those collected entries are discarded and the
    ``icasdk.ApiException`` propagates. Callers should re-run on failure.
    """
    base = base_ica_folder_path.rstrip('/') + '/'

    def _list_children(parent: str, type_: str) -> list[dict]:
        items: list[dict] = []
        page_token: str | None = None
        while True:
            query_params: dict[str, object] = {
                'parentFolderPath': parent,
                'type': type_,
                'pageSize': '1000',
            }
            if page_token:
                query_params['pageToken'] = page_token
            api_response = api_instance.get_project_data_list(  # pyright: ignore[reportUnknownVariableType]
                path_params=path_parameters,  # pyright: ignore[reportArgumentType]
                query_params=query_params,  # type: ignore[reportArgumentType]
            )
            items.extend(api_response.body.get('items', []))  # pyright: ignore[reportUnknownArgumentType]
            page_token = api_response.body.get('nextPageToken')  # pyright: ignore[reportUnknownVariableType]
            if not page_token:
                break
        return items

    files: list[tuple[str, str]] = []

    def _walk(parent: str, relative_prefix: str) -> None:
        for item in _list_children(parent, 'FILE'):
            details = item['data'].get('details', {})  # pyright: ignore[reportUnknownVariableType]
            name = details.get('name')  # pyright: ignore[reportUnknownVariableType]
            fid = item['data'].get('id')  # pyright: ignore[reportUnknownVariableType]
            if not name or not fid:
                logger.warning(f'Skipping item with missing name or id under {parent}: {item}')
                continue
            files.append((f'{relative_prefix}{name}', fid))  # pyright: ignore[reportUnknownArgumentType]

        if not recursive:
            return

        for item in _list_children(parent, 'FOLDER'):
            details = item['data'].get('details', {})  # pyright: ignore[reportUnknownVariableType]
            name = details.get('name')  # pyright: ignore[reportUnknownVariableType]
            if not name:
                continue
            _walk(f'{parent}{name}/', f'{relative_prefix}{name}/')

    _walk(base, '')
    logger.info(f'List under {base} (recursive={recursive}) found {len(files)} files.')
    return files


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
    cram_data = ica_api_utils.get_file_details_from_ica(
        api_instance,
        path_params,
        ica_folder_path,
        cram_name,
    )
    if cram_data:
        return cram_data['details']['status']  # pyright: ignore[reportUnknownVariableType]
    return None


def finalise_upload(
    api_instance: 'project_data_api.ProjectDataApi',
    path_params: dict[str, str],
    paths: dict[str, str],
    output_path_str: str,
) -> None:
    """
    Re-fetches the file ID from ICA and writes the output JSON file.
    (Used by upload_data_to_ica.py)
    """
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

    # Write only the CRAM FID to the output JSON
    output_data = {'cram_fid': cram_fid}
    with cpg_utils.to_path(output_path_str).open('w') as f:
        json.dump(output_data, f)

    logger.info(
        f'Successfully uploaded {paths["cram_name"]} for {paths["sg_name"]}.',
    )
