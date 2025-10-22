"""
Download specific files (e.g., CRAM, GVCF) from ICA using the Python SDK.
"""

import hashlib
import json
from typing import Literal

import cpg_utils
import icasdk
import requests
from cpg_flow.targets import SequencingGroup
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from google.cloud import exceptions as gcs_exceptions
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import utils
from dragen_align_pa.constants import (
    BUCKET_NAME,
    ICA_REST_ENDPOINT,
)


def _initalise_download_job(sequencing_group: SequencingGroup, job_name: str) -> PythonJob:
    """
    Initialise a PythonJob for downloading specific files from ICA.
    """
    download_job: PythonJob = get_batch().new_python_job(
        name=job_name,
        attributes=(sequencing_group.get_job_attrs() or {}) | {'tool': 'ICA-Python'},  # type: ignore[ReportUnknownVariableType]
    )

    download_job.image(image=get_driver_image())
    download_job.storage('8Gi')  # Based on original BashJob
    download_job.memory('8Gi')  # Based on original BashJob
    download_job.spot(is_spot=False)

    return download_job


def download_data_from_ica(
    job_name: str,
    sequencing_group: SequencingGroup,
    filetype: str,
    gcp_folder_for_ica_download: str,
    pipeline_id_arguid_path: cpg_utils.Path,
) -> PythonJob:
    """
    Creates a PythonJob to download a specific data file, its index, and its MD5
    from an ICA analysis run, verify the MD5, and upload all three to GCS.
    """
    sg_name: str = sequencing_group.name
    logger.info(f'Queueing Python job to download {filetype} for {sg_name}')

    job: PythonJob = _initalise_download_job(sequencing_group=sequencing_group, job_name=job_name)

    job.call(
        _run,
        sequencing_group=sequencing_group,
        filetype=filetype,
        gcp_folder_for_ica_download=gcp_folder_for_ica_download,
        pipeline_id_arguid_path=pipeline_id_arguid_path,
    )
    return job


def _get_pipeline_details(
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


def _find_file_id_by_name(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    parent_folder_path: str,
    file_name: str,
) -> str:
    """
    Finds a specific file ID in an ICA folder by its exact name.
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
            raise FileNotFoundError(f'File not found in ICA: {parent_folder_path}{file_name}')
        if len(items) > 1:  # pyright: ignore[reportUnknownArgumentType]
            logger.warning(f"Found multiple files named '{file_name}'; using the first one.")

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


def _get_md5_from_ica(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    md5_file_id: str,
) -> tuple[str, str]:
    """
    Downloads the content of the MD5 file from ICA.
    Returns (expected_hash, file_content).
    """
    try:
        url_response = api_instance.create_download_url_for_data(  # pyright: ignore[reportUnknownVariableType]
            path_params=path_parameters | {'dataId': md5_file_id}
        )
        download_url = url_response.body['url']  # pyright: ignore[reportUnknownVariableType]

        response = requests.get(download_url, timeout=300)  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
        response.raise_for_status()

        content = response.text  # pyright: ignore[reportUnknownVariableType]
        # Handle both md5sum (hash filename) and md5 (hash only) formats
        expected_hash = content.split()[0]  # pyright: ignore[reportUnknownVariableType]
        return expected_hash, content  # pyright: ignore[reportUnknownVariableType]

    except icasdk.ApiException as e:
        logger.error(f'Failed to get download URL for MD5 file (ID: {md5_file_id}): {e}')
        raise
    except requests.RequestException as e:
        logger.error(f'Failed to download MD5 content (ID: {md5_file_id}): {e}')
        raise


def _stream_file_to_gcs_and_verify(
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
    """
    gcs_blob_path = f'{gcs_prefix}/{file_name}'
    blob = gcs_bucket.blob(gcs_blob_path)
    bucket_name = gcs_bucket.name  # pyright: ignore[reportUnknownVariableType]

    logger.info(f'Streaming {file_name} (ID: {file_id}) to gs://{bucket_name}/{gcs_blob_path}')

    try:
        # 1. Get pre-signed URL
        url_response = api_instance.create_download_url_for_data(  # pyright: ignore[reportUnknownVariableType]
            path_params=path_parameters | {'dataId': file_id}  # pyright: ignore[reportArgumentType]
        )
        download_url = url_response.body['url']  # pyright: ignore[reportUnknownVariableType]

        # 2. Download and upload as a stream
        md5_hasher = hashlib.md5()
        with requests.get(download_url, stream=True, timeout=600) as r:  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
            r.raise_for_status()

            # Stream directly to GCS
            with blob.open('wb', timeout=600) as gcs_file:  # pyright: ignore[reportUnknownArgumentType]
                for chunk in r.iter_content(chunk_size=1024 * 1024 * 8):  # pyright: ignore[reportUnknownVariableType] # 8MB chunks
                    gcs_file.write(chunk)  # pyright: ignore[reportUnknownArgumentType]
                    md5_hasher.update(chunk)  # pyright: ignore[reportUnknownArgumentType]

        actual_md5_hash = md5_hasher.hexdigest()
        logger.info(f'Finished streaming {file_name}. Actual MD5: {actual_md5_hash}')

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
                    logger.error(f'Failed to delete corrupted file {gcs_blob_path}: {del_e}')
                raise ValueError(f'MD5 mismatch for {file_name}')
            logger.info(f'MD5 checksum OK for {file_name}.')

    except icasdk.ApiException as e:
        logger.error(f'Failed to get download URL for {file_name} (ID: {file_id}): {e}')
        raise
    except requests.RequestException as e:
        logger.error(f'Failed to stream/download {file_name} (ID: {file_id}): {e}')
        raise
    except gcs_exceptions.GoogleCloudError as e:
        logger.error(f'An error occurred uploading to GCS for {file_name}: {e}')
        raise


def _get_file_suffixes(filetype: str) -> tuple[str, str, str, str]:
    """
    Maps a filetype string to its corresponding GCS prefix and
    file name suffixes for data, index, and md5.

    Returns:
        (gcs_prefix, data_suffix, index_suffix, md5_suffix)
    """
    if filetype == 'cram':
        return 'cram', 'cram', 'cram.crai', 'md5sum'

    if filetype == 'base_gvcf':
        return (
            'base_gvcf',
            'hard-filtered.gvcf.gz',
            'hard-filtered.gvcf.gz.tbi',
            'md5sum',
        )

    if filetype == 'recal_gvcf':
        return (
            'recal_gvcf',
            'hard-filtered.recal.gvcf.gz',
            'hard-filtered.recal.gvcf.gz.tbi',
            'md5',  # Note: .md5, not .md5sum
        )

    logger.error(f'Unknown filetype: {filetype}')
    raise ValueError(f'Unknown filetype: {filetype}')


def _orchestrate_download(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    base_ica_folder_path: str,
    gcs_bucket: Bucket,
    gcs_output_path_prefix: str,
    main_file_name: str,
    index_file_name: str,
    md5_file_name: str,
    md5_gcp_name: str,
) -> None:
    """
    Finds, downloads, verifies, and uploads the set of files.
    This function contains the core operational logic.
    """
    try:
        # --- 1. Find all three file IDs ---
        main_file_id = _find_file_id_by_name(api_instance, path_parameters, base_ica_folder_path, main_file_name)
        index_file_id = _find_file_id_by_name(api_instance, path_parameters, base_ica_folder_path, index_file_name)
        md5_file_id = _find_file_id_by_name(api_instance, path_parameters, base_ica_folder_path, md5_file_name)

        # --- 2. Get expected MD5 hash ---
        expected_hash, md5_content = _get_md5_from_ica(api_instance, path_parameters, md5_file_id)
        logger.info(f'Expected MD5 for {main_file_name} is {expected_hash}')

        # --- 3. Stream main file, verifying MD5 ---
        _stream_file_to_gcs_and_verify(
            api_instance=api_instance,
            path_parameters=path_parameters,
            file_id=main_file_id,
            file_name=main_file_name,
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_output_path_prefix,
            expected_md5_hash=expected_hash,
        )

        # --- 4. Stream index file (no verification) ---
        _stream_file_to_gcs_and_verify(
            api_instance=api_instance,
            path_parameters=path_parameters,
            file_id=index_file_id,
            file_name=index_file_name,
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_output_path_prefix,
            expected_md5_hash=None,
        )

        # --- 5. Upload the MD5 file itself ---
        logger.info(f'Uploading MD5 file to {gcs_output_path_prefix}/{md5_gcp_name}')
        md5_blob = gcs_bucket.blob(f'{gcs_output_path_prefix}/{md5_gcp_name}')
        md5_blob.upload_from_string(md5_content)

    except Exception as e:
        logger.error(f'Failed to process files: {e}')
        raise  # Re-raise to fail the job


def _run(
    sequencing_group: SequencingGroup,
    filetype: str,
    gcp_folder_for_ica_download: str,
    pipeline_id_arguid_path: cpg_utils.Path,
) -> None:
    """
    The main Python function for the download job.
    Coordinates helper functions to list, filter, and stream files.
    """
    sg_name: str = sequencing_group.name
    ica_analysis_output_folder: str = config_retrieve(['ica', 'data_prep', 'output_folder'])
    logger.info(f'Downloading {filetype} data for {sg_name}.')

    # --- 1. Determine file names ---
    gcp_prefix, data_suffix, index_suffix, md5_suffix = _get_file_suffixes(filetype)

    main_file_name = f'{sg_name}.{data_suffix}'
    index_file_name = f'{sg_name}.{index_suffix}'
    md5_file_name = f'{sg_name}.{data_suffix}.{md5_suffix}'
    md5_gcp_name = f'{sg_name}.{data_suffix}.md5sum'  # Always save as .md5sum in GCS

    # --- 2. Get Pipeline ID and AR GUID ---
    pipeline_id, ar_guid = _get_pipeline_details(pipeline_id_arguid_path)
    base_ica_folder_path = (
        f'/{BUCKET_NAME}/{ica_analysis_output_folder}/{sg_name}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name}/'
    )
    logger.info(f'Targeting ICA folder: {base_ica_folder_path}')

    # --- 3. Setup GCS Client ---
    gcs_output_path_prefix = f'{gcp_folder_for_ica_download}/{gcp_prefix}'
    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(BUCKET_NAME)

    # --- 4. Secure ICA Authentication ---
    secrets: dict[Literal['projectID', 'apiKey'], str] = utils.get_ica_secrets()
    configuration = icasdk.Configuration(host=ICA_REST_ENDPOINT)
    configuration.api_key['ApiKeyAuth'] = secrets['apiKey']
    path_parameters: dict[str, str] = {'projectId': secrets['projectID']}

    # --- 5. Run Orchestration ---
    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        _orchestrate_download(
            api_instance=api_instance,
            path_parameters=path_parameters,
            base_ica_folder_path=base_ica_folder_path,
            gcs_bucket=gcs_bucket,
            gcs_output_path_prefix=gcs_output_path_prefix,
            main_file_name=main_file_name,
            index_file_name=index_file_name,
            md5_file_name=md5_file_name,
            md5_gcp_name=md5_gcp_name,
        )

    logger.info(f'Successfully downloaded and verified all files for {sg_name}.')
