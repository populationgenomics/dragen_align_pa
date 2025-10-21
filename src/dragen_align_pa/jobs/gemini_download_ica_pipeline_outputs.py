"""
Download all non CRAM / GVCF outputs from ICA using the Python SDK.
"""

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
    BUCKET,
    GCP_FOLDER_FOR_ICA_DOWNLOAD,
    ICA_REST_ENDPOINT,
)


def _initalise_bulk_download_job(sequencing_group: SequencingGroup) -> PythonJob:
    """
    Initialise a PythonJob for downloading bulk data from ICA.
    """
    bulk_download_job: PythonJob = get_batch().new_python_job(
        name='DownloadDataFromIca',
        attributes=(sequencing_group.get_job_attrs() or {}) | {'tool': 'ICA-Python'},  # type: ignore[ReportUnknownVariableType]
    )
    bulk_download_job.image(image=get_driver_image())
    bulk_download_job.spot(is_spot=False)
    bulk_download_job.memory(memory='8Gi')
    return bulk_download_job


def download_bulk_data_from_ica(
    sequencing_group: SequencingGroup,
    pipeline_id_arguid_path: cpg_utils.Path,
) -> PythonJob:
    """
    Creates a PythonJob to download all metric files from an ICA analysis run.
    """
    job: PythonJob = _initalise_bulk_download_job(sequencing_group=sequencing_group)

    job.call(
        _run,
        sequencing_group=sequencing_group,
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


def _list_and_filter_ica_files(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    base_ica_folder_path: str,
) -> list[tuple[str, str]]:
    """
    Lists all files in the ICA folder, handles pagination,
    and filters out CRAMs/GVCFs.
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
            logger.error(f'Exception when calling ProjectDataApi->get_project_data_list: {e}')
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
            if not file_name.endswith(('.cram', '.cram.crai', '.gvcf.gz', '.gvcf.gz.tbi')):
                files_to_download.append((file_name, file_id))  # pyright: ignore[reportUnknownArgumentType]

        page_token = api_response.body.get('nextPageToken')  # pyright: ignore[reportUnknownVariableType]
        if not page_token:
            break  # Exit loop if no more pages

    logger.info(f'Found {len(files_to_download)} files to download.')
    return files_to_download


def _stream_files_to_gcs(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    files_to_download: list[tuple[str, str]],
    gcs_bucket: Bucket,
    gcs_output_path_prefix: str,
) -> None:
    """
    Streams files from ICA pre-signed URLs directly to GCS blobs.
    """
    bucket_name = gcs_bucket.name  # pyright: ignore[reportUnknownVariableType]
    for file_name, file_id in files_to_download:
        # Define the full GCS path for the file
        gcs_blob_path = f'{gcs_output_path_prefix}/{file_name}'
        blob = gcs_bucket.blob(gcs_blob_path)

        logger.info(f'Streaming {file_name} (ID: {file_id}) to gs://{bucket_name}/{gcs_blob_path}')

        try:
            # Get a pre-signed URL
            url_response = api_instance.create_download_url_for_data(  # pyright: ignore[reportUnknownVariableType]
                path_params=path_parameters | {'dataId': file_id}  # type: ignore[reportArgumentType]
            )
            download_url = url_response.body['url']  # pyright: ignore[reportUnknownVariableType]

            # Download and upload as a stream
            with requests.get(download_url, stream=True, timeout=300) as r:  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
                r.raise_for_status()
                # r.raw is the raw byte stream (a file-like object)
                blob.upload_from_file(r.raw, timeout=300)  # pyright: ignore[reportUnknownArgumentType]

        except icasdk.ApiException as e:
            logger.error(f'Failed to get download URL for {file_name} (ID: {file_id}): {e}')
        except requests.RequestException as e:
            logger.error(f'Failed to stream/download {file_name} (ID: {file_id}): {e}')
        except gcs_exceptions.GoogleCloudError as e:  # <-- This is the more specific exception
            logger.error(f'An error occurred uploading to GCS for {file_name}: {e}')


def _run(
    sequencing_group: SequencingGroup,
    pipeline_id_arguid_path: cpg_utils.Path,
) -> None:
    """
    The main Python function for the download job.
    Coordinates helper functions to list, filter, and stream files.
    """
    sg_name: str = sequencing_group.name
    ica_analysis_output_folder: str = config_retrieve(['ica', 'data_prep', 'output_folder'])
    logger.info(f'Downloading bulk ICA data for {sg_name}.')

    # --- Get Pipeline ID and AR GUID ---
    pipeline_id, ar_guid = _get_pipeline_details(pipeline_id_arguid_path)
    base_ica_folder_path = (
        f'/{BUCKET}/{ica_analysis_output_folder}/{sg_name}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name}/'
    )
    logger.info(f'Targeting ICA folder: {base_ica_folder_path}')

    # --- Setup GCS Client ---
    gcs_output_path_prefix = f'{GCP_FOLDER_FOR_ICA_DOWNLOAD}/dragen_metrics/{sg_name}'
    bucket_name = str(BUCKET).removeprefix('gs://')
    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(bucket_name)

    # --- Secure ICA Authentication ---
    secrets: dict[Literal['projectID', 'apiKey'], str] = utils.get_ica_secrets()
    configuration = icasdk.Configuration(host=ICA_REST_ENDPOINT)
    configuration.api_key['ApiKeyAuth'] = secrets['apiKey']
    path_parameters: dict[str, str] = {'projectId': secrets['projectID']}

    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)

        # --- List, filter, and download files ---
        files_to_download = _list_and_filter_ica_files(
            api_instance=api_instance,
            path_parameters=path_parameters,
            base_ica_folder_path=base_ica_folder_path,
        )

        _stream_files_to_gcs(
            api_instance=api_instance,
            path_parameters=path_parameters,
            files_to_download=files_to_download,
            gcs_bucket=gcs_bucket,
            gcs_output_path_prefix=gcs_output_path_prefix,
        )

    logger.info('All files streamed to GCS successfully.')
