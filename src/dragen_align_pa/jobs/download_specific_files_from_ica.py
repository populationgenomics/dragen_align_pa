"""
Download specific files (e.g., CRAM, GVCF) from ICA using the Python SDK.
"""

import json
from typing import Literal

import cpg_utils
import icasdk
from cpg_flow.targets import SequencingGroup
from cpg_utils.config import config_retrieve, get_driver_image
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_utils, utils
from dragen_align_pa.constants import (
    BUCKET_NAME,
    GCP_FOLDER_FOR_ICA_DOWNLOAD,
    ICA_REST_ENDPOINT,
)


def download_data_from_ica(
    job_name: str,
    sequencing_group: SequencingGroup,
    filetype: str,
    pipeline_id_arguid_path: cpg_utils.Path,
) -> PythonJob:
    """
    Creates a PythonJob to download a specific data file, its index, and its MD5
    from an ICA analysis run, verify the MD5, and upload all three to GCS.
    """
    sg_name: str = sequencing_group.name
    logger.info(f'Queueing Python job to download {filetype} for {sg_name}')

    job: PythonJob = utils.initialise_python_job(
        job_name=job_name,
        target=sequencing_group,
        tool_name='ICA-Python',
    )
    job.image(image=get_driver_image())
    job.storage('8Gi')
    job.memory('8Gi')
    job.spot(is_spot=False)

    job.call(
        _run,
        sequencing_group=sequencing_group,
        filetype=filetype,
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
        main_file_id = ica_utils.find_file_id_by_name(
            api_instance,
            path_parameters,
            base_ica_folder_path,
            main_file_name,
        )
        index_file_id = ica_utils.find_file_id_by_name(
            api_instance,
            path_parameters,
            base_ica_folder_path,
            index_file_name,
        )
        md5_file_id = ica_utils.find_file_id_by_name(
            api_instance,
            path_parameters,
            base_ica_folder_path,
            md5_file_name,
        )

        # --- 2. Get expected MD5 hash ---
        expected_hash, md5_content = ica_utils.get_md5_from_ica(
            api_instance,
            path_parameters,
            md5_file_id,
        )
        logger.info(f'Expected MD5 for {main_file_name} is {expected_hash}')

        # --- 3. Stream main file, verifying MD5 ---
        ica_utils.stream_file_to_gcs_and_verify(
            api_instance=api_instance,
            path_parameters=path_parameters,
            file_id=main_file_id,
            file_name=main_file_name,
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_output_path_prefix,
            expected_md5_hash=expected_hash,
        )

        # --- 4. Stream index file (no verification) ---
        ica_utils.stream_file_to_gcs_and_verify(
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
    pipeline_id_arguid_path: cpg_utils.Path,
) -> None:
    """
    The main Python function for the download job.
    Coordinates helper functions to list, filter, and stream files.
    """
    sg_name: str = sequencing_group.name
    ica_analysis_output_folder: str = config_retrieve(
        ['ica', 'data_prep', 'output_folder'],
    )
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
    gcs_output_path_prefix = f'{GCP_FOLDER_FOR_ICA_DOWNLOAD}/{gcp_prefix}'
    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(BUCKET_NAME)

    # --- 4. Secure ICA Authentication ---
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_utils.get_ica_secrets()
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
