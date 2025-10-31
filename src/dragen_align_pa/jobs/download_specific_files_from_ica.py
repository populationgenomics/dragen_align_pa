"""
Download specific files (e.g., CRAM, GVCF) from ICA using the Python SDK.
"""

from dataclasses import dataclass
from typing import Literal

import cpg_utils
from cpg_flow.targets import SequencingGroup
from cpg_utils.config import config_retrieve
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_utils, utils
from dragen_align_pa.constants import (
    BUCKET_NAME,
)


@dataclass
class FileTypeSpec:
    """Groups the file suffixes and prefixes for a downloadable file type."""

    gcs_prefix: str  # e.g., 'cram'
    data_suffix: str  # e.g., 'cram'
    index_suffix: str  # e.g., 'cram.crai'
    md5_suffix: str  # e.g., 'md5sum' or 'md5'


def _get_file_suffixes(filetype: str) -> FileTypeSpec:
    """
    Maps a filetype string to its corresponding GCS prefix and
    file name suffixes for data, index, and md5.
    """
    if filetype == 'cram':
        return FileTypeSpec(
            gcs_prefix='cram',
            data_suffix='cram',
            index_suffix='cram.crai',
            md5_suffix='md5sum',
        )

    if filetype == 'base_gvcf':
        return FileTypeSpec(
            gcs_prefix='base_gvcf',
            data_suffix='hard-filtered.gvcf.gz',
            index_suffix='hard-filtered.gvcf.gz.tbi',
            md5_suffix='md5sum',
        )

    if filetype == 'recal_gvcf':
        return FileTypeSpec(
            gcs_prefix='recal_gvcf',
            data_suffix='hard-filtered.recal.gvcf.gz',
            index_suffix='hard-filtered.recal.gvcf.gz.tbi',
            md5_suffix='md5',  # Note: .md5, not .md5sum
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
        ica_utils.stream_ica_file_to_gcs(
            api_instance=api_instance,
            path_parameters=path_parameters,
            file_id=main_file_id,
            file_name=main_file_name,
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_output_path_prefix,
            expected_md5_hash=expected_hash,
        )

        # --- 4. Stream index file (no verification) ---
        ica_utils.stream_ica_file_to_gcs(
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


def run(
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
    spec: FileTypeSpec = _get_file_suffixes(filetype)

    main_file_name: str = f'{sg_name}.{spec.data_suffix}'
    index_file_name: str = f'{sg_name}.{spec.index_suffix}'
    md5_file_name: str = f'{sg_name}.{spec.data_suffix}.{spec.md5_suffix}'
    md5_gcp_name: str = f'{sg_name}.{spec.data_suffix}.md5sum'  # Always save as .md5sum in GCS

    # --- 2. Get Pipeline ID and AR GUID ---
    pipeline_id, ar_guid = ica_utils.get_pipeline_details(pipeline_id_arguid_path)
    base_ica_folder_path = (
        f'/{BUCKET_NAME}/{ica_analysis_output_folder}/{sg_name}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name}/'
    )
    logger.info(f'Targeting ICA folder: {base_ica_folder_path}')

    # --- 3. Setup GCS Client ---
    gcs_output_path_prefix = str(utils.get_output_path(f'{spec.gcs_prefix}')).removeprefix(f'gs://{BUCKET_NAME}/')
    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(BUCKET_NAME)

    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_utils.get_ica_secrets()
    path_parameters: dict[str, str] = {'projectId': secrets['projectID']}

    # --- 5. Run Orchestration ---
    with ica_utils.get_ica_api_client() as api_client:
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
