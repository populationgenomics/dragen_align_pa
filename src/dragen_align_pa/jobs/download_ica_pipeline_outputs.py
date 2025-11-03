"""
Download all non CRAM / GVCF outputs from ICA using the Python SDK.
"""

from typing import Literal

import cpg_utils
from cpg_flow.targets import SequencingGroup
from cpg_utils.config import config_retrieve
from google.cloud import storage
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils, utils
from dragen_align_pa.constants import (
    BUCKET_NAME,
)


def run(
    sequencing_group: SequencingGroup,
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
    logger.info(f'Downloading bulk ICA data for {sg_name}.')

    # --- Get Pipeline ID and AR GUID ---
    pipeline_id, ar_guid = ica_api_utils.get_pipeline_details(pipeline_id_arguid_path)
    base_ica_folder_path = (
        f'/{BUCKET_NAME}/{ica_analysis_output_folder}/{sg_name}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name}/'
    )
    logger.info(f'Targeting ICA folder: {base_ica_folder_path}')

    # --- Setup GCS Client ---
    gcs_output_path_prefix = str(utils.get_output_path(filename=f'dragen_metrics/{sg_name}')).removeprefix(
        f'gs://{BUCKET_NAME}/'
    )
    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(BUCKET_NAME)

    # --- Secure ICA Authentication ---
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    path_parameters: dict[str, str] = {'projectId': secrets['projectID']}

    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)

        # --- List, filter, and download files ---
        files_to_download = ica_utils.list_and_filter_ica_files(
            api_instance=api_instance,
            path_parameters=path_parameters,
            base_ica_folder_path=base_ica_folder_path,
        )

        for file_name, file_id in files_to_download:
            logger.info(f'Preparing to download file: {file_name} (ID: {file_id})')
            ica_utils.stream_ica_file_to_gcs(
                api_instance=api_instance,
                path_parameters=path_parameters,
                file_id=file_id,
                file_name=file_name,
                gcs_bucket=gcs_bucket,
                gcs_prefix=gcs_output_path_prefix,
                expected_md5_hash=None,
            )

    logger.info('All files streamed to GCS successfully.')
