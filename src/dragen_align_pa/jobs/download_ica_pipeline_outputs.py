"""
Download all non CRAM / GVCF outputs from ICA using the Python SDK.
"""

import json
from typing import Literal

import cpg_utils
import icasdk
from cpg_flow.targets import SequencingGroup
from cpg_utils.config import config_retrieve, get_driver_image
from google.cloud import storage
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_utils, utils
from dragen_align_pa.constants import (
    BUCKET_NAME,
    GCP_FOLDER_FOR_ICA_DOWNLOAD,
    ICA_REST_ENDPOINT,
)


def download_bulk_data_from_ica(
    sequencing_group: SequencingGroup,
    pipeline_id_arguid_path: cpg_utils.Path,
) -> PythonJob:
    """
    Creates a PythonJob to download all metric files from an ICA analysis run.
    """
    job: PythonJob = utils.initialise_python_job(
        job_name='Download ICA bulk data',
        target=sequencing_group,
        tool_name='ICA-Python',
    )
    job.image(image=get_driver_image())
    job.spot(is_spot=False)
    job.memory(memory='8Gi')

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


def _run(
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
    pipeline_id, ar_guid = _get_pipeline_details(pipeline_id_arguid_path)
    base_ica_folder_path = (
        f'/{BUCKET_NAME}/{ica_analysis_output_folder}/{sg_name}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name}/'
    )
    logger.info(f'Targeting ICA folder: {base_ica_folder_path}')

    # --- Setup GCS Client ---
    gcs_output_path_prefix = f'{GCP_FOLDER_FOR_ICA_DOWNLOAD}/dragen_metrics/{sg_name}'
    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(BUCKET_NAME)

    # --- Secure ICA Authentication ---
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_utils.get_ica_secrets()
    configuration = icasdk.Configuration(host=ICA_REST_ENDPOINT)
    configuration.api_key['ApiKeyAuth'] = secrets['apiKey']
    path_parameters: dict[str, str] = {'projectId': secrets['projectID']}

    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)

        # --- List, filter, and download files ---
        files_to_download = ica_utils.list_and_filter_ica_files(
            api_instance=api_instance,
            path_parameters=path_parameters,
            base_ica_folder_path=base_ica_folder_path,
        )

        ica_utils.stream_files_to_gcs(
            api_instance=api_instance,
            path_parameters=path_parameters,
            files_to_download=files_to_download,
            gcs_bucket=gcs_bucket,
            gcs_output_path_prefix=gcs_output_path_prefix,
        )

    logger.info('All files streamed to GCS successfully.')
