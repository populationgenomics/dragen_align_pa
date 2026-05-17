"""
Download all non CRAM / GVCF outputs from ICA using the Python SDK.
"""

from typing import Literal

from cpg_flow.targets import SequencingGroup
from google.cloud import storage
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils, utils
from dragen_align_pa.constants import (
    BUCKET_NAME,
)


def run(
    sequencing_group: SequencingGroup,
    ica_folder_path: str,
) -> None:
    """Stream per-sample ICA artefacts to GCS.

    `ica_folder_path` is the resolved ICA folder for this SG's batch output
    (see `utils.get_ica_sample_folder`). Only files inside this folder are
    downloaded — batch-root artefacts (`passfail.json`, `summary.json`,
    `reports/`) sit one level up and are handled by `DownloadBatchArtefactsFromIca`.
    """
    sg_name: str = sequencing_group.name
    logger.info(f'Downloading bulk ICA data for {sg_name} from {ica_folder_path}')

    gcs_output_path_prefix = str(utils.get_output_path(filename=f'dragen_metrics/{sg_name}')).removeprefix(
        f'gs://{BUCKET_NAME}/',
    )
    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(BUCKET_NAME)

    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    path_parameters: dict[str, str] = {'projectId': secrets['projectID']}

    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)

        # --- List + inline filter for CRAM/gVCF (handled by sibling stages) ---
        files = ica_utils.list_ica_files(
            api_instance=api_instance,
            path_parameters=path_parameters,
            base_ica_folder_path=ica_folder_path,
        )
        files_to_download = [
            (name, fid) for name, fid in files
            if not name.endswith(('.cram', '.cram.crai', '.gvcf.gz', '.gvcf.gz.tbi'))
        ]

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
