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
    cohort_name: str,
) -> None:
    """Stream per-sample ICA artefacts to GCS.

    Resolves the ICA folder for this SG's batch output via
    `utils.get_ica_sample_folder`, reading `pipeline_id_arguid_path` (the
    per-SG state file written by `ManageDragenPipeline`) + `cohort_name`.
    Only files inside the resolved folder are downloaded — batch-root
    artefacts (`passfail.json`, `summary.json`, `reports/`) sit one level
    up and are handled by `DownloadBatchArtefactsFromIca`.

    Resolution is done inside this entrypoint (rather than in a
    `_resolve_then_download_bulk` shim in `stages.py`) so the resolver
    and the downloader share a single Hail PythonJob — `stages.py` is
    reserved for cpg-flow stage definitions only.
    """
    sg_name: str = sequencing_group.name
    ica_folder_path = utils.get_ica_sample_folder(
        pipeline_id_arguid_path,
        sg_name=sg_name,
        cohort_name=cohort_name,
    )
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

        # Mint pre-signed URLs in batches via the :createDownloadUrls endpoint
        # rather than one :createDownloadUrl POST per file. This collapses the
        # rate-limited per-file call volume (the dominant 429 source on large
        # folders) from N to ceil(N / url_batch_size). URLs are minted
        # just-in-time per chunk so they are fresh when streamed.
        url_batch_size = int(config_retrieve(['ica', 'download', 'url_batch_size'], default=50))
        for i in range(0, len(files_to_download), url_batch_size):
            chunk = files_to_download[i : i + url_batch_size]
            urls = ica_utils.batch_create_download_urls(
                api_instance=api_instance,
                path_parameters=path_parameters,
                file_ids=[fid for _, fid in chunk],
            )
            for file_name, file_id in chunk:
                # urls.get(file_id) is None if the batch response omitted this
                # id; stream_ica_file_to_gcs then falls back to a per-file mint.
                ica_utils.stream_ica_file_to_gcs(
                    api_instance=api_instance,
                    path_parameters=path_parameters,
                    file_id=file_id,
                    file_name=file_name,
                    gcs_bucket=gcs_bucket,
                    gcs_prefix=gcs_output_path_prefix,
                    expected_md5_hash=None,
                    download_url=urls.get(file_id),
                )

    logger.info('All files streamed to GCS successfully.')
