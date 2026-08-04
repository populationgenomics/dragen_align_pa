"""
Download all non CRAM / GVCF outputs from ICA using the Python SDK.
"""

import cpg_utils.config
from cpg_flow.targets import SequencingGroup
from cpg_utils.config import config_retrieve
from google.cloud import storage
from loguru import logger

from dragen_align_pa import gcs_utils, ica_api_utils, ica_utils, paths, utils
from dragen_align_pa.constants.ica_constants import BUCKET_NAME
from dragen_align_pa.constants.constants_registry import ROLE_DRAGEN_ALIGN


def run(
    sequencing_group: SequencingGroup,
    pipeline_id_arguid_path: cpg_utils.Path,
    cohort_name: str,
) -> None:
    """Stream per-sample ICA artefacts to GCS.

    Resolves the ICA folder for this SG's batch output via
    `ica_utils.get_ica_sample_folder`, reading `pipeline_id_arguid_path` (the
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
    ica_folder_path = ica_utils.get_ica_sample_folder(
        pipeline_id_arguid_path,
        sg_name=sg_name,
        cohort_name=cohort_name,
    )
    logger.info(f'Downloading bulk ICA data for {sg_name} from {ica_folder_path}')

    gcs_output_path_prefix = paths.gcs_relative_key(utils.get_output_path(filename=f'dragen_metrics/{sg_name}'))
    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(BUCKET_NAME)

    # Resolved BEFORE any URL is minted, so a re-run after a part-way failure mints URLs only for
    # what is missing rather than re-minting all 100+. The marker sits outside the prefix it
    # guards so it cannot make the stage's declared output folder exist prematurely.
    marker_key = paths.gcs_relative_key(utils.get_output_path(filename=f'download_state/{sg_name}.json'))
    already_downloaded = gcs_utils.files_already_downloaded(
        gcs_bucket,
        marker_key,
        gcs_output_path_prefix,
        ica_folder_path,
    )

    with ica_api_utils.ica_project_data_api(ROLE_DRAGEN_ALIGN) as (api_instance, path_parameters):
        # --- List + inline filter for CRAM/gVCF (handled by sibling stages) ---
        files = ica_utils.list_ica_files(
            api_instance=api_instance,
            path_parameters=path_parameters,
            base_ica_folder_path=ica_folder_path,
        )
        wanted = [
            (name, fid) for name, fid in files if not name.endswith(('.cram', '.cram.crai', '.gvcf.gz', '.gvcf.gz.tbi'))
        ]
        files_to_download = [(name, fid) for name, fid in wanted if name not in already_downloaded]
        if skipped := len(wanted) - len(files_to_download):
            logger.info(
                f'{sg_name}: {skipped} of {len(wanted)} files already in GCS; '
                f'downloading the remaining {len(files_to_download)}.',
            )
        if not wanted:
            # An empty ICA folder is not a completed download: claiming success here would
            # write _SUCCESS over a sequencing group that produced nothing.
            raise ValueError(
                f'{sg_name}: ICA folder {ica_folder_path} lists no downloadable outputs. '
                f'Check the analysis actually produced results before re-running.',
            )
        if not files_to_download:
            logger.info(f'{sg_name}: all bulk ICA outputs already present in GCS; nothing to download.')
            gcs_utils.write_success_sentinel(gcs_bucket, gcs_output_path_prefix)
            return

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
                    # The pre-filter above already decided this file must be written, and it
                    # knows about run provenance where the per-file existence check does not.
                    skip_existing=False,
                )

    # Only now is the download complete: cpg-flow gates the stage on this sentinel, so writing
    # it earlier would mark a part-way download done and skip the rest on the next run.
    gcs_utils.write_success_sentinel(gcs_bucket, gcs_output_path_prefix)
    logger.info(f'{sg_name}: all {len(wanted)} bulk ICA outputs are in GCS.')
