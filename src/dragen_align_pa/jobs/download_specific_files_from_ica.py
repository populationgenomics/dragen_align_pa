"""
Download specific files (e.g., CRAM, GVCF) from ICA using the Python SDK.
"""

from typing import TYPE_CHECKING, Literal

import cpg_utils
from cpg_flow.targets import SequencingGroup
from cpg_utils.config import get_driver_image
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.file_types import FileTypeSpec
from dragen_align_pa.utils import get_ica_sample_folder, initialise_python_job

if TYPE_CHECKING:
    from hailtop.batch.job import PythonJob


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
        main_file_id = ica_api_utils.find_file_id_by_name(
            api_instance,
            path_parameters,
            base_ica_folder_path,
            main_file_name,
        )
        index_file_id = ica_api_utils.find_file_id_by_name(
            api_instance,
            path_parameters,
            base_ica_folder_path,
            index_file_name,
        )
        md5_file_id = ica_api_utils.find_file_id_by_name(
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
    file_spec: FileTypeSpec,
    ica_folder_path: str,
    gcs_output_dir: cpg_utils.Path,
) -> None:
    """
    The main Python function for the download job.
    Coordinates helper functions to list, filter, and stream files.

    `ica_folder_path` is the pre-resolved ICA folder (caller resolves it via
    `utils.get_ica_sample_folder`, which reads the per-SG state file and
    builds `/{BUCKET}/{output_folder}/{cohort}/{user_reference}-{pipeline_id}/{sg}/`).

    `gcs_output_dir` is the directory the calling stage declared in `expected_outputs`
    (e.g. `outputs['gvcf'].parent`); both bucket and prefix are derived from it so the
    download lands exactly where the stage promised.
    """
    sg_name: str = sequencing_group.name
    logger.info(f'Downloading {file_spec.gcs_prefix} data for {sg_name}.')

    main_file_name: str = f'{sg_name}.{file_spec.data_suffix}'
    index_file_name: str = f'{sg_name}.{file_spec.index_suffix}'
    md5_file_name: str = f'{sg_name}.{file_spec.data_suffix}.{file_spec.md5_suffix}'
    md5_gcp_name: str = f'{sg_name}.{file_spec.data_suffix}.md5sum'  # Always save as .md5sum in GCS

    logger.info(f'Targeting ICA folder: {ica_folder_path}')

    # --- 3. Setup GCS Client ---
    gcs_output_bucket_name, _, gcs_output_path_prefix = (
        str(gcs_output_dir).removeprefix('gs://').partition('/')
    )
    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(gcs_output_bucket_name)

    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    path_parameters: dict[str, str] = {'projectId': secrets['projectID']}

    # --- 5. Run Orchestration ---
    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        _orchestrate_download(
            api_instance=api_instance,
            path_parameters=path_parameters,
            base_ica_folder_path=ica_folder_path,
            gcs_bucket=gcs_bucket,
            gcs_output_path_prefix=gcs_output_path_prefix,
            main_file_name=main_file_name,
            index_file_name=index_file_name,
            md5_file_name=md5_file_name,
            md5_gcp_name=md5_gcp_name,
        )

    logger.info(f'Successfully downloaded and verified all files for {sg_name}.')


def resolve_and_run(
    sequencing_group: SequencingGroup,
    file_spec: FileTypeSpec,
    pipeline_id_arguid_path: cpg_utils.Path,
    cohort_name: str,
    gcs_output_dir: cpg_utils.Path,
) -> None:
    """Resolve the SG's batched ICA folder from the per-SG state file, then download.

    Wraps `get_ica_sample_folder` + `run` so callers don't need to thread the
    folder path through themselves.
    """
    ica_folder_path = get_ica_sample_folder(
        pipeline_id_arguid_path=pipeline_id_arguid_path,
        sg_name=sequencing_group.name,
        cohort_name=cohort_name,
    )
    run(
        sequencing_group=sequencing_group,
        file_spec=file_spec,
        ica_folder_path=ica_folder_path,
        gcs_output_dir=gcs_output_dir,
    )


def make_download_job(
    job_name: str,
    sequencing_group: SequencingGroup,
    file_spec: FileTypeSpec,
    pipeline_id_arguid_path: cpg_utils.Path,
    cohort_name: str,
    gcs_output_dir: cpg_utils.Path,
) -> 'PythonJob':
    """Build a Hail PythonJob that resolves the SG's ICA folder and downloads `file_spec`.

    The three Download*FromIca stages share identical job-construction boilerplate
    (image, storage, memory, spot, then `resolve_and_run`); this is the single place
    that boilerplate lives.
    """
    job = initialise_python_job(job_name=job_name, target=sequencing_group, tool_name='ICA-Python')
    job.image(image=get_driver_image())
    job.storage('8Gi')
    job.memory('8Gi')
    job.spot(is_spot=False)
    job.call(
        resolve_and_run,
        sequencing_group=sequencing_group,
        file_spec=file_spec,
        pipeline_id_arguid_path=pipeline_id_arguid_path,
        cohort_name=cohort_name,
        gcs_output_dir=gcs_output_dir,
    )
    return job
