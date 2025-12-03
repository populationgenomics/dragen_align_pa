import os
from collections.abc import Callable
from functools import partial
from typing import Literal

import cpg_utils
import pandas as pd
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, try_get_ar_guid
from icasdk.apis.tags import project_analysis_api, project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_cli_utils, ica_utils, utils
from dragen_align_pa.constants import BUCKET_NAME
from dragen_align_pa.jobs import run_intake_qc_pipeline
from dragen_align_pa.jobs.ica_pipeline_manager import manage_ica_pipeline_loop


def _get_fastq_ica_id_list(
    fastq_filenames: list[str],
    fastq_ids_outpath: cpg_utils.Path,
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
) -> list[str]:
    """
    Finds ICA file IDs for a list of fastq filenames.
    """
    fastq_ids: list[str] = []

    # Handle potentially large lists by batching API calls
    batch_size = config_retrieve(['ica', 'api', 'batch_size'], default=20)
    for i in range(0, len(fastq_filenames), batch_size):
        batch_filenames = fastq_filenames[i : i + batch_size]
        logger.info(
            f'Querying ICA for {len(batch_filenames)} FASTQ IDs (batch {i // batch_size + 1})...',
        )
        api_response = api_instance.get_project_data_list(  # pyright: ignore[reportUnknownVariableType]
            path_params=path_parameters,  # pyright: ignore[reportArgumentType]
            query_params={'filename': batch_filenames, 'filenameMatchMode': 'EXACT'},
        )  # type: ignore[no-untyped-call]
        for item in api_response.body['items']:  # pyright: ignore[reportUnknownArgumentType]
            file_id = item['data']['id']  # pyright: ignore[reportUnknownVariableType]
            fastq_ids.append(file_id)  # type: ignore[arg-type]

    if len(fastq_ids) != len(fastq_filenames):
        logger.warning(
            f'Mismatch: Found {len(fastq_ids)} file IDs in ICA, '
            f'but {len(fastq_filenames)} were expected from manifest.',
        )
        # This could be a critical error, depending on requirements
        # For now, we'll just log and continue with the files we found.

    with fastq_ids_outpath.open('w') as fq_outpath:
        fq_outpath.write('\n'.join(fastq_ids))

    logger.info(f'Found {len(fastq_ids)} total FASTQ file IDs.')
    return fastq_ids


def _create_md5_output_folder(
    folder_path: str,
    api_instance: project_data_api.ProjectDataApi,
    cohort_name: str,
    path_parameters: dict[str, str],
) -> str:
    """

    Creates the output folder in ICA for the MD5 pipeline.
    """
    object_id, _ = ica_utils.create_upload_object_id(
        api_instance=api_instance,
        path_params=path_parameters,
        sg_name=cohort_name,
        file_name=cohort_name,  # Folder name is the cohort name
        folder_path=folder_path,
        object_type='FOLDER',
    )
    return object_id


def _submit_md5_run(
    cohort_name: str,
    fastq_list_file_id: str,
    ar_guid: str,
    md5_outputs_folder_id: str,
) -> str:
    """
    Submits the MD5 intake QC pipeline to ICA.
    (This is the original submit function from this file)
    """
    logger.info(f'Submitting new MD5 ICA pipeline for {cohort_name}')
    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_analysis_api.ProjectAnalysisApi(api_client)
        md5_pipeline_id: str = run_intake_qc_pipeline.run_md5_pipeline(
            cohort_name=cohort_name,
            fastq_list_file_id=fastq_list_file_id,
            api_instance=api_instance,
            ar_guid=ar_guid,
            md5_outputs_folder_id=md5_outputs_folder_id,
        )
    return md5_pipeline_id


def run(
    cohort: Cohort,
    outputs: dict[str, cpg_utils.Path],
    manifest_file_path: cpg_utils.Path,
) -> None:
    """
    This function runs inside the PythonJob.
    It performs the pre-submission setup (getting FASTQ IDs, creating folders)
    and then calls the generic pipeline manager.

    """

    cohort_name: str = cohort.name
    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        supplied_manifest_data: pd.DataFrame = pd.read_csv(
            manifest_fh,
            usecols=[config_retrieve(['manifest', 'filenames'])],
        )
    fastq_filenames: list[str] = supplied_manifest_data[config_retrieve(['manifest', 'filenames'])].to_list()

    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    project_id: str = secrets['projectID']

    path_parameters: dict[str, str] = {'projectId': project_id}
    ar_guid: str = try_get_ar_guid()
    fastq_list_file_id: str
    md5_outputs_folder_id: str

    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)

        # Get all ica file ids for the fastq files
        ica_fastq_ids: list[str] = _get_fastq_ica_id_list(
            fastq_filenames=fastq_filenames,
            fastq_ids_outpath=outputs['fastq_ids_outpath'],
            api_instance=api_instance,
            path_parameters=path_parameters,
        )

        if not ica_fastq_ids:
            logger.error('No FASTQ file IDs found in ICA. Cannot start MD5 pipeline.')
            raise ValueError('No FASTQ file IDs found in ICA.')

        # Define local path for the FASTQ ID list
        batch_tmpdir = os.environ.get('BATCH_TMPDIR', '/io')
        local_fastq_list_path = os.path.join(batch_tmpdir, f'{cohort_name}_{ar_guid}_fastq_ids.txt')

        # Download the file from GCS to the local path
        gcs_fastq_list_path = str(outputs['fastq_ids_outpath'])
        utils.run_subprocess_with_log(
            ['gcloud', 'storage', 'cp', gcs_fastq_list_path, local_fastq_list_path],
            f'Download {os.path.basename(gcs_fastq_list_path)} from GCS',
        )

        # Upload the FASTQ ID list to ICA
        fastq_list_folder = (
            f'/{BUCKET_NAME}/{config_retrieve(["ica", "data_prep", "output_folder"])}/{cohort_name}/fastq_lists/'
        )
        fastq_list_filename = f'{cohort_name}_{ar_guid}_fastq_ids.txt'

        ica_cli_utils.authenticate_ica_cli()
        ica_cli_utils.upload_local_file(
            local_file_path=local_fastq_list_path,
            ica_folder_path=fastq_list_folder,
        )

        # Find the uploaded file to get its ID
        fastq_list_file_details = ica_api_utils.get_file_details_from_ica(
            api_instance=api_instance,
            path_params=path_parameters,
            ica_folder_path=fastq_list_folder,
            file_name=fastq_list_filename,
        )
        if not fastq_list_file_details:
            raise FileNotFoundError(f'Could not find uploaded fastq list file in ICA: {fastq_list_folder}')
        fastq_list_file_id = fastq_list_file_details['id']

        # Create output folder
        folder_path: str = f'/{BUCKET_NAME}/{config_retrieve(["ica", "data_prep", "output_folder"])}'
        md5_outputs_folder_id = _create_md5_output_folder(
            folder_path=folder_path,
            api_instance=api_instance,
            cohort_name=cohort_name,
            path_parameters=path_parameters,
        )

    submit_callable = partial(
        _submit_md5_run,
        cohort_name=cohort_name,
        fastq_list_file_id=fastq_list_file_id,
        ar_guid=ar_guid,
        md5_outputs_folder_id=md5_outputs_folder_id,
    )

    def _create_submit_callable_factory(target_name: str) -> Callable[[], str]:
        _ = target_name
        return submit_callable

    manage_ica_pipeline_loop(
        targets_to_process=[cohort],
        outputs=outputs,
        pipeline_name='MD5 Checksum',
        is_mlr_pipeline=False,
        success_file_key_template='md5sum_pipeline_success',
        pipeline_id_file_key_template='md5sum_pipeline_run',
        error_log_key=f'{cohort_name}_md5_errors',
        submit_function_factory=_create_submit_callable_factory,
        allow_retry=True,
        sleep_time_seconds=300,
    )
