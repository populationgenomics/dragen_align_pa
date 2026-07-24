import json
import os
import time
from functools import partial

import cpg_utils.config
import pandas as pd
from cpg_flow.targets import Cohort
from cpg_utils.config import try_get_ar_guid
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_cli_utils, ica_utils
from dragen_align_pa.constants.constants_registry import ROLE_DRAGEN_ALIGN
from dragen_align_pa.paths import IcaPath
from dragen_align_pa.jobs import run_intake_qc_pipeline
from dragen_align_pa.jobs.ica_pipeline_manager import manage_ica_pipeline_loop


def _get_fastq_ica_id_list(
    fastq_filenames: list[str],
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
) -> dict[str, str]:
    """Finds ICA file IDs for a list of FASTQ filenames.

    Queries ICA in batches for the given filenames and reconciles the result
    against the manifest: every expected filename must resolve to exactly one
    ICA file ID.

    Args:
        fastq_filenames: FASTQ filenames expected from the manifest.
        api_instance: ICA project-data API client used to run the queries.
        path_parameters: ICA path parameters (e.g. project ID) for the query.

    Returns:
        A mapping of ICA file ID to filename for every resolved FASTQ file.

    Raises:
        ValueError: If the number of resolved file IDs does not match the
            number of expected filenames. The message names the files that
            were missing in ICA.
    """
    ica_fastq_info: dict[str, str] = {}

    # Handle potentially large lists by batching API calls
    batch_size = cpg_utils.config.config_retrieve(['ica', 'api', 'batch_size'], default=20)
    for i in range(0, len(fastq_filenames), batch_size):
        batch_filenames = fastq_filenames[i : i + batch_size]
        logger.info(
            f'Querying ICA for {len(batch_filenames)} FASTQ IDs (batch {i // batch_size + 1})...',
        )
        api_response = ica_api_utils.ica_retry(
            api_instance.get_project_data_list,
            path_params=path_parameters,
            query_params={'filename': batch_filenames, 'filenameMatchMode': 'EXACT'},
        )
        for item in api_response.body['items']:
            file_name: str = item['data']['details']['name']
            file_id: str = item['data']['id']
            ica_fastq_info[file_id] = file_name

    if len(ica_fastq_info) != len(fastq_filenames):
        found_filenames: set[str] = set(ica_fastq_info.values())
        missing_filenames: list[str] = [name for name in fastq_filenames if name not in found_filenames]
        message: str = (
            f'Mismatch: Found {len(ica_fastq_info)} file IDs in ICA, '
            f'but {len(fastq_filenames)} were expected from manifest. '
            f'{len(missing_filenames)} file(s) missing in ICA: {missing_filenames}'
        )
        logger.error(message)
        raise ValueError(message)

    logger.info(f'Found {len(ica_fastq_info)} total FASTQ file IDs.')
    return ica_fastq_info


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
        folder_name=cohort_name,
        file_name=cohort_name,
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
    with ica_api_utils.ica_project_analysis_api(ROLE_DRAGEN_ALIGN) as (api_instance, path_parameters):
        md5_pipeline_id: str = run_intake_qc_pipeline.run_md5_pipeline(
            cohort_name=cohort_name,
            fastq_list_file_id=fastq_list_file_id,
            api_instance=api_instance,
            path_parameters=path_parameters,
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
        try:
            supplied_manifest_data: pd.DataFrame = pd.read_csv(
                manifest_fh,
                usecols=[cpg_utils.config.config_retrieve(['manifest', 'filenames'])],
            )
            fastq_filenames: list[str] = supplied_manifest_data[
                cpg_utils.config.config_retrieve(['manifest', 'filenames'])
            ].to_list()
        except ValueError:
            manifest_fh.seek(0)
            header: list[str] = manifest_fh.readline().split()
            logger.error(
                f'Expected to read the column: {cpg_utils.config.config_retrieve(["manifest", "filenames"])} \n'
                f'from the manifest file. Got instead: {header}'
            )
            raise

    ar_guid: str = try_get_ar_guid()
    fastq_list_file_id: str
    md5_outputs_folder_id: str

    with ica_api_utils.ica_project_data_api(ROLE_DRAGEN_ALIGN) as (api_instance, path_parameters):
        # Get all ica file ids for the fastq files
        ica_fastq_info: dict[str, str] = _get_fastq_ica_id_list(
            fastq_filenames=fastq_filenames,
            api_instance=api_instance,
            path_parameters=path_parameters,
        )

        if not ica_fastq_info:
            logger.error('No FASTQ file IDs found in ICA. Cannot start MD5 pipeline.')
            raise ValueError('No FASTQ file IDs found in ICA.')

        # Upload the FASTQ ID list to ICA
        fastq_list_folder = (ica_utils.ica_cohort_path(cohort_name) / 'fastq_lists').as_folder()
        fastq_list_filename = f'{cohort_name}_{ar_guid}_fastq_ids.txt'

        # Write the FASTQ ID list to a temporary file
        # If not running with Hail Batch, make the file in the working directory
        if not os.environ.get('BATCH_TMPDIR'):
            fastq_list_filename_path: str = os.path.join('.', fastq_list_filename)
        else:
            fastq_list_filename_path = os.path.join(os.environ['BATCH_TMPDIR'], fastq_list_filename)
        with open(fastq_list_filename_path, 'w') as fq_outpath:
            fq_outpath.write('\n'.join(ica_fastq_info.keys()))

        ica_cli_utils.authenticate_ica_cli(ROLE_DRAGEN_ALIGN)
        ica_cli_utils.upload_local_file(
            local_file_path=fastq_list_filename_path,
            ica_folder_path=fastq_list_folder,
        )
        with outputs['fastq_ids_outpath'].open('w') as fq_outpath:
            json.dump(ica_fastq_info, fq_outpath)

        # Find the uploaded file to get its ID, with retries for eventual consistency
        fastq_list_file_details = None
        max_retries = 5
        retry_delay_seconds = 15
        for attempt in range(max_retries):
            fastq_list_file_details = ica_api_utils.get_file_details_from_ica(
                api_instance=api_instance,
                path_params=path_parameters,
                ica_folder_path=fastq_list_folder,
                file_name=fastq_list_filename,
            )
            if fastq_list_file_details:
                status = fastq_list_file_details.get('details', {}).get('status')
                if status == 'AVAILABLE':
                    logger.info('File found and is AVAILABLE.')
                    break
                logger.warning(f"File found, but status is '{status}'. Retrying...")

            if attempt < max_retries - 1:
                time.sleep(retry_delay_seconds)

        if not fastq_list_file_details:
            raise FileNotFoundError(
                f'Could not find uploaded fastq list file in ICA after {max_retries} attempts: '
                f'{fastq_list_folder}{fastq_list_filename}',
            )
        fastq_list_file_id = fastq_list_file_details['id']

        # Create output folder
        folder_path: str = IcaPath.output_root().as_folder()
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

    manage_ica_pipeline_loop(
        targets_to_process=[cohort],
        outputs=outputs,
        pipeline_name='MD5 Checksum',
        is_mlr_pipeline=False,
        success_file_key_template='md5sum_pipeline_success',
        pipeline_id_file_key_template='md5sum_pipeline_run',
        error_log_key=f'{cohort_name}_md5_errors',
        submit_function_factory=lambda _target_name: submit_callable,
        allow_retry=True,
        sleep_time_seconds=300,
        # Zero-tolerance (loop default): any FAILED_FINAL aborts the cohort (the 5%-rate gate was removed branch-wide).
        raise_on_failed_final=True,
    )
