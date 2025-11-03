from collections.abc import Callable
from functools import partial
from typing import Literal

import cpg_utils
import pandas as pd
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, try_get_ar_guid
from icasdk.apis.tags import project_analysis_api, project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
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
    fastq_ids_and_filenames: list[str] = []

    # Handle potentially large lists by batching API calls
    batch_size = 100
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
            file_name = item['data']['details']['name']  # pyright: ignore[reportUnknownVariableType]
            fastq_ids.append(file_id)  # type: ignore[arg-type]
            fastq_ids_and_filenames.append(f'{file_id}\t{file_name}')

    if len(fastq_ids) != len(fastq_filenames):
        logger.warning(
            f'Mismatch: Found {len(fastq_ids)} file IDs in ICA, '
            f'but {len(fastq_filenames)} were expected from manifest.',
        )
        # This could be a critical error, depending on requirements
        # For now, we'll just log and continue with the files we found.

    with fastq_ids_outpath.open('w') as fq_outpath:
        fq_outpath.write('\n'.join(fastq_ids_and_filenames))

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
    ica_fastq_ids: list[str],
    api_instance: project_analysis_api.ProjectAnalysisApi,
    project_id: str,
    ar_guid: str,
    md5_outputs_folder_id: str,
) -> str:
    """
    Submits the MD5 intake QC pipeline to ICA.
    (This is the original submit function from this file)
    """
    logger.info(f'Submitting new MD5 ICA pipeline for {cohort_name}')
    md5_pipeline_id: str = run_intake_qc_pipeline.run_md5_pipeline(
        cohort_name=cohort_name,
        ica_fastq_ids=ica_fastq_ids,
        api_instance=api_instance,
        project_id=project_id,
        ar_guid=ar_guid,
        md5_outputs_folder_id=md5_outputs_folder_id,
    )
    return md5_pipeline_id


def run(
    cohort: Cohort,
    outputs: dict[str, cpg_utils.Path],
) -> None:
    """
    This function runs inside the PythonJob.
    It performs the pre-submission setup (getting FASTQ IDs, creating folders)
    and then calls the generic pipeline manager.

    """

    cohort_name: str = cohort.name
    manifest_file_path: cpg_utils.Path = config_retrieve(
        ['workflow', 'manifest_gcp_path'],
    )
    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        supplied_manifest_data: pd.DataFrame = pd.read_csv(
            manifest_fh,
            usecols=['Filenames'],
        )
    fastq_filenames: list[str] = supplied_manifest_data['Filenames'].to_list()

    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    project_id: str = secrets['projectID']

    path_parameters: dict[str, str] = {'projectId': project_id}

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

        folder_path: str = f'/{BUCKET_NAME}/{config_retrieve(["ica", "data_prep", "output_folder"])}'

        md5_outputs_folder_id: str = _create_md5_output_folder(
            folder_path=folder_path,
            api_instance=api_instance,
            cohort_name=cohort_name,
            path_parameters=path_parameters,
        )

    ar_guid: str = try_get_ar_guid()
    submit_callable = partial(
        _submit_md5_run,
        cohort_name=cohort_name,
        ica_fastq_ids=ica_fastq_ids,
        api_instance=api_instance,
        project_id=project_id,
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
