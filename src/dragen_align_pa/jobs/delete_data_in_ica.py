import json
from typing import Literal

import cpg_utils
import icasdk
from cpg_flow.targets import Cohort
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api
from icasdk.exceptions import ApiException, ApiValueError
from loguru import logger

from dragen_align_pa import ica_utils, utils
from dragen_align_pa.constants import ICA_REST_ENDPOINT


def delete_data_in_ica(
    cohort: Cohort,
    analysis_output_fids_paths: dict[str, cpg_utils.Path],
    cram_fid_paths_dict: dict[str, cpg_utils.Path] | None,
    fastq_ids_list_path: cpg_utils.Path | None,
) -> PythonJob:
    delete_job: PythonJob = utils.initialise_python_job(
        job_name='DeleteDataInIca',
        target=cohort,
        tool_name='ICA',
    )
    delete_job.call(
        _run,
        analysis_output_fids_paths=analysis_output_fids_paths,
        cram_fid_paths_dict=cram_fid_paths_dict,
        fastq_ids_list_path=fastq_ids_list_path,
    )
    return delete_job


def _run(
    analysis_output_fids_paths: dict[str, cpg_utils.Path],
    cram_fid_paths_dict: dict[str, cpg_utils.Path] | None,
    fastq_ids_list_path: cpg_utils.Path | None,
) -> None:
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_utils.get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']

    path_params: dict[str, str] = {'projectId': project_id}
    fids: list[str] = []

    configuration = icasdk.Configuration(host=ICA_REST_ENDPOINT)
    configuration.api_key['ApiKeyAuth'] = api_key

    # 1. Collect all generated data FIDs (analysis output folders)
    logger.info(f'Collecting {len(analysis_output_fids_paths)} analysis output folder FIDs...')
    for sg_name, path in analysis_output_fids_paths.items():
        try:
            with path.open() as fid_handle:
                fids.append(json.load(fid_handle)['analysis_output_fid'])
        except Exception as e:
            logger.warning(f'Could not read analysis_output_fid for {sg_name}: {e}')

    # 2. Collect source CRAM FIDs if they exist
    if cram_fid_paths_dict:
        logger.info(f'Collecting {len(cram_fid_paths_dict)} source CRAM FIDs...')
        for sg_name, path in cram_fid_paths_dict.items():
            try:
                with path.open() as fid_handle:
                    fids.append(json.load(fid_handle)['cram_fid'])
            except Exception as e:
                logger.warning(f'Could not read cram_fid for {sg_name}: {e}')

    # 3. Collect source FASTQ FIDs if they exist
    if fastq_ids_list_path:
        logger.info(f'Collecting source FASTQ FIDs from {fastq_ids_list_path}...')
        try:
            with fastq_ids_list_path.open() as fastq_handle:
                for line in fastq_handle:
                    if line.strip():
                        # File format is 'file_id\tfile_name'
                        file_id = line.split()[0]
                        fids.append(file_id)
        except Exception as e:
            logger.warning(f'Could not read FASTQ FIDs from {fastq_ids_list_path}: {e}')

    # 4. Delete all collected FIDs
    logger.info(f'Attempting to delete {len(fids)} total data objects from ICA...')
    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        for f_id in fids:
            request_path_params = path_params | {'dataId': f_id}
            try:
                # The API returns None (invalid as defined by the sdk) but deletes
                # the data anyway.
                # We replace contextlib.suppress with an explicit try/except
                # to log the suppressed error.
                logger.info(f'Requesting deletion of ICA FID: {f_id}')
                api_instance.delete_data(  # type: ignore[ReportUnknownVariableType]
                    path_params=request_path_params,  # type: ignore[ReportUnknownVariableType]
                )
            except ApiValueError as e:
                logger.warning(
                    f'Suppressed spurious ApiValueError for f_id {f_id}. '
                    f'Deletion is expected to have proceeded. Error: {e}',
                )
            except ApiException as e:
                logger.warning(
                    f'API exception for {f_id}. Has it already been deleted? Error: {e}',
                )
            except Exception as e:
                logger.error(f'Unexpected error deleting {f_id}: {e}')

    logger.info('ICA data deletion job complete.')
