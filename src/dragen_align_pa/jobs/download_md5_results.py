"""
Job to download the 'all_md5.txt' result from a completed
MD5 Checksum pipeline in ICA.
"""

import json

import cpg_utils.config
import requests
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.constants_registry import ROLE_DRAGEN_ALIGN


def run(
    cohort_name: str,
    md5_pipeline_file: cpg_utils.Path,
    md5_outpath: cpg_utils.Path,
) -> None:
    """
    Main function for the job.
    """
    with ica_api_utils.ica_project_session(ROLE_DRAGEN_ALIGN) as (api_client, path_parameters):
        api_instance = project_data_api.ProjectDataApi(api_client)

        # Get the ID
        with md5_pipeline_file.open('r') as pipeline_fh:
            pipeline_data: dict[str, str] = json.load(pipeline_fh)
            pipeline_id: str = pipeline_data['pipeline_id']
            ar_guid: str = pipeline_data['ar_guid']

        logger.info(f'Finding MD5 results for pipeline {pipeline_id}...')

        # Routes through find_file_id_by_name so the parentFolderPath slash
        # normalisation lives in one place (raises FileNotFoundError if absent).
        parent_folder_path = ica_utils.ica_md5_run_path(cohort_name, ar_guid, pipeline_id).as_folder()
        md5sum_results_id: str = ica_api_utils.find_file_id_by_name(
            api_instance=api_instance,
            path_parameters=path_parameters,
            parent_folder_path=parent_folder_path,
            file_name='all_md5.txt',
        )
        logger.info(f'Found MD5 results file ID: {md5sum_results_id}')

        # Get a pre-signed URL
        url_api_response = ica_api_utils.ica_retry(
            api_instance.create_download_url_for_data,  # pyright: ignore[reportUnknownVariableType]
            path_params=path_parameters | {'dataId': md5sum_results_id},  # pyright: ignore[reportArgumentType]
        )  # type: ignore  # noqa: PGH003

        md5_file_contents = requests.get(  # pyright: ignore[reportUnknownVariableType]
            url=url_api_response.body['url'],  # pyright: ignore[reportUnknownArgumentType]
            timeout=60,
        ).text  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]

        with md5_outpath.open('w') as md5_path_fh:
            md5_path_fh.write(
                md5_file_contents,  # pyright: ignore[reportUnknownArgumentType]
            )  # pyright: ignore[reportUnknownArgumentType]

    logger.info(f'Successfully downloaded MD5 results to {md5_outpath}')
