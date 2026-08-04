"""
Job to download the 'all_md5.txt' result from a completed
MD5 Checksum pipeline in ICA.
"""

import json

import cpg_utils.config
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.constants.constants_registry import ROLE_DRAGEN_ALIGN


def run(
    cohort_name: str,
    md5_pipeline_file: cpg_utils.Path,
    md5_outpath: cpg_utils.Path,
) -> None:
    """
    Main function for the job.
    """
    with ica_api_utils.ica_project_data_api(ROLE_DRAGEN_ALIGN) as (api_instance, path_parameters):
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

        # `fetch_ica_file_body` checks the status: without that, an expired pre-signed URL
        # writes S3's XML error body into all_md5.txt as if it were checksums.
        md5_file_contents = ica_utils.fetch_ica_file_body(
            api_instance,
            path_parameters,
            md5sum_results_id,
        ).text

        with md5_outpath.open('w') as md5_path_fh:
            md5_path_fh.write(
                md5_file_contents,  # pyright: ignore[reportUnknownArgumentType]
            )  # pyright: ignore[reportUnknownArgumentType]

    logger.info(f'Successfully downloaded MD5 results to {md5_outpath}')
