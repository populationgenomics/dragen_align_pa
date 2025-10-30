"""
Job to download the 'all_md5.txt' result from a completed
MD5 Checksum pipeline in ICA.
"""

import json
from typing import Literal

import cpg_utils
import requests
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_driver_image
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_utils, utils
from dragen_align_pa.constants import BUCKET_NAME


def download_md5_results_job(
    cohort: Cohort,
    md5_pipeline_file: cpg_utils.Path,
    md5_outpath: cpg_utils.Path,
) -> PythonJob:
    """
    Creates a PythonJob to download the MD5 results.
    """
    job: PythonJob = utils.initialise_python_job(
        job_name='DownloadMd5Results',
        target=cohort,
        tool_name='ICA-Python',
    )
    job.image(image=get_driver_image())
    job.call(
        _run,
        cohort_name=cohort.name,
        md5_pipeline_file=md5_pipeline_file,
        md5_outpath=md5_outpath,
    )
    return job


def _run(
    cohort_name: str,
    md5_pipeline_file: cpg_utils.Path,
    md5_outpath: cpg_utils.Path,
) -> None:
    """
    Main function for the job.
    """
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_utils.get_ica_secrets()
    project_id: str = secrets['projectID']

    path_parameters: dict[str, str] = {'projectId': project_id}

    folder_path: str = f'/{BUCKET_NAME}/{config_retrieve(["ica", "data_prep", "output_folder"])}'

    with ica_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)

        # Get the ID
        with md5_pipeline_file.open('r') as pipeline_fh:
            pipeline_data: dict[str, str] = json.load(pipeline_fh)
            pipeline_id: str = pipeline_data['pipeline_id']
            ar_guid: str = pipeline_data['ar_guid']

        logger.info(f'Finding MD5 results for pipeline {pipeline_id}...')

        api_response = api_instance.get_project_data_list(  # pyright: ignore[reportUnknownVariableType]
            path_params=path_parameters,  # pyright: ignore[reportArgumentType]
            query_params={
                'filename': ['all_md5.txt'],
                'filenameMatchMode': 'EXACT',
                'parentFolderPath': f'{folder_path}/{cohort_name}/{cohort_name}_{ar_guid}-{pipeline_id}/',
            },  # pyright: ignore[reportArgumentType]
        )  # type: ignore

        if not api_response.body['items']:  # pyright: ignore[reportUnknownVariableType]
            raise FileNotFoundError(f'Could not find "all_md5.txt" in output folder for pipeline {pipeline_id}')

        md5sum_results_id: str = api_response.body['items'][0]['data']['id']  # pyright: ignore[reportUnknownVariableType]
        logger.info(f'Found MD5 results file ID: {md5sum_results_id}')

        # Get a pre-signed URL
        url_api_response = api_instance.create_download_url_for_data(  # pyright: ignore[reportUnknownVariableType]
            path_params=path_parameters | {'dataId': md5sum_results_id},  # pyright: ignore[reportArgumentType]
        )  # type: ignore

        md5_file_contents = requests.get(
            url=url_api_response.body['url'],
            timeout=60,
        ).text  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]

        with md5_outpath.open('w') as md5_path_fh:
            md5_path_fh.write(
                md5_file_contents,
            )  # pyright: ignore[reportUnknownArgumentType]

    logger.info(f'Successfully downloaded MD5 results to {md5_outpath}')
