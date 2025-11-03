import json
from typing import Literal

import cpg_utils
import pandas as pd
import requests
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.constants import BUCKET_NAME


def run(
    cohort: Cohort,
    outputs: dict[str, cpg_utils.Path],
    fastq_list_file_path_dict: dict[str, cpg_utils.Path],
) -> None:
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    project_id: str = secrets['projectID']

    path_parameters: dict[str, str] = {'projectId': project_id}

    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance: project_data_api.ProjectDataApi = project_data_api.ProjectDataApi(  # pyright: ignore[reportUnknownVariableType]
            api_client,
        )
        for sequencing_group in cohort.get_sequencing_groups():
            sg_name: str = sequencing_group.name
            fastq_list_file_path = fastq_list_file_path_dict[sg_name]
            fastq_list_file_name: str = fastq_list_file_path.name

            # Read local csv to get the list of R1/R2 filenames
            try:
                with fastq_list_file_path.open('r') as fh:
                    fastq_filenames_df: pd.DataFrame = pd.read_csv(fh)
                    sg_fastq_filenames: list[str] = list(
                        fastq_filenames_df['Read1File'] + list(fastq_filenames_df['Read2File'])
                    )
                    logger.info(f'Found {len(sg_fastq_filenames)} FASTQ files for sequencing group {sg_name}.')
            except OSError as e:
                logger.error(f'Error reading FASTQ list file for {sg_name}: {e}')
                raise

            folder_path: str = f'/{BUCKET_NAME}/{config_retrieve(["ica", "data_prep", "output_folder"])}/{sg_name}'

            file_id, file_status = ica_utils.create_upload_object_id(
                api_instance=api_instance,
                path_params=path_parameters,
                sg_name=sg_name,
                file_name=fastq_list_file_name,
                folder_path=folder_path,
                object_type='FILE',
            )

            if file_status == 'AVAILABLE':
                logger.info(f"File {fastq_list_file_name} is 'AVAILABLE'. Skipping upload.")
            else:
                # File is NEW or PARTIAL, so we upload (or re-upload)
                logger.info(
                    f"File {fastq_list_file_name} has status '{file_status}'. Proceeding with upload to ID {file_id}."
                )
                upload_url: str = api_instance.create_upload_url_for_data(  # pyright: ignore[reportUnknownVariableType]
                    path_params=path_parameters | {'dataId': file_id},  # pyright: ignore[reportArgumentType]
                ).body['url']  # type: ignore[ReportUnknownVariableType]

                with fastq_list_file_path.open('rb') as fastq_list_fh:
                    response: requests.Response = requests.put(
                        url=upload_url,
                        data=fastq_list_fh,
                        timeout=300,
                    )  # pyright: ignore[reportUnknownVariableType]
                    if isinstance(response, requests.Response):
                        response.raise_for_status()
                        logger.info(
                            f'Upload of {fastq_list_file_name} to ICA successful.',
                        )
                    else:
                        logger.error(
                            'Error: Did not receive a valid response from ICA upload endpoint.',
                        )

            output_data: dict[str, str | list[str]] = {
                'fastq_list_fid': file_id,
                'sg_fastq_filenames': sg_fastq_filenames,
            }
            with outputs[sg_name].open('w') as out_fh:
                json.dump(output_data, out_fh)

    logger.info('Successfully processed and saved FASTQ list FIDs and filenames for all sequencing groups.')
