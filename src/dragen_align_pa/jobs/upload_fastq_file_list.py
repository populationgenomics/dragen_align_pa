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


def upload_fastq_file_list(
    cohort: Cohort,
    outputs: cpg_utils.Path,
    fastq_list_file_path_dict: dict[str, cpg_utils.Path],
) -> PythonJob:
    """Upload the fastq file list to ICA and return the job.

    Args:
        cohort: The cohort to upload the fastq file list for.
        outputs: A dictionary with the output paths.
        analysis_output_fids_path: A dictionary with the analysis output fids path.
    """
    job: PythonJob = utils.initialise_python_job(
        job_name='UploadFastqFileList',
        target=cohort,
        tool_name='ICA',
    )
    job.image(image=get_driver_image())
    job.call(
        _run,
        cohort=cohort,
        outputs=outputs,
        fastq_list_file_path_dict=fastq_list_file_path_dict,
    )
    return job


def _run(
    cohort: Cohort,
    outputs: cpg_utils.Path,
    fastq_list_file_path_dict: dict[str, cpg_utils.Path],
) -> None:
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_utils.get_ica_secrets()
    project_id: str = secrets['projectID']

    path_parameters: dict[str, str] = {'projectId': project_id}
    sg_and_fastq_list: list[str] = []

    with ica_utils.get_ica_api_client() as api_client:
        api_instance: project_data_api.ProjectDataApi = project_data_api.ProjectDataApi(  # pyright: ignore[reportUnknownVariableType]
            api_client,
        )
        for sequencing_group in cohort.get_sequencing_group_ids():
            fastq_list_file_name: str = fastq_list_file_path_dict[sequencing_group].name
            folder_path: str = (
                f'/{BUCKET_NAME}/{config_retrieve(["ica", "data_prep", "output_folder"])}/{sequencing_group}'
            )

            file_id, file_status = ica_utils.create_upload_object_id(
                api_instance=api_instance,
                path_params=path_parameters,
                sg_name=sequencing_group,
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

                with fastq_list_file_path_dict[sequencing_group].open(
                    'r',
                ) as fastq_list_fh:
                    data: str = fastq_list_fh.read()
                    response: requests.Response = requests.put(
                        url=upload_url,
                        data=data,
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

            # Always append the correct file ID
            sg_and_fastq_list.append(f'{sequencing_group}:{file_id}')

    # Write the sequencing group and fastq list ICA file IDs to the outputs path
    with outputs.open('w') as out_fh:
        out_fh.write('\n'.join(sg_and_fastq_list))
