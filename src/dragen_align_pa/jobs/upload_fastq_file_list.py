from typing import TYPE_CHECKING, Literal

import cpg_utils
import icasdk
import requests
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from icasdk import Configuration
from icasdk.apis.tags import project_data_api

from dragen_align_pa.utils import create_upload_object_id, get_ica_secrets

if TYPE_CHECKING:
    from _io import BufferedReader, _BufferedReaderStream


def _inisalise_fastq_upload_job(cohort: Cohort) -> PythonJob:
    job: PythonJob = get_batch().new_python_job(
        name='UploadFastqFileList',
        attributes=cohort.get_job_attrs() or {} | {'tool': 'ICA'},  # pyright: ignore[reportUnknownArgumentType]
    )
    job.image(image=get_driver_image())
    return job


def upload_fastq_file_list(
    cohort: Cohort,
    outputs: cpg_utils.Path,
    analysis_output_fids_path: dict[str, cpg_utils.Path],
    fastq_list_file_path_dict: dict[str, cpg_utils.Path],
    api_root: str,
    bucket: cpg_utils.Path,
) -> PythonJob:
    """Upload the fastq file list to ICA and return the job.

    Args:
        cohort: The cohort to upload the fastq file list for.
        outputs: A dictionary with the output paths.
        analysis_output_fids_path: A dictionary with the analysis output fids path.
    """
    job: PythonJob = _inisalise_fastq_upload_job(cohort=cohort)
    job.call(
        _run,
        cohort=cohort,
        outputs=outputs,
        analysis_output_fids_path=analysis_output_fids_path,
        fastq_list_file_path_dict=fastq_list_file_path_dict,
        api_root=api_root,
        bucket=bucket,
    )
    return job


def _run(
    cohort: Cohort,
    outputs: cpg_utils.Path,
    analysis_output_fids_path: str,
    fastq_list_file_path_dict: dict[str, cpg_utils.Path],
    api_root: str,
    bucket: cpg_utils.Path,
) -> None:
    secrets: dict[Literal['projectID', 'apiKey'], str] = get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']
    configuration: Configuration = Configuration(host=api_root)
    configuration.api_key['ApiKeyAuth'] = api_key
    path_parameters: dict[str, str] = {'projectId': project_id}

    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance: project_data_api.ProjectDataApi = project_data_api.ProjectDataApi(  # pyright: ignore[reportUnknownVariableType]
            api_client
        )
        for sequencing_group in cohort.get_sequencing_group_ids():
            fastq_list_file_name: str = fastq_list_file_path_dict[sequencing_group].name
            bucket_name: str = str(bucket).removeprefix('gs://')
            folder_path: str = (
                f'/{bucket_name}{config_retrieve(["ica", "data_prep", "output_folder"])}/{sequencing_group}'
            )

            fastq_list_ica_file_id: str = create_upload_object_id(
                api_instance=api_instance,
                path_params=path_parameters,
                sg_name=sequencing_group,
                file_name=fastq_list_file_name,
                folder_path=folder_path,
                object_type='FILE',
            )
            upload_url: str = api_instance.create_upload_url_for_data(  # pyright: ignore[reportUnknownVariableType]
                path_params=path_parameters | {'dataId': fastq_list_ica_file_id}  # pyright: ignore[reportArgumentType]
            ).body['url']  # type: ignore[ReportUnknownVariableType]

            files: dict[str, BufferedReader[_BufferedReaderStream]] = {
                fastq_list_file_name: fastq_list_file_path_dict[sequencing_group].open('rb')
            }
            requests.post(url=upload_url, files=files, timeout=300)
            # api_instance.upload_data_file_in_project(  # pyright: ignore[reportUnknownVariableType]
            #     path_params=path_parameters,  # pyright: ignore[reportArgumentType]
            #     data_id=create_upload_object_id(
            #         api_instance=api_instance,
            #         path_params=path_parameters,
            #         sg_name=sequencing_group,
            #         file_name=fastq_list_file_name,
            #         folder_path=folder_path,
            #         object_type='FILE',
            #     ),
            #     file=fastq_list_file_path_dict[sequencing_group].open('rb'),  # pyright: ignore[reportUnknownArgumentType]
            # )
