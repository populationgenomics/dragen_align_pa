import json
from typing import Literal

import cpg_utils
import icasdk
import pandas as pd
import requests
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api

from dragen_align_pa.constants import BUCKET, ICA_REST_ENDPOINT
from dragen_align_pa.jobs import manage_md5_pipeline
from dragen_align_pa.utils import create_upload_object_id, get_ica_secrets


def _initalise_md5_job(cohort: Cohort) -> PythonJob:
    md5_job: PythonJob = get_batch().new_python_job(
        name='FastqIntakeQc',
        attributes=cohort.get_job_attrs() or {} | {'tool': 'ICA'},  # type: ignore[ReportUnknownVariableType]
    )
    md5_job.image(image=get_driver_image())
    return md5_job


def _get_fastq_ica_id_list(
    fastq_filenames: list[str],
    fastq_ids_outpath: cpg_utils.Path,
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
) -> list[str]:
    fastq_ids: list[str] = []
    fastq_ids_and_filenames: list[str] = []
    api_response = api_instance.get_project_data_list(  # pyright: ignore[reportUnknownVariableType]
        path_params=path_parameters,  # pyright: ignore[reportArgumentType]
        query_params={'filename': fastq_filenames, 'filenameMatchMode': 'EXACT'},
    )  # type: ignore
    for item in list(range(len(api_response.body['items']))):  # pyright: ignore[reportUnknownArgumentType]
        fastq_ids.append(api_response.body['items'][item]['data']['id'])  # pyright: ignore[reportUnknownArgumentType]
        fastq_ids_and_filenames.append(
            api_response.body['items'][item]['data']['id']
            + '\t'
            + api_response.body['items'][item]['data']['details']['name']  # pyright: ignore[reportUnknownArgumentType]
        )

    with fastq_ids_outpath.open('w') as fq_outpath:
        fq_outpath.write('\n'.join(fastq_ids_and_filenames))
    return fastq_ids


def _create_md5_output_folder(
    folder_path: str,
    api_instance: project_data_api.ProjectDataApi,
    cohort_name: str,
    path_parameters: dict[str, str],
) -> str:
    return create_upload_object_id(
        api_instance=api_instance,
        path_params=path_parameters,
        sg_name=cohort_name,
        file_name=cohort_name,
        folder_path=folder_path,
        object_type='FOLDER',
    )


def _get_md5_pipeline_outputs(
    folder_path: str,
    path_parameters: dict[str, str],
    api_instance: project_data_api.ProjectDataApi,
    md5_pipeline_file: cpg_utils.Path,
    cohort_name: str,
    md5_outpath: cpg_utils.Path,
) -> cpg_utils.Path:
    # Get the ID
    with md5_pipeline_file.open('r') as pipeline_fh:
        pipeline_data: dict[str, str] = json.load(pipeline_fh)
        pipeline_id: str = pipeline_data['pipeline_id']
        ar_guid: str = pipeline_data['ar_guid']
    api_response = api_instance.get_project_data_list(  # pyright: ignore[reportUnknownVariableType]
        path_params=path_parameters,  # pyright: ignore[reportArgumentType]
        query_params={
            'filename': ['all_md5.txt'],
            'filenameMatchMode': 'EXACT',
            'parentFolderPath': f'{folder_path}/{cohort_name}/{cohort_name}_{ar_guid}-{pipeline_id}/',
        },  # pyright: ignore[reportArgumentType]
    )  # type: ignore
    md5sum_results_id: str = api_response.body['items'][0]['data']['id']  # pyright: ignore[reportUnknownVariableType]

    # Get a pre-signed URL
    url_api_response = api_instance.create_download_url_for_data(  # pyright: ignore[reportUnknownVariableType]
        path_params=path_parameters | {'dataId': md5sum_results_id}  # pyright: ignore[reportArgumentType]
    )  # type: ignore

    md5_file_contents = requests.get(url=url_api_response.body['url'], timeout=60).text  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
    with md5_outpath.open('w') as md5_path_fh:
        md5_path_fh.write(md5_file_contents)  # pyright: ignore[reportUnknownArgumentType]
    return md5_outpath


def run_md5_job(cohort: Cohort, outputs: dict[str, cpg_utils.Path]) -> PythonJob:
    job: PythonJob = _initalise_md5_job(cohort=cohort)
    md5_pipeline_file = job.call(_run, cohort=cohort, outputs=outputs).as_str()
    with outputs['md5sum_pipeline_success'].open('w') as success_fh:
        success_fh.write(md5_pipeline_file)
    return job


def _run(cohort: Cohort, outputs: dict[str, cpg_utils.Path]) -> cpg_utils.Path:
    manifest_file_path: cpg_utils.Path = config_retrieve(['workflow', 'manifest_gcp_path'])
    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        supplied_manifest_data: pd.DataFrame = pd.read_csv(manifest_fh, usecols=['Filenames'])

    fastq_filenames: list[str] = supplied_manifest_data['Filenames'].to_list()

    secrets: dict[Literal['projectID', 'apiKey'], str] = get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']

    configuration = icasdk.Configuration(host=ICA_REST_ENDPOINT)
    configuration.api_key['ApiKeyAuth'] = api_key
    path_parameters: dict[str, str] = {'projectId': project_id}

    cohort_name: str = cohort.name

    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)

        # Get all ica file ids for the fastq files by matching exactly on filename
        fastq_ids: list[str] = _get_fastq_ica_id_list(
            fastq_filenames=fastq_filenames,
            fastq_ids_outpath=outputs['fastq_ids_outpath'],
            api_instance=api_instance,
            path_parameters=path_parameters,
        )

        bucket_name: str = str(BUCKET).removeprefix('gs://')
        folder_path: str = f'/{bucket_name}{config_retrieve(["ica", "data_prep", "output_folder"])}'

        md5_outputs_folder_id: str = _create_md5_output_folder(
            folder_path=folder_path, api_instance=api_instance, cohort_name=cohort_name, path_parameters=path_parameters
        )

        md5_pipeline_file: cpg_utils.Path = manage_md5_pipeline.manage_md5_pipeline(
            cohort_name=cohort_name,
            ica_fastq_ids=fastq_ids,
            outputs=outputs,
            api_config=configuration,
            project_id=project_id,
            md5_outputs_folder_id=md5_outputs_folder_id,
        )

        md5_results_id: cpg_utils.Path = _get_md5_pipeline_outputs(
            folder_path=folder_path,
            path_parameters=path_parameters,
            api_instance=api_instance,
            md5_pipeline_file=md5_pipeline_file,
            cohort_name=cohort_name,
            md5_outpath=outputs['ica_md5sum_file'],
        )

    # compare the md5sums to the supplied sums new stage??.
    return md5_results_id
