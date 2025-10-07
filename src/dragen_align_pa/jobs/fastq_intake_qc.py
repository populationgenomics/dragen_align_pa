from typing import Literal

import cpg_utils
import icasdk
import pandas as pd
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api

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
    api_response = api_instance.get_project_data_list(  # pyright: ignore[reportUnknownVariableType]
        path_params=path_parameters,  # pyright: ignore[reportArgumentType]
        query_params={'filename': fastq_filenames, 'filenameMatchMode': 'EXACT'},
    )  # type: ignore
    for item in list(range(len(api_response.body['items']))):  # pyright: ignore[reportUnknownArgumentType]
        fastq_ids.append(api_response.body['items'][item]['data']['id'])  # pyright: ignore[reportUnknownArgumentType]

    with fastq_ids_outpath.open('w') as fq_outpath:
        fq_outpath.write('\n'.join(fastq_ids))
    return fastq_ids


def _create_md5_output_folder(
    bucket: cpg_utils.Path,
    api_instance: project_data_api.ProjectDataApi,
    cohort_name: str,
    path_parameters: dict[str, str],
) -> str:
    bucket_name: str = str(bucket).removeprefix('gs://')
    folder_path: str = f'/{bucket_name}{config_retrieve(["ica", "data_prep", "output_folder"])}/{cohort_name}'
    return create_upload_object_id(
        api_instance=api_instance,
        path_params=path_parameters,
        sg_name=cohort_name,
        file_name=cohort_name,
        folder_path=folder_path,
        object_type='FOLDER',
    )


def run_md5_job(cohort: Cohort, outputs: dict[str, cpg_utils.Path], api_root: str, bucket: cpg_utils.Path) -> PythonJob:
    job: PythonJob = _initalise_md5_job(cohort=cohort)
    md5_pipeline_file = job.call(_run, cohort=cohort, outputs=outputs, api_root=api_root, bucket=bucket).as_json()

    return job


def _run(cohort: Cohort, outputs: dict[str, cpg_utils.Path], api_root: str, bucket: cpg_utils.Path) -> cpg_utils.Path:
    manifest_file_path: cpg_utils.Path = config_retrieve(['workflow', 'manifest_gcp_path'])
    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        supplied_manifest_data: pd.DataFrame = pd.read_csv(manifest_fh, usecols=['Filenames'])

    fastq_filenames: list[str] = supplied_manifest_data['Filenames'].to_list()

    secrets: dict[Literal['projectID', 'apiKey'], str] = get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']

    configuration = icasdk.Configuration(host=api_root)
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

        md5_outputs_folder_id: str = _create_md5_output_folder(
            bucket=bucket, api_instance=api_instance, cohort_name=cohort_name, path_parameters=path_parameters
        )

    md5_pipeline_file: cpg_utils.Path = manage_md5_pipeline.manage_md5_pipeline(
        cohort_name=cohort_name,
        ica_fastq_ids=fastq_ids,
        outputs=outputs,
        api_root=api_root,
        api_config=configuration,
        project_id=project_id,
        md5_outputs_folder_id=md5_outputs_folder_id,
    )

    # Pull all_md5.txt from ICA
    # compare the md5sums to the supplied sums new stage??.
    return md5_pipeline_file
