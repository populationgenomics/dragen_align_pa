from typing import Literal

import cpg_utils
import icasdk
import pandas as pd
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa.utils import get_ica_secrets


def _initalise_md5_job(cohort: Cohort) -> PythonJob:
    md5_job: PythonJob = get_batch().new_python_job(
        name='FastqIntakeQc',
        attributes=cohort.get_job_attrs() or {} | {'tool': 'ICA'},  # type: ignore[ReportUnknownVariableType]
    )
    md5_job.image(image=get_driver_image())
    return md5_job


def run_md5_job(cohort: Cohort, outputs: cpg_utils.Path, api_root: str) -> PythonJob:
    job: PythonJob = _initalise_md5_job(cohort=cohort)
    fastq_ids: list[str] = _run(cohort=cohort, api_root=api_root)
    with outputs.open('w') as output_fh:
        output_fh.write('\n'.join(fastq_ids))
    logger.info(fastq_ids)
    return job


def _run(cohort: Cohort, api_root: str) -> list[str]:
    manifest_file_path: cpg_utils.Path = config_retrieve(['workflow', 'manifest_gcp_path'])
    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        supplied_checksum_data: pd.DataFrame = pd.read_csv(manifest_fh, usecols=['Filenames', 'Checksum'])

    secrets: dict[Literal['projectID', 'apiKey'], str] = get_ica_secrets()
    project_id: str = secrets['projectID']
    api_key: str = secrets['apiKey']

    configuration = icasdk.Configuration(host=api_root)
    configuration.api_key['ApiKeyAuth'] = api_key
    path_parameters: dict[str, str] = {'projectId': project_id}

    with icasdk.ApiClient(configuration=configuration) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        fastq_ids: list[str] = []
        api_response = api_instance.get_project_data_list(
            path_params=path_parameters,
            query_params={'filename': supplied_checksum_data['Filenames'].to_list(), 'filenameMatchMode': 'EXACT'},
        )
        for item in list(range(len(api_response.body['items']))):
            fastq_ids.append(api_response.body[item]['data']['id'])

    return fastq_ids
