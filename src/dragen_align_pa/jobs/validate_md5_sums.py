import subprocess

import cpg_utils
import pandas as pd
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob


def _initalise_md5sum_validation_job(cohort: Cohort) -> PythonJob:
    job: PythonJob = get_batch().new_python_job(
        name='ValidateMd5Sums',
        attributes=cohort.get_job_attrs() or {} | {'tool': 'md5sum'},  # type: ignore[ReportUnknownVariableType]
    )
    job.image(image=get_driver_image())
    return job


def validate_md5_sums(ica_md5sum_file_path: cpg_utils.Path, cohort: Cohort) -> PythonJob:
    job: PythonJob = _initalise_md5sum_validation_job(cohort=cohort)

    return job


def _run(ica_md5sum_file_path) -> None:
    manifest_file_path: cpg_utils.Path = config_retrieve(['workflow', 'manifest_gcp_path'])
    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        supplied_manifest_data: pd.DataFrame = pd.read_csv(manifest_fh, usecols=['Filenames', 'Checksum'])
        supplied_manifest_data.to_csv('supplied_checksum.txt', sep='\t', header=False, index=False)
    with ica_md5sum_file_path.open('r') as ica_md5_fh, open('ica_md5.txt', 'w') as ica_out_fh:
        for line in ica_md5_fh:
            ica_out_fh.write(line)
    md5_result = subprocess.run(['md5sum', '-c', 'supplied_checksum.txt', 'ica_md5.txt'], check=False)
