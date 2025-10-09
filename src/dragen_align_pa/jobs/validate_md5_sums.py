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

    job.call(_run, ica_md5sum_file_path=ica_md5sum_file_path)

    return job


def _run(ica_md5sum_file_path: cpg_utils.Path) -> None:
    manifest_file_path: cpg_utils.Path = config_retrieve(['workflow', 'manifest_gcp_path'])
    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        supplied_manifest_data: pd.DataFrame = pd.read_csv(
            manifest_fh, usecols=['Filenames', 'Checksum'], dtype={'Filenames': 'object', 'Checksum': 'object'}
        )
    with ica_md5sum_file_path.open('r') as ica_md5_fh:
        ica_md5_data: pd.DataFrame = pd.read_csv(
            ica_md5_fh,
            sep=r'\s+',
            names=['IcaChecksum', 'Filenames'],
            dtype={'IcaChecksum': 'object', 'Filenames': 'object'},
        )
    print(supplied_manifest_data)
    print(ica_md5_data)
    print(supplied_manifest_data.info(verbose=True))
    print(ica_md5_data.info(verbose=True))
    merged_checksum_data: pd.DataFrame = supplied_manifest_data.merge(ica_md5_data, on='Filenames', how='outer')
    merged_checksum_data['Match'] = merged_checksum_data['Checksum'].equals(merged_checksum_data['IcaChecksum'])
    if not merged_checksum_data['Match'].all():
        raise Exception(
            f'The following files have non-matching checksums: {(merged_checksum_data[merged_checksum_data["Match"]]["Filenames"])}'
        )
