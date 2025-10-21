import cpg_utils
import pandas as pd
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob

from dragen_align_pa.constants import BUCKET, GCP_FOLDER_FOR_ICA_PREP


def _initalise_md5sum_validation_job(cohort: Cohort) -> PythonJob:
    job: PythonJob = get_batch().new_python_job(
        name='ValidateMd5Sums',
        attributes=cohort.get_job_attrs() or {} | {'tool': 'md5sum'},  # type: ignore[ReportUnknownVariableType]
    )
    job.image(image=get_driver_image())
    return job


def validate_md5_sums(
    ica_md5sum_file_path: cpg_utils.Path,
    cohort: Cohort,
    outputs: cpg_utils.Path,
) -> PythonJob:
    job: PythonJob = _initalise_md5sum_validation_job(cohort=cohort)

    validation_success: str = job.call(
        _run,
        ica_md5sum_file_path=ica_md5sum_file_path,
        cohort_name=cohort.name,
    ).as_str()

    get_batch().write_output(resource=validation_success, dest=outputs)

    return job


def _run(ica_md5sum_file_path: cpg_utils.Path, cohort_name: str) -> str:
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
    merged_checksum_data: pd.DataFrame = supplied_manifest_data.merge(ica_md5_data, on='Filenames', how='outer')
    merged_checksum_data['Match'] = merged_checksum_data['Checksum'].equals(merged_checksum_data['IcaChecksum'])
    if not merged_checksum_data['Match'].all():
        error_log: cpg_utils.Path = BUCKET / GCP_FOLDER_FOR_ICA_PREP / f'{cohort_name}_md5_errors.log'
        with error_log.open() as error_fh:
            merged_checksum_data[~merged_checksum_data['Match']]['Filenames'].map(lambda x: error_fh.write(x))
        raise Exception(
            f'The following files have non-matching checksums: {merged_checksum_data[~merged_checksum_data["Match"]]["Filenames"].map(lambda x: print(x))}. Check the log file at {error_log}.'  # noqa: E501
        )
    return 'SUCCESS'
