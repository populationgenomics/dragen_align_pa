import cpg_utils
import pandas as pd
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob


def _initalise_fastq_list_job(cohort: Cohort) -> PythonJob:
    job: PythonJob = get_batch().new_python_job(
        name='MakeFastqFileList',
        attributes=cohort.get_job_attrs() or {} | {'tool': 'ICA'},  # pyright: ignore[reportUnknownArgumentType]
    )
    job.image(image=get_driver_image())
    return job


def make_fastq_list_file(
    outputs: dict[str, cpg_utils.Path],
    analysis_output_fids_path: dict[str, cpg_utils.Path],
    cohort: Cohort,
    api_root: str,
) -> PythonJob:
    job: PythonJob = _initalise_fastq_list_job(cohort=cohort)
    output = job.call(
        _run, outputs=outputs, analysis_outputs_fid_path=analysis_output_fids_path, cohort=cohort, api_root=api_root
    )

    return job


def _run(
    outputs: dict[str, cpg_utils.Path],
    analysis_outputs_fid_path: dict[str, cpg_utils.Path],
    cohort: Cohort,
    api_root: str,
) -> None:
    manifest_file_path: cpg_utils.Path = config_retrieve(['workflow', 'manifest_gcp_path'])
    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        supplied_manifest_data: pd.DataFrame = pd.read_csv(manifest_fh)

    for sequencing_group in cohort.get_sequencing_groups():
        for assay in sequencing_group.assays:
            for item in assay.meta:
                if 'reads' not in item:
                    continue
                print(item['basename'])
        #  assay_filenames: list[str] = [sequencing_group.assays[0].meta['reads'][read]['basename'] for read in sequencing_group.assays[0].meta['reads']]
