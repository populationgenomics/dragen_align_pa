import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob


def _inisalise_fastq_upload_job(cohort: Cohort) -> PythonJob:
    job: PythonJob = get_batch().new_python_job(
        name='UploadFastqFileList',
        attributes=cohort.get_job_attrs() or {} | {'tool': 'ICA'},  # pyright: ignore[reportUnknownArgumentType]
    )
    job.image(image=get_driver_image())
    return job


def upload_fastq_file_list(
    cohort: Cohort,
    outputs: dict[str, cpg_utils.Path],
    analysis_output_fids_path: dict[str, cpg_utils.Path],
) -> PythonJob:
    """Upload the fastq file list to ICA and return the job.

    Args:
        cohort: The cohort to upload the fastq file list for.
        outputs: A dictionary with the output paths.
        analysis_output_fids_path: A dictionary with the analysis output fids path.
    """
    job: PythonJob = _inisalise_fastq_upload_job(cohort=cohort)
    job.call(_run, outputs=outputs, analysis_output_fids_path=analysis_output_fids_path)
    return job

def _run(output: str, analysis_output_fids_path: str) -> None:)
