import os
import shutil
import subprocess

from cpg_flow.targets import Cohort
from cpg_utils import Path
from cpg_utils.config import get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger


def _initialise_multiqc_job(cohort: Cohort) -> PythonJob:
    """Initialise a PythonJob for running MultiQC."""
    py_job = get_batch().new_python_job(
        name='RunMultiQc',
        attributes=(cohort.get_job_attrs() or {} | {'tool': 'MultiQC'}),  # pyright: ignore[reportUnknownArgumentType]
    )
    py_job.image(get_driver_image())
    py_job.storage('10Gi')
    return py_job


def _run_subprocess_with_log(
    cmd: list[str],
    step_name: str,
    stdin_input: str | None = None,  # Added optional stdin input
) -> None:
    """Runs a subprocess command and logs details, raising an error on failure."""
    logger.info(f'Running {step_name} command: {" ".join(cmd)}')
    try:
        process = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            input=stdin_input,  # Pass stdin if provided
        )
        logger.info(f'{step_name} completed successfully.')
        if process.stdout:
            logger.info(f'{step_name} STDOUT:\n{process.stdout.strip()}')
        if process.stderr:
            logger.info(f'{step_name} STDERR:\n{process.stderr.strip()}')
    except subprocess.CalledProcessError as e:
        logger.error(f'{step_name} failed with return code {e.returncode}')
        logger.error(f'CMD: {" ".join(e.cmd)}')
        logger.error(f'STDOUT: {e.stdout}')
        logger.error(f'STDERR: {e.stderr}')
        raise


def _copy_inputs_to_local(input_paths_str: list[str], local_input_dir: str) -> None:
    """
    Copies input files from GCS to a local directory using gcloud storage cp -I
    by reading paths from standard input.
    """
    os.makedirs(local_input_dir, exist_ok=True)
    logger.info(f'Copying {len(input_paths_str)} input files to {local_input_dir} using gcloud storage cp -I...')

    # Prepare the list of GCS paths as a single string, newline-separated, for stdin
    stdin_data = '\n'.join(input_paths_str)

    # Command to copy files listed in stdin to the local directory
    # -m enables parallel copies
    cmd = ['gcloud', 'storage', 'cp', '-m', '-I', local_input_dir]

    try:
        # Pass the newline-separated paths via stdin
        _run_subprocess_with_log(cmd, 'Copy inputs via gcloud storage cp -I', stdin_input=stdin_data)
        logger.info(f'Finished copying input files to {local_input_dir}.')
    except Exception as e:
        # Catch potential errors during the copy process
        # gcloud storage cp -I might fail if *any* file is missing.
        # Log a warning and continue, as MultiQC can often handle missing files.
        logger.warning(
            f"Copying files with 'gcloud storage cp -I' encountered an error (some files might be missing): {e}"
        )
        logger.warning('Proceeding with MultiQC execution...')


def _run_multiqc_cmd(local_input_dir: str, local_output_dir: str, cohort_name: str) -> None:
    """Runs the multiqc command."""
    os.makedirs(local_output_dir, exist_ok=True)
    report_name = f'{cohort_name}_multiqc_report'
    # Ensure multiqc command uses only necessary quotes if paths have spaces (unlikely in GCS)
    command = [
        'multiqc',
        local_input_dir,
        '-o',
        local_output_dir,
        '--title',
        f'MultiQC Report for {cohort_name}',
        '--filename',
        f'{report_name}.html',
        '--cl-config',
        'max_table_rows: 10000',
    ]
    _run_subprocess_with_log(command, 'MultiQC execution')


def _upload_outputs(local_output_dir: str, cohort_name: str, outputs: dict[str, str]) -> None:
    """Uploads the MultiQC JSON and HTML outputs to GCS using gcloud storage cp."""
    report_name = f'{cohort_name}_multiqc_report'
    local_html_path = os.path.join(local_output_dir, f'{report_name}.html')
    local_json_data_path = os.path.join(local_output_dir, f'{report_name}_data', 'multiqc_data.json')
    # Target name for the JSON file in GCS (matches expected_outputs)
    final_gcs_json_path = outputs['multiqc_data']

    if os.path.exists(local_html_path):
        _run_subprocess_with_log(
            ['gcloud', 'storage', 'cp', local_html_path, outputs['multiqc_report']], 'Upload HTML report'
        )
    else:
        logger.error(f'MultiQC HTML report not found at {local_html_path}')
        raise FileNotFoundError(f'MultiQC HTML report not found: {local_html_path}')

    if os.path.exists(local_json_data_path):
        # Upload the multiqc_data.json file directly to the final GCS path
        _run_subprocess_with_log(
            ['gcloud', 'storage', 'cp', local_json_data_path, final_gcs_json_path], 'Upload JSON data'
        )
    else:
        # It's possible MultiQC ran but produced no data if all inputs were bad/missing
        logger.warning(f'MultiQC JSON data not found at {local_json_data_path}, skipping upload.')


def _cleanup_local_dirs(dirs_to_remove: list[str]) -> None:
    """Removes local directories."""
    logger.info('Cleaning up local directories...')
    for dir_path in dirs_to_remove:
        if os.path.exists(dir_path) and os.path.isdir(dir_path):
            try:
                shutil.rmtree(dir_path)
                logger.info(f'Removed directory: {dir_path}')
            except OSError as e:
                logger.warning(f'Could not remove directory {dir_path}: {e}')
    logger.info('Cleanup complete.')


def _run(cohort_name: str, input_paths_str: list[str], outputs: dict[str, str]) -> None:
    """
    Core logic for the MultiQC PythonJob.
    Copies inputs locally using gcloud storage cp -I, runs multiqc, uploads outputs.
    """
    batch_tmpdir = os.environ.get('BATCH_TMPDIR', '/io')
    local_input_dir = os.path.join(batch_tmpdir, 'input_data')
    local_output_dir = os.path.join(batch_tmpdir, 'output')
    dirs_to_cleanup = [local_input_dir, local_output_dir]

    try:
        _copy_inputs_to_local(input_paths_str, local_input_dir)
        _run_multiqc_cmd(local_input_dir, local_output_dir, cohort_name)
        _upload_outputs(local_output_dir, cohort_name, outputs)
    except Exception as e:
        logger.error(f'MultiQC job failed: {e}')
        raise
    finally:
        _cleanup_local_dirs(dirs_to_cleanup)


def run_multiqc(
    cohort: Cohort,
    input_paths: list[Path],
    outputs: dict[str, str],
) -> PythonJob:
    """
    Creates and calls the PythonJob to run MultiQC.
    """
    py_job: PythonJob = _initialise_multiqc_job(cohort=cohort)

    # Convert Path objects to strings for the job function
    input_paths_str: list[str] = [str(p) for p in input_paths]

    py_job.call(
        _run,
        cohort_name=cohort.name,
        input_paths_str=input_paths_str,
        outputs=outputs,
    )

    return py_job
