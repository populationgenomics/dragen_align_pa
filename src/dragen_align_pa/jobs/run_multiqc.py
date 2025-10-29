import os
import shutil

from cpg_flow.stage import StageInput, StageInputNotFoundError
from cpg_flow.targets import Cohort
from cpg_utils import Path, to_path
from cpg_utils.config import get_driver_image
from hailtop.batch.job import PythonJob
from loguru import logger

from dragen_align_pa import utils


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
    cmd = ['gcloud', 'storage', 'cp', '-I', local_input_dir]

    try:
        # Pass the newline-separated paths via stdin
        utils.run_subprocess_with_log(cmd, 'Copy inputs via gcloud storage cp -I', stdin_input=stdin_data)
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
    utils.run_subprocess_with_log(command, 'MultiQC execution')


def _upload_outputs(local_output_dir: str, cohort_name: str, outputs: dict[str, str]) -> None:
    """Uploads the MultiQC JSON and HTML outputs to GCS using gcloud storage cp."""
    report_name = f'{cohort_name}_multiqc_report'
    local_html_path = os.path.join(local_output_dir, f'{report_name}.html')
    local_json_data_path = os.path.join(local_output_dir, f'{report_name}_data', 'multiqc_data.json')
    # Target name for the JSON file in GCS (matches expected_outputs)
    final_gcs_json_path = outputs['multiqc_data']

    if os.path.exists(local_html_path):
        utils.run_subprocess_with_log(
            ['gcloud', 'storage', 'cp', local_html_path, outputs['multiqc_report']], 'Upload HTML report'
        )
    else:
        logger.error(f'MultiQC HTML report not found at {local_html_path}')
        raise FileNotFoundError(f'MultiQC HTML report not found: {local_html_path}')

    if os.path.exists(local_json_data_path):
        # Upload the multiqc_data.json file directly to the final GCS path
        utils.run_subprocess_with_log(
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
    inputs: StageInput,
    outputs: dict[str, str],
) -> PythonJob | None:
    """
    Creates and calls the PythonJob to run MultiQC.
    Gathers all required QC input paths.
    """
    from dragen_align_pa.stages import DownloadDataFromIca, SomalierExtract

    # 1. Get Dragen metric directory prefixes for each SG
    dragen_metric_prefixes: list[Path] = []
    for sg in cohort.get_sequencing_groups():
        try:
            # The output of DownloadDataFromIca is the directory prefix
            prefix = inputs.as_path(target=sg, stage=DownloadDataFromIca)
            dragen_metric_prefixes.append(prefix)
        except StageInputNotFoundError:
            logger.warning(f'Dragen metrics directory not found for {sg.id}, skipping for MultiQC')

    # 2. Get Somalier paths for each SG
    somalier_paths_dict: dict[str, Path] = inputs.as_path_by_target(stage=SomalierExtract)
    somalier_paths: list[Path] = list(somalier_paths_dict.values())

    # 3. Collect all individual Dragen CSV file paths
    all_dragen_csv_paths: list[Path] = []
    for prefix in dragen_metric_prefixes:
        try:
            # Use rglob to find all CSV files recursively within the SG's metric directory
            found_paths = [to_path(p) for p in prefix.rglob('*.csv')]
            all_dragen_csv_paths.extend(found_paths)
        except FileNotFoundError:
            logger.warning(f'Directory {prefix} not found when searching for Dragen CSVs.')
        except Exception as e:
            logger.error(f'Error searching for CSVs in {prefix}: {e}')

    # 4. Combine Dragen CSV paths and Somalier paths
    all_qc_paths: list[Path] = all_dragen_csv_paths + somalier_paths

    if not all_qc_paths:
        logger.warning('No QC files (Dragen CSVs or Somalier) found to aggregate with MultiQC')
        return None  # Return None to signal the stage to skip

    logger.info(f'Found {len(all_qc_paths)} QC files for MultiQC aggregation.')
    if all_qc_paths:
        logger.info(f'Example QC paths: {all_qc_paths[:5]}')

    # 5. Create the PythonJob
    py_job: PythonJob = utils.initialise_python_job(
        job_name='MultiQC',
        target=cohort,
        tool_name='MultiQC',
    )
    py_job.image(image=get_driver_image())
    py_job.storage('10Gi')

    # Convert Path objects to strings for the job function
    input_paths_str: list[str] = [str(p) for p in all_qc_paths]

    py_job.call(
        _run,
        cohort_name=cohort.name,
        input_paths_str=input_paths_str,
        outputs=outputs,
    )

    return py_job
