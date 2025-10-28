"""
Create Hail Batch jobs for Somalier extract using a PythonJob, following
the standard workflow job structure.
"""

import os
import shutil

from cpg_flow.filetypes import CramPath
from cpg_flow.targets import SequencingGroup
from cpg_flow.utils import can_reuse
from cpg_utils import Path, to_path
from cpg_utils.config import reference_path
from hailtop.batch.job import PythonJob
from loguru import logger

from dragen_align_pa import utils


def _copy_inputs_locally(
    gcs_paths: dict[str, str],
    local_dir: str,
) -> dict[str, str]:
    """Copies files from GCS to the specified local directory using gcloud storage."""
    local_paths = {}
    # Ensure the target directory exists (it should be $BATCH_TMPDIR)
    os.makedirs(local_dir, exist_ok=True)
    for key, gcs_path in gcs_paths.items():
        local_path = os.path.join(local_dir, os.path.basename(gcs_path))
        utils.run_subprocess_with_log(['gcloud', 'storage', 'cp', gcs_path, local_path], f'Copy {key}')
        local_paths[key] = local_path
    logger.info(f'Successfully copied all input files to {local_dir}.')
    return local_paths


def _execute_somalier(
    local_cram_path: str,
    local_sites_path: str,
    local_ref_fasta_path: str,
    output_dir: str = 'extracted',
) -> str:
    """Executes the somalier extract command."""
    os.makedirs(output_dir, exist_ok=True)
    command = [
        'somalier',
        'extract',
        '-d',
        output_dir,
        '--sites',
        local_sites_path,
        '-f',
        local_ref_fasta_path,
        local_cram_path,
    ]
    utils.run_subprocess_with_log(command, 'Somalier extract')
    return output_dir


def _find_and_upload_output(local_output_dir: str, gcs_output_path: str) -> str:
    """Finds the .somalier output file and uploads it to GCS."""
    somalier_files = list(to_path(local_output_dir).glob('*.somalier'))
    if not somalier_files:
        raise FileNotFoundError(f'Somalier output file not found in {local_output_dir}')
    if len(somalier_files) > 1:
        logger.warning(
            f'Found multiple .somalier files in {local_output_dir}, using the first one: {somalier_files[0]}'
        )
    local_output_path = str(somalier_files[0])

    utils.run_subprocess_with_log(['gcloud', 'storage', 'mv', local_output_path, gcs_output_path], 'Upload output')
    return local_output_path


def _cleanup_local_files(paths_to_remove: list[str]) -> None:
    """Removes local files and directories."""
    logger.info('Cleaning up local files...')
    for path in paths_to_remove:
        try:
            if os.path.isfile(path):
                os.remove(path)
                logger.info(f'Removed file: {path}')
            elif os.path.isdir(path):
                shutil.rmtree(path)
                logger.info(f'Removed directory: {path}')
        except OSError as e:
            logger.warning(f'Could not remove {path}: {e}')
    logger.info('Cleanup complete.')


def _run_somalier_extract(
    cram_path_str: str,
    crai_path_str: str,
    out_somalier_path_str: str,
    ref_fasta_path_str: str,
    ref_fai_path_str: str,
    sites_path_str: str,
) -> None:
    """
    Orchestrates the somalier extract process: copy inputs, run, upload output, cleanup.
    """
    gcs_input_paths = {
        'cram': cram_path_str,
        'crai': crai_path_str,
        'sites': sites_path_str,
        'ref_fasta': ref_fasta_path_str,
        'ref_fai': ref_fai_path_str,
    }
    batch_tmpdir = os.environ.get('BATCH_TMPDIR', '/io')
    logger.info(f'Using BATCH_TMPDIR: {batch_tmpdir}')
    local_paths: dict[str, str] = {}
    local_output_dir = os.path.join(batch_tmpdir, 'extracted')
    files_to_cleanup: list[str] = [local_output_dir]

    try:
        # 1. Copy inputs
        local_paths = _copy_inputs_locally(gcs_input_paths, local_dir=batch_tmpdir)
        files_to_cleanup.extend(local_paths.values())

        # 2. Execute Somalier
        _execute_somalier(
            local_cram_path=local_paths['cram'],
            local_sites_path=local_paths['sites'],
            local_ref_fasta_path=local_paths['ref_fasta'],
            output_dir=local_output_dir,
        )

        # 3. Find and Upload Output
        _find_and_upload_output(
            local_output_dir=local_output_dir,
            gcs_output_path=out_somalier_path_str,
        )

    except Exception as e:
        logger.error(f'Somalier extract process failed: {e}')
        # Re-raise the exception to fail the Hail Batch job
        raise
    finally:
        # 4. Cleanup regardless of success or failure
        _cleanup_local_files(files_to_cleanup)


def somalier_extract(
    sequencing_group: SequencingGroup,
    cram_path: CramPath,
    out_somalier_path: Path,
    overwrite: bool = True,
) -> PythonJob | None:
    """
    Public function to create and configure the Somalier extract PythonJob.
    """
    if can_reuse(out_somalier_path, overwrite):
        logger.info(f'Reusing existing Somalier output: {out_somalier_path}')
        return None

    if not cram_path.index_path:
        raise ValueError(f'CRAM for somalier is required to have CRAI index ({cram_path})')

    # Initialize the job using the helper function, passing cram_path for storage calc
    somnalier_job: PythonJob = utils.initialise_python_job(
        job_name=f'Somalier extract {sequencing_group.id}',
        target=sequencing_group,
        tool_name='somalier',
    )
    somnalier_job.image(image=utils.get_driver_image())
    somnalier_job.storage(storage=utils.calculate_needed_storage(cram_path=cram_path.path))
    somnalier_job.memory('8Gi')

    # Get resource file paths
    ref_fasta = reference_path('broad/ref_fasta')
    somalier_sites = reference_path('somalier_sites')

    # Schedule the core logic function (_run_somalier_extract) to run within the job
    somnalier_job.call(
        _run_somalier_extract,
        cram_path_str=str(cram_path.path),
        crai_path_str=str(cram_path.index_path),
        out_somalier_path_str=str(out_somalier_path),
        ref_fasta_path_str=str(ref_fasta),
        ref_fai_path_str=str(ref_fasta) + '.fai',
        sites_path_str=str(somalier_sites),
    )

    return somnalier_job
