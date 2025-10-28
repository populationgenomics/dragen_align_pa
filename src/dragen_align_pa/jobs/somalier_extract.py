"""
Create Hail Batch jobs for Somalier extract using a PythonJob, following
the standard workflow job structure.
"""

import os
import shutil
import subprocess

from cpg_flow.filetypes import CramPath
from cpg_flow.targets import SequencingGroup  # Added import
from cpg_flow.utils import can_reuse
from cpg_utils import Path, to_path
from cpg_utils.config import image_path, reference_path
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger

from dragen_align_pa import utils


def _initialise_somalier_job(
    sequencing_group: SequencingGroup,
) -> PythonJob:
    """
    Initialise a PythonJob for running Somalier extract.
    """
    job_name = f'Somalier extract {sequencing_group.id}'
    py_job = get_batch().new_python_job(job_name, (sequencing_group.get_job_attrs() or {}) | {'tool': 'somalier'})
    py_job.image(image_path('somalier'))  # Ensure this image has somalier, python, and gsutil

    # Configure resources
    storage_gb = utils.calculate_needed_storage(sequencing_group.cram.path)
    py_job.storage(f'{storage_gb}Gi')

    return py_job


def _run_somalier_extract(
    cram_path_str: str,
    crai_path_str: str,
    out_somalier_path_str: str,
    ref_fasta_path_str: str,
    ref_fai_path_str: str,  # Added FAI path explicitly
    sites_path_str: str,
):
    """
    Internal function to run somalier extract via subprocess within the PythonJob.
    """
    local_cram_path = os.path.join(os.getcwd(), os.path.basename(cram_path_str))
    local_crai_path = os.path.join(os.getcwd(), os.path.basename(crai_path_str))
    local_sites_path = os.path.join(os.getcwd(), os.path.basename(sites_path_str))
    local_ref_fasta_path = os.path.join(os.getcwd(), os.path.basename(ref_fasta_path_str))
    local_ref_fai_path = os.path.join(os.getcwd(), os.path.basename(ref_fai_path_str))

    output_dir = 'extracted'
    os.makedirs(output_dir, exist_ok=True)

    # Attempt to copy inputs locally using gsutil.
    try:
        subprocess.run(['gsutil', 'cp', cram_path_str, local_cram_path], check=True, capture_output=True, text=True)
        subprocess.run(['gsutil', 'cp', crai_path_str, local_crai_path], check=True, capture_output=True, text=True)
        subprocess.run(['gsutil', 'cp', sites_path_str, local_sites_path], check=True, capture_output=True, text=True)
        subprocess.run(
            ['gsutil', 'cp', ref_fasta_path_str, local_ref_fasta_path], check=True, capture_output=True, text=True
        )
        subprocess.run(
            ['gsutil', 'cp', ref_fai_path_str, local_ref_fai_path], check=True, capture_output=True, text=True
        )
        logger.info('Successfully copied input files locally.')
    except subprocess.CalledProcessError as e:
        logger.error('Failed to copy input files using gsutil.')
        logger.error(f'CMD: {" ".join(e.cmd)}')
        logger.error(f'STDOUT: {e.stdout}')
        logger.error(f'STDERR: {e.stderr}')
        raise

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

    try:
        logger.info(f'Running command: {" ".join(command)}')
        # Run somalier extract
        process = subprocess.run(command, check=True, text=True, capture_output=True)
        logger.info('Somalier extract completed successfully.')
        logger.info(f'Somalier STDOUT:\n{process.stdout}')
        logger.info(f'Somalier STDERR:\n{process.stderr}')

        # Find the output file (somalier extract names it based on sample ID in CRAM)
        somalier_files = list(to_path(output_dir).glob('*.somalier'))
        if not somalier_files:
            raise FileNotFoundError(f'Somalier output file not found in {output_dir}')
        if len(somalier_files) > 1:
            logger.warning(f'Found multiple .somalier files in {output_dir}, using the first one: {somalier_files[0]}')

        local_output_path = str(somalier_files[0])

        # Move the output file to the expected GCS path using gsutil
        logger.info(f'Moving {local_output_path} to {out_somalier_path_str}')
        subprocess.run(
            ['gsutil', 'mv', local_output_path, out_somalier_path_str], check=True, capture_output=True, text=True
        )

    except subprocess.CalledProcessError as e:
        logger.error(f'Command failed with return code {e.returncode}')
        logger.error(f'CMD: {" ".join(e.cmd)}')
        logger.error(f'STDOUT: {e.stdout}')
        logger.error(f'STDERR: {e.stderr}')
        raise
    except Exception as e:
        logger.error(f'An unexpected error occurred: {e}')
        raise
    finally:
        # Clean up local files
        logger.info('Cleaning up local files...')
        if os.path.exists(local_cram_path):
            os.remove(local_cram_path)
        if os.path.exists(local_crai_path):
            os.remove(local_crai_path)
        if os.path.exists(local_sites_path):
            os.remove(local_sites_path)
        if os.path.exists(local_ref_fasta_path):
            os.remove(local_ref_fasta_path)
        if os.path.exists(local_ref_fai_path):
            os.remove(local_ref_fai_path)
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        logger.info('Cleanup complete.')


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

    # Initialize the job using the helper function
    py_job = _initialise_somalier_job(
        sequencing_group=sequencing_group,
    )

    # Get resource file paths
    ref_fasta = reference_path('broad/ref_fasta')
    somalier_sites = reference_path('somalier_sites')

    # Schedule the core logic function (_run_somalier_extract) to run within the job
    py_job.call(
        _run_somalier_extract,
        cram_path_str=str(cram_path.path),
        crai_path_str=str(cram_path.index_path),
        out_somalier_path_str=str(out_somalier_path),
        ref_fasta_path_str=str(ref_fasta),
        ref_fai_path_str=str(ref_fasta) + '.fai',
        sites_path_str=str(somalier_sites),
    )

    return py_job
