import re
import subprocess
from math import ceil
from typing import TYPE_CHECKING, Any

import cpg_utils
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_utils.config import get_access_level, get_driver_image, output_path
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger
from metamist.graphql import gql, query

from dragen_align_pa.constants import DRAGEN_VERSION

if TYPE_CHECKING:
    from graphql import DocumentNode


def validate_cli_path_input(path: str, arg_name: str) -> None:
    """
    Validates that a path string does not contain shell metacharacters
    to prevent potential injection vulnerabilities.
    """
    # Regex for common shell metacharacters and whitespace,
    # excluding GCS 'gs://' prefix, path slashes '/', and underscores '_'
    if re.search(r'[;&|$`(){}[\]<>*?!#\s]', path):
        logger.error(f'Invalid characters found in {arg_name}: {path}')
        raise ValueError(f'Potential unsafe characters in {arg_name}')
    logger.info(f'Path validation passed for {arg_name}.')


def delete_pipeline_id_file(pipeline_id_file: str) -> None:
    logger.info(f'Deleting the pipeline run ID file {pipeline_id_file}')
    subprocess.run(  # noqa: S603
        ['gcloud', 'storage', 'rm', pipeline_id_file],  # noqa: S607
        check=True,
    )


def calculate_needed_storage(
    cram_path: cpg_utils.Path,
) -> str:
    logger.info(f'Checking blob size for {cram_path}')

    storage_size: int = cram_path.stat().st_size
    # Added a buffer (3GB) and increased multiplier slightly (1.2 -> 1.3)
    # Ceil ensures we get whole GiB, adding buffer helps avoid edge cases
    calculated_gb = ceil((storage_size / (1024**3)) + 3) * 1.3
    # Ensure a minimum storage request (e.g., 10GiB)
    final_storage_gb = max(10, ceil(calculated_gb))
    logger.info(f'Calculated storage need: {final_storage_gb}GiB for {cram_path}')
    return f'{final_storage_gb}Gi'


def run_subprocess_with_log(
    cmd: str | list[str],
    step_name: str,
    stdin_input: str | None = None,
    shell: bool = False,
) -> subprocess.CompletedProcess[Any]:
    """
    Runs a subprocess command with robust logging.
    Logs the command, its output, and errors if any occur.
    """
    cmd_str = cmd if isinstance(cmd, str) else ' '.join(cmd)
    executable = '/bin/bash' if shell else None
    logger.info(f'Running {step_name} command: {cmd_str}')
    try:
        process: subprocess.CompletedProcess[str] = subprocess.run(  # noqa: S603
            cmd,
            check=True,
            capture_output=True,
            text=True,
            input=stdin_input,
            shell=shell,
            executable=executable,
        )
        logger.info(f'{step_name} completed successfully.')
        if process.stdout:
            logger.info(f'{step_name} STDOUT:\n{process.stdout.strip()}')
        if process.stderr:
            logger.info(f'{step_name} STDERR:\n{process.stderr.strip()}')
        return process
    except subprocess.CalledProcessError as e:
        logger.error(f'{step_name} failed with return code {e.returncode}')
        logger.error(f'CMD: {cmd_str}')
        logger.error(f'STDOUT: {e.stdout}')
        logger.error(f'STDERR: {e.stderr}')
        raise


def initialise_python_job(
    job_name: str,
    target: Cohort | SequencingGroup,
    tool_name: str,
) -> PythonJob:
    """
    Initialises a standard PythonJob with common attributes.
    """
    py_job: PythonJob = get_batch().new_python_job(
        name=job_name,
        attributes=(target.get_job_attrs() or {}) | {'tool': tool_name},  # pyright: ignore[reportUnknownArgumentType]
    )
    py_job.image(get_driver_image())
    return py_job


def get_prep_path(filename: str) -> cpg_utils.Path:
    """Gets a path in the 'prepare' directory."""
    return cpg_utils.to_path(output_path(f'ica/{DRAGEN_VERSION}/prepare/{filename}'))


def get_pipeline_path(filename: str) -> cpg_utils.Path:
    """Gets a path in the 'pipelines' (state) directory."""
    return cpg_utils.to_path(output_path(f'ica/{DRAGEN_VERSION}/pipelines/{filename}'))


def get_output_path(filename: str, category: str | None = None) -> cpg_utils.Path:
    """Gets a path in the final 'output' directory."""
    return cpg_utils.to_path(output_path(f'ica/{DRAGEN_VERSION}/output/{filename}', category=category))


def get_qc_path(filename: str, category: str | None = None) -> cpg_utils.Path:
    """Gets a path in the 'qc' directory."""
    return cpg_utils.to_path(output_path(f'ica/{DRAGEN_VERSION}/qc/{filename}', category=category))


def get_manifest_path_for_cohort(cohort: Cohort) -> cpg_utils.Path:
    """
    Queries Metamist for the 'manifest' analysis for a given cohort
    and returns the GCS path to the manifest file.

    If access_level is 'test', it fetches the 'control' manifest.
    Otherwise, it fetches the 'production' manifest.
    """
    logger.info(f'Querying Metamist for manifest path for cohort {cohort.id}')
    access_level: str = get_access_level()
    logger.info(f'Using access level: {access_level}')

    if access_level == 'test':
        manifest_type: str = 'control'
        required_basename_str: str = 'control_manifest'
        required_dirname_str: str = 'control_manifests'
    else:
        manifest_type: str = 'production'
        required_basename_str: str = 'production_manifest'
        required_dirname_str: str = 'production_manifests'

    logger.info(f'Searching for {manifest_type} manifest analysis in Metamist')

    manifest_query: DocumentNode = gql(
        request_string="""
        query GetCohortManifest($cohortId: String!) {
          cohorts(id: {eq: $cohortId}) {
            id
            name
            analyses {
              id
              type
              outputs
            }
          }
        }
    """
    )

    try:
        result = query(manifest_query, variables={'cohortId': cohort.id})

        if not result.get('cohorts'):
            raise ValueError(f'No cohort found in Metamist with ID {cohort.id}')

        analyses = result['cohorts'][0].get('analyses', [])
        if not analyses:
            raise ValueError(f'No analyses found for cohort {cohort.id}')

        # 1. Filter all analyses based on all criteria
        matching_manifests = []
        for a in analyses:
            outputs = a.get('outputs')
            if (
                a.get('type') == 'manifest'
                and outputs
                and required_basename_str in outputs.get('basename', '')
                and required_dirname_str in outputs.get('dirname', '')
            ):
                matching_manifests.append(a)

        # 2. Check the number of matches
        if not matching_manifests:
            raise ValueError(
                f"No 'manifest' analysis found for cohort {cohort.id} "
                f"matching type '{manifest_type}' (basename: '{required_basename_str}', dirname: '{required_dirname_str}')."
            )

        if len(matching_manifests) > 1:
            all_ids = [m.get('id') for m in matching_manifests]
            logger.warning(
                f"Found {len(matching_manifests)} matching '{manifest_type}' analyses for "
                f'cohort {cohort.id} (IDs: {all_ids}). '
                f'Using the first one found (ID: {matching_manifests[0].get("id")}).'
            )

        manifest_entry = matching_manifests[0]
        manifest_path = manifest_entry.get('outputs', {}).get('path')

        if not manifest_path:
            raise ValueError(
                f"Matching '{manifest_type}' analysis (ID: {manifest_entry.get('id')}) found for {cohort.id}, "
                f"but 'outputs.path' is missing or null."
                f'{manifest_entry}'
            )

        logger.info(f"Found '{manifest_type}' manifest path: {manifest_path} (Analysis ID: {manifest_entry.get('id')})")
        return cpg_utils.to_path(manifest_path)

    except Exception as e:
        logger.error(f'Failed to query/parse manifest path for cohort {cohort.id}: {e}')
        raise
