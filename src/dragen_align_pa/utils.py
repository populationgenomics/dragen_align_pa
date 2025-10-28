import re
import subprocess
from math import ceil

import cpg_utils
from loguru import logger


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
    subprocess.run(
        ['gcloud', 'storage', 'rm', pipeline_id_file],
        check=True,
    )


def calculate_needed_storage(
    cram_path: cpg_utils.Path,  # <-- Changed type hint from str to Path
) -> str:
    logger.info(f'Checking blob size for {cram_path}')
    # Removed cpg_utils.to_path() conversion as input is now Path type
    storage_size: int = cram_path.stat().st_size
    # Added a buffer (3GB) and increased multiplier slightly (1.2 -> 1.3)
    # Ceil ensures we get whole GiB, adding buffer helps avoid edge cases
    calculated_gb = ceil((storage_size / (1024**3)) + 3) * 1.3
    # Ensure a minimum storage request (e.g., 10GiB)
    final_storage_gb = max(10, ceil(calculated_gb))
    logger.info(f'Calculated storage need: {final_storage_gb}GiB for {cram_path}')
    return f'{final_storage_gb}Gi'
