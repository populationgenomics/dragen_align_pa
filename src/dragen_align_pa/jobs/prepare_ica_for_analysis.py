import json

import cpg_utils.config
from cpg_flow.targets import Cohort
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.constants_registry import ROLE_DRAGEN_ALIGN
from dragen_align_pa.paths import IcaPath

# Fail-fast here so a doomed submission surfaces with context instead of
# crashing opaquely later at pipeline launch.
_TERMINAL_BAD_FOLDER_STATUSES = frozenset({'ARCHIVED', 'DELETING', 'UNARCHIVING'})


def run(cohort: Cohort, output: cpg_utils.Path) -> None:
    """Create (or find) a single ICA folder for the cohort's pipeline outputs.

    This pipeline writes per-batch (not per-SG) analysis folders directly under
    this parent folder.
    """
    folder_path: str = IcaPath.output_root().as_folder()

    with ica_api_utils.ica_project_data_api(ROLE_DRAGEN_ALIGN) as (api_instance, path_parameters):
        folder_id, status = ica_utils.create_upload_object_id(
            api_instance=api_instance,
            path_params=path_parameters,
            folder_name=cohort.name,
            file_name=cohort.name,
            folder_path=folder_path,
            object_type='FOLDER',
        )
    logger.info(f'Cohort output folder for {cohort.name} (status {status}): {folder_id}')

    if status in _TERMINAL_BAD_FOLDER_STATUSES:
        raise RuntimeError(
            f'Cohort output folder for {cohort.name} is in terminal-bad status '
            f'{status!r} (folder_id={folder_id}). DRAGEN pipeline submissions '
            f'against this folder will fail. Manually unarchive or recreate the '
            f'folder in ICA, then re-run the cohort.',
        )
    if status != 'AVAILABLE':
        logger.warning(
            f'Cohort output folder for {cohort.name} has non-AVAILABLE status '
            f'{status!r} (folder_id={folder_id}). The folder is likely in a '
            f'transient state; if subsequent pipeline submissions fail, verify '
            f"the folder's state in ICA before retrying.",
        )

    with output.open('w') as fh:
        json.dump({'analysis_output_fid': folder_id}, fh)
