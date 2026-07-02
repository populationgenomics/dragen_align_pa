import json

import cpg_utils.config
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.constants import BUCKET_NAME, resolve_ica_project_id

# Fail-fast here so a doomed submission surfaces with context instead of
# crashing opaquely later at pipeline launch.
_TERMINAL_BAD_FOLDER_STATUSES = frozenset({'ARCHIVED', 'DELETING', 'UNARCHIVING'})


def run(cohort: Cohort, output: cpg_utils.Path) -> None:
    """Create (or find) a single ICA folder for the cohort's pipeline outputs.

    This pipeline writes per-batch (not per-SG) analysis folders directly under
    this parent folder.
    """
    path_parameters: dict[str, str] = {
        'projectId': resolve_ica_project_id(cpg_utils.config.config_retrieve(['ica', 'projects', 'dragen_align']))
    }
    output_folder: str = config_retrieve(['ica', 'data_prep', 'output_folder'])
    folder_path: str = f'/{BUCKET_NAME}/{output_folder}'

    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
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
