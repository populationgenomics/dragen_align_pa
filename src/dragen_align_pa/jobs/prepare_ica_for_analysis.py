import json
from typing import Literal

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.constants import BUCKET_NAME

# ICA folder statuses that are terminally incompatible with subsequent
# pipeline submissions. ARCHIVED / DELETING / UNARCHIVING all guarantee that
# writes into the folder will fail — fail-fast at the prep step instead of
# deferring to an opaque error during pipeline launch.
_TERMINAL_BAD_FOLDER_STATUSES = frozenset({'ARCHIVED', 'DELETING', 'UNARCHIVING'})


def run(cohort: Cohort, output: cpg_utils.Path) -> None:
    """Create (or find) a single ICA folder for the cohort's pipeline outputs.

    The new unified pipeline writes per-batch analysis folders directly under
    this parent folder, so we no longer need per-SG parent folders.
    """
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    project_id: str = secrets['projectID']
    output_folder: str = config_retrieve(['ica', 'data_prep', 'output_folder'])
    folder_path: str = f'/{BUCKET_NAME}/{output_folder}'

    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        folder_id, status = ica_utils.create_upload_object_id(
            api_instance=api_instance,
            path_params={'projectId': project_id},
            sg_name=cohort.name,
            file_name=cohort.name,
            folder_path=folder_path,
            object_type='FOLDER',
        )
    logger.info(f'Cohort output folder for {cohort.name} (status {status}): {folder_id}')

    # ICA folder statuses are documented as `PARTIAL`, `AVAILABLE`, `ARCHIVING`,
    # `ARCHIVED`, `UNARCHIVING`, `DELETING`. Split by operator-actionability:
    # - terminal-bad (ARCHIVED / DELETING / UNARCHIVING): downstream submissions
    #   WILL fail with opaque ICA errors. Raise at the prep step so the failure
    #   surfaces with actionable context here instead of much later.
    # - in-flight (ARCHIVING / PARTIAL): transient states that may resolve
    #   before the orchestrator's first submission. Warn so operators can
    #   monitor without blocking the pipeline.
    if status in _TERMINAL_BAD_FOLDER_STATUSES:
        raise RuntimeError(
            f'Cohort output folder for {cohort.name} is in terminal-bad status '
            f'{status!r} (folder_id={folder_id}). DRAGEN pipeline submissions '
            f'against this folder will fail. Manually unarchive or recreate the '
            f"folder in ICA, then re-run the cohort.",
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
