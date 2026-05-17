import json
from typing import Literal

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.constants import BUCKET_NAME


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

    with output.open('w') as fh:
        json.dump({'analysis_output_fid': folder_id}, fh)
