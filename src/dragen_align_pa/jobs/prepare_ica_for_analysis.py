import json
from typing import Literal

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.constants import BUCKET_NAME


def run(
    cohort: Cohort,
    output: dict[str, cpg_utils.Path],
) -> None:
    """Prepare ICA pipeline runs by generating a folder ID for the
    outputs of the Dragen pipeline.

    Args:
        api_root (str): The ICA API endpoint
        sg_name (str): The name of the sequencing group
        bucket_name (str): The  name of the GCP bucket that the data reside in

    Returns:
        dict [str, str] noting the analysis ID.
    """
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    project_id: str = secrets['projectID']

    path_parameters: dict[str, str] = {'projectId': project_id}

    ica_analysis_output_folder: str = config_retrieve(
        ['ica', 'data_prep', 'output_folder'],
    )

    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        folder_path: str = f'/{BUCKET_NAME}/{ica_analysis_output_folder}'
        for sg_name in cohort.get_sequencing_group_ids():
            object_id, _ = ica_utils.create_upload_object_id(
                api_instance=api_instance,
                path_params=path_parameters,
                sg_name=sg_name,
                file_name=sg_name,
                folder_path=folder_path,
                object_type='FOLDER',
            )
            logger.info(
                f'Created folder ID {object_id} for analysis outputs for sequencing group {sg_name}',
            )
            with output[sg_name].open('w') as opath:
                opath.write(json.dumps({'analysis_output_fid': object_id}))
