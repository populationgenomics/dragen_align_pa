import json
import pathlib

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_access_level, get_config
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils

FidList = list[str]
FidSet = set[str]
PathDict = dict[str, cpg_utils.Path]
ProjectId = str


def _delete_analysis_output_folders(
    api_instance: project_data_api.ProjectDataApi,
    project_id: ProjectId,
    analysis_output_fids_paths: PathDict,
) -> None:
    """Deletes analysis output folders created by the pipeline."""
    logger.info('--- Deleting analysis output folders ---')
    fids = _collect_fids_from_json(
        analysis_output_fids_paths,
        'analysis_output_fid',
        'analysis output folder',
    )
    if not fids:
        logger.warning('No analysis output folder FIDs found to delete.')
        return
    logger.info(f'Will delete the following analysis output folders: {fids}')
    # ica_utils.delete_data_by_ids(set(fids), api_instance, project_id)


def _delete_cram_upload_folder(
    api_instance: project_data_api.ProjectDataApi,
    project_id: ProjectId,
    cohort: Cohort,
) -> None:
    """Deletes the entire folder where CRAMs were uploaded."""
    logger.info('--- Deleting CRAM upload folder ---')
    upload_folder_name: str = config_retrieve(['ica', 'data_prep', 'upload_folder'])

    # Construct the full path
    dataset = cohort.dataset
    access_level = get_access_level()
    full_folder_path = f'/cpg-{dataset}-{access_level}/{upload_folder_name}'

    folder_ids = ica_utils.get_folder_ids_from_paths(
        {full_folder_path},
        api_instance,
        project_id,
    )
    if not folder_ids:
        logger.warning(f"Could not find folder ID for '{full_folder_path}' to delete.")
        return
    logger.info(f'Will delete the CRAM upload folder: {folder_ids}')
    # ica_utils.delete_data_by_ids(folder_ids, api_instance, project_id)


def _delete_fastq_files(
    api_instance: project_data_api.ProjectDataApi,
    project_id: ProjectId,
    fastq_ids_list_path: cpg_utils.Path,
) -> None:
    """Deletes flowcell fodlers in ICA based off the unique folders for a list of FASTQ files."""
    logger.info('--- Deleting uploaded flowcell data ---')
    fids = _collect_fids_from_txt(fastq_ids_list_path)
    if not fids:
        logger.warning('No FASTQ FIDs found.')
        return

    data_details_list = ica_utils.get_data_details_from_fids(
        fids,
        api_instance,
        project_id,
    )

    parent_paths: set[str] = set()
    for details_body in data_details_list:
        details = details_body.get('details', {})
        file_path = details.get('path')

        if not file_path:
            logger.warning(f'Skipping item with missing path: {details_body}')
            continue

        parent_path = str(pathlib.Path(file_path).parent)
        if parent_path and parent_path != '/':
            parent_paths.add(parent_path)

    logger.info(f'Found {len(parent_paths)} unique parent folder paths from FASTQ files.')
    parent_folder_fids = ica_utils.get_folder_ids_from_paths(
        parent_paths,
        api_instance,
        project_id,
    )
    logger.info(f'Will delete the following flowcell folders: {parent_folder_fids}')
    # ica_utils.delete_data_by_ids(parent_folder_fids, api_instance, project_id)


def _collect_fids_from_json(
    path_dict: PathDict,
    json_key: str,
    description: str,
) -> FidList:
    """Helper to collect FIDs from a dictionary of JSON file paths."""
    fids: FidList = []
    logger.info(f'Collecting {len(path_dict)} {description} FIDs...')
    for sg_name, path in path_dict.items():
        try:
            with path.open() as fid_handle:
                fids.append(json.load(fid_handle)[json_key])
        except Exception as e:
            logger.warning(f'Could not read {json_key} for {sg_name}: {e}')
    return fids


def _collect_fids_from_txt(txt_path: cpg_utils.Path) -> FidList:
    """Helper to collect FIDs from a text file (one FID per line)."""
    fids: FidList = []
    logger.info(f'Collecting source FASTQ FIDs from {txt_path}...')
    try:
        with txt_path.open() as fastq_handle:
            for line in fastq_handle:
                if line.strip():
                    fids.append(line.split()[0])
    except Exception as e:
        logger.warning(f'Could not read FASTQ FIDs from {txt_path}: {e}')
    return fids


def run(
    cohort: Cohort,
    analysis_output_fids_paths: PathDict | None,
    cram_fid_paths_dict: PathDict | None,
    fastq_ids_list_path: cpg_utils.Path | None,
) -> None:
    """
    Deletes analysis outputs and optionally source data (CRAMs or FASTQs)
    from their respective ICA projects.
    """
    dragen_project: ProjectId = config_retrieve(['ica', 'projects', 'dragen_align'])
    fastq_project: ProjectId = config_retrieve(['ica', 'projects', 'fastq_upload_project'])
    reads_type: str = get_config()['workflow']['reads_type']

    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)

        # 1. Delete analysis output folders in the Dragen project
        if analysis_output_fids_paths:
            _delete_analysis_output_folders(
                api_instance,
                dragen_project,
                analysis_output_fids_paths,
            )

        # 2. Delete source data based on reads_type
        if reads_type == 'cram':
            if cram_fid_paths_dict:
                _delete_cram_upload_folder(api_instance, dragen_project, cohort)
        elif reads_type == 'fastq':
            if fastq_ids_list_path:
                _delete_fastq_files(api_instance, fastq_project, fastq_ids_list_path)

    logger.info('ICA data deletion job complete.')
