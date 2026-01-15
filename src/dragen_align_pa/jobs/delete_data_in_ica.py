import json
import pathlib

import cpg_utils
from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils

FidList = list[str]
PathDict = dict[str, cpg_utils.Path]


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
        except Exception as e:  # noqa: BLE001
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
    except Exception as e:  # noqa: BLE001
        logger.warning(f'Could not read FASTQ FIDs from {txt_path}: {e}')
    return fids


def _collect_fids(
    analysis_output_fids_paths: PathDict | None,
    cram_fid_paths_dict: PathDict | None,
    fastq_ids_list_path: cpg_utils.Path | None,
) -> FidList:
    """Collect all file and folder IDs from various sources."""
    all_fids: FidList = []
    logger.info('--- Step 1: Collecting all file IDs for deletion ---')

    if analysis_output_fids_paths:
        all_fids.extend(
            _collect_fids_from_json(
                analysis_output_fids_paths,
                'analysis_output_fid',
                'analysis output folder',
            ),
        )

    if cram_fid_paths_dict:
        all_fids.extend(
            _collect_fids_from_json(cram_fid_paths_dict, 'cram_fid', 'source CRAM'),
        )

    if fastq_ids_list_path:
        all_fids.extend(_collect_fids_from_txt(fastq_ids_list_path))

    logger.info(f'Collected a total of {len(all_fids)} file and folder IDs.')
    return all_fids


# --- Main Orchestrator ---


def run(
    analysis_output_fids_paths: PathDict | None,
    cram_fid_paths_dict: PathDict | None,
    fastq_ids_list_path: cpg_utils.Path | None,
) -> None:
    """
    Identifies all unique parent folders for a list of ICA file IDs and
    deletes the folders. This avoids leaving behind orphaned files or
    empty folder structures.
    """
    project_id: str = config_retrieve(['ica', 'projects', 'data_deletion_project'])

    fids = _collect_fids(
        analysis_output_fids_paths,
        cram_fid_paths_dict,
        fastq_ids_list_path,
    )
    if not fids:
        logger.warning('No file IDs were collected for deletion. Exiting.')
        return

    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)

        # 1. Get details for all data objects
        data_details_list = ica_utils.get_data_details_from_fids(
            fids,
            api_instance,
            project_id,
        )

        # 2. Process details to find parent folder paths and direct folder IDs
        logger.info('--- Step 2: Finding unique parent folder paths ---')
        parent_paths = set()
        direct_folder_fids = set()
        for details_body in data_details_list:
            details = details_body.get('details', {})
            file_path = details.get('path')
            data_id = details_body.get('id')

            if not file_path or not data_id:
                logger.warning(f'Skipping item with missing path or id: {details_body}')
                continue

            if details.get('dataType') == 'FOLDER':
                logger.info(f'ID {data_id} is a folder. Adding to deletion list.')
                direct_folder_fids.add(data_id)
            else:
                parent_path = str(pathlib.Path(file_path).parent)
                if parent_path and parent_path != '/':
                    parent_paths.add(parent_path)
        logger.info(f'Found {len(parent_paths)} unique parent folder paths from files.')

        # 3. Get folder IDs from the paths
        parent_folder_fids = ica_utils.get_folder_ids_from_paths(
            parent_paths,
            api_instance,
            project_id,
        )

        # 4. Combine all folder IDs and delete them
        all_folder_fids = direct_folder_fids.union(parent_folder_fids)
        ica_utils.delete_data_by_ids(all_folder_fids, api_instance, project_id)

    logger.info('ICA data deletion job complete.')
