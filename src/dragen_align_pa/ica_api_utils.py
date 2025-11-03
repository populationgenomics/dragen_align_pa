"""
This module centralizes all direct interactions with the Illumina Connected
Analytics (ICA) Python SDK. It handles authentication and provides thin
wrappers around specific API endpoints.
"""

import contextlib
import json
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Final, Literal

import icasdk
from google.cloud import secretmanager
from icasdk import ApiClient, Configuration
from icasdk.apis.tags import project_analysis_api, project_data_api
from icasdk.exceptions import ApiException
from loguru import logger

if TYPE_CHECKING:
    from collections.abc import Sequence


# --- Constants ---

ICA_REST_ENDPOINT: Final = 'https://ica.illumina.com/ica/rest'
SECRET_CLIENT: Final = secretmanager.SecretManagerServiceClient()
SECRET_PROJECT: Final = 'cpg-common'
SECRET_NAME: Final = 'illumina_cpg_workbench_api'
SECRET_VERSION: Final = 'latest'


# --- Secret Management & Auth ---


def get_ica_secrets() -> dict[Literal['projectID', 'apiKey'], str]:
    """Gets the project ID and API key used to interact with ICA

    Returns:
        dict[str, str]: A dictionary with the keys projectId and apiKey
    """
    secret_path: str = SECRET_CLIENT.secret_version_path(
        project=SECRET_PROJECT,
        secret=SECRET_NAME,
        secret_version=SECRET_VERSION,
    )
    response: secretmanager.AccessSecretVersionResponse = SECRET_CLIENT.access_secret_version(
        request={'name': secret_path},
    )
    return json.loads(response.payload.data.decode('UTF-8'))


@contextlib.contextmanager
def get_ica_api_client() -> Iterator[ApiClient]:
    """
    Provides a context-managed icasdk.ApiClient.
    Handles fetching secrets, configuring, and closing the client.
    """
    secrets: dict[Literal['projectID', 'apiKey'], str] = get_ica_secrets()
    api_key: str = secrets['apiKey']

    configuration = Configuration(host=ICA_REST_ENDPOINT)
    configuration.api_key['ApiKeyAuth'] = api_key

    with ApiClient(configuration=configuration) as api_client:
        try:
            yield api_client
        except ApiException as e:
            logger.error(f'ICA API Exception caught by context manager: {e}')
            raise
        except Exception as e:
            logger.error(f'Non-API Exception caught by context manager: {e}')
            raise


# --- API Wrappers ---


def check_ica_pipeline_status(
    api_instance: project_analysis_api.ProjectAnalysisApi,
    path_params: dict[str, str],
) -> str:
    """Check the status of an ICA pipeline via a pipeline ID

    Args:
        api_instance (project_analysis_api.ProjectAnalysisApi): An instance of the ProjectAnalysisApi
        path_params (dict[str, str]): Dict with projectId and analysisId

    Raises:
        icasdk.ApiException: Any exception if the API call is incorrect

    Returns:
        str: The status of the pipeline. Can be one of ['REQUESTED', 'AWAITINGINPUT', 'INPROGRESS', 'SUCCEEDED', 'FAILED', 'FAILEDFINAL', 'ABORTED']
    """  # noqa: E501
    try:
        api_response = api_instance.get_analysis(path_params=path_params)  # type: ignore[ReportUnknownVariableType]
        pipeline_status: str = api_response.body['status']  # type: ignore[ReportUnknownVariableType]
        return pipeline_status  # type: ignore[ReportUnknownVariableType]
    except icasdk.ApiException as e:
        raise icasdk.ApiException(
            f'Exception when calling ProjectAnalysisApi -> get_analysis: {e}',
        ) from e


def check_object_already_exists(
    api_instance: project_data_api.ProjectDataApi,
    path_params: dict[str, str],
    file_name: str,
    folder_path: str,
    object_type: str,
) -> tuple[str, str] | None:
    """Check if an object already exists in ICA.

    Args:
        api_instance (project_data_api.ProjectDataApi): An instance of the ProjectDataApi
        path_params (dict[str, str]): A dict with the projectId
        file_name (str): The name of the object that you want to check in ICA e.g.
        folder_path (str): The path to the object that you want to create in ICA.
        object_type (str): The type of hte object to create in ICA. Must be one of ['FILE', 'FOLDER']

    Raises:
        NotImplementedError: Only checks for files with the status 'PARTIAL' or 'AVAILABLE'
        icasdk.ApiException: Other API errors

    Returns:
        tuple[str, str] | None: (object_ID, object_status) if it exists, or else None
    """
    query_params: dict[str, Sequence[str] | list[str] | str] = {
        'filePath': [f'{folder_path}/{file_name}'],
        'filePathMatchMode': 'STARTS_WITH_CASE_INSENSITIVE',
        'type': object_type,
    }
    if object_type == 'FILE':
        query_params = {  # pyright: ignore[reportUnknownVariableType]
            'filename': [file_name],
            'filenameMatchMode': 'EXACT',
        } | query_params
    logger.info(
        f'Checking to see if the {object_type} object already exists at {folder_path}/{file_name}',
    )
    try:
        api_response = api_instance.get_project_data_list(  # type: ignore[ReportUnknownVariableType]
            path_params=path_params,  # type: ignore[ReportUnknownVariableType]
            query_params=query_params,  # type: ignore[ReportUnknownVariableType]
        )  # type: ignore[ReportUnknownVariableType]

        if len(api_response.body['items']) == 0:  # type: ignore[ReportUnknownVariableType]
            return None

        object_data = api_response.body['items'][0]['data']  # pyright: ignore[reportUnknownVariableType]
        object_id = object_data['id']  # pyright: ignore[reportUnknownVariableType]
        status: str = object_data['details'].get('status', 'UNKNOWN')  # pyright: ignore[reportUnknownVariableType]

        if object_type == 'FOLDER':
            return object_id, status  # Folders have status, e.g., 'AVAILABLE'

        if status in ('PARTIAL', 'AVAILABLE'):
            return object_id, status

        # Statuses are ["PARTIAL", "AVAILABLE", "ARCHIVING", "ARCHIVED", "UNARCHIVING", "DELETING", ]
        raise NotImplementedError(f'Checking for file status "{status}" is not implemented yet.')
    except icasdk.ApiException as e:
        raise icasdk.ApiException(
            f'Exception when calling ProjectDataApi -> get_project_data_list: {e}',
        ) from e


def find_file_id_by_name(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    parent_folder_path: str,
    file_name: str,
) -> str:
    """
    Finds a specific file ID in an ICA folder by its exact name.
    (Used by download_specific_files_from_ica.py)
    """
    logger.info(f"Searching for file '{file_name}' in '{parent_folder_path}'...")
    try:
        api_response = api_instance.get_project_data_list(  # pyright: ignore[reportUnknownVariableType]
            path_params=path_parameters,
            query_params={  # pyright: ignore[reportUnknownVariableType]
                'parentFolderPath': parent_folder_path,
                'filename': [file_name],
                'filenameMatchMode': 'EXACT',
                'pageSize': '2',
            },
        )

        items = api_response.body.get('items', [])  # pyright: ignore[reportUnknownVariableType]
        if len(items) == 0:  # pyright: ignore[reportUnknownArgumentType]
            raise FileNotFoundError(
                f'File not found in ICA: {parent_folder_path}{file_name}',
            )
        if len(items) > 1:  # pyright: ignore[reportUnknownArgumentType]
            logger.warning(
                f"Found multiple files named '{file_name}'; using the first one.",
            )

        file_id = items[0]['data'].get('id')  # pyright: ignore[reportUnknownVariableType]
        if not file_id:
            raise ValueError(f"Found file item for '{file_name}' but it has no ID.")

        logger.info(f'Found file ID: {file_id}')
        return file_id  # pyright: ignore[reportUnknownVariableType]

    except icasdk.ApiException as e:
        logger.error(f"API Error finding file '{file_name}': {e}")
        raise
    except Exception as e:
        logger.error(f"Error finding file '{file_name}': {e}")
        raise


def get_file_details_from_ica(
    api_instance: project_data_api.ProjectDataApi,
    path_params: dict[str, str],
    ica_folder_path: str,
    file_name: str,
) -> dict[str, Any] | None:
    """
    Checks if a file exists in ICA and returns its 'data' block if found.
    (Used by upload_data_to_ica.py)
    """
    try:
        query_params: dict[str, Any] = {
            'parentFolderPath': ica_folder_path,
            'filename': [file_name],
            'filenameMatchMode': 'EXACT',
            'pageSize': '2',
        }

        api_response = api_instance.get_project_data_list(
            path_params=path_params,
            query_params=query_params,
        )
        items = api_response.body.get('items', [])
        if len(items) > 0:
            return items[0]['data']  # pyright: ignore[reportUnknownVariableType]

    except icasdk.ApiException as e:
        logger.error(f'API error checking for file {file_name}: {e}')
        # Don't raise, just return None
    return None
