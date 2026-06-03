"""
This module centralizes all direct interactions with the Illumina Connected
Analytics (ICA) Python SDK. It handles authentication and provides thin
wrappers around specific API endpoints.
"""

import contextlib
import functools
import json
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, Final, Literal, TypeVar

import icasdk
from cpg_utils.config import config_retrieve
from google.api_core import exceptions as gax_exceptions
from google.cloud import secretmanager
from icasdk import ApiClient, Configuration
from icasdk.apis.tags import project_analysis_api, project_data_api
from icasdk.exceptions import ApiException
from icasdk.model.create_nextflow_analysis import CreateNextflowAnalysis
from loguru import logger
from tenacity import (
    RetryCallState,
    Retrying,
    retry,
    retry_if_exception,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
    wait_random_exponential,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

_T = TypeVar('_T')


# --- Constants ---

ICA_REST_ENDPOINT: Final = 'https://ica.illumina.com/ica/rest'
SECRET_PROJECT: Final = 'cpg-common'
SECRET_NAME: Final = 'illumina_cpg_workbench_api'
SECRET_VERSION: Final = 'latest'

# Default retries (after the initial attempt) for transient ICA 429/503 on any
# data-plane call. Override per-run via [ica.retry] max_retries.
_DEFAULT_ICA_MAX_RETRIES: Final = 10


# --- Secret Management & Auth ---

# Secret Manager occasionally returns gRPC 504 (DeadlineExceeded) on transient
# load spikes; GAPIC's default retry policy does NOT cover DeadlineExceeded,
# so a single blip propagates and crashes whichever job is calling. We retry
# both that and ServiceUnavailable (gRPC UNAVAILABLE / HTTP 503).
_TRANSIENT_SECRET_MANAGER_EXCEPTIONS: Final = (
    gax_exceptions.DeadlineExceeded,
    gax_exceptions.ServiceUnavailable,
)


@functools.cache
def _secret_client() -> secretmanager.SecretManagerServiceClient:
    # Lazy so the module can be imported without GCP ADC (e.g. in CI test collection).
    return secretmanager.SecretManagerServiceClient()


@functools.lru_cache(maxsize=1)
@retry(
    retry=retry_if_exception_type(_TRANSIENT_SECRET_MANAGER_EXCEPTIONS),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
def get_ica_secrets() -> dict[Literal['projectID', 'apiKey'], str]:
    """Gets the project ID and API key used to interact with ICA.

    Cached for the lifetime of the process (the secret payload doesn't
    change during a run). The previous behaviour fetched fresh on every
    monitor-loop poll and every batch submission, which meant a single
    transient Secret Manager 504 could tear down a long-running monitor
    job. With the cache, only the very first call hits Secret Manager; all
    subsequent calls return the cached dict without an RPC.

    Retries transient Secret Manager failures (DeadlineExceeded /
    ServiceUnavailable) with exponential backoff before giving up. Other
    error classes (e.g. PermissionDenied, NotFound) propagate immediately
    — they indicate IAM / config problems that retrying won't fix.

    Returns:
        dict[str, str]: A dictionary with the keys projectID and apiKey
    """
    client = _secret_client()
    secret_path: str = client.secret_version_path(
        project=SECRET_PROJECT,
        secret=SECRET_NAME,
        secret_version=SECRET_VERSION,
    )
    response: secretmanager.AccessSecretVersionResponse = client.access_secret_version(
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


def _is_retryable_ica_error(exc: BaseException) -> bool:
    """Tenacity predicate: retry only on transient ICA-side errors.

    `icasdk.exceptions.ApiException` is a single class with `.status: int`,
    so we cannot use `retry_if_exception_type` with a subclass. We check
    `.status` directly. 429 = rate-limit (well-known production failure
    mode); 503 = ICA backend unavailable. 404/500/etc propagate immediately
    — retrying a permanent error just delays the real signal.
    """
    return isinstance(exc, ApiException) and exc.status in (429, 503)  # pyright: ignore[reportAttributeAccessIssue]


def _log_ica_retry(retry_state: RetryCallState) -> None:
    """tenacity ``before_sleep`` hook: surface every transient-error retry.

    Without this, a retried 429/503 is silent — making the retry machinery
    "appear to do nothing" in the logs even when it is working. Fires only when
    a retry is actually scheduled (i.e. on a retryable error with attempts left).
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    status = getattr(exc, 'status', '?')
    fn_name = getattr(retry_state.fn, '__name__', repr(retry_state.fn))
    sleep = retry_state.next_action.sleep if retry_state.next_action else 0.0
    logger.warning(
        f'ICA {fn_name} returned {status}; retrying '
        f'(attempt {retry_state.attempt_number}) after {sleep:.1f}s',
    )


def _ica_retrying() -> Retrying:
    """Build the shared tenacity controller for transient ICA 429/503 errors.

    Read at call time (not at import) so the retry count can be tuned via
    `[ica.retry] max_retries` in config without rebuilding the image, and to
    avoid an import-time config_retrieve (config is not loaded when this module
    is first imported).

    `max_retries` is retries *after* the initial attempt, so total attempts is
    `max_retries + 1`. Defaults to 10 retries.
    """
    max_retries = int(
        config_retrieve(['ica', 'retry', 'max_retries'], default=_DEFAULT_ICA_MAX_RETRIES),
    )
    return Retrying(
        retry=retry_if_exception(_is_retryable_ica_error),
        stop=stop_after_attempt(max_retries + 1),
        # wait_random_exponential gives a fully-randomised exponential backoff
        # (each wait is random in [0, min(2^attempt, max)]), which desynchronises
        # concurrent retries against a shared rate limit — non-negotiable at
        # 16-wide fan-out, where lockstep backoff is a self-inflicted second
        # wave. The additive wait_fixed(2) is a hard 2s floor: wait_random_exponential
        # can otherwise pick a near-zero first wait, letting a worker hammer ICA
        # instantly after a 429. NB: worst-case backoff scales with max_retries
        # (~32s per retry at the cap), so a large max_retries can exceed a tight
        # polling cycle (e.g. MLR's 330s) — tune both together.
        wait=wait_random_exponential(multiplier=1, min=2, max=30) + wait_fixed(2),
        before_sleep=_log_ica_retry,
        reraise=True,
    )


def ica_retry(fn: Callable[..., _T], /, *args: Any, **kwargs: Any) -> _T:
    """Invoke a single ICA SDK call with transient-429/503 retry.

    Wraps `fn(*args, **kwargs)` in the shared jittered-backoff controller (see
    `_ica_retrying`). Only 429 (rate limit) and 503 (backend unavailable) are
    retried; every other error propagates on the first occurrence. The
    controller is rebuilt per call so `[ica.retry] max_retries` is read from
    config at call time.

    Wrap only the SDK call itself, inside any existing try/except, so the
    caller's error logging still fires on the *final* failure while
    `_log_ica_retry` reports each intermediate retry.
    """
    return _ica_retrying()(fn, *args, **kwargs)


def check_ica_pipeline_status(
    api_instance: project_analysis_api.ProjectAnalysisApi,
    path_params: dict[str, str],
) -> str:
    """Check the status of an ICA pipeline via a pipeline ID

    Transient ICA 429/503 errors are retried with jittered exponential backoff
    (see `ica_retry`); other errors propagate on the first occurrence.

    Args:
        api_instance (project_analysis_api.ProjectAnalysisApi): An instance of the ProjectAnalysisApi
        path_params (dict[str, str]): Dict with projectId and analysisId

    Raises:
        icasdk.ApiException: Any exception if the API call is incorrect

    Returns:
        str: The status of the pipeline. Can be one of ['REQUESTED', 'AWAITINGINPUT', 'INPROGRESS', 'SUCCEEDED', 'FAILED', 'FAILEDFINAL', 'ABORTED']
    """  # noqa: E501
    try:
        api_response = ica_retry(api_instance.get_analysis, path_params=path_params)  # type: ignore[ReportUnknownVariableType]
        pipeline_status: str = api_response.body['status']  # type: ignore[ReportUnknownVariableType]
        return pipeline_status  # type: ignore[ReportUnknownVariableType]
    except icasdk.ApiException as e:
        logger.error(
            f'ProjectAnalysisApi.get_analysis raised for path_params={path_params}: '
            f'status={e.status} reason={e.reason}',
        )
        raise


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
        object_type (str): The type of the object to create in ICA. Must be one of ['FILE', 'FOLDER']

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
    try:
        api_response = ica_retry(
            api_instance.get_project_data_list,  # type: ignore[ReportUnknownVariableType]
            path_params=path_params,  # type: ignore[ReportUnknownVariableType]
            query_params=query_params,  # type: ignore[ReportUnknownVariableType]
        )  # type: ignore[ReportUnknownVariableType]

        if not api_response.body['items']:  # type: ignore[ReportUnknownVariableType]
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
        logger.error(
            f'ProjectDataApi.get_project_data_list raised for path_params={path_params}, '
            f'file_name={file_name!r}: status={e.status} reason={e.reason}',
        )
        raise


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
    try:
        api_response = ica_retry(
            api_instance.get_project_data_list,  # pyright: ignore[reportUnknownVariableType]
            path_params=path_parameters,
            query_params={  # pyright: ignore[reportUnknownVariableType]
                'parentFolderPath': parent_folder_path,
                'filename': [file_name],
                'filenameMatchMode': 'EXACT',
                'pageSize': '2',
            },
        )

        items = api_response.body.get('items', [])  # pyright: ignore[reportUnknownVariableType]
        if not items:  # pyright: ignore[reportUnknownArgumentType]
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

        api_response = ica_retry(
            api_instance.get_project_data_list,
            path_params=path_params,
            query_params=query_params,
        )
        items = api_response.body.get('items', [])
        if items:
            return items[0]['data']  # pyright: ignore[reportUnknownVariableType]

    except icasdk.ApiException as e:
        logger.error(f'API error checking for file {file_name}: {e}')
        # Don't raise, just return None
    return None


def submit_nextflow_analysis(
    api_instance: project_analysis_api.ProjectAnalysisApi,
    path_params: dict[str, str],
    body: CreateNextflowAnalysis,
    header_params: dict[str, Any] | None = None,
) -> str:
    """
    Submits a Nextflow analysis to ICA and returns the analysis ID.
    Centralizes the try/except logic for pipeline submission.

    Args:
        api_instance: An instance of the ProjectAnalysisApi.
        path_params: Dict with projectId.
        body: The CreateNextflowAnalysis request body.
        header_params: Optional header parameters.

    Raises:
        icasdk.ApiException: If the API call fails.

    Returns:
        str: The analysis ID of the submitted pipeline.
    """
    if header_params is None:
        header_params = {}
    try:
        api_response = ica_retry(
            api_instance.create_nextflow_analysis,  # type: ignore[ReportUnknownVariableType]
            path_params=path_params,
            header_params=header_params,
            body=body,
        )
        analysis_id: str = api_response.body['id']  # type: ignore[ReportUnknownVariableType]
        return analysis_id
    except icasdk.ApiException as e:
        logger.error(
            f'ProjectAnalysisApi.create_nextflow_analysis raised for path_params={path_params}: '
            f'status={e.status} reason={e.reason}',
        )
        raise
