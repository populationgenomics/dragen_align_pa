"""Download and parse the per-batch `passfail.json` from ICA.

`passfail.json` is at the batch's analysis-output root and maps
`sample_id → "Success" | "Fail"`. In our pipeline `sample_id` == `sg_name`:
- FASTQ mode: `MakeFastqFileList` writes RGSM = SG name in every row.
- CRAM mode: the original CRAM's RG SM tag is preserved through the unified
  pipeline's input handling. If a CRAM cohort surfaces RGSM != sg_name, the
  defensive filter in `_on_succeeded` warns and drops the unexpected keys
  before they reach the retry path.
"""

import json
from pathlib import Path

import cpg_utils
import icasdk
import requests
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils

_HTTP_FORBIDDEN = 403


def parse_passfail_file(path: Path | cpg_utils.Path) -> dict[str, str]:
    """Load a passfail.json file from disk and return the {sample_id: status} mapping."""
    with path.open('r') as fh:
        return json.load(fh)


def fetch_passfail_from_ica(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    ica_folder_path: str,
) -> dict[str, str] | None:
    """Fetch passfail.json from an ICA folder and parse it in-memory.

    Returns the parsed `{sample_id: status}` mapping on success, or `None`
    **only** when passfail.json is legitimately absent (a catastrophically-
    failed batch that didn't produce one). The file is small (KB-scale), so
    we never stage to GCS or disk.

    Transient errors **raise** so the caller (the transactional `on_succeeded`
    in `manage_dragen_pipeline.py`) leaves the batch's status as INPROGRESS
    and re-fires on the next poll cycle — eventually escalating to
    FAILED_FINAL via the on_succeeded failure cap if the issue is persistent.
    Without this distinction, a transient network blip would be silently
    treated as "no passfail.json", marking every SG in the batch as Fail and
    triggering a wasted retry pass on samples that actually succeeded.

    Failure handling:
    - `FileNotFoundError` from the lookup → return None (legitimate absence).
    - `icasdk.ApiException` from lookup, URL minting, or any other ICA call
      → log and re-raise.
    - `requests.RequestException` from the GET (network, timeout, etc.) →
      log and re-raise.
    - `requests.HTTPError` with 403 → presigned URL expired between minting
      and reading; mint a fresh URL once and retry. A second 403 surfaces
      via `response.raise_for_status()` and is re-raised by the
      `RequestException` catch.
    - `json.JSONDecodeError` on `response.json()` → the presigned URL
      occasionally serves a non-JSON body (e.g. an upstream proxy returning
      a maintenance HTML page with status 200, slipping past
      `raise_for_status`). Log and re-raise.
    """
    try:
        file_id = ica_api_utils.find_file_id_by_name(
            api_instance=api_instance,
            path_parameters=path_parameters,
            parent_folder_path=ica_folder_path,
            file_name='passfail.json',
        )
    except FileNotFoundError:
        return None
    except icasdk.ApiException as e:
        logger.warning(f'ICA API error finding passfail.json in {ica_folder_path}: {e}')
        raise

    def _mint_and_fetch() -> requests.Response:
        url_response = api_instance.create_download_url_for_data(
            path_params=path_parameters | {'dataId': file_id},
        )
        download_url: str = url_response.body['url']
        return requests.get(download_url, timeout=60)

    try:
        response = _mint_and_fetch()
        if response.status_code == _HTTP_FORBIDDEN:
            # Presigned URL expired between minting and reading; mint a fresh one.
            logger.warning(
                f'passfail.json presigned URL returned 403 for {ica_folder_path}; re-minting and retrying once.',
            )
            response = _mint_and_fetch()
        response.raise_for_status()
    except icasdk.ApiException as e:
        logger.warning(f'ICA API error minting download URL for passfail.json in {ica_folder_path}: {e}')
        raise
    except requests.RequestException as e:
        logger.warning(f'Network error fetching passfail.json from {ica_folder_path}: {e}')
        raise

    logger.info(f'Fetched passfail.json from {ica_folder_path}')
    try:
        return response.json()
    except json.JSONDecodeError as e:
        logger.warning(
            f'passfail.json at {ica_folder_path} returned non-JSON body '
            f'(status={response.status_code}): {e}',
        )
        raise
