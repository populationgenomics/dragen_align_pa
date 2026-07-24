"""Download and parse the per-batch `passfail.json` from ICA.

`passfail.json` is at the batch's analysis-output root and maps
`sample_id → "Success" | "Failed"` (DRAGEN's own spelling; `batches.record_passfail`
normalises `"Failed"` to the canonical `"Fail"` on write). In our pipeline
`sample_id` == `sg_name`:
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


# Returns None ONLY on genuine absence — never on a transient blip. The
# transactional on_succeeded caller leaves the batch INPROGRESS and re-fires on a
# raised error; treating a blip as "no passfail.json" would instead mark every SG
# Fail and wastefully retry samples that actually succeeded.
def fetch_passfail_from_ica(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    ica_folder_path: str,
) -> dict[str, str] | None:
    """Fetch and parse passfail.json from an ICA folder in-memory (KB-scale, no staging).

    Returns the `{sample_id: status}` mapping, or `None` only when passfail.json is
    legitimately absent (a catastrophically-failed batch that didn't produce one).

    Raises:
        icasdk.ApiException / requests.RequestException / json.JSONDecodeError: On
            transient ICA, network, or non-JSON-body (200 from a proxy maintenance
            page) errors.
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
        url_response = ica_api_utils.ica_retry(
            api_instance.create_download_url_for_data,
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
            f'passfail.json at {ica_folder_path} returned non-JSON body (status={response.status_code}): {e}',
        )
        raise
