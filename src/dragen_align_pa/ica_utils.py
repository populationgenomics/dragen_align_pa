"""
This module provides high-level helper functions and business logic for
interacting with ICA. It orchestrates calls to the low-level ica_api
and ica_cli modules.
"""

import base64
import hashlib
import json
import time
from typing import TYPE_CHECKING

import cpg_utils
import icasdk
import requests
import tenacity
from google.cloud import exceptions as gcs_exceptions
from icasdk.model.create_data import CreateData
from icasdk.model.data_id_or_path_list import DataIdOrPathList
from loguru import logger

from dragen_align_pa import http_utils, ica_api_utils
from dragen_align_pa.paths import IcaPath
from dragen_align_pa.utils import load_per_sg_state

if TYPE_CHECKING:
    from datetime import datetime

    from google.cloud.storage.blob import Blob
    from google.cloud.storage.bucket import Bucket
    from icasdk.apis.tags import project_data_api


# --- ICA folder builders --------------------------------------------------------------
# These return a composable `IcaPath`; the caller picks the terminal form (`.as_folder()`,
# `.as_url(role)`, or append `/ segment` first). Returning `IcaPath` — not str — keeps the
# run-folder layout defined once with the terminal-form choice explicit at the call site.
# `get_ica_sample_folder` is the exception: it has real behaviour (reads/validates the per-SG
# state file), so it returns the finished folder string directly.


def ica_cohort_path(cohort_name: str) -> IcaPath:
    """`IcaPath` for one cohort's ICA output folder: `{output_root}/{cohort}`."""
    return IcaPath.output_root() / cohort_name


def ica_run_path(cohort_name: str, user_reference: str, pipeline_id: str) -> IcaPath:
    """`IcaPath` for one batch's run folder `{output_root}/{cohort}/{user_reference}-{pipeline_id}`.

    The single definition of the run-folder layout, so the folder, per-SG, and `ica://` URL
    forms all derive from it. `user_reference` ends in `_`, so the hyphen yields a
    `…_-{pipeline_id}` folder name. Append `/ sg_name` for a per-SG folder.
    """
    return ica_cohort_path(cohort_name) / f'{user_reference}-{pipeline_id}'


def ica_md5_run_path(cohort_name: str, ar_guid: str, pipeline_id: str) -> IcaPath:
    """`IcaPath` for the (unbatched) MD5 run folder `{output_root}/{cohort}/{cohort}_{ar_guid}-{pipeline_id}`.

    Distinct from `ica_run_path`: the MD5 pipeline processes all of a cohort's SGs in a single
    pass, so its run folder has no batch segment and keys on `{cohort}_{ar_guid}` rather than a
    DRAGEN batch's `user_reference`.
    """
    return ica_cohort_path(cohort_name) / f'{cohort_name}_{ar_guid}-{pipeline_id}'


def get_ica_sample_folder(
    pipeline_id_arguid_path: cpg_utils.Path,
    sg_name: str,
    cohort_name: str,
) -> str:
    """Resolve the ICA folder for a single SG's batch output.

    Reads `user_reference` and `pipeline_id` from the per-SG state file, then composes the
    per-SG run folder. A schema-mismatched or missing-key state file raises here rather than
    downstream, so operators can recover by rerunning with `force_resubmit=true` or deleting
    the offending per-SG file.

    Args:
        pipeline_id_arguid_path: Path to the SG's per-SG state file.
        sg_name: Sequencing-group name, appended as the final folder segment.
        cohort_name: Cohort the SG belongs to.

    Returns:
        The ICA REST folder form
        `{output_root}/{cohort_name}/{user_reference}-{pipeline_id}/{sg_name}/`.

    Raises:
        FileNotFoundError: If the per-SG state file does not exist.
        KeyError: If the state file is missing a required key.
        ValueError: If the state file predates the current per-SG state schema.
    """
    state = load_per_sg_state(
        pipeline_id_arguid_path,
        required_keys=('user_reference', 'pipeline_id', 'batch_index'),
        expected_cohort_name=cohort_name,
    )
    return (ica_run_path(cohort_name, state['user_reference'], state['pipeline_id']) / sg_name).as_folder()


def create_upload_object_id(
    api_instance: 'project_data_api.ProjectDataApi',
    path_params: dict[str, str],
    folder_name: str,
    file_name: str,
    folder_path: str,
    object_type: str,
) -> tuple[str, str]:
    """Create an object in ICA to upload data to, or to write analysis outputs into.

    Args:
        api_instance: An instance of the ProjectDataApi.
        path_params: A dict with the projectId.
        folder_name: Name used when creating a FOLDER object (ignored for FILE).
        file_name: The name of the file to upload e.g. CPGxxxx.CRAM.
        folder_path: The base path to the object in ICA to create.
        object_type: The type of the object to create. Must be one of ['FILE', 'FOLDER'].

    Raises:
        icasdk.ApiException: Any API error.

    Returns:
        (object_ID, status), where status is from ICA, e.g. 'AVAILABLE', 'PARTIAL'.
    """
    # Normalise to a single leading + trailing slash so the existence check and
    # CreateData below don't produce a double slash when a caller passes a
    # trailing slash of their own.
    folder_path = IcaPath.from_relpath(folder_path).as_folder()

    if object_type == 'FILE':
        body = CreateData(name=file_name, folderPath=folder_path, dataType=object_type)
    else:
        body = CreateData(name=folder_name, folderPath=folder_path, dataType=object_type)

    def _find_or_create() -> tuple[str, str]:
        # The existence check sits inside the retry boundary: create_data_in_project is not
        # idempotent, and its 409 (ICA_DATA_105) is a retryable conflict, so a retry re-checks
        # first and returns the already-landed object instead of minting a duplicate.
        # retry=False: the outer `ica_retry_create` already retries 429/503 for this whole
        # callable; letting the check retry too would square the budget (~11x11) rather than
        # the intended single ~11-attempt boundary.
        existing_object_details = ica_api_utils.check_object_already_exists(
            api_instance=api_instance,
            path_params=path_params,
            file_name=file_name,
            folder_path=folder_path,
            object_type=object_type,
            retry=False,
        )
        if existing_object_details:
            object_id, status = existing_object_details
            logger.info(f'Found existing {object_type} with ID {object_id} and status {status}')
            return object_id, status

        api_response = api_instance.create_data_in_project(  # type: ignore[ReportUnknownVariableType]
            path_params=path_params,  # type: ignore[ReportUnknownVariableType]
            body=body,
        )
        new_object_id = api_response.body['data']['id']  # type: ignore[ReportUnknownVariableType]
        new_status = api_response.body['data']['details']['status']  # type: ignore[ReportUnknownVariableType]
        logger.info(f'Created new {object_type} with ID {new_object_id} and status {new_status}')
        return new_object_id, new_status

    try:
        # ica_retry_create (not ica_retry): create_data_in_project is not
        # idempotent, so 409 is only retried here where _find_or_create re-checks
        # for the landed object first.
        return ica_api_utils.ica_retry_create(_find_or_create)
    except icasdk.ApiException as e:
        raise icasdk.ApiException(
            f'Exception when calling ProjectDataApi -> create_data_in_project: {e}',
        ) from e


def get_md5_from_ica(
    api_instance: 'project_data_api.ProjectDataApi',
    path_parameters: dict[str, str],
    md5_file_id: str,
) -> tuple[str, str]:
    """
    Downloads the content of the MD5 file from ICA.
    (Used by download_specific_files_from_ica.py)
    Returns (expected_hash, file_content).
    """
    try:
        url_response = ica_api_utils.ica_retry(
            api_instance.create_download_url_for_data,  # pyright: ignore[reportUnknownVariableType]
            path_params=path_parameters | {'dataId': md5_file_id},
        )
        download_url = url_response.body['url']  # pyright: ignore[reportUnknownVariableType]

        response = http_utils.download_session().get(
            download_url,
            timeout=http_utils.SMALL_FILE_TIMEOUT,
        )  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
        response.raise_for_status()

        content = response.text  # pyright: ignore[reportUnknownVariableType]
        # Handle both md5sum (hash filename) and md5 (hash only) formats
        expected_hash = content.split()[0]  # pyright: ignore[reportUnknownVariableType]
        return expected_hash, content  # pyright: ignore[reportUnknownVariableType]

    except icasdk.ApiException as e:
        logger.error(
            f'Failed to get download URL for MD5 file (ID: {md5_file_id}): {e}',
        )
        raise
    except requests.RequestException as e:
        logger.error(f'Failed to download MD5 content (ID: {md5_file_id}): {e}')
        raise


def batch_create_download_urls(
    api_instance: 'project_data_api.ProjectDataApi',
    path_parameters: dict[str, str],
    file_ids: list[str],
) -> dict[str, str]:
    """Mint pre-signed download URLs for many files in ONE ICA API call.

    Uses the batch `:createDownloadUrls` endpoint instead of one `:createDownloadUrl` POST per
    file, collapsing per-file call volume (the dominant 429 source) from N to 1. Returns a
    `{dataId: url}` map so callers match URLs back to the file IDs they hold. An empty
    `file_ids` short-circuits without an API call. Goes through `ica_retry`.
    """
    if not file_ids:
        return {}
    response = ica_api_utils.ica_retry(
        api_instance.create_download_urls_for_data,
        body=DataIdOrPathList(dataIds=file_ids),
        path_params=path_parameters,  # pyright: ignore[reportArgumentType]
    )
    return {
        item['dataId']: item['url']  # pyright: ignore[reportUnknownVariableType]
        for item in response.body['items']  # pyright: ignore[reportUnknownVariableType]
    }


def _gcs_md5_hex(blob: 'Blob') -> str | None:
    """Return an object's GCS-recorded MD5 as lowercase hex.

    Args:
        blob: A blob whose properties have been loaded (e.g. from `Bucket.get_blob`).

    Returns:
        The hex digest, or None if GCS holds no MD5 for the object (composite
        objects, which our resumable uploads never produce).
    """
    if not blob.md5_hash:
        return None
    return base64.b64decode(blob.md5_hash).hex()


# A finalized GCS object means a prior run wrote that file end to end: BlobWriter.__exit__
# calls terminate() on error, so an interrupted stream abandons its resumable session and
# never finalizes. Where ICA also gives us a hash we don't have to lean on that argument —
# GCS records each object's MD5, so the existing copy is checked rather than trusted.
def existing_gcs_object_is_complete(
    gcs_bucket: 'Bucket',
    gcs_blob_path: str,
    expected_md5_hash: str | None = None,
) -> bool:
    """Report whether GCS already holds a complete copy of an object.

    Args:
        gcs_bucket: Bucket to look in.
        gcs_blob_path: Full object key within the bucket.
        expected_md5_hash: Hex MD5 the stored object must match. When None, any
            finalized object counts as complete.

    Returns:
        True if the object exists and, when `expected_md5_hash` is given, matches it.
    """
    existing = gcs_bucket.get_blob(gcs_blob_path)  # pyright: ignore[reportUnknownVariableType]
    if existing is None:
        return False
    if expected_md5_hash is None:
        return True

    stored_md5_hash = _gcs_md5_hex(existing)  # pyright: ignore[reportUnknownArgumentType]
    if stored_md5_hash is None:
        logger.warning(
            f'{gcs_blob_path} exists but GCS holds no MD5 for it; re-downloading rather than trusting it.',
        )
        return False
    if stored_md5_hash != expected_md5_hash:
        logger.warning(
            f'{gcs_blob_path} exists but its MD5 {stored_md5_hash} does not match the expected '
            f'{expected_md5_hash}; re-downloading.',
        )
        return False
    return True


# `requests` transparently decodes a Content-Encoding'd body, so Content-Length — which
# describes the *encoded* size — would not match the bytes we count. Only an identity-encoded
# response carries a length we can compare against.
def _declared_content_length(response: requests.Response) -> int | None:
    """Return the body length the server declared, if it is comparable to bytes received.

    Args:
        response: A streamed response whose headers have arrived.

    Returns:
        The `Content-Length` value, or None when it is absent (e.g. chunked transfer
        encoding) or not comparable.
    """
    encoding: str = response.headers.get('Content-Encoding', 'identity').strip().lower()
    if encoding not in ('', 'identity'):
        return None
    declared: str | None = response.headers.get('Content-Length')
    if declared is None:
        return None
    try:
        return int(declared)
    except ValueError:
        logger.warning(f'Ignoring malformed Content-Length header {declared!r}')
        return None


# GCS output prefixes are not scoped to an ICA run (`dragen_metrics/{sg}` carries no cohort or
# pipeline id), so re-analysing an SG writes new outputs over the same prefix. Without a record
# of which run the existing objects came from, skip-if-exists cannot tell "already fetched this
# run" from "left over from the previous analysis", and would silently keep stale outputs. The
# marker's own GCS creation time is the reference instant, so the comparison never depends on
# the client clock. The marker deliberately lives OUTSIDE the prefix it describes: a stage
# declares that prefix as its expected output, and an object inside it would make the folder
# exist — and so look complete to cpg-flow — before a single file had been downloaded.
def claim_download_for_run(gcs_bucket: 'Bucket', marker_key: str, ica_folder_path: str) -> 'datetime':
    """Record which ICA run owns a download destination, and report when ownership began.

    Writes a fresh marker when the destination is unclaimed or was last written by a
    different ICA run; keeps the existing marker when it already names `ica_folder_path`.

    Args:
        gcs_bucket: Bucket holding both the marker and the destination prefix.
        marker_key: Object key for the marker, outside the prefix it describes.
        ica_folder_path: The ICA folder being downloaded, which identifies the run.

    Returns:
        The marker's GCS creation time. Objects older than this were written by some
        other run and must not be treated as already-downloaded.
    """
    existing = gcs_bucket.get_blob(marker_key)  # pyright: ignore[reportUnknownVariableType]
    if existing is not None:
        recorded = json.loads(existing.download_as_bytes()).get('ica_folder')  # pyright: ignore[reportUnknownMemberType]
        if recorded == ica_folder_path:
            return _marker_created_time(existing, marker_key)  # pyright: ignore[reportUnknownArgumentType]
        logger.warning(
            f'Previous download of gs://{gcs_bucket.name}/{marker_key} came from a different '
            f'ICA run ({recorded}); re-downloading everything for {ica_folder_path}.',
        )

    marker = gcs_bucket.blob(marker_key)
    marker.upload_from_string(
        json.dumps({'ica_folder': ica_folder_path}, indent=2),
        content_type='application/json',
    )
    return _marker_created_time(marker, marker_key)


def _marker_created_time(marker: 'Blob', marker_path: str) -> 'datetime':
    """Return a provenance marker's GCS creation time, reloading it once if unset.

    Args:
        marker: The marker blob.
        marker_path: Its object key, for the error message.

    Raises:
        ValueError: If GCS reports no creation time for the marker.

    Returns:
        The marker's creation time.
    """
    if marker.time_created is None:
        marker.reload()
    if marker.time_created is None:
        raise ValueError(
            f'GCS reports no creation time for provenance marker {marker_path}; cannot tell '
            f'which objects belong to this run, so refusing to skip any as already-downloaded.',
        )
    return marker.time_created


def list_gcs_names_written_since(gcs_bucket: 'Bucket', gcs_prefix: str, since: 'datetime') -> set[str]:
    """List objects under a prefix created at or after `since`, named relative to it.

    Args:
        gcs_bucket: Bucket to list.
        gcs_prefix: Prefix to list under, without a trailing slash.
        since: Cutoff; objects created before this are omitted, being another run's.

    Returns:
        Object names relative to `gcs_prefix`, e.g. `{'a.csv', 'reports/b.html'}`.
    """
    prefix = f'{gcs_prefix}/'
    return {
        blob.name.removeprefix(prefix)  # pyright: ignore[reportUnknownMemberType]
        for blob in gcs_bucket.list_blobs(prefix=prefix)  # pyright: ignore[reportUnknownVariableType]
        if blob.time_created >= since
    }


def stream_ica_file_to_gcs(
    api_instance: 'project_data_api.ProjectDataApi',
    path_parameters: dict[str, str],
    file_id: str,
    file_name: str,
    gcs_bucket: 'Bucket',
    gcs_prefix: str,
    expected_md5_hash: str | None = None,
    download_url: str | None = None,
    *,
    skip_existing: bool = True,
) -> bool:
    """Stream a file from ICA to GCS, optionally verifying its MD5.

    Args:
        api_instance: An instance of the ProjectDataApi.
        path_parameters: Dict with the projectId.
        file_id: ICA data id of the file to stream.
        file_name: Object name to write under `gcs_prefix`; may contain slashes.
        gcs_bucket: Destination bucket.
        gcs_prefix: Destination prefix within the bucket.
        expected_md5_hash: Hex MD5 to verify the transfer against. A mismatch
            deletes the uploaded object and raises.
        download_url: A pre-minted pre-signed URL (e.g. from
            `batch_create_download_urls`). When None, one is minted for this file.
        skip_existing: Return without transferring when GCS already holds a complete copy.
            Pass False when the caller has already decided this file must be written —
            it owns the decision and this check would only second-guess it.

    A failed transfer is restarted from the beginning (with a freshly minted URL) up to
    `[ica.download] max_transfer_attempts` times before the error propagates.

    Raises:
        icasdk.ApiException: Minting the download URL failed.
        requests.RequestException | urllib3.exceptions.HTTPError | ssl.SSLError: Every
            transfer attempt failed.
        google.cloud.exceptions.GoogleCloudError: The upload to GCS failed.
        ValueError: The transferred bytes did not match `expected_md5_hash`.

    Returns:
        True if the file was streamed, False if it was already present and skipped.
    """
    gcs_blob_path = f'{gcs_prefix}/{file_name}'
    blob = gcs_bucket.blob(gcs_blob_path)
    bucket_name = gcs_bucket.name  # pyright: ignore[reportUnknownVariableType]

    if skip_existing and existing_gcs_object_is_complete(gcs_bucket, gcs_blob_path, expected_md5_hash):
        logger.info(
            f'Skipping {file_name}: gs://{bucket_name}/{gcs_blob_path} already holds a complete copy.',
        )
        return False

    logger.info(
        f'Streaming {file_name} (ID: {file_id}) to gs://{bucket_name}/{gcs_blob_path}',
    )

    def _mint_download_url() -> str:
        url_response = ica_api_utils.ica_retry(
            api_instance.create_download_url_for_data,  # pyright: ignore[reportUnknownVariableType]
            path_params=path_parameters | {'dataId': file_id},  # pyright: ignore[reportArgumentType]
        )
        return url_response.body['url']  # pyright: ignore[reportUnknownVariableType]

    # Restarting writes the object from the beginning rather than resuming: an interrupted
    # BlobWriter terminates its resumable session, so there is no partial upload to resume
    # and no half-written object left behind for the next attempt to append to.
    def _transfer(url: str) -> str:
        md5_hasher = hashlib.md5()  # noqa: S324
        with http_utils.download_session().get(
            url,
            stream=True,
            timeout=http_utils.STREAM_TIMEOUT,
        ) as r:  # pyright: ignore[reportUnknownVariableType]
            r.raise_for_status()
            expected_bytes = _declared_content_length(r)  # pyright: ignore[reportUnknownArgumentType]

            # Stream directly to GCS
            with blob.open('wb', timeout=600) as gcs_file:  # pyright: ignore[reportUnknownArgumentType]
                written = 0
                for chunk in r.iter_content(
                    chunk_size=1024 * 1024 * 8,
                ):  # pyright: ignore[reportUnknownVariableType] # 8MB chunks
                    gcs_file.write(chunk)  # pyright: ignore[reportUnknownArgumentType]
                    md5_hasher.update(chunk)  # pyright: ignore[reportUnknownArgumentType]
                    written += len(chunk)  # pyright: ignore[reportUnknownArgumentType]

                # Raised INSIDE the writer's context so BlobWriter.__exit__ terminates the
                # resumable session and nothing is finalized; as a RequestException it also
                # routes through the transfer retry rather than surfacing as a hard failure.
                if expected_bytes is not None and written != expected_bytes:
                    raise requests.exceptions.ChunkedEncodingError(
                        f'{file_name}: received {written} bytes but the server declared '
                        f'{expected_bytes}; treating as a truncated transfer.',
                    )
        return md5_hasher.hexdigest()

    attempts_made = 0

    def _mint_and_transfer() -> str:
        nonlocal attempts_made
        attempts_made += 1
        # A pre-minted URL (the batch path) is only good for the first attempt: a retry can
        # be a minute or more later, by which point the URL may have expired (ICA answers an
        # expired pre-signed URL with 403).
        url = download_url if download_url is not None and attempts_made == 1 else _mint_download_url()
        return _transfer(url)

    try:
        actual_md5_hash = http_utils.transfer_retrying(f'{file_name} (ID: {file_id})')(_mint_and_transfer)

        logger.info(
            f'Finished streaming {file_name}. Actual MD5: {actual_md5_hash}',
        )

        # Verify MD5 if provided
        if expected_md5_hash:
            if actual_md5_hash != expected_md5_hash:
                logger.error(f'MD5 MISMATCH for {file_name}!')
                logger.error(f'  Expected: {expected_md5_hash}')
                logger.error(f'  Actual:   {actual_md5_hash}')
                # Delete the corrupted file from GCS
                try:
                    blob.delete()
                except gcs_exceptions.GoogleCloudError as del_e:
                    logger.error(
                        f'Failed to delete corrupted file {gcs_blob_path}: {del_e}',
                    )
                raise ValueError(f'MD5 mismatch for {file_name}')
            logger.info(f'MD5 checksum OK for {file_name}.')

    except icasdk.ApiException as e:
        logger.error(
            f'Failed to get download URL for {file_name} (ID: {file_id}): {e}',
        )
        raise
    except http_utils.TRANSPORT_ERRORS as e:
        logger.error(
            f'Failed to stream/download {file_name} (ID: {file_id}) after every attempt: {e}. '
            f'Re-run the stage to resume; files already in GCS are skipped.',
        )
        raise
    except gcs_exceptions.GoogleCloudError as e:
        logger.error(f'An error occurred uploading to GCS for {file_name}: {e}')
        raise

    return True


def list_ica_files(
    api_instance: 'project_data_api.ProjectDataApi',
    path_parameters: dict[str, str],
    base_ica_folder_path: str,
    *,
    recursive: bool = False,
) -> list[tuple[str, str]]:
    """List files under an ICA folder, returning ``(name_or_relative_path, file_id)`` tuples.

    Pagination is handled internally. With ``recursive=False`` (default), lists only files
    directly inside ``base_ica_folder_path`` and the first tuple element is the leaf file name.
    With ``recursive=True``, walks subfolders and returns relative paths (e.g.
    ``'report_files/samples/foo.csv'``) suitable to pass directly to ``stream_ica_file_to_gcs``
    as ``file_name`` so the GCS object key preserves the nested layout. No extension filtering —
    callers compose any filter they need.

    Folder traversal uses separate ``type=FOLDER`` queries (the SDK exposes no recursive flag).
    The walk is not transactional: a subfolder query failing mid-walk discards collected entries
    and propagates the ``icasdk.ApiException`` — callers should re-run on failure.
    """
    base = IcaPath.from_relpath(base_ica_folder_path).as_folder()

    def _list_children(parent: str, type_: str) -> list[dict]:
        items: list[dict] = []
        page_token: str | None = None
        while True:
            query_params: dict[str, object] = {
                'parentFolderPath': parent,
                'type': type_,
                'pageSize': '1000',
            }
            if page_token:
                query_params['pageToken'] = page_token
            api_response = ica_api_utils.ica_retry(
                api_instance.get_project_data_list,  # pyright: ignore[reportUnknownVariableType]
                path_params=path_parameters,  # pyright: ignore[reportArgumentType]
                query_params=query_params,  # type: ignore[reportArgumentType]
            )
            items.extend(api_response.body.get('items', []))  # pyright: ignore[reportUnknownArgumentType]
            page_token = api_response.body.get('nextPageToken')  # pyright: ignore[reportUnknownVariableType]
            if not page_token:
                break
        return items

    files: list[tuple[str, str]] = []

    def _walk(parent: str, relative_prefix: str) -> None:
        for item in _list_children(parent, 'FILE'):
            details = item['data'].get('details', {})  # pyright: ignore[reportUnknownVariableType]
            name = details.get('name')  # pyright: ignore[reportUnknownVariableType]
            fid = item['data'].get('id')  # pyright: ignore[reportUnknownVariableType]
            if not name or not fid:
                logger.warning(f'Skipping item with missing name or id under {parent}: {item}')
                continue
            files.append((f'{relative_prefix}{name}', fid))  # pyright: ignore[reportUnknownArgumentType]

        if not recursive:
            return

        for item in _list_children(parent, 'FOLDER'):
            details = item['data'].get('details', {})  # pyright: ignore[reportUnknownVariableType]
            name = details.get('name')  # pyright: ignore[reportUnknownVariableType]
            if not name:
                continue
            _walk(f'{parent}{name}/', f'{relative_prefix}{name}/')

    _walk(base, '')
    logger.info(f'List under {base} (recursive={recursive}) found {len(files)} files.')
    return files


def check_file_existence(
    api_instance: 'project_data_api.ProjectDataApi',
    path_params: dict[str, str],
    ica_folder_path: str,
    file_name: str,
) -> str | None:
    """
    Checks if the file already exists in ICA and returns its status.
    (Used by upload_data_to_ica.py)
    """
    file_data = ica_api_utils.get_file_details_from_ica(
        api_instance,
        path_params,
        ica_folder_path,
        file_name,
    )
    if file_data:
        return file_data['details']['status']  # pyright: ignore[reportUnknownVariableType]
    return None


def finalise_upload(
    api_instance: 'project_data_api.ProjectDataApi',
    path_params: dict[str, str],
    paths: dict[str, str],
    output_path_str: str,
) -> None:
    """
    Re-fetches the file ID from ICA and writes the output JSON file.
    (Used by upload_data_to_ica.py)
    """
    wait_for_file_available(
        api_instance=api_instance,
        path_params=path_params,
        file_name=paths['cram_name'],
        folder_path=paths['ica_folder_path'],
    )
    cram_data = ica_api_utils.get_file_details_from_ica(
        api_instance,
        path_params,
        paths['ica_folder_path'],
        paths['cram_name'],
    )

    cram_fid = cram_data['id'] if cram_data else None  # pyright: ignore[reportUnknownVariableType]

    if not cram_fid:
        raise ValueError(
            f'Failed to find file ID in ICA after upload for {paths["sg_name"]}.',
        )

    # Write only the CRAM FID to the output JSON
    output_data = {'cram_fid': cram_fid}
    with cpg_utils.to_path(output_path_str).open('w') as f:
        json.dump(output_data, f)

    logger.info(
        f'Successfully uploaded {paths["cram_name"]} for {paths["sg_name"]}.',
    )


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(FileNotFoundError),
    stop=tenacity.stop_after_attempt(4),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
def wait_for_file_available(
    api_instance: 'project_data_api.ProjectDataApi',
    path_params: dict[str, str],
    file_name: str,
    folder_path: str,
) -> bool:
    """Wait for a just-uploaded file to become AVAILABLE in ICA.

    Files aren't available immediately after upload, so this retries the existence check up to 4
    times, with an initial 2s sleep to guard against the first check racing the upload.
    """
    time.sleep(2)  # Guard against race condition where file is not yet available
    result: str | None = check_file_existence(
        api_instance=api_instance,
        path_params=path_params,
        ica_folder_path=folder_path,
        file_name=file_name,
    )
    if not result or result != 'AVAILABLE':
        raise FileNotFoundError(f'File: {file_name} not found at path: {folder_path} immediately after calling upload')
    logger.info(f'File: {file_name} is available (status: {result})')
    return True
