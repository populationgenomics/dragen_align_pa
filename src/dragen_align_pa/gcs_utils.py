"""GCS-side helpers for the ICA download path.

Answering "is this object already here, and did *this* ICA run put it here" is GCS
plumbing rather than ICA business logic, so it lives apart from `ica_utils`.
"""

import base64
import json
from typing import TYPE_CHECKING, Final

from cpg_utils.config import config_retrieve
from loguru import logger

if TYPE_CHECKING:
    from google.cloud.storage.blob import Blob
    from google.cloud.storage.bucket import Bucket


SUCCESS_OBJECT_NAME: Final = '_SUCCESS'
"""Sentinel written into an output prefix once every file has transferred."""


# A finalized GCS object is proof of a completed write on its own: an interrupted BlobWriter
# terminates its resumable session, so a partial transfer never publishes an object. Listing the
# destination therefore answers "what landed" without any per-file bookkeeping. What listing
# cannot answer is *which run* wrote it — output prefixes carry no pipeline id, so re-analysing a
# sequencing group lands on the same paths — and that is all the marker records.
def files_already_downloaded(
    gcs_bucket: 'Bucket',
    marker_key: str,
    gcs_prefix: str,
    ica_folder_path: str,
) -> set[str]:
    """Claim a destination for one ICA run and list what it already holds for that run.

    Writes the marker when the destination is unclaimed or was last written by a different ICA
    run, and reports nothing already downloaded in that case, so the previous analysis's outputs
    are replaced rather than inherited. Reads `[ica.download] force_redownload`, which forces an
    empty result.

    Args:
        gcs_bucket: Bucket holding both the marker and the destination prefix.
        marker_key: Object key for the marker, outside the prefix it describes.
        gcs_prefix: The destination prefix, without a trailing slash.
        ica_folder_path: The ICA folder being downloaded, which identifies the run.

    Returns:
        Object names relative to `gcs_prefix` that this run may skip.
    """
    if config_retrieve(['ica', 'download', 'force_redownload'], default=False):
        logger.info(f'force_redownload is set; re-downloading everything under {gcs_prefix}.')
        _claim_for_run(gcs_bucket, marker_key, ica_folder_path)
        return set()

    marker = gcs_bucket.get_blob(marker_key)  # pyright: ignore[reportUnknownVariableType]
    recorded = json.loads(marker.download_as_bytes()).get('ica_folder') if marker is not None else None  # pyright: ignore[reportUnknownMemberType]
    if recorded == ica_folder_path:
        return list_gcs_names(gcs_bucket, gcs_prefix)

    if recorded is not None:
        logger.warning(
            f'gs://{gcs_bucket.name}/{gcs_prefix} holds output from a different ICA run '
            f'({recorded}); re-downloading everything for {ica_folder_path}.',
        )
    _claim_for_run(gcs_bucket, marker_key, ica_folder_path)
    return set()


def _claim_for_run(gcs_bucket: 'Bucket', marker_key: str, ica_folder_path: str) -> None:
    """Record which ICA run owns a download destination."""
    gcs_bucket.blob(marker_key).upload_from_string(
        json.dumps({'ica_folder': ica_folder_path}, indent=2),
        content_type='application/json',
    )


def list_gcs_names(gcs_bucket: 'Bucket', gcs_prefix: str) -> set[str]:
    """List objects under a prefix, named relative to it.

    Args:
        gcs_bucket: Bucket to list.
        gcs_prefix: Prefix to list under, without a trailing slash.

    Returns:
        Object names relative to `gcs_prefix`, e.g. `{'a.csv', 'reports/b.html'}`,
        excluding the `_SUCCESS` sentinel.
    """
    prefix = f'{gcs_prefix}/'
    return {
        name
        for blob in gcs_bucket.list_blobs(prefix=prefix)  # pyright: ignore[reportUnknownVariableType]
        if (name := blob.name.removeprefix(prefix)) != SUCCESS_OBJECT_NAME  # pyright: ignore[reportUnknownMemberType]
    }


def write_success_sentinel(gcs_bucket: 'Bucket', gcs_prefix: str) -> None:
    """Mark an output prefix complete.

    Args:
        gcs_bucket: Bucket holding the prefix.
        gcs_prefix: The output prefix, without a trailing slash.
    """
    gcs_bucket.blob(f'{gcs_prefix}/{SUCCESS_OBJECT_NAME}').upload_from_string('', content_type='text/plain')


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


# Where ICA also gives us a hash we don't have to lean on the finalized-means-complete argument
# above — GCS records each object's MD5, so the existing copy is checked rather than trusted.
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
