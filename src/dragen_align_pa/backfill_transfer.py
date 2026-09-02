"""Server-side GCS transfers for the backfill entry point: copy staged files from
-upload to their final -main paths, certify them by crc32c, and (opt-in) delete the
sources after verification.

Invoked as a CLI (`python3 -m dragen_align_pa.backfill_transfer
copy|verify|copy-tree|delete`) from the backfill stages' BashJobs. The fixed-name
per-file transfers shell out to gcloud through `run_subprocess_with_log` as an argv
list, so no shell ever interprets a path. The tree transfers (DRAGEN metrics
folders, an arbitrary per-SG file set of ~100 objects) use the storage client
instead: one listing per side carries every crc32c, where the gcloud path would
cost two describe subprocesses per file.
"""

import functools
import json
import re
import subprocess
import urllib.parse
from argparse import ArgumentParser
from pathlib import Path

from google.cloud import storage
from loguru import logger

from dragen_align_pa.gcs_utils import SUCCESS_OBJECT_NAME
from dragen_align_pa.utils import run_subprocess_with_log

# gcloud prints not-found describe failures with wording that has varied across
# releases; match the stable fragments, but only when the message also names the
# source URL — gcloud's not-found error names the object, while auth/transport
# errors don't. Anything unmatched is a real failure and must fail the job.
_GCLOUD_NOT_FOUND_PATTERN = re.compile('not found|matched no objects|404', re.IGNORECASE)

Pairs = list[tuple[str, str]]


def _describe_crc32c(url: str, log_failure: bool = True) -> str:
    # `crc32c_hash` verified as the storage object resource's field name in
    # google-cloud-sdk 577.0.0 (command_lib/storage/resources/resource_reference.py).
    process = run_subprocess_with_log(
        ['gcloud', 'storage', 'objects', 'describe', url, "--format=value(crc32c_hash)"],
        step_name=f'describe {url}',
        log_failure=log_failure,
    )
    return process.stdout.strip()


def _assert_checksums_match(source: str, destination: str) -> None:
    # An empty describe result (exit 0 but no value, e.g. after a gcloud field
    # rename) must not certify anything — least of all the delete path's rm.
    src_hash = _describe_crc32c(source)
    dst_hash = _describe_crc32c(destination)
    if not src_hash or not dst_hash:
        raise ValueError(
            f'Empty crc32c for {source} or {destination}; cannot certify the copy '
            f'(if both objects exist, check whether the gcloud crc32c_hash field was renamed)'
        )
    if src_hash != dst_hash:
        raise ValueError(f'Checksum mismatch for {destination}: source {src_hash} vs destination {dst_hash}')


def copy_files(pairs: Pairs) -> None:
    """Copy each (source, destination) pair server-side, then certify by crc32c.

    `--no-clobber` makes stage re-runs cheap: an existing destination object is
    skipped rather than re-copied. Because a skipped destination may be a stale
    pre-existing object rather than a prior copy of this source, the checksum
    comparison is what actually certifies the copy — a mismatch fails the job.
    """
    for source, destination in pairs:
        run_subprocess_with_log(
            ['gcloud', 'storage', 'cp', '--no-clobber', source, destination],
            step_name=f'copy {source}',
        )
        _assert_checksums_match(source, destination)


def verify_files(pairs: Pairs) -> None:
    """Certify each destination matches its source by crc32c, copying nothing.

    Used for files whose copy stage may have been reused without running (its
    outputs pre-existed), so the checksum comparison still happens exactly once
    before registration.
    """
    for source, destination in pairs:
        _assert_checksums_match(source, destination)


def _encoded_gs_url(url: str) -> str:
    """The URL as gcloud's not-found error renders it: object name percent-encoded.

    GcsNotFoundError builds `gs://{instance_name} not found: {status_code}.` from
    the request URL's resource path, where apitools has percent-encoded the object
    name (every `/` becomes `%2F`) — verified in google-cloud-sdk 577.0.0
    api_lib/storage/errors.py.
    """
    bucket, _, object_name = url.removeprefix('gs://').partition('/')
    return f'gs://{bucket}/{urllib.parse.quote(object_name, safe="")}'


def _source_is_absent(source: str, error: subprocess.CalledProcessError) -> bool:
    stderr = error.stderr or ''
    names_source = source in stderr or _encoded_gs_url(source) in stderr
    return names_source and bool(_GCLOUD_NOT_FOUND_PATTERN.search(stderr))


# Lazy so the module imports without ADC (e.g. in CI test collection), and cached so
# one process reuses one client across trees.
@functools.cache
def _storage_client() -> storage.Client:
    return storage.Client()


def _split_gs_url(url: str) -> tuple[str, str]:
    """Split `gs://bucket/key` into (bucket, key without any trailing slash)."""
    bucket_name, _, key = url.removeprefix('gs://').partition('/')
    return bucket_name, key.rstrip('/')


def _tree_blobs(client: storage.Client, prefix_url: str) -> dict[str, 'storage.Blob']:
    """Blobs under a gs:// prefix, keyed by prefix-relative name, with crc32c loaded."""
    bucket_name, key = _split_gs_url(prefix_url)
    prefix = f'{key}/'
    return {blob.name.removeprefix(prefix): blob for blob in client.list_blobs(bucket_name, prefix=prefix)}


def _assert_tree_crc32c_matches(source: 'storage.Blob', destination: 'storage.Blob') -> None:
    # A None/empty crc32c must not certify anything, same as the gcloud path's
    # empty-describe guard (GCS records crc32c for every object, composites included,
    # so an empty value means the listing metadata is broken).
    if not source.crc32c or not destination.crc32c:
        raise ValueError(f'Empty crc32c for {source.name} or {destination.name}; cannot certify the copy')
    if source.crc32c != destination.crc32c:
        raise ValueError(
            f'Checksum mismatch for {destination.name}: source {source.crc32c} vs destination {destination.crc32c}',
        )


def copy_tree(source_prefix: str, dest_prefix: str) -> None:
    """Server-side copy of a staged folder, certifying by crc32c, sentinel strictly last.

    The staged folder must already carry the `_SUCCESS` sentinel (written on NCI
    after the ICA -> NCI -> GCP transfer); its absence means the staging itself may
    be incomplete, so the copy refuses to start. Every other file is copied (or, if
    already at the destination, checksum-certified) first, and the sentinel is
    placed only after all of them verified — the sentinel is the consuming stage's
    expected output, so an early copy would let a part-way failure present as a
    completed folder.
    """
    client = _storage_client()
    sources = _tree_blobs(client, source_prefix)
    if SUCCESS_OBJECT_NAME not in sources:
        raise ValueError(
            f'{source_prefix} has no {SUCCESS_OBJECT_NAME} sentinel; the staged metrics '
            f'folder is missing or was not fully transferred — re-stage it before re-running',
        )
    destinations = _tree_blobs(client, dest_prefix)
    dest_bucket_name, dest_key = _split_gs_url(dest_prefix)
    dest_bucket = client.bucket(dest_bucket_name)

    sentinel = sources.pop(SUCCESS_OBJECT_NAME)
    copied = 0
    for rel, source_blob in sorted(sources.items()):
        existing = destinations.get(rel)
        if existing is not None:
            _assert_tree_crc32c_matches(source_blob, existing)
            continue
        new_blob = source_blob.bucket.copy_blob(source_blob, dest_bucket, f'{dest_key}/{rel}')
        _assert_tree_crc32c_matches(source_blob, new_blob)
        copied += 1
    sentinel.bucket.copy_blob(sentinel, dest_bucket, f'{dest_key}/{SUCCESS_OBJECT_NAME}')
    logger.info(
        f'copy-tree {source_prefix}: {copied} of {len(sources)} files copied '
        f'({len(sources) - copied} already present), sentinel placed',
    )


def delete_tree(source_prefix: str, dest_prefix: str, outcomes: list[str]) -> None:
    """Verify every staged file against its destination, then delete the staged folder.

    Verify-all-before-delete-any: one bad file leaves the whole staged folder
    untouched. The `_SUCCESS` sentinel only needs to exist at the destination — the
    ICA flow writes its own empty sentinel, so its content is not comparable. An
    empty staged folder is recorded as `already-absent` (a re-run after a prior
    delete must not fail).
    """
    client = _storage_client()
    sources = _tree_blobs(client, source_prefix)
    if not sources:
        logger.info(f'Staged folder already absent, skipping: {source_prefix}')
        outcomes.append(f'already-absent {source_prefix}/')
        return
    destinations = _tree_blobs(client, dest_prefix)
    for rel, source_blob in sorted(sources.items()):
        destination = destinations.get(rel)
        if destination is None:
            raise ValueError(f'{dest_prefix}/{rel} is missing; refusing to delete the staged {rel}')
        if rel != SUCCESS_OBJECT_NAME:
            _assert_tree_crc32c_matches(source_blob, destination)
    source_bucket_name, _ = _split_gs_url(source_prefix)
    for _rel, source_blob in sorted(sources.items()):
        source_blob.delete()
        outcomes.append(f'deleted gs://{source_bucket_name}/{source_blob.name}')


def delete_files(pairs: Pairs, trees: list[tuple[str, str]], results_file: Path | str) -> None:
    """Delete each source only after its destination matches its crc32c checksum.

    Each source's actual outcome (`deleted` / `already-absent`) is recorded in
    `results_file` as it is handled. A genuinely absent source is skipped (a
    re-run after a part-way failure must not fail on files deleted last time);
    any other describe failure (429/503, auth, missing gcloud) propagates so the
    stage re-runs instead of silently orphaning the -upload file. A present
    source whose destination is missing, differs, or yields an empty checksum
    raises before any rm. `trees` are (source, destination) folder prefixes
    handled by `delete_tree` after the per-file pairs.
    """
    outcomes: list[str] = []
    for source, destination in pairs:
        try:
            src_hash = _describe_crc32c(source, log_failure=False)
        except subprocess.CalledProcessError as e:
            if not _source_is_absent(source, e):
                logger.error(f'describe {source} failed and is not an absence: {e.stderr}')
                raise
            logger.info(f'Source already absent, skipping: {source}')
            outcomes.append(f'already-absent {source}')
            continue

        dst_hash = _describe_crc32c(destination)
        if not src_hash or not dst_hash:
            raise ValueError(
                f'Empty crc32c for {source} or {destination}; refusing to delete '
                f'(if both objects exist, check whether the gcloud crc32c_hash field was renamed)'
            )
        if src_hash != dst_hash:
            raise ValueError(f'Checksum mismatch for {source}: source {src_hash} vs destination {dst_hash}')

        run_subprocess_with_log(
            ['gcloud', 'storage', 'rm', source],
            step_name=f'delete {source}',
        )
        outcomes.append(f'deleted {source}')

    for source_prefix, dest_prefix in trees:
        delete_tree(source_prefix, dest_prefix, outcomes)

    # Written only when every pair and tree succeeded — the outcome lines record
    # exactly what the job did (a failed job never uploads the file at all).
    Path(results_file).write_text(''.join(f'{line}\n' for line in outcomes))


def main() -> None:
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest='action', required=True)
    for action in ('copy', 'verify', 'delete'):
        subparser = subparsers.add_parser(action)
        subparser.add_argument('--pairs-json', required=True, help='JSON list of [source, destination] pairs')
        if action == 'delete':
            subparser.add_argument('--trees-json', required=True, help='JSON list of [source, dest] folder prefixes')
            subparser.add_argument('--results-file', required=True)
    tree_parser = subparsers.add_parser('copy-tree')
    tree_parser.add_argument('--source-prefix', required=True, help='Staged gs:// folder prefix')
    tree_parser.add_argument('--dest-prefix', required=True, help='Final gs:// folder prefix')
    args = parser.parse_args()

    if args.action == 'copy-tree':
        copy_tree(args.source_prefix, args.dest_prefix)
        return
    pairs: Pairs = [(source, destination) for source, destination in json.loads(args.pairs_json)]
    if args.action == 'copy':
        copy_files(pairs)
    elif args.action == 'verify':
        verify_files(pairs)
    else:
        trees = [(source, destination) for source, destination in json.loads(args.trees_json)]
        delete_files(pairs, trees=trees, results_file=args.results_file)


if __name__ == '__main__':
    main()
