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
from google.cloud.storage.retry import DEFAULT_RETRY
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


def _assert_checksums_match(src_hash: str, source: str, destination: str) -> None:
    # An empty describe result (exit 0 but no value, e.g. after a gcloud field
    # rename) must not certify anything — least of all the delete path's rm.
    dst_hash = _describe_crc32c(destination)
    if not src_hash or not dst_hash:
        raise ValueError(
            f'Empty crc32c for {source} or {destination}; cannot certify the copy '
            f'(if both objects exist, check whether the gcloud crc32c_hash field was renamed)'
        )
    if src_hash != dst_hash:
        raise ValueError(f'Checksum mismatch for {destination}: source {src_hash} vs destination {dst_hash}')


def _source_crc32c_or_none(source: str) -> str | None:
    """The source's crc32c, or None if the object is genuinely absent.

    Any other describe failure (429/503, auth, missing gcloud) propagates so the job
    fails instead of treating the source as gone.
    """
    try:
        return _describe_crc32c(source, log_failure=False)
    except subprocess.CalledProcessError as e:
        if not _source_is_absent(source, e):
            logger.error(f'describe {source} failed and is not an absence: {e.stderr}')
            raise
        return None


# A destination whose source is gone cannot be certified against it. The delete stage
# removes a source only after its destination matched by crc32c and records that as a
# `deleted` line, so the record stands in for the comparison. `already-absent` lines do
# not: they mean a previous run found nothing to certify. A re-run of the delete stage
# rewrites the record, so it carries earlier certificates forward as `deleted-earlier`
# lines rather than dropping them.
_CERTIFYING_OUTCOMES: tuple[str, ...] = ('deleted ', 'deleted-earlier ')


class _DeleteRecord:
    """The per-file outcomes `delete_files` wrote for one sequencing group, read on first use."""

    def __init__(self, url: str) -> None:
        self.url = url
        self._certified: frozenset[str] | None = None

    def certifies(self, source: str) -> bool:
        """Whether the record shows `source` was deleted after a crc32c match, by this or an earlier run."""
        if self._certified is None:
            bucket_name, key = _split_gs_url(self.url)
            blob = _storage_client().bucket(bucket_name).blob(key)
            lines = blob.download_as_text().splitlines() if blob.exists() else []
            self._certified = frozenset(
                line.removeprefix(prefix)
                for line in lines
                for prefix in _CERTIFYING_OUTCOMES
                if line.startswith(prefix)
            )
        return source in self._certified


def _assert_ingested_without_source(source: str, destination: str, record: _DeleteRecord) -> None:
    if not record.certifies(source):
        raise ValueError(
            f'{source} is absent and {record.url} does not record deleting it after certification; '
            f'cannot certify {destination}'
        )
    # A missing destination fails the describe: the record says the copy existed when the
    # source was deleted, so its absence now is a lost output, not an ingested one.
    if not _describe_crc32c(destination):
        raise ValueError(f'Empty crc32c for {destination}; cannot confirm the ingested copy')
    logger.info(f'Already ingested; source deleted after certification by an earlier run: {destination}')


def copy_files(pairs: Pairs, delete_record: str) -> None:
    """Copy each (source, destination) pair server-side, then certify by crc32c.

    `--no-clobber` makes stage re-runs cheap: an existing destination object is
    skipped rather than re-copied. Because a skipped destination may be a stale
    pre-existing object rather than a prior copy of this source, the checksum
    comparison is what actually certifies the copy — a mismatch fails the job. A
    source an earlier run deleted is accepted only if `delete_record` (the
    sequencing group's `backfill_delete` outcomes) shows it was deleted after a match.
    """
    record = _DeleteRecord(delete_record)
    for source, destination in pairs:
        src_hash = _source_crc32c_or_none(source)
        if src_hash is None:
            _assert_ingested_without_source(source, destination, record)
            continue
        run_subprocess_with_log(
            ['gcloud', 'storage', 'cp', '--no-clobber', source, destination],
            step_name=f'copy {source}',
        )
        _assert_checksums_match(src_hash, source, destination)


def verify_files(pairs: Pairs, delete_record: str) -> None:
    """Certify each destination matches its source by crc32c, copying nothing.

    Used for files whose copy stage may have been reused without running (its
    outputs pre-existed), so the checksum comparison still happens exactly once
    before registration. Sources an earlier run deleted are certified through
    `delete_record` as in `copy_files`.
    """
    record = _DeleteRecord(delete_record)
    for source, destination in pairs:
        src_hash = _source_crc32c_or_none(source)
        if src_hash is None:
            _assert_ingested_without_source(source, destination, record)
            continue
        _assert_checksums_match(src_hash, source, destination)


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


# `copy_blob` defaults to DEFAULT_RETRY_IF_GENERATION_SPECIFIED, which resolves to no
# retry unless the caller pins a generation, so a single 429/503 among ~100 copies
# per folder would fail the job (`list_blobs` and `delete` retry by default). An
# unconditional copy is safe to repeat — the destination just receives the same
# bytes again — so the plain retry is correct. Not `if_generation_match=0`: that
# also enables retries but means "only if the destination doesn't exist yet", and
# the sentinel is re-copied on every forced re-run.
def _copy_blob(source: 'storage.Blob', dest_bucket: 'storage.Bucket', dest_name: str) -> 'storage.Blob':
    return source.bucket.copy_blob(source, dest_bucket, dest_name, retry=DEFAULT_RETRY)


def copy_tree(source_prefix: str, dest_prefix: str) -> None:
    """Server-side copy of a staged folder, certifying by crc32c, sentinel strictly last.

    The staged folder must already carry the `_SUCCESS` sentinel (written on NCI by
    popgen_ica_nci_transfer after the ICA -> NCI -> GCP transfer); its absence means
    the staging itself may be incomplete, so the copy refuses to start. Every other
    file is copied (or, if already at the destination, checksum-certified) first,
    and the sentinel is placed only after all of them verified — the sentinel is the
    consuming stage's expected output, so an early copy would let a part-way failure
    present as a completed folder.
    """
    client = _storage_client()
    sources = _tree_blobs(client, source_prefix)
    if SUCCESS_OBJECT_NAME not in sources:
        raise ValueError(
            f'{source_prefix} has no {SUCCESS_OBJECT_NAME} sentinel; the staged metrics '
            f'folder is missing or was not fully transferred (popgen_ica_nci_transfer '
            f'writes the sentinel last) — re-stage it before re-running',
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
        new_blob = _copy_blob(source_blob, dest_bucket, f'{dest_key}/{rel}')
        _assert_tree_crc32c_matches(source_blob, new_blob)
        copied += 1
    _copy_blob(sentinel, dest_bucket, f'{dest_key}/{SUCCESS_OBJECT_NAME}')
    logger.info(
        f'copy-tree {source_prefix}: {copied} of {len(sources)} files copied '
        f'({len(sources) - copied} already present), sentinel placed',
    )


def delete_tree(source_prefix: str, dest_prefix: str, outcomes: list[str]) -> None:
    """Verify every staged file against its destination, then delete the staged folder.

    Verify-all-before-delete-any: one bad file leaves the whole staged folder
    untouched. The `_SUCCESS` sentinel only needs to exist at the destination — the
    ICA flow writes its own empty sentinel, so its content is not comparable — and
    is deleted last, so a part-way failure leaves the staged folder still carrying
    the sentinel `copy_tree` requires. Every destination file with no staged
    counterpart is recorded as `already-absent`: the copy placed the whole folder,
    so those are the files a previous run deleted, which keeps the per-file record
    complete across a re-run and lets a re-run after a full delete pass.
    """
    client = _storage_client()
    sources = _tree_blobs(client, source_prefix)
    destinations = _tree_blobs(client, dest_prefix)
    for rel, source_blob in sorted(sources.items()):
        destination = destinations.get(rel)
        if destination is None:
            raise ValueError(f'{dest_prefix}/{rel} is missing; refusing to delete the staged {rel}')
        if rel != SUCCESS_OBJECT_NAME:
            _assert_tree_crc32c_matches(source_blob, destination)
    for rel in sorted(destinations.keys() - sources.keys()):
        outcomes.append(f'already-absent {source_prefix}/{rel}')
    if not sources:
        logger.info(f'Staged folder already absent, skipping: {source_prefix}')
        return

    sentinel = sources.pop(SUCCESS_OBJECT_NAME, None)
    ordered = [sources[rel] for rel in sorted(sources)]
    if sentinel is not None:
        ordered.append(sentinel)
    source_bucket_name, _ = _split_gs_url(source_prefix)
    for source_blob in ordered:
        source_blob.delete()
        outcomes.append(f'deleted gs://{source_bucket_name}/{source_blob.name}')


def delete_files(pairs: Pairs, trees: list[tuple[str, str]], results_file: Path | str, delete_record: str) -> None:
    """Delete each source only after its destination matches its crc32c checksum.

    Each source's actual outcome (`deleted` / `deleted-earlier` / `already-absent`) is
    recorded in `results_file` as it is handled. A genuinely absent source is skipped
    (a re-run after a part-way failure must not fail on files deleted last time) and
    recorded as `deleted-earlier` if the existing record at `delete_record` certifies
    it, else `already-absent`; any other describe failure (429/503, auth, missing
    gcloud) propagates so the stage re-runs instead of silently orphaning the -upload
    file. A present source whose destination is missing, differs, or yields an empty
    checksum raises before any rm. `trees` are (source, destination) folder prefixes
    handled by `delete_tree` after the per-file pairs, recorded per file too.
    """
    record = _DeleteRecord(delete_record)
    outcomes: list[str] = []
    for source, destination in pairs:
        src_hash = _source_crc32c_or_none(source)
        if src_hash is None:
            if record.certifies(source):
                logger.info(f'Source deleted by an earlier run after certification: {source}')
                outcomes.append(f'deleted-earlier {source}')
            else:
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
        subparser.add_argument(
            '--delete-record',
            required=True,
            help="gs:// URL of this sequencing group's existing backfill_delete record (may not exist yet)",
        )
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
        copy_files(pairs, delete_record=args.delete_record)
    elif args.action == 'verify':
        verify_files(pairs, delete_record=args.delete_record)
    else:
        trees = [(source, destination) for source, destination in json.loads(args.trees_json)]
        delete_files(pairs, trees=trees, results_file=args.results_file, delete_record=args.delete_record)


if __name__ == '__main__':
    main()
