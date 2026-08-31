"""Server-side GCS transfers for the backfill entry point: copy staged files from
-upload to their final -main paths, certify them by crc32c, and (opt-in) delete the
sources after verification.

Invoked as a CLI (`python3 -m dragen_align_pa.backfill_transfer copy|verify|delete`)
from the backfill stages' BashJobs; every gcloud call goes through
`run_subprocess_with_log` as an argv list, so no shell ever interprets a path.
"""

import json
import re
import subprocess
from argparse import ArgumentParser
from pathlib import Path

from loguru import logger

from dragen_align_pa.utils import run_subprocess_with_log

# gcloud prints not-found describe failures with wording that has varied across
# releases; match the stable fragments, but only when the message also names the
# source URL — gcloud's not-found error names the object, while auth/transport
# errors don't. Anything unmatched is a real failure and must fail the job.
_GCLOUD_NOT_FOUND_PATTERN = re.compile('not found|matched no objects|404', re.IGNORECASE)

Pairs = list[tuple[str, str]]


def _describe_crc32c(url: str, log_failure: bool = True) -> str:
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
        raise ValueError(f'Empty crc32c for {source} or {destination}; cannot certify the copy')
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


def _source_is_absent(source: str, error: subprocess.CalledProcessError) -> bool:
    stderr = error.stderr or ''
    return source in stderr and bool(_GCLOUD_NOT_FOUND_PATTERN.search(stderr))


def delete_files(pairs: Pairs, results_file: Path | str) -> None:
    """Delete each source only after its destination matches its crc32c checksum.

    Each source's actual outcome (`deleted` / `already-absent`) is recorded in
    `results_file` as it is handled. A genuinely absent source is skipped (a
    re-run after a part-way failure must not fail on files deleted last time);
    any other describe failure (429/503, auth, missing gcloud) propagates so the
    stage re-runs instead of silently orphaning the -upload file. A present
    source whose destination is missing, differs, or yields an empty checksum
    raises before any rm.
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
            raise ValueError(f'Empty crc32c for {source} or {destination}; refusing to delete')
        if src_hash != dst_hash:
            raise ValueError(f'Checksum mismatch for {source}: source {src_hash} vs destination {dst_hash}')

        run_subprocess_with_log(
            ['gcloud', 'storage', 'rm', source],
            step_name=f'delete {source}',
        )
        outcomes.append(f'deleted {source}')

    # Written only when every pair succeeded — the outcome lines record exactly
    # what the job did (a failed job never uploads the file at all).
    Path(results_file).write_text(''.join(f'{line}\n' for line in outcomes))


def main() -> None:
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest='action', required=True)
    for action in ('copy', 'verify', 'delete'):
        subparser = subparsers.add_parser(action)
        subparser.add_argument('--pairs-json', required=True, help='JSON list of [source, destination] pairs')
        if action == 'delete':
            subparser.add_argument('--results-file', required=True)
    args = parser.parse_args()

    pairs: Pairs = [(source, destination) for source, destination in json.loads(args.pairs_json)]
    if args.action == 'copy':
        copy_files(pairs)
    elif args.action == 'verify':
        verify_files(pairs)
    else:
        delete_files(pairs, results_file=args.results_file)


if __name__ == '__main__':
    main()
