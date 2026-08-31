"""Hail Batch jobs for the backfill entry point: copy externally produced
outputs from the -upload bucket into their final -main locations, register the
CRAM and gVCFs in metamist, and (opt-in) delete the -upload sources once verified.

All jobs are BashJobs: the copies are server-side `gcloud storage cp` between
buckets (nothing is localized), and registration shells out to the
`dragen_align_pa.backfill_registration` CLI on the driver image.
"""

import json
import shlex
from typing import TYPE_CHECKING

from cpg_flow.targets import SequencingGroup
from cpg_utils.config import get_access_level, get_driver_image
from cpg_utils.hail_batch import authenticate_cloud_credentials_in_job, copy_common_env, get_batch

from dragen_align_pa.utils import get_backfill_source_path, get_output_path

if TYPE_CHECKING:
    import cpg_utils
    from hailtop.batch.job import BashJob

# gcloud prints not-found describe failures with wording that has varied across
# releases; match the stable fragments, but only on lines that also name the
# source URL — gcloud's not-found error names the object, while auth/transport
# errors and bash's own `command not found` don't. Anything unmatched is a real
# failure and must fail the job — see delete_command.
_GCLOUD_NOT_FOUND_PATTERN = 'not found|matched no objects|404'


def _checksum_compare_block(source: str, destination: str) -> str:
    """Bash certifying destination == source by crc32c, failing loud on any doubt.

    An empty describe result (exit 0 but no value, e.g. after a gcloud field
    rename) must not certify anything — least of all the delete path's rm.
    """
    quoted_source, quoted_destination = shlex.quote(source), shlex.quote(destination)
    return f"""src_hash=$(gcloud storage objects describe {quoted_source} --format='value(crc32c_hash)')
dst_hash=$(gcloud storage objects describe {quoted_destination} --format='value(crc32c_hash)')
if [ -z "$src_hash" ] || [ -z "$dst_hash" ]; then
    printf 'Empty crc32c for %s or %s; cannot certify the copy\\n' {quoted_source} {quoted_destination} >&2
    exit 1
fi
if [ "$src_hash" != "$dst_hash" ]; then
    printf 'Checksum mismatch for %s: source %s vs destination %s\\n' {quoted_destination} "$src_hash" "$dst_hash" >&2
    exit 1
fi"""


def copy_command(pairs: list[tuple[str, str]]) -> str:
    """Bash to copy each (source, destination) pair, then compare crc32c checksums.

    `--no-clobber` makes stage re-runs cheap: an existing destination object is
    skipped rather than re-copied. Because a skipped destination may be a stale
    pre-existing object rather than a prior copy of this source, the checksum
    comparison is what actually certifies the copy — a mismatch fails the job.
    """
    blocks = []
    for source, destination in pairs:
        copy_line = f'gcloud storage cp --no-clobber {shlex.quote(source)} {shlex.quote(destination)}'
        blocks.append(f'{copy_line}\n{_checksum_compare_block(source, destination)}')
    return 'set -euo pipefail\n' + '\n'.join(blocks)


def verify_command(pairs: list[tuple[str, str]]) -> str:
    """Bash certifying each destination matches its source by crc32c, copying nothing.

    Used for files whose copy stage may have been reused without running (its
    outputs pre-existed), so the checksum comparison still happens exactly once
    before registration.
    """
    return 'set -euo pipefail\n' + '\n'.join(
        _checksum_compare_block(source, destination) for source, destination in pairs
    )


def delete_command(pairs: list[tuple[str, str]]) -> str:
    """Bash to delete each source only after its destination matches its crc32c checksum.

    Expects the caller to have set `RESULTS` to a writable path; each source is
    recorded there as `deleted` or `already-absent` as it is actually handled.

    A genuinely absent source is skipped (a re-run after a part-way failure must
    not fail on files deleted last time), recognised by gcloud's not-found error
    text on a line naming the source URL; any other describe failure (429/503,
    auth, missing gcloud) fails the job so the stage re-runs instead of silently
    orphaning the -upload file. A present source whose destination is missing,
    differs, or yields an empty checksum aborts before any rm.
    """
    blocks = []
    for source, destination in pairs:
        quoted_source, quoted_destination = shlex.quote(source), shlex.quote(destination)
        describe_source = f"gcloud storage objects describe {quoted_source} --format='value(crc32c_hash)'"
        blocks.append(
            f"""if src_hash=$({describe_source} 2> describe_err.txt); then
    dst_hash=$(gcloud storage objects describe {quoted_destination} --format='value(crc32c_hash)')
    if [ -z "$src_hash" ] || [ -z "$dst_hash" ]; then
        printf 'Empty crc32c for %s or %s; refusing to delete\\n' {quoted_source} {quoted_destination} >&2
        exit 1
    fi
    if [ "$src_hash" != "$dst_hash" ]; then
        printf 'Checksum mismatch for %s: source %s vs destination %s\\n' {quoted_source} "$src_hash" "$dst_hash" >&2
        exit 1
    fi
    gcloud storage rm {quoted_source}
    echo deleted {quoted_source} >> "$RESULTS"
elif grep -F {quoted_source} describe_err.txt | grep -qiE {shlex.quote(_GCLOUD_NOT_FOUND_PATTERN)}; then
    echo already-absent {quoted_source} >> "$RESULTS"
else
    cat describe_err.txt >&2
    exit 1
fi"""
        )
    return 'set -euo pipefail\n: > "$RESULTS"\n' + '\n'.join(blocks)


def _source_destination_pairs(rel_filenames: list[str]) -> list[tuple[str, str]]:
    return [(str(get_backfill_source_path(rel)), str(get_output_path(rel))) for rel in rel_filenames]


def _new_gcloud_job(job_name: str, sequencing_group: SequencingGroup) -> 'BashJob':
    job: BashJob = get_batch().new_bash_job(
        name=f'{job_name} {sequencing_group.id}',
        attributes=(sequencing_group.get_job_attrs() or {}) | {'tool': 'gcloud'},
    )
    job.image(get_driver_image())
    authenticate_cloud_credentials_in_job(job)
    return job


def copy_from_upload_job(
    job_name: str,
    sequencing_group: SequencingGroup,
    rel_filenames: list[str],
    verify_only_rel_filenames: list[str] | None = None,
) -> 'BashJob':
    """Server-side copy of this SG's files from -upload to their final -main paths.

    `verify_only_rel_filenames` are certified (crc32c source == destination) without
    copying — for files owned by another stage that may have been reused without
    running its copy job, e.g. a pre-existing -main CRAM.
    """
    job = _new_gcloud_job(job_name, sequencing_group)
    job.command(copy_command(_source_destination_pairs(rel_filenames)))
    if verify_only_rel_filenames:
        job.command(verify_command(_source_destination_pairs(verify_only_rel_filenames)))
    return job


def register_backfill_job(
    sequencing_group: SequencingGroup,
    cram: 'cpg_utils.Path',
    base_gvcf: 'cpg_utils.Path',
    recal_gvcf: 'cpg_utils.Path',
    marker_path: 'cpg_utils.Path',
    stage_name: str,
) -> 'BashJob':
    """Register the cram, base gVCF and recal gVCF in metamist, in that order, in one process."""
    b = get_batch()
    job: BashJob = b.new_bash_job(
        name=f'Register backfill analyses {sequencing_group.id}',
        attributes=(sequencing_group.get_job_attrs() or {}) | {'tool': 'metamist', 'stage': stage_name},
    )
    job.image(get_driver_image())
    authenticate_cloud_credentials_in_job(job)
    copy_common_env(job)  # CPG_CONFIG_PATH, so metamist/config resolution works in the job

    # Mirror cpg-flow's project naming for decorator-registered analyses
    # (cpg_flow.stage bumps the project to `-test` at test access level).
    project_name = sequencing_group.dataset.name
    if get_access_level() == 'test' and 'test' not in project_name:
        project_name = f'{project_name}-test'

    meta = (sequencing_group.get_job_attrs() or {}) | {'stage': stage_name}
    job.command(
        'set -euo pipefail\n'
        'python3 -m dragen_align_pa.backfill_registration '
        f'--cram {shlex.quote(str(cram))} '
        f'--base-gvcf {shlex.quote(str(base_gvcf))} '
        f'--recal-gvcf {shlex.quote(str(recal_gvcf))} '
        f'--sg-id {shlex.quote(sequencing_group.id)} '
        f'--project-name {shlex.quote(project_name)} '
        f'--meta-json {shlex.quote(json.dumps(meta))} '
        f'--marker-file {job.ofile}'
    )
    b.write_output(job.ofile, str(marker_path))
    return job


def delete_upload_job(
    sequencing_group: SequencingGroup,
    rel_filenames: list[str],
    marker_path: 'cpg_utils.Path',
) -> 'BashJob':
    """Verify every -main destination matches its -upload source, then delete the sources.

    The marker records each source's actual outcome (`deleted` / `already-absent`),
    written by the script as it goes, never claims computed ahead of execution.
    """
    b = get_batch()
    job = _new_gcloud_job('DeleteBackfillUpload', sequencing_group)
    job.command(f'RESULTS={job.ofile}')
    job.command(delete_command(_source_destination_pairs(rel_filenames)))
    b.write_output(job.ofile, str(marker_path))
    return job
