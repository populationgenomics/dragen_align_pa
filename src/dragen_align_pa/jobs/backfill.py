"""Hail Batch jobs for the backfill entry point: copy externally produced
outputs from the -upload bucket into their final -main locations, register the
gVCFs in metamist, and (opt-in) delete the -upload sources once verified.

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


def copy_command(pairs: list[tuple[str, str]]) -> str:
    """Bash to copy each (source, destination) pair, then verify the destination exists.

    `--no-clobber` makes stage re-runs cheap: a destination object that already
    exists is complete (GCS object creation is atomic), so it is skipped rather
    than re-copied. The describe re-verifies the skipped case too.
    """
    blocks = [
        f"gcloud storage cp --no-clobber '{source}' '{destination}'\n"
        f"gcloud storage objects describe '{destination}' --format='value(name)' > /dev/null"
        for source, destination in pairs
    ]
    return 'set -euo pipefail\n' + '\n'.join(blocks)


def delete_command(pairs: list[tuple[str, str]]) -> str:
    """Bash to delete each source only after its destination matches it in size.

    An already-absent source is skipped (a re-run after a part-way failure must
    not fail on files deleted last time), but a present source whose destination
    is missing or differs in size aborts the job before any rm.
    """
    blocks = [
        f"""if src_size=$(gcloud storage objects describe '{source}' --format='value(size)'); then
    dst_size=$(gcloud storage objects describe '{destination}' --format='value(size)')
    if [ "$src_size" != "$dst_size" ]; then
        echo "Size mismatch for {source}: source $src_size vs destination $dst_size" >&2
        exit 1
    fi
    gcloud storage rm '{source}'
else
    echo "Source already absent, skipping: {source}"
fi"""
        for source, destination in pairs
    ]
    return 'set -euo pipefail\n' + '\n'.join(blocks)


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
) -> 'BashJob':
    """Server-side copy of this SG's files from -upload to their final -main paths."""
    job = _new_gcloud_job(job_name, sequencing_group)
    job.command(copy_command(_source_destination_pairs(rel_filenames)))
    return job


def register_gvcfs_job(
    sequencing_group: SequencingGroup,
    base_gvcf: 'cpg_utils.Path',
    recal_gvcf: 'cpg_utils.Path',
    marker_path: 'cpg_utils.Path',
    stage_name: str,
) -> 'BashJob':
    """Register the base then recal gVCF in metamist, in that order, in one process."""
    b = get_batch()
    job: BashJob = b.new_bash_job(
        name=f'Register backfill gVCFs {sequencing_group.id}',
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
        f'--base-gvcf {shlex.quote(str(base_gvcf))} '
        f'--recal-gvcf {shlex.quote(str(recal_gvcf))} '
        f'--sg-id {shlex.quote(sequencing_group.id)} '
        f'--project-name {shlex.quote(project_name)} '
        f'--meta-json {shlex.quote(json.dumps(meta))} '
        f'> {job.ofile}'
    )
    b.write_output(job.ofile, str(marker_path))
    return job


def delete_upload_job(
    sequencing_group: SequencingGroup,
    rel_filenames: list[str],
    marker_path: 'cpg_utils.Path',
) -> 'BashJob':
    """Verify every -main destination matches its -upload source, then delete the sources."""
    b = get_batch()
    job = _new_gcloud_job('DeleteBackfillUpload', sequencing_group)
    pairs = _source_destination_pairs(rel_filenames)
    job.command(delete_command(pairs))
    job.command(f'echo {shlex.quote(json.dumps({"deleted_sources": [source for source, _ in pairs]}))} > {job.ofile}')
    b.write_output(job.ofile, str(marker_path))
    return job
