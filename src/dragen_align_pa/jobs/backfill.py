"""Hail Batch jobs for the backfill entry point: copy externally produced
outputs from the -upload bucket into their final -main locations, register the
CRAM and gVCFs in metamist, and (opt-in) delete the -upload sources once verified.

All jobs are BashJobs on the driver image, each a one-line invocation of a
package CLI: `dragen_align_pa.backfill_transfer` for the server-side gcloud
copy/verify/delete (see that module for the checksum and absence semantics) and
`dragen_align_pa.backfill_registration` for the ordered metamist registration.
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


def _pairs_json(rel_filenames: list[str]) -> str:
    pairs = [[str(get_backfill_source_path(rel)), str(get_output_path(rel))] for rel in rel_filenames]
    return json.dumps(pairs)


def _new_backfill_job(job_name: str, sequencing_group: SequencingGroup, tool: str) -> 'BashJob':
    job: BashJob = get_batch().new_bash_job(
        name=f'{job_name} {sequencing_group.id}',
        attributes=(sequencing_group.get_job_attrs() or {}) | {'tool': tool},
    )
    job.image(get_driver_image())
    authenticate_cloud_credentials_in_job(job)
    copy_common_env(job)  # CPG_CONFIG_PATH, so the package CLIs can read config
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
    job = _new_backfill_job(job_name, sequencing_group, tool='gcloud')
    job.command(
        'set -euo pipefail\n'
        f'python3 -m dragen_align_pa.backfill_transfer copy '
        f'--pairs-json {shlex.quote(_pairs_json(rel_filenames))}'
    )
    if verify_only_rel_filenames:
        job.command(
            f'python3 -m dragen_align_pa.backfill_transfer verify '
            f'--pairs-json {shlex.quote(_pairs_json(verify_only_rel_filenames))}'
        )
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
    job = _new_backfill_job('Register backfill analyses', sequencing_group, tool='metamist')

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
        f'--marker-file {job.ofile} '
        f'--marker-gcs-path {shlex.quote(str(marker_path))}'
    )
    b.write_output(job.ofile, str(marker_path))
    return job


def delete_upload_job(
    sequencing_group: SequencingGroup,
    rel_filenames: list[str],
    marker_path: 'cpg_utils.Path',
) -> 'BashJob':
    """Verify every -main destination matches its -upload source, then delete the sources.

    The marker records each source's actual outcome (`deleted` / `already-absent`)
    as the CLI handles it, never claims computed ahead of execution.
    """
    b = get_batch()
    job = _new_backfill_job('DeleteBackfillUpload', sequencing_group, tool='gcloud')
    job.command(
        'set -euo pipefail\n'
        f'python3 -m dragen_align_pa.backfill_transfer delete '
        f'--pairs-json {shlex.quote(_pairs_json(rel_filenames))} '
        f'--results-file {job.ofile}'
    )
    b.write_output(job.ofile, str(marker_path))
    return job
