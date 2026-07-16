"""Delete cohort outputs + source CRAMs/FASTQs from ICA to release storage.

Two deletion passes share one ICA client — both projects are in the same dataset family, so the
family API key authenticates both and only the `projectId` differs:
- DRAGEN-align project: the cohort-level analysis output folder (cascades to per-batch analyses,
  per-SG outputs, per-batch FASTQ list CSVs) + the per-SG uploaded CRAM file IDs.
- FASTQ-upload project (FASTQ mode only): the linked FASTQ file IDs from `FastqIntakeQc`'s
  outpath. Skipped when the family's `can_delete_fastq` is false (collaborator-managed).

Each pass fires all deletes, sleeps `settle_seconds` (default 60) for ICA's async delete state
machine, then verifies via `get_project_data` (404 or `status='DELETING'` is success). Failures
across both passes are aggregated into a TSV log at
`get_pipeline_path('{cohort}_delete_errors.log')`; on any failure the job raises so the cpg-flow
stage shows red.
"""

import json
import time
from dataclasses import dataclass
from typing import Any

import cpg_utils
from icasdk.apis.tags import project_data_api
from icasdk.exceptions import ApiException, ApiValueError
from loguru import logger

from dragen_align_pa import ica_api_utils
from dragen_align_pa.constants_registry import (
    ROLE_DRAGEN_ALIGN,
    ROLE_FASTQ_UPLOAD,
    ica_can_delete_fastq,
    ica_project_id,
    ica_project_name,
)
from dragen_align_pa.utils import get_pipeline_path

_DELETING_STATUS = 'DELETING'
_HTTP_NOT_FOUND = 404


@dataclass(frozen=True)
class DeleteFailure:
    """One row in the failure log. `kind` is human-readable for the TSV."""

    project_id: str
    fid: str
    kind: str
    context: str

    def as_tsv(self) -> str:
        return f'{self.project_id}\t{self.fid}\t{self.kind}\t{self.context}'


def _read_cohort_folder_fid(path: cpg_utils.Path) -> str:
    with path.open() as fh:
        return json.load(fh)['analysis_output_fid']


def _read_cram_fids(paths: dict[str, cpg_utils.Path]) -> list[str]:
    fids: list[str] = []
    for path in paths.values():
        with path.open() as fh:
            fids.append(json.load(fh)['cram_fid'])
    logger.info(f'Collected {len(fids)} CRAM FIDs across {len(paths)} SGs')
    return fids


def _read_fastq_fids(path: cpg_utils.Path) -> list[str]:
    with path.open() as fh:
        fids: list[str] = list(json.load(fh).keys())
    logger.info(f'Collected {len(fids)} FASTQ FIDs from {path}')
    return fids


def _delete_and_verify(
    api_instance: project_data_api.ProjectDataApi,
    project_id: str,
    fids: list[str],
    settle_seconds: int,
) -> list[DeleteFailure]:
    """Fire all deletes, wait for ICA's async state machine, then verify each."""
    if not fids:
        return []

    failures: list[DeleteFailure] = []
    delete_errors: dict[str, str] = {}

    for fid in fids:
        path_params = {'projectId': project_id, 'dataId': fid}
        try:
            api_instance.delete_data(path_params=path_params)
        except ApiValueError:
            # icasdk returns None from a non-Optional signature; the call
            # actually succeeded. Verify will confirm.
            pass
        except ApiException as e:
            delete_errors[fid] = repr(e)

    # ICA's delete is async — wait for state to advance to DELETING (or 404).
    if settle_seconds > 0:
        logger.info(f'Sleeping {settle_seconds}s for ICA delete propagation in project {project_id}')
        time.sleep(settle_seconds)

    for fid in fids:
        path_params = {'projectId': project_id, 'dataId': fid}
        try:
            response = api_instance.get_project_data(path_params=path_params)
            status = response.body['data']['details'].get('status', 'UNKNOWN')
            if status == _DELETING_STATUS:
                continue
            failures.append(
                DeleteFailure(
                    project_id=project_id,
                    fid=fid,
                    kind=f'still_present (status={status})',
                    context=delete_errors.get(fid, ''),
                )
            )
        except ApiException as e:
            if getattr(e, 'status', None) == _HTTP_NOT_FOUND:
                continue
            failures.append(
                DeleteFailure(
                    project_id=project_id,
                    fid=fid,
                    kind='verify_failed',
                    context=f'{e!r} | delete: {delete_errors.get(fid, "")}',
                )
            )

    return failures


def _write_failure_log(cohort_name: str, failures: list[DeleteFailure]) -> cpg_utils.Path:
    error_log_path = get_pipeline_path(filename=f'{cohort_name}_delete_errors.log')
    with error_log_path.open('w') as fh:
        fh.write('project_id\tfid\tkind\tcontext\n')
        for f in failures:
            fh.write(f.as_tsv() + '\n')
    return error_log_path


def _write_success_marker(
    output_path: cpg_utils.Path,
    cohort_name: str,
    cohort_folder_fid: str,
    cram_count: int,
    fastq_count: int | None,
    fastq_skipped: bool,
) -> None:
    """Write the audit marker recording what this run deleted.

    Args:
        output_path: Where to write the marker JSON.
        cohort_name: The cohort processed.
        cohort_folder_fid: The deleted cohort-level analysis output folder id.
        cram_count: Number of per-SG CRAM file ids deleted from the DRAGEN project.
        fastq_count: Number of FASTQ file ids in scope, or `None` in CRAM-only mode. When the
            FASTQ pass was skipped, these were not deleted (see `fastq_skipped`).
        fastq_skipped: `True` if the FASTQ pass was skipped (collaborator-managed family), so
            `fastq_count` reflects files left for the collaborator to delete, not files deleted.
    """
    payload: dict[str, Any] = {
        'cohort_name': cohort_name,
        'runs_project': {
            'cohort_folder': cohort_folder_fid,
            'cram_count': cram_count,
        },
    }
    if fastq_count is not None:
        payload['fastq_source_project'] = {'fastq_count': fastq_count, 'skipped': fastq_skipped}
    with output_path.open('w') as fh:
        json.dump(payload, fh)


def run(
    cohort_name: str,
    output_path: cpg_utils.Path,
    cohort_analysis_output_fid_path: cpg_utils.Path,
    cram_fid_paths_dict: dict[str, cpg_utils.Path] | None,
    fastq_ids_list_path: cpg_utils.Path | None,
    settle_seconds: int = 60,
) -> None:
    """Entry point — called from the `DeleteDataInIca` stage's PythonJob.

    `settle_seconds` is a parameter (not a constant) so unit tests can pass 0
    to skip the production 60s wait. Production callers always use the default.
    """
    failures: list[DeleteFailure] = []
    cohort_folder_fid: str = _read_cohort_folder_fid(cohort_analysis_output_fid_path)
    cram_fids: list[str] = _read_cram_fids(cram_fid_paths_dict) if cram_fid_paths_dict else []
    fastq_fids: list[str] = _read_fastq_fids(fastq_ids_list_path) if fastq_ids_list_path else []

    if not cram_fid_paths_dict and not fastq_ids_list_path:
        raise ValueError('Either cram_fid_paths_dict or fastq_ids_list_path must be provided')

    # The DRAGEN runs project and the FASTQ-upload project are in the same dataset family, so one
    # client (authenticated with the family API key) serves both — only the `projectId` in
    # path_params differs. Open it once via the DRAGEN-align session and reuse it for both passes.
    fastq_skipped = False
    with ica_api_utils.ica_project_data_api(ROLE_DRAGEN_ALIGN) as (api_instance, path_params):
        # DRAGEN runs project: cohort output folder (cascades) + per-SG CRAMs.
        failures += _delete_and_verify(
            api_instance=api_instance,
            project_id=path_params['projectId'],
            fids=[cohort_folder_fid, *cram_fids],
            settle_seconds=settle_seconds,
        )

        # FASTQ-upload project. `can_delete_fastq` gates whether we touch it at all; ICA enforces
        # the same permission independently, so this is a pre-check that skips a family whose
        # upload area we don't control rather than attempting a delete ICA would refuse.
        if fastq_ids_list_path:
            if ica_can_delete_fastq():
                failures += _delete_and_verify(
                    api_instance=api_instance,
                    project_id=ica_project_id(ROLE_FASTQ_UPLOAD),
                    fids=fastq_fids,
                    settle_seconds=settle_seconds,
                )
            else:
                fastq_skipped = True
                logger.info(
                    f'FASTQ source project {ica_project_name(ROLE_FASTQ_UPLOAD)!r} is collaborator-managed '
                    f'(can_delete_fastq=false); skipping FASTQ deletion (collaborators delete on request).',
                )

    if failures:
        error_log_path = _write_failure_log(cohort_name, failures)
        raise RuntimeError(
            f'{len(failures)} FIDs failed cleanup; see {error_log_path}. '
            f'Re-run the stage to retry (already-deleted items return 404 quickly).',
        )

    _write_success_marker(
        output_path=output_path,
        cohort_name=cohort_name,
        cohort_folder_fid=cohort_folder_fid,
        cram_count=len(cram_fids),
        fastq_count=len(fastq_fids) if fastq_ids_list_path is not None else None,
        fastq_skipped=fastq_skipped,
    )
