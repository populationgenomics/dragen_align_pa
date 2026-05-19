"""Delete cohort outputs + source CRAMs/FASTQs from ICA to release storage.

Two project-scoped passes:
- DRAGEN runs project: the cohort-level analysis output folder (cascades
  to per-batch analyses, per-SG outputs, per-batch FASTQ list CSVs) + the
  per-SG uploaded CRAM file IDs.
- Supplier project (FASTQ mode only): the linked FASTQ file IDs from
  `FastqIntakeQc`'s outpath.

Each pass fires all deletes, sleeps `settle_seconds` (default 60) for ICA's
async delete state machine, then verifies via `get_project_data` (404 or
`status='DELETING'` is success). Failures across both passes are aggregated
into a TSV log at `get_pipeline_path('{cohort}_delete_errors.log')`; on any
failure the job raises so the cpg-flow stage shows red.
"""

import json
import time
from dataclasses import dataclass
from typing import Any, Literal

import cpg_utils
from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_data_api
from icasdk.exceptions import ApiException, ApiValueError
from loguru import logger

from dragen_align_pa import ica_api_utils
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
        fids = [line.split()[0] for line in fh if line.strip()]
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
            failures.append(DeleteFailure(
                project_id=project_id,
                fid=fid,
                kind=f'still_present (status={status})',
                context=delete_errors.get(fid, ''),
            ))
        except ApiException as e:
            if getattr(e, 'status', None) == _HTTP_NOT_FOUND:
                continue
            failures.append(DeleteFailure(
                project_id=project_id,
                fid=fid,
                kind='verify_failed',
                context=f'{e!r} | delete: {delete_errors.get(fid, "")}',
            ))

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
) -> None:
    payload: dict[str, Any] = {
        'cohort_name': cohort_name,
        'runs_project': {
            'cohort_folder': cohort_folder_fid,
            'cram_count': cram_count,
        },
    }
    if fastq_count is not None:
        payload['fastq_source_project'] = {'fastq_count': fastq_count}
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
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    runs_project_id = secrets['projectID']

    cohort_folder_fid = _read_cohort_folder_fid(cohort_analysis_output_fid_path)
    cram_fids = _read_cram_fids(cram_fid_paths_dict) if cram_fid_paths_dict else []
    fastq_fids = _read_fastq_fids(fastq_ids_list_path) if fastq_ids_list_path else []

    runs_project_fids = [cohort_folder_fid, *cram_fids]

    failures: list[DeleteFailure] = []
    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)

        failures += _delete_and_verify(
            api_instance=api_instance,
            project_id=runs_project_id,
            fids=runs_project_fids,
            settle_seconds=settle_seconds,
        )

        if fastq_fids:
            fastq_project_id: str = config_retrieve(
                ['ica', 'projects', 'fastq_source_project_id'],
            )
            failures += _delete_and_verify(
                api_instance=api_instance,
                project_id=fastq_project_id,
                fids=fastq_fids,
                settle_seconds=settle_seconds,
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
    )
