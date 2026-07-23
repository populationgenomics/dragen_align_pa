"""Orchestrate the unified DRAGEN pipeline across a cohort.

Responsibilities:
- Chunk the cohort into deterministic batches (IcaBatch dataclass).
- Persist `{cohort}_batches.json` plus per-SG state files (extended schema).
- Submit batches via `submit_dragen_batch.run`, monitored by the shared
  `manage_ica_pipeline_loop` (now generic over IcaBatch targets).
- After the first pass completes, read passfail across all batches; if any SGs
  are marked Fail and have not been retried, form retry batches and run the
  loop a second time. Single retry only.
- After the retry pass, raise if any SG is still failed: there is no
  failure-rate tolerance, so a single unrecovered failure halts the cohort.
  The completion marker records the failure count on a clean run.
"""

import json
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime

import cpg_utils
import icasdk
import requests
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve
from loguru import logger

from dragen_align_pa import ica_api_utils
from dragen_align_pa.batches import BatchesFile, IcaBatch, chunk_sgs_into_batches
from dragen_align_pa.constants.batch_constants import (
    ACTIVE_BATCH_STATUSES,
    BATCH_STATUS_CANCELLED,
    BATCH_STATUS_FAILED,
    BATCH_STATUS_INPROGRESS,
    BATCH_STATUS_SUCCEEDED,
    CANONICAL_PASSFAIL_FAIL,
    DEFAULT_BATCH_SIZE,
    HTTP_NOT_FOUND,
    ICA_STATUS_SUCCEEDED,
    ICA_TERMINAL_FAILURE_STATUSES,
)
from dragen_align_pa.constants.constants_registry import ROLE_DRAGEN_ALIGN
from dragen_align_pa.jobs import cancel_ica_pipeline_run, monitor_dragen_pipeline, submit_dragen_batch
from dragen_align_pa.jobs.ica_pipeline_manager import (
    MonitoredTarget,
    PipelineStatus,
    manage_ica_pipeline_loop,
)
from dragen_align_pa.jobs.parse_passfail import fetch_passfail_from_ica
from dragen_align_pa.ica_utils import ica_cohort_path, ica_run_path
from dragen_align_pa.utils import PER_SG_STATE_SCHEMA_VERSION, get_pipeline_path, load_per_sg_state


# Distinct exception type so operators can tell a user-initiated cancellation
# apart from a true pipeline failure. N818's `…Error` suffix is intentionally skipped.
class CohortCancelled(RuntimeError):  # noqa: N818
    """Raised when `cancel_cohort_run=true` has terminated the cohort."""


def _build_submit_callable(
    batch: IcaBatch,
    analysis_output_fid_path: cpg_utils.Path,
    cram_state_paths: dict[str, cpg_utils.Path] | None,
    fastq_ids_path: cpg_utils.Path | None,
    per_sg_fastq_list_paths: dict[str, cpg_utils.Path] | None,
    batches_file: BatchesFile,
    outputs: dict[str, cpg_utils.Path],
) -> Callable[[], str]:
    """Wrap `submit_dragen_batch.run` to return just the pipeline_id and persist
    the submission identity into the batches file + per-SG state files.
    """

    def _submit() -> str:
        # error_strategy is set on creation (initialise / add_retry_batch), never
        # changed here, so it is not written back below.
        entry = batches_file.batch_entry(batch.batch_index)
        error_strategy = entry['error_strategy']
        result = submit_dragen_batch.run(
            batch=batch,
            analysis_output_fid_path=analysis_output_fid_path,
            cram_state_paths=cram_state_paths,
            fastq_ids_path=fastq_ids_path,
            per_sg_fastq_list_paths=per_sg_fastq_list_paths,
            error_strategy=error_strategy,
        )
        # Narrow return-dict values for the type-checker: these fields are always str.
        pipeline_id_v = result['pipeline_id']
        ar_guid_v = result['ar_guid']
        user_reference_v = result['user_reference']
        assert isinstance(pipeline_id_v, str) and isinstance(ar_guid_v, str) and isinstance(user_reference_v, str)

        # Persistence ordering (canonical: BatchesFile "Note on atomic writes"):
        # per-SG state FIRST, batches.json SECOND (the commit point), so a crash
        # between leaves the batch PENDING to re-submit rather than exposing a
        # state file for a batch batches.json doesn't acknowledge.
        _persist_per_sg_state_for_batch(
            outputs,
            batch,
            {
                'pipeline_id': pipeline_id_v,
                'ar_guid': ar_guid_v,
                'user_reference': user_reference_v,
            },
        )
        batches_file.record_pipeline_submission(
            batch_index=batch.batch_index,
            pipeline_id=pipeline_id_v,
            ar_guid=ar_guid_v,
            user_reference=user_reference_v,
        )
        if 'fastq_list_fid' in result:
            fid = result['fastq_list_fid']
            assert isinstance(fid, str)
            batches_file.record_fastq_list_fid(batch.batch_index, fid)
        if 'cram_fids' in result:
            fids = result['cram_fids']
            assert isinstance(fids, list)
            batches_file.record_cram_fids(batch.batch_index, fids)
        batches_file.write()
        return pipeline_id_v

    return _submit


# Single writer for the per-SG state schema (shared by the submit-time and
# force_retry paths) so the two can't drift. Schema must match what
# utils.load_per_sg_state / ica_utils.get_ica_sample_folder validate on read.
def _write_per_sg_state(
    path: cpg_utils.Path,
    *,
    pipeline_id: str,
    ar_guid: str,
    user_reference: str,
    batch_index: int,
) -> None:
    """Write one SG's per-SG state file.

    Args:
        path: Destination per-SG state file.
        pipeline_id: The SG's ICA analysis (pipeline) id.
        ar_guid: The submission's analysis-runner GUID.
        user_reference: The batch's ICA `user_reference` (names the folder).
        batch_index: The batch the SG's output belongs to.
    """
    with path.open('w') as fh:
        json.dump(
            {
                'schema_version': PER_SG_STATE_SCHEMA_VERSION,
                'pipeline_id': pipeline_id,
                'ar_guid': ar_guid,
                'user_reference': user_reference,
                'batch_index': batch_index,
            },
            fh,
        )


def _persist_per_sg_state_for_batch(
    outputs: dict[str, cpg_utils.Path],
    batch: IcaBatch,
    submission_result: dict[str, str],
) -> None:
    """Write per-SG state files for one batch immediately on submission.

    On retry this rewrites the SG's state with the retry batch's identifiers;
    cpg-flow's `required_stages` ordering guarantees downstream stages (MLR,
    downloads) read only the latest generation's identifiers.
    """
    for sg_name in batch.sg_names:
        key = f'{sg_name}_pipeline_id_and_arguid'
        if key not in outputs:
            # run() pre-validates these keys, so reaching here means the cohort SG
            # list changed mid-run. Raise loudly rather than orphan the submission
            # with no per-SG state file (downstream Download* would fail less clearly).
            raise KeyError(
                f'Per-SG state output {key!r} not declared in outputs for batch '
                f"{batch.name}. run()'s pre-check should have caught this — "
                f'cohort SG list likely changed mid-run, or ManageDragenPipeline.'
                f'expected_outputs is undercounting.',
            )
        _write_per_sg_state(
            outputs[key],
            pipeline_id=submission_result['pipeline_id'],
            ar_guid=submission_result['ar_guid'],
            user_reference=submission_result['user_reference'],
            batch_index=batch.batch_index,
        )


# Errors that a passfail/folder ICA fetch can surface: the ICA SDK's API error,
# a network failure from the presigned-URL GET, and a non-JSON body slipping past
# `raise_for_status`. `FileNotFoundError` (folder/file absent) is caught locally.
_PASSFAIL_FETCH_ERRORS = (icasdk.ApiException, requests.RequestException, json.JSONDecodeError)


def _fetch_batch_passfail_and_folder(
    batch: IcaBatch,
    user_reference: str,
    pipeline_id: str,
) -> tuple[dict[str, str] | None, str | None]:
    """Fetch a SUCCEEDED batch's `passfail.json` and (best-effort) its output-folder fid.

    Opens one ICA project-data API context. Shared by the live `on_succeeded`
    callback and `force_retry` reconciliation.

    Args:
        batch: The batch whose ICA analysis output is being read.
        user_reference: The batch's stored ICA `user_reference` (names the folder).
        pipeline_id: The batch's stored ICA analysis (pipeline) id.

    Returns:
        A `(passfail, folder_fid)` tuple. `passfail` is the parsed `{sg: status}`
        mapping, or `None` when the file is legitimately absent (a catastrophically-
        failed batch that produced none). `folder_fid` is the analysis output-folder
        id, or `None` if it could not be resolved (best-effort).

    Raises:
        icasdk.ApiException: On an ICA API error fetching passfail.json.
        requests.RequestException: On a network error fetching the presigned URL.
        json.JSONDecodeError: If the presigned URL serves a non-JSON body.
    """
    # ICA names the analysis folder `{user_reference}-{pipeline_id}` (user_reference
    # ends in `_`, so `…_-{pipeline_id}/`), inside the cohort-level parent folder.
    # Built via the shared builders to stay in lockstep with get_ica_sample_folder.
    analysis_folder_name = f'{user_reference}-{pipeline_id}'
    ica_parent = ica_cohort_path(batch.cohort_name).as_folder()
    ica_folder = ica_run_path(batch.cohort_name, user_reference, pipeline_id).as_folder()

    folder_fid: str | None = None
    with ica_api_utils.ica_project_data_api(ROLE_DRAGEN_ALIGN) as (api_instance, path_parameters):
        passfail = fetch_passfail_from_ica(
            api_instance=api_instance,
            path_parameters=path_parameters,
            ica_folder_path=ica_folder,
        )
        # Best-effort folder-ID lookup; useful for future cleanup. Not fatal if missing.
        try:
            folder_fid = ica_api_utils.find_file_id_by_name(
                api_instance=api_instance,
                path_parameters=path_parameters,
                parent_folder_path=ica_parent,
                file_name=analysis_folder_name,
            )
        except (icasdk.ApiException, FileNotFoundError) as e:
            logger.warning(
                f'Batch {batch.name} (analysis {pipeline_id}, folder {analysis_folder_name}): '
                f'could not resolve analysis output folder ID: {e}',
            )
    return passfail, folder_fid


def _record_succeeded_batch(
    batches_file: BatchesFile,
    batch: IcaBatch,
    pipeline_id: str,
    passfail: dict[str, str] | None,
    folder_fid: str | None,
) -> None:
    """Persist a SUCCEEDED batch's passfail outcome (+ folder fid + status) to the batches file.

    Shared by `on_succeeded` and `force_retry`. Writes the batches file.

    Args:
        batches_file: The cohort batches file to record into (mutated + written).
        batch: The batch being recorded.
        pipeline_id: The batch's ICA analysis id (for log context).
        passfail: The parsed `{sg: status}` mapping, or `None`. `None` means the
            analysis produced no passfail.json; all SGs are then recorded Fail.
        folder_fid: The analysis output-folder id, or `None` if unresolved.
    """
    if passfail is None:
        logger.warning(
            f'Batch {batch.name} (analysis {pipeline_id}): passfail.json not found at ICA root; '
            f'treating all SGs as Fail.',
        )
        batches_file.record_passfail(batch.batch_index, dict.fromkeys(batch.sg_names, CANONICAL_PASSFAIL_FAIL))
    else:
        # Defensive: passfail keys MUST match batch.sg_names (RGSM == sg_name invariant).
        expected = set(batch.sg_names)
        unexpected = set(passfail) - expected
        missing = expected - set(passfail)
        if unexpected:
            logger.warning(
                f'Batch {batch.name} (analysis {pipeline_id}): passfail.json contains unexpected '
                f'sample IDs {unexpected}; dropping them. This usually means RGSM != sg_name '
                f'(CRAM mode: original SM tag differs from the cpg-flow SG ID).',
            )
        if missing:
            logger.warning(
                f'Batch {batch.name} (analysis {pipeline_id}): passfail.json missing entries for '
                f'SGs {missing}; marking them as Fail so they enter the retry path.',
            )
        filtered = {sg: passfail[sg] for sg in batch.sg_names if sg in passfail}
        for sg in missing:
            filtered[sg] = CANONICAL_PASSFAIL_FAIL
        batches_file.record_passfail(batch.batch_index, filtered)
    if folder_fid is not None:
        batches_file.record_analysis_output_folder_fid(batch.batch_index, folder_fid)
    batches_file.record_status(batch.batch_index, BATCH_STATUS_SUCCEEDED)
    batches_file.write()


def _on_succeeded_factory(
    batches_file: BatchesFile,
    batches_by_name: dict[str, IcaBatch],
) -> Callable[[MonitoredTarget], None]:
    """Callback invoked when a batch's ICA analysis reaches SUCCEEDED.

    Fetches `passfail.json` from the batch root in-memory (it's KB-scale),
    persists per-SG Success/Fail status, and opportunistically looks up the
    batch's ICA output folder ID for cleanup. Wrapped in best-effort error
    handling so transient ICA blips don't kill the whole orchestrator loop.
    """

    def _on_succeeded(monitored: MonitoredTarget) -> None:
        batch = batches_by_name.get(monitored.name)
        if batch is None:
            logger.warning(f'on_succeeded called for unknown target {monitored.name}; ignoring.')
            return

        entry = batches_file.batch_entry(batch.batch_index)
        try:
            passfail, folder_fid = _fetch_batch_passfail_and_folder(
                batch,
                entry['user_reference'],
                entry['pipeline_id'],
            )
        except _PASSFAIL_FETCH_ERRORS as e:
            # RAISE (don't return): the loop's transactional contract leaves the
            # target INPROGRESS and re-fires next poll. A silent return would let
            # the loop set SUCCEEDED while batches.json stays INPROGRESS.
            raise RuntimeError(
                f'Batch {batch.name}: ICA fetch failed in on_succeeded ({e}); '
                f'leaving status INPROGRESS so the next poll can re-fetch.',
            ) from e

        _record_succeeded_batch(batches_file, batch, entry['pipeline_id'], passfail, folder_fid)

    return _on_succeeded


def _on_status_change_factory(
    batches_file: BatchesFile,
    batches_by_name: dict[str, IcaBatch],
    failed_final_sink: set[int],
) -> Callable[[MonitoredTarget, PipelineStatus], None]:
    """Mirror the loop's terminal non-success transitions into `{cohort}_batches.json`.

    Without this, FAILED_FINAL / CANCELLED transitions never reach the batches
    file (stuck INPROGRESS), making the retry path's batch-level branch
    unreachable and re-polling dead analyses on resume. SUCCEEDED is NOT routed
    here — `_on_succeeded_factory` records it transactionally.

    Args:
        batches_file: The cohort batches file to mirror transitions into.
        batches_by_name: Map of batch name → `IcaBatch` for the targets in this loop.
        failed_final_sink: A set that receives the `batch_index` of every
            FAILED_FINAL transition the loop reports.

    Returns:
        The `on_status_change` callback for `manage_ica_pipeline_loop`.
    """

    def _on_status_change(monitored: MonitoredTarget, new_status: PipelineStatus) -> None:
        batch = batches_by_name.get(monitored.name)
        if batch is None:
            logger.warning(
                f'on_status_change called for unknown target {monitored.name} '
                f'(new_status={new_status.name}); ignoring.',
            )
            return
        if new_status == PipelineStatus.FAILED_FINAL:
            failed_final_sink.add(batch.batch_index)
            # Clear stale passfail so this is an unambiguous whole-batch failure
            # (a FAILED entry with passfail would be read via the per-SG branch).
            _mark_failed_and_clear(batches_file, batch.batch_index)
        elif new_status == PipelineStatus.CANCELLED:
            batches_file.record_status(batch.batch_index, BATCH_STATUS_CANCELLED)
        else:
            # Defensive: the loop only fires this callback for FAILED_FINAL /
            # CANCELLED. Anything else is a future-proofing surprise.
            logger.warning(
                f'on_status_change: unexpected new_status={new_status.name} for {monitored.name}; '
                f'not mirrored into batches.json.',
            )
            return
        batches_file.write()

    return _on_status_change


def _mark_failed_and_clear(batches_file: BatchesFile, batch_index: int) -> None:
    """Mark a batch FAILED and clear its passfail.

    Args:
        batches_file: The cohort batches file (mutated in place).
        batch_index: The batch to mark.
    """
    # Clearing the stale passfail turns the batch into a whole-batch failure so
    # every SG resubmits rather than being harvested from a gone/failed analysis.
    batches_file.record_status(batch_index, BATCH_STATUS_FAILED)
    batches_file.clear_passfail(batch_index)


def _mark_inprogress_and_clear(batches_file: BatchesFile, batch_index: int) -> None:
    """Mark a batch INPROGRESS and clear its passfail.

    Args:
        batches_file: The cohort batches file (mutated in place).
        batch_index: The batch to mark.
    """
    # Clearing the stale passfail preserves the "INPROGRESS batch has empty
    # passfail" invariant, so `_build_retry_batches`'s `in_flight_sgs` guard
    # excludes the re-monitored batch instead of pulling its SGs into a fresh
    # retry while the resume loop re-monitors the same analysis (double submit).
    batches_file.record_status(batch_index, BATCH_STATUS_INPROGRESS)
    batches_file.clear_passfail(batch_index)


def _reconcile_batches_with_ica(cohort_name: str, batches_file: BatchesFile) -> None:
    """`force_retry`: overwrite each submitted batch's GCS status with ICA reality.

    For every batch that reached ICA (has a `pipeline_id`), query the live analysis
    status and — for a SUCCEEDED analysis — its `passfail.json`, then rewrite the
    batches file to match. A batch whose analysis is gone (404) or terminally failed
    is marked FAILED with its stale passfail cleared; an INPROGRESS analysis, or a
    SUCCEEDED one whose passfail fetch transiently fails, is set INPROGRESS for the
    normal resume to monitor.
    Batches with no `pipeline_id` are skipped. Writes the batches file.

    Args:
        cohort_name: The cohort being reconciled (for building ICA paths + logs).
        batches_file: The cohort batches file to reconcile in place (read from disk,
            already loaded; mutated + written here).

    Raises:
        icasdk.ApiException: On any non-404 ICA error querying an analysis status
            (a 404 is handled as "analysis gone", not re-raised).
    """
    counts: Counter[str] = Counter()
    for entry in batches_file.batches:
        if entry['status'] == BATCH_STATUS_CANCELLED:
            # CANCELLED is terminal — never relabel it (ABORTED would map to FAILED
            # and resubmit). Defence-in-depth; run() already refuses this path.
            continue
        pipeline_id = entry['pipeline_id']
        if not pipeline_id:
            continue
        batch = IcaBatch.from_entry(cohort_name, entry)
        try:
            ica_status = monitor_dragen_pipeline.run(ica_pipeline_id=pipeline_id, is_mlr=False)
        except icasdk.ApiException as e:
            if e.status != HTTP_NOT_FOUND:
                raise
            logger.warning(
                f'Batch {batch.name}: ICA analysis {pipeline_id} not found (gone); '
                f'marking {BATCH_STATUS_FAILED} to resubmit fresh.',
            )
            _mark_failed_and_clear(batches_file, batch.batch_index)
            counts['gone'] += 1
            continue

        if ica_status == ICA_STATUS_SUCCEEDED:
            try:
                passfail, folder_fid = _fetch_batch_passfail_and_folder(batch, entry['user_reference'], pipeline_id)
            except _PASSFAIL_FETCH_ERRORS as e:
                # A transient fetch blip on one succeeded batch must not abort the
                # whole recovery: leave it INPROGRESS so the resume loop re-fetches.
                logger.warning(
                    f'Batch {batch.name}: passfail fetch failed during reconciliation ({e}); '
                    f'leaving {BATCH_STATUS_INPROGRESS} for the resume loop to re-fetch.',
                )
                _mark_inprogress_and_clear(batches_file, batch.batch_index)
                counts['in_progress'] += 1
                continue
            _record_succeeded_batch(batches_file, batch, pipeline_id, passfail, folder_fid)
            counts['succeeded'] += 1
        elif ica_status in ICA_TERMINAL_FAILURE_STATUSES:
            _mark_failed_and_clear(batches_file, batch.batch_index)
            counts['failed'] += 1
        else:
            # REQUESTED / AWAITINGINPUT / INPROGRESS — still running; let the resume monitor it.
            _mark_inprogress_and_clear(batches_file, batch.batch_index)
            counts['in_progress'] += 1

    batches_file.write()
    logger.info(
        f'force_retry reconciliation for {cohort_name}: {counts["succeeded"]} succeeded, '
        f'{counts["failed"]} failed, {counts["gone"]} gone (resubmit fresh), '
        f'{counts["in_progress"]} still running.',
    )


# The per-SG state file is the sole input Download* uses to locate an SG's ICA
# output folder, overwritten on each resubmission. force_retry can restore an
# earlier generation to SUCCEEDED after a later retry overwrote the pointer,
# leaving it aimed at the failed generation's absent folder; this corrects that.
def _repoint_per_sg_state_to_winning_generation(
    outputs: dict[str, cpg_utils.Path],
    batches_file: BatchesFile,
) -> None:
    """Repoint each succeeded SG's per-SG state file at its winning generation.

    A file is rewritten only when its recorded `batch_index` differs from the
    generation `find_batch_for_sg` resolves as the SG's winner.

    Args:
        outputs: The stage's declared outputs, holding per-SG state file paths.
        batches_file: The reconciled cohort batches file (read from memory).
    """
    for sg_name in batches_file.successful_sg_names():
        # successful_sg_names() derives from the same batches, so a winner exists.
        winner = batches_file.find_batch_for_sg(sg_name)
        assert winner is not None, f'No batch found for succeeded SG {sg_name}'
        key = f'{sg_name}_pipeline_id_and_arguid'
        if key not in outputs:
            # The SG succeeded in a prior run but is no longer in the current
            # cohort, so no Download* stage will run for it and there is nothing
            # to correct.
            logger.warning(
                f'Succeeded SG {sg_name} has no per-SG state output declared '
                f'(likely dropped from the cohort); skipping state correction.',
            )
            continue
        state_path = outputs[key]
        try:
            current = load_per_sg_state(state_path, required_keys=('batch_index',))
        except (FileNotFoundError, ValueError, KeyError) as e:
            # A missing / old-schema / truncated file still needs repointing at
            # the winner so the download can find the CRAM: rewrite, don't skip.
            logger.warning(
                f'SG {sg_name}: per-SG state at {state_path} unreadable ({e}); '
                f'rewriting to winning generation batch {winner["batch_index"]}.',
            )
            current = None
        if current is not None and current['batch_index'] == winner['batch_index']:
            continue
        _write_per_sg_state(
            state_path,
            pipeline_id=winner['pipeline_id'],
            ar_guid=winner['ar_guid'],
            user_reference=winner['user_reference'],
            batch_index=winner['batch_index'],
        )
        logger.info(
            f'SG {sg_name}: repointed per-SG state to winning generation '
            f'(batch {winner["batch_index"]}, analysis {winner["pipeline_id"]}).',
        )


def _build_retry_batches(
    cohort_name: str,
    batches_file: BatchesFile,
    batch_size: int,
    *,
    force: bool = False,
) -> list[IcaBatch]:
    """Form retry batches from per-sample failures across batches.

    Normal mode retries only `retry_generation == 0` batches not yet
    `has_been_retried`; retry batches are created with `has_been_retried=True`
    (and `continue` for single-sample batches) so a second pass short-circuits —
    the single-retry invariant. `force=True` ignores that gate and spans ALL
    generations, excluding SGs already succeeded or with a retry still in flight
    (those are re-monitored by `run()`'s resume path).

    CANCELLED is terminal (empty passfail + CANCELLED status → neither branch
    selects it); only FAILED feeds the retry path when no passfail.json exists.

    Args:
        cohort_name: The cohort the retry batches belong to.
        batches_file: The cohort batches file to read failures from and append
            retry batches to (mutated + written when any retry is formed).
        batch_size: Max sequencing groups per retry batch.
        force: If True, run in operator `force_retry` mode (ignore the single-retry
            gate, span all generations, exclude already-succeeded and in-flight SGs).

    Returns:
        The newly-appended retry batches (empty if there is nothing to retry).
    """
    # Map each failed SG to its source batch so `retried_sgs` can be recorded per
    # source. A CANCELLED batch's passfail is empty, so the `if b['passfail']:`
    # branch never re-enables it (CANCELLED stays terminal). In force mode, exclude
    # SGs already succeeded and SGs whose retry is still in flight (an ACTIVE batch
    # with no passfail yet) — the resume path owns those, so resubmitting here would
    # run two concurrent ICA analyses for one SG.
    already_succeeded = set(batches_file.successful_sg_names()) if force else set()
    in_flight_sgs = (
        {
            sg
            for b in batches_file.batches
            if b['status'] in ACTIVE_BATCH_STATUSES and not b['passfail']
            for sg in b['sg_names']
        }
        if force
        else set()
    )
    excluded = already_succeeded | in_flight_sgs
    sg_to_source: dict[str, int] = {}
    for b in batches_file.batches:
        if not force and (b['has_been_retried'] or b['retry_generation'] != 0):
            continue
        if b['passfail']:
            for sg, status in b['passfail'].items():
                if status == CANONICAL_PASSFAIL_FAIL and sg not in excluded:
                    sg_to_source[sg] = b['batch_index']
        elif b['status'] == BATCH_STATUS_FAILED:
            # CANCELLED is terminal — only FAILED (infrastructure failure) is
            # retried at the batch level when no passfail.json was produced.
            for sg in b['sg_names']:
                if sg not in excluded:
                    sg_to_source[sg] = b['batch_index']

    if not sg_to_source:
        return []

    # Chunk the eligible SGs into new batches. We use `chunk_sgs_into_batches`
    # purely for its sort+chunk logic; the resulting batch_index values are
    # remapped by `add_retry_batch` (which appends with the correct global index).
    eligible = sorted(sg_to_source)
    pseudo_batches = chunk_sgs_into_batches(
        cohort_name=cohort_name,
        sg_names=eligible,
        batch_size=batch_size,
    )

    new_batches: list[IcaBatch] = []
    source_to_retried: dict[int, list[str]] = {}
    for pseudo in pseudo_batches:
        new_index = batches_file.add_retry_batch(sg_names=pseudo.sg_names)
        new_batches.append(
            IcaBatch(cohort_name=cohort_name, batch_index=new_index, sg_names=list(pseudo.sg_names)),
        )
        for sg in pseudo.sg_names:
            source_to_retried.setdefault(sg_to_source[sg], []).append(sg)

    for source_idx, sg_names in source_to_retried.items():
        batches_file.mark_sgs_retried(source_batch_idx=source_idx, sg_names=sg_names)
    batches_file.write()
    return new_batches


def _build_loop_outputs_for_batches(batches: list[IcaBatch]) -> dict[str, cpg_utils.Path]:
    """Build the shared loop's `outputs` dict keyed by batch name.

    Per-batch success / pipeline_id paths are internal orchestrator state built
    directly via `get_pipeline_path()`; declaring them in `expected_outputs`
    would force a heuristic batch-count bound that triggers spurious re-runs.
    """
    keys: dict[str, cpg_utils.Path] = {}
    for b in batches:
        keys[f'{b.name}_success'] = get_pipeline_path(filename=f'{b.name}_pipeline_success.json')
        keys[f'{b.name}_pipeline_id'] = get_pipeline_path(filename=f'{b.name}_pipeline_id.json')
    return keys


def _project_pipeline_id_files(
    batches: list[IcaBatch],
    batches_file: BatchesFile,
    loop_outputs: dict[str, cpg_utils.Path],
) -> None:
    """Write each monitored batch's loop-resume `{name}_pipeline_id.json` from the batches file.

    Writes the loop's per-target resume file for every batch that has a recorded
    `pipeline_id`; a batch with no `pipeline_id` (never submitted) is skipped.

    Args:
        batches: The batches about to be handed to the loop.
        batches_file: The cohort batches file (already loaded).
        loop_outputs: The loop's outputs dict, keyed `{name}_pipeline_id`.
    """
    # batches.json is authoritative; materialise the loop's resume files from it so
    # a crash between the batches.json submission write and the loop's own file
    # write cannot resubmit an already-running batch.
    for batch in batches:
        entry = batches_file.batch_entry(batch.batch_index)
        pipeline_id = entry['pipeline_id']
        if not pipeline_id:
            continue
        path = loop_outputs[f'{batch.name}_pipeline_id']
        with path.open('w') as f:
            f.write(json.dumps({'pipeline_id': pipeline_id, 'ar_guid': entry['ar_guid']}))


def _active_batches(batches_file: BatchesFile, cohort_name: str, generation: int) -> list[IcaBatch]:
    """Batches of the given retry generation whose status is still active.

    Args:
        batches_file: The cohort batches file (already loaded).
        cohort_name: The cohort the batches belong to.
        generation: The `retry_generation` to select (0 = initial, 1 = retry).

    Returns:
        The `IcaBatch`es with that generation and a PENDING/INPROGRESS status.
    """
    return [
        IcaBatch.from_entry(cohort_name, b)
        for b in batches_file.batches
        if b['retry_generation'] == generation and b['status'] in ACTIVE_BATCH_STATUSES
    ]


def _handle_management_flags(
    cohort_name: str,
    batches_file_path: cpg_utils.Path,
    outputs: dict[str, cpg_utils.Path],
    sg_names: list[str],
) -> None:
    """Apply the four management flags BEFORE constructing the BatchesFile.

    The flags are mutually exclusive (more than one raises `ValueError`). Their
    per-branch behaviour is documented at each branch below:
    - `monitor_previous` / `force_retry`: validate only (raise if no batches file).
    - `force_resubmit`: delete batches file, completion marker, and per-SG state
      (raise if no prior state exists). `force_retry`'s reconcile + rerun runs in
      `run()`, which has the loaded BatchesFile and drives the loop.
    - `cancel_cohort_run`: abort in-flight batches and raise `CohortCancelled`.

    Args:
        cohort_name: The cohort being managed (for messages + per-SG state keys).
        batches_file_path: Path to `{cohort}_batches.json`.
        outputs: The stage's declared outputs (per-SG state files + completion marker),
            used to locate/delete state on `force_resubmit`.
        sg_names: The cohort's sequencing-group names (to resolve per-SG state keys).

    Raises:
        ValueError: If more than one management flag is set.
        FileNotFoundError: If `monitor_previous` or `force_retry` is set but no
            batches file exists.
        RuntimeError: If `force_resubmit` is set but no prior state exists.
        CohortCancelled: If `cancel_cohort_run` is set (terminates the run).
    """
    force_resubmit = config_retrieve(['ica', 'management', 'force_resubmit'], default=False)
    monitor_previous = config_retrieve(['ica', 'management', 'monitor_previous'], default=False)
    cancel_cohort_run = config_retrieve(['ica', 'management', 'cancel_cohort_run'], default=False)
    force_retry = config_retrieve(['ica', 'management', 'force_retry'], default=False)

    # The four flags express orthogonal intents; enforce exactly one.
    active_flags = [
        name
        for name, value in (
            ('force_resubmit', force_resubmit),
            ('monitor_previous', monitor_previous),
            ('cancel_cohort_run', cancel_cohort_run),
            ('force_retry', force_retry),
        )
        if value
    ]
    if len(active_flags) > 1:
        raise ValueError(
            f'Cohort {cohort_name}: management flags {active_flags} are mutually exclusive — '
            f'set at most one of force_resubmit / monitor_previous / cancel_cohort_run / force_retry. '
            f'force_resubmit starts a fresh submission; monitor_previous resumes monitoring; '
            f'cancel_cohort_run aborts in-flight batches; force_retry reconciles against ICA and '
            f'reruns genuine failures.',
        )

    if monitor_previous and not batches_file_path.exists():
        raise FileNotFoundError(
            f'monitor_previous=true but {batches_file_path} does not exist — nothing to resume.',
        )

    if force_retry and not batches_file_path.exists():
        raise FileNotFoundError(
            f'force_retry=true but {batches_file_path} does not exist — there is no prior run to '
            f'reconcile against ICA. Submit the cohort normally first.',
        )

    if force_resubmit:
        per_sg_state_paths = [outputs[k] for sg in sg_names if (k := f'{sg}_pipeline_id_and_arguid') in outputs]
        if not batches_file_path.exists() and not any(p.exists() for p in per_sg_state_paths):
            # Nothing to force-resubmit. Falling through to a fresh submission would
            # burn money on an unintended ICA run, so raise for an explicit un-set.
            raise RuntimeError(
                f'force_resubmit=true for cohort {cohort_name} but no prior state '
                f'(batches.json or per-SG state files) exists. Remove force_resubmit '
                f'from the config to submit fresh.',
            )
        logger.warning(
            f'force_resubmit=true for cohort {cohort_name}: deleting batches '
            f'file, completion marker, and per-SG state — fresh slate.',
        )
        batches_file_path.unlink(missing_ok=True)
        # Delete the completion marker too, else a stale marker advertises the
        # cohort complete after force_resubmit wiped its state. Defensive on the
        # key so an older outputs schema still works.
        complete_path = outputs.get(f'{cohort_name}_pipeline_complete')
        if complete_path is not None:
            complete_path.unlink(missing_ok=True)
        for p in per_sg_state_paths:
            p.unlink(missing_ok=True)
        return

    if cancel_cohort_run and not batches_file_path.exists():
        logger.warning(
            f'cancel_cohort_run=true for cohort {cohort_name} but no batches file exists — '
            f'nothing to cancel. Exiting cleanly.',
        )
        raise CohortCancelled(
            f'Cohort {cohort_name} cancelled by user request (cancel_cohort_run=true; no in-flight state to abort).',
        )

    if cancel_cohort_run and batches_file_path.exists():
        logger.warning(f'cancel_cohort_run=true for cohort {cohort_name}: aborting in-flight batches.')
        existing = BatchesFile(path=batches_file_path)
        existing.read()
        n_aborted = 0
        for b in existing.batches:
            if b['status'] not in ACTIVE_BATCH_STATUSES:
                # Already-SUCCEEDED batches are left alone.
                continue
            pipeline_id = b.get('pipeline_id')
            if pipeline_id:
                try:
                    # is_mlr=False: cohort-level cancel applies only to DRAGEN batches.
                    cancel_ica_pipeline_run.run(ica_pipeline_id=pipeline_id, is_mlr=False)
                    logger.info(f'Aborted ICA analysis {pipeline_id} for batch {b["batch_index"]}')
                except Exception as e:  # noqa: BLE001
                    logger.error(
                        f'Failed to abort ICA analysis {pipeline_id} for batch '
                        f'{b["batch_index"]}: {e}. Marking CANCELLED in file anyway.',
                    )
            else:
                logger.info(
                    f'Batch {b["batch_index"]} status={b["status"]} has no pipeline_id '
                    f'(never reached ICA); marking CANCELLED only.',
                )
            # Via record_status so the value is validated against ALLOWED_BATCH_STATUSES.
            existing.record_status(b['batch_index'], BATCH_STATUS_CANCELLED)
            n_aborted += 1
        existing.write()
        # Do NOT delete per-SG state files here: preserving them keeps AR GUIDs for
        # a later force_resubmit harvest and an audit trail. They remain valid
        # pointers to aborted ICA analyses (a clear "analysis not found" downstream).
        raise CohortCancelled(
            f'Cohort {cohort_name} cancelled by user request (cancel_cohort_run=true). '
            f'{n_aborted} in-flight batches aborted; SUCCEEDED batches preserved. '
            f'Rerun with force_resubmit=true to start a fresh submission.',
        )

    return


def run(
    cohort: Cohort,
    outputs: dict[str, cpg_utils.Path],
    cram_state_paths: dict[str, cpg_utils.Path] | None,
    fastq_ids_path: cpg_utils.Path | None,
    per_sg_fastq_list_paths: dict[str, cpg_utils.Path] | None,
    analysis_output_fid_path: cpg_utils.Path,
) -> None:
    """Build batches, submit them, retry per-sample failures once, and raise if any SG still failed.

    Args:
        cohort: The cohort to process; its sequencing groups are chunked into batches.
        outputs: The stage's declared outputs (per-SG state files, batches file,
            completion marker) keyed as `ManageDragenPipeline.expected_outputs` builds them.
        cram_state_paths: CRAM-mode per-SG state paths, or `None` in FASTQ mode.
        fastq_ids_path: FASTQ-mode combined ICA file-ID list path, or `None` in CRAM mode.
        per_sg_fastq_list_paths: FASTQ-mode per-SG fastq-list paths, or `None` in CRAM mode.
        analysis_output_fid_path: Path holding the ICA output parent-folder id.

    Raises:
        ValueError: If the cohort has no sequencing groups.
        KeyError: If `expected_outputs` is missing a required per-SG state entry.
        CohortCancelled: If `cancel_cohort_run=true`, or a prior run left CANCELLED batches.
        RuntimeError: If any SG is still failed after the retry pass, or the loop's
            reported FAILED_FINAL set diverges from the persisted batches file.
    """
    batch_size: int = config_retrieve(
        ['dragen_align_pa', 'manage_dragen_pipeline', 'batch_size'],
        default=DEFAULT_BATCH_SIZE,
    )
    # Operator recovery flag: reconcile persisted state against ICA, then rerun only
    # genuine failures. Validated in _handle_management_flags.
    force_retry: bool = config_retrieve(['ica', 'management', 'force_retry'], default=False)
    sg_names = [sg.name for sg in cohort.get_sequencing_groups()]
    if not sg_names:
        raise ValueError(f'Cohort {cohort.name} has no sequencing groups.')

    # Pre-validate per-SG state output keys BEFORE any ICA submission, so a missing
    # expected_outputs entry surfaces as one startup error, not a leaked ICA analysis.
    missing_state_keys = [
        f'{sg}_pipeline_id_and_arguid' for sg in sg_names if f'{sg}_pipeline_id_and_arguid' not in outputs
    ]
    if missing_state_keys:
        raise KeyError(
            f'Cohort {cohort.name}: expected_outputs missing per-SG state file entries '
            f'{missing_state_keys}. Downstream per-SG download stages need these to '
            f'resolve ICA folder paths. Check ManageDragenPipeline.expected_outputs.',
        )

    batches_file_path: cpg_utils.Path = outputs[
        f'{cohort.name}_{config_retrieve(["workflow", "sequencing_type"])}_'
        f'{config_retrieve(["workflow", "reads_type"])}_batches'
    ]
    # Error-log sink for the monitor loop + the failed-SG summary. Internal scratch
    # (via get_pipeline_path) so its variable existence doesn't trigger cpg-flow re-runs.
    errors_path = get_pipeline_path(filename=f'{cohort.name}_errors.log')

    # Raises CohortCancelled on cancel_cohort_run (propagated so cpg-flow fails the
    # stage); deletes prior state on force_resubmit for a clean-slate run.
    _handle_management_flags(
        cohort_name=cohort.name,
        batches_file_path=batches_file_path,
        outputs=outputs,
        sg_names=sg_names,
    )

    batches_file = BatchesFile(path=batches_file_path)
    if batches_file_path.exists():
        logger.info(f'Resuming from existing batches file {batches_file_path}')
        batches_file.read()
        if force_retry:
            # CANCELLED is terminal: force_retry must not resurrect a cancelled cohort
            # (its ABORTED analyses would reconcile to FAILED and resubmit). Refuse up
            # front; force_resubmit is the sanctioned recovery.
            cancelled_sgs = batches_file.cancelled_sg_names()
            if cancelled_sgs:
                raise CohortCancelled(
                    f'Cohort {cohort.name} has {len(cancelled_sgs)} SG(s) in CANCELLED batches; '
                    f'force_retry cannot recover a cancelled cohort. Rerun with force_resubmit=true '
                    f'to start a fresh submission.',
                )
            # Rewrite each batch's status to ICA reality FIRST so the filters below
            # act on the truth (stale FAILED→SUCCEEDED harvested; running→INPROGRESS
            # re-monitored). This is the whole point of force_retry.
            logger.warning(
                f'force_retry=true for cohort {cohort.name}: reconciling persisted state against ICA.',
            )
            _reconcile_batches_with_ica(cohort.name, batches_file)
        # Resume the first pass from initial (generation 0) active batches; retry-batch
        # resumption is handled below.
        initial_batches = _active_batches(batches_file, cohort.name, generation=0)
    else:
        # Fresh cohort or post-force_resubmit re-batching: always re-batch from the
        # current SG list (membership may have changed).
        # Defensive: if force_resubmit's unlink silently failed, the old file would
        # still be here and initialise() would overwrite it outside the resume path —
        # data loss. Hard-assert so that surfaces here instead of corrupting state.
        assert not batches_file_path.exists(), (
            f'{batches_file_path} should have been deleted by '
            f'_handle_management_flags but still exists. Refusing to '
            f'overwrite — investigate the deletion failure.'
        )
        initial_batches = chunk_sgs_into_batches(
            cohort_name=cohort.name,
            sg_names=sg_names,
            batch_size=batch_size,
        )
        batches_file.initialise(batch_size=batch_size, batches=initial_batches)
        batches_file.write()

    # Every FAILED_FINAL the loop reports lands here (even if its persist failed);
    # reconciled against the persisted file before the completion marker.
    loop_failed_final: set[int] = set()

    batches_by_name = {b.name: b for b in initial_batches}
    on_succeeded = _on_succeeded_factory(batches_file, batches_by_name)
    on_status_change = _on_status_change_factory(batches_file, batches_by_name, loop_failed_final)

    def submit_factory(batch_name: str) -> Callable[[], str]:
        return _build_submit_callable(
            batch=batches_by_name[batch_name],
            analysis_output_fid_path=analysis_output_fid_path,
            cram_state_paths=cram_state_paths,
            fastq_ids_path=fastq_ids_path,
            per_sg_fastq_list_paths=per_sg_fastq_list_paths,
            batches_file=batches_file,
            outputs=outputs,
        )

    if initial_batches:
        loop_outputs = _build_loop_outputs_for_batches(initial_batches)
        # Materialise loop-resume files from the authoritative batches file so a crash
        # between writes can't resubmit an already-running batch (duplicate analysis).
        _project_pipeline_id_files(initial_batches, batches_file, loop_outputs)
        # allow_retry=False: per-sample retry is owned by run(), not the loop.
        # raise_on_failed_final=False: FAILED_FINAL must survive to _build_retry_batches;
        # run()'s post-retry check owns the abort.
        manage_ica_pipeline_loop(
            targets_to_process=initial_batches,
            outputs=loop_outputs | {f'{cohort.name}_errors': errors_path},
            pipeline_name='Dragen',
            is_mlr_pipeline=False,
            success_file_key_template='{target_name}_success',
            pipeline_id_file_key_template='{target_name}_pipeline_id',
            error_log_key=f'{cohort.name}_errors',
            submit_function_factory=submit_factory,
            allow_retry=False,
            sleep_time_seconds=600,
            on_succeeded=on_succeeded,
            on_status_change=on_status_change,
            raise_on_failed_final=False,
        )

    retry_batches = _build_retry_batches(
        cohort_name=cohort.name,
        batches_file=batches_file,
        batch_size=batch_size,
        force=force_retry,
    )
    # Resume scenario: a previous run already created retry batches but crashed
    # before they completed. Pick them back up here.
    existing_retry_in_flight = _active_batches(batches_file, cohort.name, generation=1)
    # `_build_retry_batches` may have appended the same batches we just resumed. Dedupe.
    seen_names = {b.name for b in retry_batches}
    retry_batches = retry_batches + [b for b in existing_retry_in_flight if b.name not in seen_names]

    if retry_batches:
        logger.info(f'Retry batches to monitor: {[b.name for b in retry_batches]}')
        retry_batches_by_name = {b.name: b for b in retry_batches}
        retry_on_succeeded = _on_succeeded_factory(batches_file, retry_batches_by_name)
        retry_on_status_change = _on_status_change_factory(batches_file, retry_batches_by_name, loop_failed_final)

        def retry_submit_factory(batch_name: str) -> Callable[[], str]:
            return _build_submit_callable(
                batch=retry_batches_by_name[batch_name],
                analysis_output_fid_path=analysis_output_fid_path,
                cram_state_paths=cram_state_paths,
                fastq_ids_path=fastq_ids_path,
                per_sg_fastq_list_paths=per_sg_fastq_list_paths,
                batches_file=batches_file,
                outputs=outputs,
            )

        retry_loop_outputs = _build_loop_outputs_for_batches(retry_batches)
        # Same resume-safety projection for in-flight retry batches (see the
        # initial-pass call above).
        _project_pipeline_id_files(retry_batches, batches_file, retry_loop_outputs)
        manage_ica_pipeline_loop(
            targets_to_process=retry_batches,
            outputs=retry_loop_outputs | {f'{cohort.name}_errors': errors_path},
            pipeline_name='Dragen',
            is_mlr_pipeline=False,
            success_file_key_template='{target_name}_success',
            pipeline_id_file_key_template='{target_name}_pipeline_id',
            error_log_key=f'{cohort.name}_errors',
            submit_function_factory=retry_submit_factory,
            allow_retry=False,
            sleep_time_seconds=600,
            on_succeeded=retry_on_succeeded,
            on_status_change=retry_on_status_change,
            # Same rationale as the initial pass: `run()`'s post-retry
            # check below owns the abort decision, so the loop must not raise here.
            raise_on_failed_final=False,
        )

    # Reconcile the loop's FAILED_FINAL set against the PERSISTED file: on_status_change
    # persists best-effort (write errors swallowed), so a dropped write could leave a
    # batch INPROGRESS on disk. This file is the source of truth for failed_sg_names()
    # below, so a stale INPROGRESS would silently drop the failure. Re-read from disk.
    if loop_failed_final:
        persisted = BatchesFile(path=batches_file_path)
        persisted.read()
        # Resolve by `batch_index`, not list position — `read()` does not enforce
        # that the list is ordered by batch_index (see `BatchesFile.batch_entry`).
        unrecorded = sorted(
            idx for idx in loop_failed_final if persisted.batch_entry(idx)['status'] != BATCH_STATUS_FAILED
        )
        if unrecorded:
            raise RuntimeError(
                f'Cohort {cohort.name}: the monitor loop reported batch(es) {unrecorded} as '
                f'FAILED_FINAL, but {batches_file_path} does not record them as '
                f'{BATCH_STATUS_FAILED!r} (a status write was likely dropped). Refusing to '
                f'declare the cohort complete — re-run to reconcile the batches file.',
            )

    # Resume-after-cancel guard: any CANCELLED batch blocks this rerun. CANCELLED is
    # terminal; without this guard the loop would reach the completion marker and
    # downstream stages would explode on per-SG state pointing at aborted analyses.
    # force_resubmit is the only sanctioned recovery (it harvests partial successes).
    cancelled_sgs = batches_file.cancelled_sg_names()
    if cancelled_sgs:
        raise CohortCancelled(
            f'Cohort {cohort.name} has {len(cancelled_sgs)} SG(s) in CANCELLED '
            f'batches from a previous run. CANCELLED is terminal; rerun with '
            f'force_resubmit=true to start a fresh submission (AR GUIDs from '
            f'the preserved per-SG state will be reused).',
        )

    # force_retry can reconcile an earlier generation back to SUCCEEDED after a later
    # retry failed and overwrote the SG's state file; repoint each succeeded SG at its
    # winning generation before completing (Download* resolves the folder from it).
    if force_retry:
        _repoint_per_sg_state_to_winning_generation(outputs, batches_file)

    # failed_sg_names() excludes CANCELLED by design. Any SG still failed after the
    # retry pass aborts the run — no rate tolerance, a single failure halts.
    n_total = len(sg_names)
    failed = batches_file.failed_sg_names()
    n_failed = len(failed)
    if n_failed > 0:
        # Append (don't truncate) the failed-SG summary so it sits alongside the
        # loop's ICA-level detail already flushed here. A timestamped header keeps
        # each stage invocation an attributable block.
        run_marker = datetime.now(tz=UTC).isoformat()
        with errors_path.open('a') as fh:
            fh.write(
                f'\n===== {cohort.name} DRAGEN failure summary @ {run_marker} =====\n'
                f'{n_failed}/{n_total} SG(s) failed the DRAGEN pipeline after the retry pass.\n'
                f'Failed SGs: {", ".join(failed)}\n',
            )
        raise RuntimeError(
            f'{n_failed}/{n_total} SG(s) failed the DRAGEN pipeline after retry. '
            f'See {errors_path} for the failure list.',
        )

    # Completion marker: the "stage completed" signal cpg-flow checks. It's the LAST
    # write of run(), so any earlier raise leaves the stage looking failed (retryable).
    # Payload records the outcome summary for auditing without re-reading batches.json.
    complete_path = outputs[f'{cohort.name}_pipeline_complete']
    with complete_path.open('w') as fh:
        json.dump(
            {
                'cohort_name': cohort.name,
                'n_sgs_total': n_total,
                'n_batches': len(batches_file.batches),
            },
            fh,
        )
