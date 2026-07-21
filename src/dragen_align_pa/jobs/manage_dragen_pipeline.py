"""Orchestrate the unified DRAGEN pipeline across a cohort.

Responsibilities:
- Chunk the cohort into deterministic batches (IcaBatch dataclass).
- Persist `{cohort}_batches.json` plus per-SG state files (extended schema).
- Submit batches via `submit_dragen_batch.run`, monitored by the shared
  `manage_ica_pipeline_loop` (now generic over IcaBatch targets).
- After the first pass completes, read passfail across all batches; if any SGs
  are marked Fail and have not been retried, form retry batches and run the
  loop a second time. Single retry only.
- Apply the 5% threshold over sequencing groups (not over batches).
"""

import json
from collections.abc import Callable

import cpg_utils.config
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve
from loguru import logger

from dragen_align_pa import ica_api_utils
from dragen_align_pa.batches import (
    ACTIVE_BATCH_STATUSES,
    BATCH_STATUS_CANCELLED,
    BATCH_STATUS_FAILED,
    BATCH_STATUS_SUCCEEDED,
    CANONICAL_PASSFAIL_FAIL,
    BatchesFile,
    IcaBatch,
    chunk_sgs_into_batches,
)
from dragen_align_pa.constants_registry import ROLE_DRAGEN_ALIGN
from dragen_align_pa.jobs import cancel_ica_pipeline_run, submit_dragen_batch
from dragen_align_pa.jobs.ica_pipeline_manager import (
    MonitoredTarget,
    PipelineStatus,
    manage_ica_pipeline_loop,
)
from dragen_align_pa.jobs.parse_passfail import fetch_passfail_from_ica
from dragen_align_pa.ica_utils import ica_cohort_path, ica_run_path
from dragen_align_pa.utils import get_pipeline_path

# Single source of truth for the default batch chunking width. Used both by
# this orchestrator (to chunk SGs at submit time) and by `ManageDragenPipeline.
# expected_outputs` (to declare enough per-batch output keys for the loop to
# resolve). Both readers MUST use this constant — divergence between the two
# defaults would cause `_build_loop_outputs_for_batches` to raise with a stale
# "force_resubmit" message even when no user override is in play.
DEFAULT_BATCH_SIZE = 5


class CohortCancelled(RuntimeError):  # noqa: N818
    """Raised when `cancel_cohort_run=true` has terminated the cohort.

    Distinct exception type so cpg-flow / operators can distinguish a
    user-initiated cancellation from a true pipeline failure (e.g. the
    threshold-breach `RuntimeError`). Both halt the stage and skip downstream
    work via cpg-flow's required-stage propagation; only the message differs.

    Named for the *event*, not the failure category — `…Cancelled` reads more
    naturally at the catch site (`except CohortCancelled`) and matches the
    plan's vocabulary. N818's `…Error` suffix is intentionally skipped.
    """


def _build_submit_callable(
    batch: IcaBatch,
    analysis_output_fid_path: cpg_utils.Path,
    cram_state_paths: dict[str, cpg_utils.Path] | None,
    fastq_ids_path: cpg_utils.Path | None,
    per_sg_fastq_list_paths: dict[str, cpg_utils.Path] | None,
    batches_file: BatchesFile,
    outputs: dict[str, cpg_utils.Path],
) -> Callable[[], str]:
    """Wraps `submit_dragen_batch.run` so it returns just the pipeline_id (the
    shared loop expects a `Callable[[], str]`), and persists ar_guid /
    user_reference / fastq_list_fid into the cohort batches file plus per-SG
    state files at submission time (so MLR and downstream download stages see
    a consistent state even if the orchestrator job is later killed mid-flight).
    """

    def _submit() -> str:
        # Use the batch entry's recorded error_strategy (set on creation by either
        # `BatchesFile.initialise` or `BatchesFile.add_retry_batch`).
        entry = batches_file.batches[batch.batch_index]
        error_strategy = entry.get('error_strategy', 'auto')
        result = submit_dragen_batch.run(
            batch=batch,
            analysis_output_fid_path=analysis_output_fid_path,
            cram_state_paths=cram_state_paths,
            fastq_ids_path=fastq_ids_path,
            per_sg_fastq_list_paths=per_sg_fastq_list_paths,
            error_strategy=error_strategy,
        )
        # Narrow the return-dict values to satisfy type-checkers — submit_dragen_batch.run
        # returns dict[str, str | list[str]] but the str-keyed fields are always strings.
        pipeline_id_v = result['pipeline_id']
        ar_guid_v = result['ar_guid']
        user_reference_v = result['user_reference']
        assert isinstance(pipeline_id_v, str) and isinstance(ar_guid_v, str) and isinstance(user_reference_v, str)

        # Persistence ordering (see BatchesFile docstring "Note on atomic writes"):
        # per-SG state files FIRST (best-effort projections), batches.json SECOND
        # (the commit point). If we crash between, batches.json still shows the
        # batch as PENDING; the next orchestrator pass re-submits, generating a
        # new pipeline_id that overwrites per-SG state. The orphan ICA submission
        # is leaked but the system reconverges. We accept this in exchange for
        # never serving a downstream stage a per-SG state file that references a
        # batch the batches.json doesn't acknowledge.
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
        batches_file.record_error_strategy(batch.batch_index, error_strategy)
        batches_file.write()
        return pipeline_id_v

    return _submit


def _persist_per_sg_state_for_batch(
    outputs: dict[str, cpg_utils.Path],
    batch: IcaBatch,
    submission_result: dict[str, str],
) -> None:
    """Write per-SG state files for one batch immediately on submission.

    Schema must match the version validated by `get_ica_sample_folder`.
    Bumping the version requires a coordinated change on both sides — the
    `monitor_previous=true` resume path will refuse to read state files written
    under a different version.

    Note on overwrite-on-retry: when an SG is included in a retry batch, this
    function rewrites its per-SG state with the retry batch's identifiers
    (pipeline_id, user_reference, batch_index). cpg-flow's `required_stages`
    ordering guarantees that `ManageDragenPipeline`'s retry pass completes
    before any downstream stage (MLR, downloads) reads the state file —
    so MLR/downloads always see the *latest* generation's identifiers.
    """
    for sg_name in batch.sg_names:
        key = f'{sg_name}_pipeline_id_and_arguid'
        if key not in outputs:
            # `run()` pre-validates per-SG state output keys before any
            # submission, so reaching this branch means the cohort SG list
            # changed mid-run (or the pre-check was bypassed). Raise loudly
            # rather than orphan the submission with no per-SG state file —
            # downstream Download* stages would otherwise fail later with a
            # less actionable error.
            raise KeyError(
                f'Per-SG state output {key!r} not declared in outputs for batch '
                f"{batch.name}. run()'s pre-check should have caught this — "
                f'cohort SG list likely changed mid-run, or ManageDragenPipeline.'
                f'expected_outputs is undercounting.',
            )
        with outputs[key].open('w') as fh:
            json.dump(
                {
                    'schema_version': 1,
                    'pipeline_id': submission_result['pipeline_id'],
                    'ar_guid': submission_result['ar_guid'],
                    'user_reference': submission_result['user_reference'],
                    'batch_index': batch.batch_index,
                },
                fh,
            )


# Spec §6 line 312: strict `>` — the threshold is breached only when more
# than 5% of SGs failed. Extracted to a tiny pure function so the production
# code in `run()` and the boundary tests in `test_manage_dragen_pipeline.py`
# share the comparison, preventing drift if the threshold ever changes.
THRESHOLD_FAILURE_FRACTION = 0.05


def _threshold_breached(n_failed: int, n_total: int) -> bool:
    if n_total == 0:
        return False
    return n_failed / n_total > THRESHOLD_FAILURE_FRACTION


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

        batch_entry = batches_file.batches[batch.batch_index]
        # ICA names the analysis folder `{user_reference}-{pipeline_id}` (see
        # `get_ica_sample_folder` in ica_utils.py — same separator). The hyphen is
        # required: user_reference ends in `_`, so the resulting folder name
        # is `…_-{pipeline_id}/`.
        analysis_folder_name = f'{batch_entry["user_reference"]}-{batch_entry["pipeline_id"]}'
        # ICA writes each batch's analysis folder INSIDE the cohort-level parent folder
        # (`PrepareIcaForDragenAnalysis` creates one folder named `cohort.name` and passes
        # its fid to ICA as `outputParentFolderId`). Both levels come from the shared
        # builders so they stay in lockstep with `ica_utils.get_ica_sample_folder`.
        ica_parent = ica_cohort_path(batch.cohort_name).as_folder()
        ica_folder = ica_run_path(
            batch.cohort_name, batch_entry['user_reference'], batch_entry['pipeline_id']
        ).as_folder()

        passfail = None
        folder_fid: str | None = None
        try:
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
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        f'Batch {batch.name} (analysis {batch_entry["pipeline_id"]}, '
                        f'folder {analysis_folder_name}): could not resolve analysis output folder ID: {e}',
                    )
        except Exception as e:  # noqa: BLE001
            # RAISE (don't `return`): the shared loop's transactional callback
            # contract catches this, logs it, and leaves the per-target status
            # at INPROGRESS. batches.json also stays INPROGRESS (we never
            # called record_status). Next poll cycle re-fires on_succeeded.
            # If we silently `return`ed here, the loop would think the
            # callback succeeded and set SUCCEEDED — diverging from
            # batches.json which would still show INPROGRESS.
            raise RuntimeError(
                f'Batch {batch.name}: ICA fetch failed in on_succeeded ({e}); '
                f'leaving status INPROGRESS so the next poll can re-fetch.',
            ) from e

        # `passfail is None` here means the file is legitimately absent (a
        # catastrophically-failed batch that never produced one). Distinct from
        # a transient ICA error — those raise above and re-fire next poll.
        # Treating an absent passfail.json as "all SGs Fail" is the spec'd
        # fallback; the retry pass then resubmits those SGs.
        if passfail is None:
            logger.warning(
                f'Batch {batch.name} (analysis {batch_entry["pipeline_id"]}, '
                f'folder {analysis_folder_name}): passfail.json not found at ICA root; '
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
                    f'Batch {batch.name} (analysis {batch_entry["pipeline_id"]}, '
                    f'folder {analysis_folder_name}): passfail.json contains unexpected '
                    f'sample IDs {unexpected}; dropping them. This usually means '
                    f'RGSM != sg_name (CRAM mode: original SM tag differs from the cpg-flow SG ID).',
                )
            if missing:
                logger.warning(
                    f'Batch {batch.name} (analysis {batch_entry["pipeline_id"]}, '
                    f'folder {analysis_folder_name}): passfail.json missing entries for SGs {missing}; '
                    f'marking them as Fail so they enter the retry path.',
                )
            filtered = {sg: passfail[sg] for sg in batch.sg_names if sg in passfail}
            for sg in missing:
                filtered[sg] = CANONICAL_PASSFAIL_FAIL
            batches_file.record_passfail(batch.batch_index, filtered)
        if folder_fid is not None:
            batches_file.record_analysis_output_folder_fid(batch.batch_index, folder_fid)
        batches_file.record_status(batch.batch_index, BATCH_STATUS_SUCCEEDED)
        batches_file.write()

    return _on_succeeded


def _on_status_change_factory(
    batches_file: BatchesFile,
    batches_by_name: dict[str, IcaBatch],
) -> Callable[[MonitoredTarget, PipelineStatus], None]:
    """Mirror the loop's terminal non-success transitions into `{cohort}_batches.json`.

    Without this, the loop's in-memory `FAILED_FINAL` / `CANCELLED` transitions
    never propagate to the batches file, leaving entries stuck at INPROGRESS
    forever. The downstream consequences are:
    - `_build_retry_batches`'s `elif b['status'] == BATCH_STATUS_FAILED:` branch becomes
      unreachable (whole-batch infrastructure failure can't trigger a retry).
    - A subsequent resume's `initial_batches` filter (`status in {PENDING,
      INPROGRESS}`) re-picks-up the dead batch and the loop polls a long-
      aborted ICA analysis.

    `SUCCEEDED` is intentionally NOT routed through this callback — the
    transactional `_on_succeeded_factory` already records SUCCEEDED via
    `batches_file.record_status(idx, BATCH_STATUS_SUCCEEDED)`.
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
            batches_file.record_status(batch.batch_index, BATCH_STATUS_FAILED)
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


def _build_retry_batches(
    cohort_name: str,
    batches_file: BatchesFile,
    batch_size: int,
) -> list[IcaBatch]:
    """Form retry batches from per-sample failures across batches.

    Only retries batches with `retry_generation == 0` (the initial cohort batches).
    Uses `BatchesFile.add_retry_batch` to append retry entries and
    `BatchesFile.mark_sgs_retried` to record the per-SG audit trail on the
    source batches. Retry batches are created with `has_been_retried=True` and
    `error_strategy` defaulting to `continue` for single-sample batches, so a
    hypothetical second retry pass short-circuits — enforcing the single-retry
    invariant.

    `CANCELLED` is treated as a **terminal** state — `cancel_cohort_run=true` is
    a user-initiated abort and the spec (§4 line 214) does not allow it to spawn
    retries. Only `FAILED` (ICA-level infrastructure failure) feeds the retry
    path when no `passfail.json` was produced.

    Resume uses `retry_generation` + `status` (NOT `has_been_retried`) so in-flight
    retry batches that crashed mid-submission can still be re-monitored.
    """
    # Map each failed SG to the source batch it came from, so we can record
    # `retried_sgs` per source batch (not just batch-level `has_been_retried`).
    # Precedence note: `passfail` populated implies a SUCCEEDED batch (the only
    # writer is `_on_succeeded`). A CANCELLED batch's `passfail` is empty by
    # construction, so the `if b['passfail']:` branch can never re-enable a
    # cancelled batch for retry — preserving CANCELLED's terminal status.
    sg_to_source: dict[str, int] = {}
    for b in batches_file.batches:
        if b['has_been_retried'] or b['retry_generation'] != 0:
            continue
        if b['passfail']:
            for sg, status in b['passfail'].items():
                if status == CANONICAL_PASSFAIL_FAIL:
                    sg_to_source[sg] = b['batch_index']
        elif b['status'] == BATCH_STATUS_FAILED:
            # CANCELLED is terminal — only FAILED (infrastructure failure) is
            # retried at the batch level when no passfail.json was produced.
            for sg in b['sg_names']:
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

    Per-batch success / pipeline_id paths are internal orchestrator state
    — `cpg-flow` does not consume them, so the orchestrator constructs
    them directly via `get_pipeline_path()` rather than reading them out
    of `ManageDragenPipeline.expected_outputs`. Declaring per-batch files
    in `expected_outputs` would force a heuristic upper bound on the
    batch count (`max_batches`), and if fewer retry batches materialise
    than the heuristic predicts, `cpg-flow` would treat the stage as
    incomplete and re-run unnecessarily.
    """
    keys: dict[str, cpg_utils.Path] = {}
    for b in batches:
        keys[f'{b.name}_success'] = get_pipeline_path(filename=f'{b.name}_pipeline_success.json')
        keys[f'{b.name}_pipeline_id'] = get_pipeline_path(filename=f'{b.name}_pipeline_id.json')
    return keys


def _handle_management_flags(
    cohort_name: str,
    batches_file_path: cpg_utils.Path,
    outputs: dict[str, cpg_utils.Path],
    sg_names: list[str],
) -> None:
    """Apply `force_resubmit` / `monitor_previous` / `cancel_cohort_run` BEFORE
    constructing the BatchesFile.

    Raises `CohortCancelled` (terminal) if `cancel_cohort_run=true` —
    short-circuits `run()` so it doesn't fall into retry-building or
    threshold-checking.

    Semantics:
    - `monitor_previous=true`: raises if the batches file is missing.
    - `force_resubmit=true`: clean slate. Deletes `{cohort}_batches.json`,
      the completion marker, and every per-SG state file. The caller
      re-batches the cohort from scratch — fresh AR GUIDs are minted on
      submission, no positional reuse. Raises if no prior state exists
      (force_resubmit on a fresh cohort is a config mistake, not a no-op).
    - `cancel_cohort_run=true`: for each batch with status PENDING/INPROGRESS,
      calls the ICA abort API if a `pipeline_id` is known, then marks the
      batch CANCELLED in the file. **Per-SG state files are NOT deleted** —
      the versioned state file is the single source of per-SG truth, and
      preserving it lets a `force_resubmit=true` recovery rerun see clearly
      what was running when the cancel fired (the files become stale
      pointers to aborted ICA analyses, which the resubmit path deletes
      wholesale). The function raises `CohortCancelled` to terminate cleanly.

    Conflicts: `force_resubmit` + any other management flag raises
    `ValueError`. The three flags express orthogonal user intents and the
    safer behaviour is to make the user pick one.
    """
    force_resubmit = config_retrieve(['ica', 'management', 'force_resubmit'], default=False)
    monitor_previous = config_retrieve(['ica', 'management', 'monitor_previous'], default=False)
    cancel_cohort_run = config_retrieve(['ica', 'management', 'cancel_cohort_run'], default=False)

    if force_resubmit and cancel_cohort_run:
        raise ValueError(
            'force_resubmit and cancel_cohort_run are mutually exclusive. '
            'To cancel then resubmit: run with cancel_cohort_run=true alone, '
            'wait for ICA aborts to settle, then rerun with force_resubmit=true.',
        )
    if force_resubmit and monitor_previous:
        raise ValueError(
            f'Cohort {cohort_name}: force_resubmit and monitor_previous are mutually '
            f'exclusive. Pick one: force_resubmit deletes existing state and starts '
            f'a fresh submission; monitor_previous resumes monitoring an in-flight '
            f'run. Unset whichever flag does not match your intent and retry.',
        )

    if monitor_previous and not batches_file_path.exists():
        raise FileNotFoundError(
            f'monitor_previous=true but {batches_file_path} does not exist — nothing to resume.',
        )

    if force_resubmit:
        per_sg_state_paths = [outputs[k] for sg in sg_names if (k := f'{sg}_pipeline_id_and_arguid') in outputs]
        if not batches_file_path.exists() and not any(p.exists() for p in per_sg_state_paths):
            # No batches file + no per-SG state means there's nothing to
            # force-resubmit. Silently falling through to a fresh submission
            # would burn money on an ICA run the user probably didn't intend
            # (force_resubmit is the destructive flag — they expected
            # existing state to be replaced). Raise so the user un-sets the
            # flag explicitly.
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
        # Delete the completion marker too — otherwise a marker left over from a
        # previous successful run would advertise this cohort as "complete" even
        # though force_resubmit just wiped the underlying state. Defensive on
        # the key presence so callers with an older outputs schema still work.
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
                    # Reuse the existing per-target cancel helper. is_mlr=False because
                    # cohort-level cancellation only applies to DRAGEN batches; MLR
                    # cancellation goes through its own per-SG path.
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
            # Go through record_status so the value is validated against
            # ALLOWED_BATCH_STATUSES — direct dict mutation works today but
            # would silently accept a future typo or enum drift.
            existing.record_status(b['batch_index'], BATCH_STATUS_CANCELLED)
            n_aborted += 1
        existing.write()
        # Per-SG state-file policy: do NOT delete the versioned per-SG state
        # files here. The user has a choice — they can later run with
        # `force_resubmit=true` (which deletes everything and re-batches, lifting
        # AR GUIDs out of the preserved state files first) or just accept the
        # cancellation. Keeping the per-SG files preserves AR GUIDs for that
        # eventual harvest and provides an audit trail of what was running when
        # cancel fired. They stay valid pointers — to aborted ICA analyses —
        # so any downstream stage that later reads them will fail with a clear
        # ICA "analysis not found" error rather than a confusing FileNotFoundError.
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
    """Build batches, submit them, retry per-sample failures once, enforce 5% threshold."""
    batch_size: int = config_retrieve(
        ['dragen_align_pa', 'manage_dragen_pipeline', 'batch_size'],
        default=DEFAULT_BATCH_SIZE,
    )
    sg_names = [sg.name for sg in cohort.get_sequencing_groups()]
    if not sg_names:
        raise ValueError(f'Cohort {cohort.name} has no sequencing groups.')

    # Pre-validate per-SG state output keys BEFORE any ICA submission, so a
    # missing `expected_outputs` entry doesn't surface as an orphaned ICA
    # analysis with no on-disk state file. Catching this at startup means
    # the operator sees a single actionable error and zero leaked ICA work.
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
    # `errors_path` is internal scratch — written only on threshold breach.
    # Computed via `get_pipeline_path` rather than declared in expected_outputs
    # because variable-existence outputs trigger spurious cpg-flow re-runs.
    errors_path = get_pipeline_path(filename=f'{cohort.name}_errors.log')

    # `_handle_management_flags` raises CohortCancelled on `cancel_cohort_run=true`
    # — we let it propagate so cpg-flow marks the stage failed and downstream
    # stages skip. On `force_resubmit=true` it deletes the previous state files
    # (batches.json, completion marker, per-SG state) so this run starts from a
    # clean slate; no AR GUIDs are preserved, fresh ones are minted at submit.
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
        # Resume uses `retry_generation == 0` (initial batches only) + status to decide
        # what to re-monitor on the first pass. Retry-batch resumption is handled in the
        # second loop call below — see retry section.
        initial_batches = [
            IcaBatch(cohort_name=cohort.name, batch_index=b['batch_index'], sg_names=b['sg_names'])
            for b in batches_file.batches
            if b['retry_generation'] == 0 and b['status'] in ACTIVE_BATCH_STATUSES
        ]
    else:
        # Fresh cohort, or post-`force_resubmit` re-batching. Cohort membership may
        # have changed (SGs added/removed) since the original submission, so we
        # always re-batch from the *current* cohort SG list rather than reusing
        # the prior `sg_names` partitions.
        #
        # Defensive: `_handle_management_flags` on the force_resubmit path
        # calls `batches_file_path.unlink(missing_ok=True)`. If that ever
        # silently fails (e.g. a hypothetical GCS rate-limit, manual hold),
        # we'd hit this branch with the old file still present and
        # `chunk_sgs_into_batches` + `batches_file.initialise` would
        # overwrite it WITHOUT the resume-from-existing path being taken
        # — a real risk of data loss. Hard-assert so any such failure
        # surfaces here instead of corrupting state.
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

    batches_by_name = {b.name: b for b in initial_batches}
    on_succeeded = _on_succeeded_factory(batches_file, batches_by_name)
    on_status_change = _on_status_change_factory(batches_file, batches_by_name)

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
        # allow_retry=False: the shared loop's whole-target retry plus its 5%-of-targets
        # threshold are bypassed for DRAGEN. Retry + threshold logic is owned by this
        # orchestrator (per-sample retry over the cohort), not by the loop (which would
        # over-trigger on small batch counts).
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
        )

    retry_batches = _build_retry_batches(
        cohort_name=cohort.name,
        batches_file=batches_file,
        batch_size=batch_size,
    )
    # Resume scenario: a previous orchestrator pass already created retry batches but
    # crashed before they completed. Pick them back up here.
    existing_retry_in_flight = [
        IcaBatch(cohort_name=cohort.name, batch_index=b['batch_index'], sg_names=b['sg_names'])
        for b in batches_file.batches
        if b['retry_generation'] == 1 and b['status'] in ACTIVE_BATCH_STATUSES
    ]
    # `_build_retry_batches` may have appended the same batches we just resumed. Dedupe.
    seen_names = {b.name for b in retry_batches}
    retry_batches = retry_batches + [b for b in existing_retry_in_flight if b.name not in seen_names]

    if retry_batches:
        logger.info(f'Retry batches to monitor: {[b.name for b in retry_batches]}')
        retry_batches_by_name = {b.name: b for b in retry_batches}
        retry_on_succeeded = _on_succeeded_factory(batches_file, retry_batches_by_name)
        retry_on_status_change = _on_status_change_factory(batches_file, retry_batches_by_name)

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
        )

    # Resume-after-cancel guard: if any batch is CANCELLED, this rerun must
    # not proceed. CANCELLED is terminal (user-initiated abort); the only
    # sanctioned recovery is `force_resubmit=true`. Without this guard, two
    # bad paths open up:
    #   1. All-CANCELLED rerun: `initial_batches` filters out CANCELLED →
    #      empty loop → `_build_retry_batches` returns [] (terminal) →
    #      threshold check passes (failed_sg_names excludes CANCELLED) →
    #      run() exits success-side → downstream Download* stages run with
    #      preserved per-SG state pointing at aborted ICA analyses → all
    #      explode with cryptic "analysis not found" errors.
    #   2. Partial-cancel-with-success: same as (1) but only the CANCELLED
    #      SGs' downstream stages explode. Still bad UX — cancelled SGs
    #      surface as per-SG ICA failures rather than a clean cohort halt.
    # Tightening the guard to `if cancelled_sgs:` (no `and not any_succeeded`
    # exception) closes both holes uniformly: any user-initiated cancellation
    # forces an explicit `force_resubmit=true` to recover. Partial successes
    # are preserved via batches.json + per-SG state files; force_resubmit
    # harvests them on re-submission.
    cancelled_sgs = batches_file.cancelled_sg_names()
    if cancelled_sgs:
        raise CohortCancelled(
            f'Cohort {cohort.name} has {len(cancelled_sgs)} SG(s) in CANCELLED '
            f'batches from a previous run. CANCELLED is terminal; rerun with '
            f'force_resubmit=true to start a fresh submission (AR GUIDs from '
            f'the preserved per-SG state will be reused).',
        )

    # `failed_sg_names()` excludes CANCELLED by design (cancellation ≠ failure;
    # see BatchesFile.failed_sg_names docstring). The threshold check thus
    # measures pipeline-failure rate, not user-action rate.
    n_total = len(sg_names)
    failed = batches_file.failed_sg_names()
    n_failed = len(failed)
    if _threshold_breached(n_failed=n_failed, n_total=n_total):
        # Persist errors.log to disk before raising, so the cohort run
        # produces a durable error artefact. `errors_path` is internal
        # scratch — not declared as a stage expected_output, so absence of
        # the file on a successful run does not trigger a re-run.
        with errors_path.open('w') as fh:
            fh.write(
                f'Cohort {cohort.name}: {n_failed}/{n_total} SGs failed the DRAGEN '
                f'pipeline ({n_failed / n_total:.1%} > 5% threshold).\n'
                f'Failed SGs: {", ".join(failed)}\n',
            )
        raise RuntimeError(
            f'More than 5% of SGs failed the DRAGEN pipeline: {n_failed}/{n_total}. '
            f'See {errors_path} for the failure list.',
        )

    # Completion marker: writes the canonical "stage completed without raising"
    # signal that cpg-flow checks via expected_outputs. The marker is the LAST
    # write of the orchestrator — any earlier raise (threshold breach, cancel,
    # ICA exception) skips this and leaves cpg-flow seeing the stage as failed,
    # so the stage will retry. Marker payload records the cohort's outcome
    # summary so operators can audit completed runs without re-reading
    # batches.json.
    complete_path = outputs[f'{cohort.name}_pipeline_complete']
    with complete_path.open('w') as fh:
        json.dump(
            {
                'cohort_name': cohort.name,
                'n_sgs_total': n_total,
                'n_sgs_failed': n_failed,
                'n_batches': len(batches_file.batches),
            },
            fh,
        )
