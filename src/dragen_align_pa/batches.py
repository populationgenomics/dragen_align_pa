from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dragen_align_pa.constants.batch_constants import (
    ALLOWED_BATCH_STATUSES,
    ALLOWED_ERROR_STRATEGIES,
    BATCH_STATUS_CANCELLED,
    BATCH_STATUS_FAILED,
    BATCH_STATUS_INPROGRESS,
    BATCH_STATUS_PENDING,
    BATCHES_SCHEMA_VERSION,
    CANONICAL_PASSFAIL_FAIL,
    PASSFAIL_STATUS_NORMALISATION,
)

if TYPE_CHECKING:
    from pathlib import Path

    import cpg_utils


class PassfailStatusError(ValueError):
    """A passfail.json status value outside the recognised input set.

    Subclasses `ValueError` so callers catching `ValueError` still match.
    """


def validate_error_strategy(value: str, *, context: str) -> None:
    """Validate an ICA `error_strategy` value.

    Args:
        value: The `error_strategy` to check.
        context: Caller description prefixed to the error message.

    Raises:
        ValueError: If `value` is not one of `ALLOWED_ERROR_STRATEGIES`.
    """
    if value not in ALLOWED_ERROR_STRATEGIES:
        raise ValueError(
            f'{context}: error_strategy must be one of {sorted(ALLOWED_ERROR_STRATEGIES)}, got {value!r}.',
        )


def normalise_passfail_status(value: str, *, context: str) -> str:
    """Map a raw passfail status onto the canonical `"Success"` / `"Fail"`.

    Args:
        value: A raw passfail status (DRAGEN writes `"Success"` / `"Failed"`).
        context: Caller description prefixed to the error message.

    Returns:
        The canonical status (`CANONICAL_PASSFAIL_SUCCESS` or `CANONICAL_PASSFAIL_FAIL`).

    Raises:
        PassfailStatusError: If `value` is outside the recognised input set.
    """
    try:
        return PASSFAIL_STATUS_NORMALISATION[value]
    except KeyError:
        raise PassfailStatusError(
            f'{context}: passfail status must be one of {sorted(PASSFAIL_STATUS_NORMALISATION)}, got {value!r}.',
        ) from None


@dataclass
class IcaBatch:
    """Internal target representing a batch of SGs for the unified DRAGEN pipeline.

    Not a cpg-flow target type — only `.name` is consumed by
    `manage_ica_pipeline_loop`. Named `IcaBatch` (rather than the more
    natural `Batch`) to avoid confusion with `hailtop.batch.Batch` —
    `cpg_utils.hail_batch.Batch` shows up in submit-time code paths and
    the unqualified `Batch` symbol there means the Hail concept.
    """

    cohort_name: str
    batch_index: int
    sg_names: list[str]

    @property
    def name(self) -> str:
        return f'{self.cohort_name}-batch{self.batch_index:04d}'


def chunk_sgs_into_batches(
    cohort_name: str,
    sg_names: list[str],
    batch_size: int,
) -> list[IcaBatch]:
    """Partition a cohort's SGs into deterministic batches.

    SGs are sorted lexicographically before chunking so re-runs with the same
    cohort produce the same batch assignment.
    """
    if not sg_names:
        raise ValueError(f'Cannot chunk empty cohort {cohort_name}')
    if batch_size < 1:
        raise ValueError(f'batch_size must be >= 1, got {batch_size}')

    sorted_sgs = sorted(sg_names)
    batches: list[IcaBatch] = []
    for i in range(0, len(sorted_sgs), batch_size):
        batches.append(
            IcaBatch(
                cohort_name=cohort_name,
                batch_index=len(batches),
                sg_names=sorted_sgs[i : i + batch_size],
            ),
        )
    return batches


class BatchesFile:
    """Reader/writer for `{cohort_name}_batches.json`.

    Both FASTQ and CRAM modes are batched identically (N SGs per ICA analysis);
    only the input-bundle shape at submission time differs. The schema records
    everything needed to identify "what was in this batch" so future cleanup
    or audit can use a single source of truth.

    Schema (top-level):
        {
            "schema_version": int,
            "batch_size": int,
            "n_batches": int,
            "batches": [
                {
                    "batch_index": int,
                    "retry_generation": int,           # 0 for initial, 1 for retry batches
                    "sg_names": [str, ...],
                    "retried_sgs": [str, ...],         # subset of sg_names pulled into retry batches
                    "user_reference": str | null,
                    "pipeline_id": str | null,
                    "ar_guid": str | null,
                    "analysis_output_folder_fid": str | null,   # populated lazily by `_on_succeeded`
                    "fastq_list_fid": str | null,      # FASTQ mode: combined per-batch CSV ID
                    "cram_fids": [str, ...] | null,    # CRAM mode: ordered list of per-SG CRAM IDs
                    "status": "PENDING" | "INPROGRESS" | "SUCCEEDED" | "FAILED" | "CANCELLED",
                    "passfail": {sg_name: "Success" | "Fail"} | null,
                    "passfail_seen": bool,             # True iff passfail.json was fetched + recorded
                    "has_been_retried": bool,          # action gate; pre-set True for retry_generation=1
                    "error_strategy": "auto" | "continue" | "terminate",
                }
            ],
        }

    Authoritative state. Per-SG `_pipeline_id_and_arguid.json` files are derived
    projections of this file, materialised eagerly to satisfy the cpg-flow per-SG
    I/O contract. If a per-SG projection is missing or stale, it can be
    reconstructed from this file (the consumer of `get_ica_sample_folder` is
    responsible for the fallback).

    Note on `retry_generation` / `has_been_retried` / `retried_sgs`:
    - `retry_generation = 0`: initial batch.
    - `retry_generation = 1`: spawned by `add_retry_batch` from failed SGs.
    - `retried_sgs`: which of *this* batch's `sg_names` have been pulled into a
      retry batch. Maintained by `mark_sgs_retried`; the per-SG audit record
      complements the batch-level `has_been_retried` flag.
    - `has_been_retried` is the action gate (`_build_retry_batches` skips
      batches where it's True). `mark_sgs_retried` flips it True on the first
      retry of any SG from this batch — the "single retry only" rule applies
      at the batch level, so partial retries still consume the batch's retry
      allowance. Retry batches have `has_been_retried = True` set at creation
      so a hypothetical second retry pass short-circuits.
    - Resume uses `retry_generation` + `status` to decide what to re-monitor
      (NOT `has_been_retried`) so in-flight retry batches survive a crash.

    Note on atomic writes:
    - GCS object PUTs are atomic at the object level — `cpg_utils.Path.open('w')`
      uploads the file in a single PUT on close, so readers either see the old
      content or the new content, never a partial. We do NOT use a `.tmp`
      sidecar + rename: that pattern is broken on GCS (rename = copy+delete,
      not atomic) and unnecessary given the single-PUT guarantee.
    - Multi-file writes (batches.json + per-SG state files) are not atomic
      across files. The submitter writes per-SG state files first (best-effort
      projections), then batches.json (the commit point). On crash between
      writes, the batches file still shows the batch as PENDING/INPROGRESS,
      the next orchestrator pass re-submits, and the per-SG state files get
      overwritten with the new pipeline_id.

    Note on input file IDs:
    - **FASTQ mode**: `fastq_list_fid` holds the per-batch combined CSV ID
      (constructed and uploaded at submission time).
    - **CRAM mode**: `cram_fids` holds the ordered list of per-SG CRAM IDs.
      Each is also persisted per-SG in `UploadDataToIca`'s `{sg_name}_fids.json`;
      keeping a per-batch copy here gives cleanup a single source of truth.
    """

    def __init__(self, path: cpg_utils.Path | Path):
        self.path = path
        self.batch_size: int = 0
        self.batches: list[dict[str, Any]] = []

    def initialise(self, batch_size: int, batches: list[IcaBatch]) -> None:
        # Mirror add_retry_batch's heuristic: DRAGEN's `auto` strategy
        # terminates single-sample runs before passfail.json is written,
        # so any 1-SG batch (initial cohort of 1, or trailing batch when
        # len(sgs) % batch_size == 1) must use 'continue'.
        self.batch_size = batch_size
        self.batches = [
            self._new_batch_entry(
                b,
                retry_generation=0,
                error_strategy='continue' if len(b.sg_names) == 1 else 'auto',
            )
            for b in batches
        ]

    @staticmethod
    def _new_batch_entry(b: IcaBatch, *, retry_generation: int, error_strategy: str = 'auto') -> dict[str, Any]:
        return {
            'batch_index': b.batch_index,
            'retry_generation': retry_generation,
            'sg_names': list(b.sg_names),
            'retried_sgs': [],
            'user_reference': None,
            'pipeline_id': None,
            'ar_guid': None,
            'analysis_output_folder_fid': None,
            'fastq_list_fid': None,
            'cram_fids': None,
            'status': BATCH_STATUS_PENDING,
            'passfail': None,
            'passfail_seen': False,
            # Retry batches pre-set has_been_retried=True so a second retry pass short-circuits.
            'has_been_retried': retry_generation > 0,
            'error_strategy': error_strategy,
        }

    # Required keys on every per-batch entry. Kept in sync with `_new_batch_entry`
    # — anything written there at construction time is required to be present
    # on read, so a truncated / hand-edited file fails fast at load with a
    # clear message naming the missing field rather than as a bare KeyError
    # much later from `failed_sg_names()` / `find_batch_for_sg()` etc.
    _REQUIRED_BATCH_KEYS = frozenset(
        {
            'batch_index',
            'retry_generation',
            'sg_names',
            'retried_sgs',
            'user_reference',
            'pipeline_id',
            'ar_guid',
            'analysis_output_folder_fid',
            'fastq_list_fid',
            'cram_fids',
            'status',
            'passfail',
            'passfail_seen',
            'has_been_retried',
            'error_strategy',
        }
    )

    def read(self) -> None:
        with self.path.open('r') as fh:
            data = json.load(fh)
        version = data.get('schema_version', 0)
        if version != BATCHES_SCHEMA_VERSION:
            raise ValueError(
                f'BatchesFile schema_version mismatch in {self.path}: '
                f'file has {version}, code expects {BATCHES_SCHEMA_VERSION}',
            )
        for required in ('batch_size', 'batches'):
            if required not in data:
                raise ValueError(
                    f'BatchesFile {self.path} missing required key {required!r}; file is corrupt.',
                )
        for i, entry in enumerate(data['batches']):
            missing = sorted(self._REQUIRED_BATCH_KEYS - entry.keys())
            if missing:
                raise ValueError(
                    f'BatchesFile {self.path}: per-batch entry at index {i} '
                    f'is missing required keys {missing}; file is corrupt or '
                    f'was written by an incompatible code version.',
                )
        self.batch_size = data['batch_size']
        self.batches = data['batches']
        # Migrate the passfail vocabulary on read. A batches.json written by an
        # earlier build stored DRAGEN's raw "Failed" verbatim (record_passfail did
        # not normalise yet). Re-normalising here means resuming an already-SUCCEEDED
        # batch does not silently drop a "Failed" sample from the retry path — the
        # write-time fix, applied to state persisted before it existed. Idempotent
        # for already-canonical values; an unrecognised status fails loud, matching
        # the write path.
        for i, b in enumerate(self.batches):
            if b['passfail'] is not None:
                b['passfail'] = {
                    sg: normalise_passfail_status(status, context=f'read {self.path} batch {i} sg={sg!r}')
                    for sg, status in b['passfail'].items()
                }

    def write(self) -> None:
        """Single-PUT atomic write — GCS object PUTs are atomic per object.

        `cpg_utils.Path.open('w')` uploads on close in a single PUT, so readers
        see either the old content or the new, never partial. No `.tmp` sidecar
        is needed; the previous tmp+rename pattern was broken on GCS (rename =
        copy+delete) and is intentionally removed here.
        """
        payload = {
            'schema_version': BATCHES_SCHEMA_VERSION,
            'batch_size': self.batch_size,
            'n_batches': len(self.batches),
            'batches': self.batches,
        }
        with self.path.open('w') as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)

    def batch_entry(self, batch_index: int) -> dict[str, Any]:
        """Return the batch entry with the given `batch_index`.

        Looks up by `batch_index`, not list position — `read()` does not guarantee
        the list is stored in index order, so positional access could mutate the
        wrong entry after a hand-edit or future reorder.
        """
        for b in self.batches:
            if b['batch_index'] == batch_index:
                return b
        raise KeyError(f'No batch with batch_index={batch_index} in {self.path}')

    def record_pipeline_submission(
        self,
        batch_index: int,
        pipeline_id: str,
        ar_guid: str,
        user_reference: str,
    ) -> None:
        b = self.batch_entry(batch_index)
        b['pipeline_id'] = pipeline_id
        b['ar_guid'] = ar_guid
        b['user_reference'] = user_reference
        b['status'] = BATCH_STATUS_INPROGRESS

    def record_status(self, batch_index: int, status: str) -> None:
        if status not in ALLOWED_BATCH_STATUSES:
            raise ValueError(
                f'record_status(batch_index={batch_index}): status must be one of '
                f'{sorted(ALLOWED_BATCH_STATUSES)}, got {status!r}. '
                f'The orchestrator enum is finer-grained — collapse FAILED_RETRYING/'
                f"FAILED_FINAL to 'FAILED' at the persistence boundary.",
            )
        self.batch_entry(batch_index)['status'] = status

    def record_passfail(self, batch_index: int, passfail: dict[str, str]) -> None:
        b = self.batch_entry(batch_index)
        b['passfail'] = {
            sg: normalise_passfail_status(status, context=f'record_passfail(batch_index={batch_index}, sg={sg!r})')
            for sg, status in passfail.items()
        }
        b['passfail_seen'] = True

    def clear_passfail(self, batch_index: int) -> None:
        """Reset a batch's passfail result to unset (`passfail=None`, `passfail_seen=False`)."""
        b = self.batch_entry(batch_index)
        b['passfail'] = None
        b['passfail_seen'] = False

    def record_analysis_output_folder_fid(self, batch_index: int, fid: str) -> None:
        self.batch_entry(batch_index)['analysis_output_folder_fid'] = fid

    def record_fastq_list_fid(self, batch_index: int, fid: str) -> None:
        self.batch_entry(batch_index)['fastq_list_fid'] = fid

    def record_cram_fids(self, batch_index: int, fids: list[str]) -> None:
        self.batch_entry(batch_index)['cram_fids'] = list(fids)

    def record_error_strategy(self, batch_index: int, error_strategy: str) -> None:
        validate_error_strategy(error_strategy, context=f'record_error_strategy(batch_index={batch_index})')
        self.batch_entry(batch_index)['error_strategy'] = error_strategy

    def mark_sgs_retried(self, source_batch_idx: int, sg_names: list[str]) -> None:
        """Record that these SGs from `source_batch_idx` have been pulled into a retry batch.

        Per-SG audit trail complementing the batch-level `has_been_retried` flag.
        `has_been_retried` is the "no second retry" action gate: it flips True
        as soon as ANY of this batch's SGs has been pulled into a retry,
        because only one retry pass is allowed per cohort (so the source batch
        has used up its retry allowance regardless of how many SGs were
        involved).
        """
        b = self.batch_entry(source_batch_idx)
        if not sg_names:
            return
        for sg in sg_names:
            if sg not in b['sg_names']:
                raise ValueError(
                    f'SG {sg!r} not in batch {source_batch_idx} (sg_names={b["sg_names"]})',
                )
            if sg not in b['retried_sgs']:
                b['retried_sgs'].append(sg)
        b['has_been_retried'] = True

    def add_retry_batch(
        self,
        sg_names: list[str],
        *,
        error_strategy: str | None = None,
    ) -> int:
        """Append a retry batch (retry_generation=1) for the given SGs.

        - `error_strategy` defaults to `'continue'` for single-sample batches
          (DRAGEN's `auto` strategy terminates single-sample runs before a
          `passfail.json` is written) and `'auto'` otherwise. Pass an explicit
          value to override.
        - Returns the new batch's `batch_index`.
        - Does NOT call `mark_sgs_retried` on the source batches — the caller
          must do that, since `BatchesFile` doesn't know which source batches
          contributed which SGs.
        """
        if not sg_names:
            raise ValueError('add_retry_batch: sg_names must be non-empty')
        if error_strategy is None:
            error_strategy = 'continue' if len(sg_names) == 1 else 'auto'
        validate_error_strategy(error_strategy, context='add_retry_batch')
        new_index = len(self.batches)
        # The cohort_name on IcaBatch is not written into the entry; the entry
        # only carries batch_index + sg_names. Pass an empty string to keep
        # `_new_batch_entry` signature uniform.
        seed = IcaBatch(cohort_name='', batch_index=new_index, sg_names=list(sg_names))
        self.batches.append(
            self._new_batch_entry(seed, retry_generation=1, error_strategy=error_strategy),
        )
        return new_index

    def _resolve_latest_outcomes(self) -> tuple[list[str], dict[str, bool]]:
        """Resolve each SG to its latest-generation determinate outcome.

        An SG's outcome is taken from its highest-`batch_index` determinate
        batch: per-sample passfail (Success/Fail), or a batch-level FAILED with
        no passfail (every SG in the batch Fail). CANCELLED batches (empty
        passfail) contribute no outcome.

        Returns:
            A `(first_seen_order, is_fail)` tuple: the SG names in first-seen
            order and a map of SG name to whether its latest outcome is Fail.
        """
        latest_idx: dict[str, int] = {}
        is_fail: dict[str, bool] = {}
        first_seen: list[str] = []
        for b in self.batches:
            if b['status'] == BATCH_STATUS_CANCELLED:
                # Cancellation is not an outcome — a batch that recorded a passfail
                # then got CANCELLED reports only through `cancelled_sg_names()`.
                continue
            if b['status'] == BATCH_STATUS_FAILED and b['passfail'] is None:
                outcomes: list[tuple[str, bool]] = [(sg, True) for sg in b['sg_names']]
            elif b['passfail']:
                outcomes = [(sg, status == CANONICAL_PASSFAIL_FAIL) for sg, status in b['passfail'].items()]
            else:
                continue
            idx = b['batch_index']
            for sg, fail in outcomes:
                if sg not in latest_idx:
                    first_seen.append(sg)
                    latest_idx[sg] = idx
                    is_fail[sg] = fail
                elif idx > latest_idx[sg]:
                    latest_idx[sg] = idx
                    is_fail[sg] = fail
        return first_seen, is_fail

    def failed_sg_names(self) -> list[str]:
        """SGs whose latest-generation outcome is Fail, across all batches.

        Each SG is resolved to the outcome of its highest-`batch_index`
        determinate batch, so a gen=0 Fail that a retry batch later records as
        Success does not count. A batch-level FAILED with no passfail marks every
        SG in the batch Fail. CANCELLED batches (empty passfail) contribute no
        outcome; call `cancelled_sg_names()` for those.

        Returns:
            The failed SG names (first-seen order); latest generation wins.
        """
        first_seen, is_fail = self._resolve_latest_outcomes()
        return [sg for sg in first_seen if is_fail[sg]]

    def cancelled_sg_names(self) -> list[str]:
        """SGs in batches marked CANCELLED (by user `cancel_cohort_run`).

        Like `failed_sg_names`, may double-count an SG that appears in both
        an initial (gen=0) and a retry (gen=1) batch if BOTH were cancelled —
        a degenerate scenario in practice (retries are spawned only from
        failures, and cancel only fires on PENDING/INPROGRESS batches).
        Callers that need unique SGs should wrap in `set(...)`.
        """
        return [sg for b in self.batches if b['status'] == BATCH_STATUS_CANCELLED for sg in b['sg_names']]

    def successful_sg_names(self) -> list[str]:
        """SGs whose latest-generation outcome is Success, across all batches.

        Symmetric with `failed_sg_names`: each SG resolves to its highest-
        `batch_index` determinate outcome, so an SG that failed a retry after an
        earlier Success does not appear here (and vice versa). Success requires
        positive confirmation from `passfail.json`; a batch-level FAILED with no
        passfail is a Fail, not a Success. CANCELLED batches (empty passfail)
        contribute no outcome.
        """
        first_seen, is_fail = self._resolve_latest_outcomes()
        return [sg for sg in first_seen if not is_fail[sg]]

    def find_batch_for_sg(self, sg_name: str) -> dict[str, Any] | None:
        """Return the most recent (highest batch_index) batch containing `sg_name`.

        SGs may appear in both an initial batch (`retry_generation=0`) and a
        retry batch (`retry_generation=1`). The retry batch is the source of
        truth for path resolution because its `pipeline_id` / `user_reference`
        are what the per-SG state file points at after the retry write.

        Selects by explicit max(batch_index) rather than relying on
        self.batches being stored in ascending order — that invariant is
        not enforced by read() and a future refactor / hand-edit could
        silently return the wrong entry.
        """
        candidates = [b for b in self.batches if sg_name in b['sg_names']]
        if not candidates:
            return None
        return max(candidates, key=lambda b: b['batch_index'])
