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
    CANONICAL_PASSFAIL_SUCCESS,
    PASSFAIL_STATUS_NORMALISATION,
)

if TYPE_CHECKING:
    from pathlib import Path

    import cpg_utils


class PassfailStatusError(ValueError):
    """A passfail.json status value outside the recognised input set."""


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

    Not a cpg-flow target type — only `.name` is consumed by the pipeline loop.
    Named `IcaBatch` (not `Batch`) to avoid confusion with `hailtop.batch.Batch`.
    """

    cohort_name: str
    batch_index: int
    sg_names: list[str]

    @property
    def name(self) -> str:
        return f'{self.cohort_name}-batch{self.batch_index:04d}'

    @classmethod
    def from_entry(cls, cohort_name: str, entry: dict[str, Any]) -> IcaBatch:
        """Hydrate an `IcaBatch` from a `{cohort}_batches.json` entry dict.

        Args:
            cohort_name: The cohort the batch belongs to.
            entry: A batches-file entry (carries `batch_index` and `sg_names`).

        Returns:
            The reconstructed `IcaBatch`.
        """
        return cls(cohort_name=cohort_name, batch_index=entry['batch_index'], sg_names=entry['sg_names'])


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
    """Authoritative reader/writer for `{cohort_name}_batches.json`.

    FASTQ and CRAM modes are batched identically (N SGs per ICA analysis); only
    the submit-time input bundle differs. The per-batch schema is defined by
    `_new_batch_entry` and enforced on read by `_REQUIRED_BATCH_KEYS`. Per-SG
    `_pipeline_id_and_arguid.json` files are derived projections of this file.

    retry_generation / has_been_retried / retried_sgs:
    - retry_generation 0 = initial batch; 1 = spawned by `add_retry_batch`.
    - `retried_sgs` (via `mark_sgs_retried`) is the per-SG audit trail.
    - `has_been_retried` is the action gate: `_build_retry_batches` skips it,
      and it flips True on the first retry of any SG (single-retry-per-batch).
      Retry batches pre-set it True so a second pass short-circuits.
    - Resume keys on retry_generation + status (NOT has_been_retried) so
      in-flight retry batches survive a crash.

    Note on atomic writes (canonical):
    - GCS object PUTs are atomic per object — `open('w')` uploads in a single
      PUT on close, so readers see old-or-new, never partial. No `.tmp`+rename
      (broken on GCS, unnecessary here).
    - Multi-file writes (batches.json + per-SG state) are NOT atomic across
      files. Submitter writes per-SG state first (best-effort), then
      batches.json (the commit point); a crash between leaves the batch
      PENDING/INPROGRESS to re-submit, overwriting per-SG state.
    """

    def __init__(self, path: cpg_utils.Path | Path):
        self.path = path
        self.batch_size: int = 0
        self.batches: list[dict[str, Any]] = []

    def initialise(self, batch_size: int, batches: list[IcaBatch]) -> None:
        # DRAGEN's `auto` strategy terminates single-sample runs before passfail.json,
        # so any 1-SG batch must use 'continue' (mirrors add_retry_batch).
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

    # Required keys on every per-batch entry, kept in sync with `_new_batch_entry`, so
    # a truncated / hand-edited file fails fast at load naming the missing field.
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
        # Migrate the passfail vocabulary on read: older files stored DRAGEN's raw
        # "Failed" verbatim. Re-normalising means resuming a SUCCEEDED batch doesn't
        # drop a failed sample from the retry path. Idempotent; unknown status fails loud.
        for i, b in enumerate(self.batches):
            if b['passfail'] is not None:
                b['passfail'] = {
                    sg: normalise_passfail_status(status, context=f'read {self.path} batch {i} sg={sg!r}')
                    for sg, status in b['passfail'].items()
                }

    def write(self) -> None:
        """Single-PUT atomic write (see the class "Note on atomic writes")."""
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

        Args:
            batch_index: The batch's `batch_index`.

        Returns:
            The matching batch entry dict.

        Raises:
            KeyError: If no batch has that `batch_index`.
        """
        # Match by batch_index, not list position: read() does not guarantee the
        # list is stored in index order.
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
        """Record that these SGs from `source_batch_idx` were pulled into a retry batch.

        Also flips `has_been_retried` True (the single-retry-per-batch gate).
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
        # cohort_name isn't written into the entry; empty string keeps _new_batch_entry uniform.
        seed = IcaBatch(cohort_name='', batch_index=new_index, sg_names=list(sg_names))
        self.batches.append(
            self._new_batch_entry(seed, retry_generation=1, error_strategy=error_strategy),
        )
        return new_index

    def _resolve_latest_outcomes(self) -> tuple[list[str], dict[str, bool]]:
        """Resolve each SG to its definitive Success/Fail outcome.

        An SG's outcome is Success if any generation produced an ICA-confirmed
        Success for it; otherwise it is Fail. Determinate outcomes come from
        per-sample passfail (Success/Fail) or a batch-level FAILED with falsy
        passfail (every SG in the batch Fail). CANCELLED batches contribute no
        outcome.

        Returns:
            A `(first_seen_order, is_fail)` tuple: the SG names in first-seen order
            and a map of SG name to whether its outcome is Fail.
        """
        succeeded: set[str] = set()
        seen: set[str] = set()
        first_seen: list[str] = []
        for b in self.batches:
            if b['status'] == BATCH_STATUS_CANCELLED:
                # Cancellation is not an outcome — a batch that recorded a passfail
                # then got CANCELLED reports only through `cancelled_sg_names()`.
                continue
            if b['status'] == BATCH_STATUS_FAILED and not b['passfail']:
                outcomes: list[tuple[str, bool]] = [(sg, True) for sg in b['sg_names']]
            elif b['passfail']:
                outcomes = [(sg, status == CANONICAL_PASSFAIL_FAIL) for sg, status in b['passfail'].items()]
            else:
                continue
            for sg, fail in outcomes:
                if sg not in seen:
                    seen.add(sg)
                    first_seen.append(sg)
                if not fail:
                    succeeded.add(sg)
        # A Success at any generation beats a later Fail: force_retry can reconcile
        # an original batch to SUCCEEDED after a superseding retry already failed,
        # and that earlier CRAM is the real output.
        is_fail = {sg: sg not in succeeded for sg in first_seen}
        return first_seen, is_fail

    def failed_sg_names(self) -> list[str]:
        """SGs whose definitive outcome is Fail, across all batches.

        Each SG resolves via `_resolve_latest_outcomes`: a Success at any
        generation wins, otherwise Fail. A batch-level FAILED with falsy
        passfail marks every SG in the batch Fail. CANCELLED batches contribute
        no outcome (see `cancelled_sg_names`).

        Returns:
            The failed SG names, in first-seen order.
        """
        first_seen, is_fail = self._resolve_latest_outcomes()
        return [sg for sg in first_seen if is_fail[sg]]

    def cancelled_sg_names(self) -> list[str]:
        """SGs in batches marked CANCELLED (by `cancel_cohort_run`).

        May list an SG twice if it appears in both an initial (gen=0) and a retry
        (gen=1) batch that were both cancelled; wrap in `set(...)` for uniqueness.

        Returns:
            The SG names across all CANCELLED batches.
        """
        return [sg for b in self.batches if b['status'] == BATCH_STATUS_CANCELLED for sg in b['sg_names']]

    def successful_sg_names(self) -> list[str]:
        """SGs whose definitive outcome is Success, across all batches.

        Symmetric with `failed_sg_names`: each SG resolves via
        `_resolve_latest_outcomes`. Success requires positive confirmation from
        `passfail.json` at some generation; a batch-level FAILED with falsy
        passfail is a Fail, not a Success. CANCELLED batches contribute no outcome.

        Returns:
            The succeeded SG names, in first-seen order.
        """
        first_seen, is_fail = self._resolve_latest_outcomes()
        return [sg for sg in first_seen if not is_fail[sg]]

    def find_batch_for_sg(self, sg_name: str) -> dict[str, Any] | None:
        """Return the batch holding `sg_name`'s outcome, or None if absent.

        Returns the batch that produced the winning outcome: the successful
        generation if any generation succeeded, else the highest-`batch_index`
        batch containing the SG. Selects by explicit `batch_index`, not
        self.batches list order.
        """
        candidates = [b for b in self.batches if sg_name in b['sg_names']]
        if not candidates:
            return None
        # Prefer a generation that succeeded for this SG (force_retry can reconcile
        # an earlier batch to SUCCEEDED after a later retry failed); otherwise the
        # highest-index generation, which is the in-flight retry during a normal
        # retry. read() does not enforce list order, so key on batch_index.
        succeeded = [
            b
            for b in candidates
            if b['status'] != BATCH_STATUS_CANCELLED
            and b['passfail']
            and b['passfail'].get(sg_name) == CANONICAL_PASSFAIL_SUCCESS
        ]
        return max(succeeded or candidates, key=lambda b: b['batch_index'])
