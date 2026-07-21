from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    import cpg_utils

SCHEMA_VERSION = 1

# DRAGEN/ICA accepts exactly these three values for the `error_strategy`
# pipeline parameter. Anything else is rejected at the API boundary with an
# opaque message; we validate locally so misuse fails fast with a clear error.
ALLOWED_ERROR_STRATEGIES = frozenset({'auto', 'continue', 'terminate'})

# Per-batch status values written into batches.json. Named so call sites
# compare against `BATCH_STATUS_FAILED` rather than the bare string 'FAILED' —
# a typo in a literal is a silent lookup miss, a typo in a name is a NameError.
# The orchestrator's in-memory `PipelineStatus` enum is finer-grained
# (FAILED_RETRYING / FAILED_FINAL); the persistence layer collapses both to
# `BATCH_STATUS_FAILED`.
BATCH_STATUS_PENDING = 'PENDING'
BATCH_STATUS_INPROGRESS = 'INPROGRESS'
BATCH_STATUS_SUCCEEDED = 'SUCCEEDED'
BATCH_STATUS_FAILED = 'FAILED'
BATCH_STATUS_CANCELLED = 'CANCELLED'

ALLOWED_BATCH_STATUSES = frozenset(
    {
        BATCH_STATUS_PENDING,
        BATCH_STATUS_INPROGRESS,
        BATCH_STATUS_SUCCEEDED,
        BATCH_STATUS_FAILED,
        BATCH_STATUS_CANCELLED,
    },
)

# Statuses that mean "a batch is still being worked (or waiting to be)". Used by
# the resume paths to decide which batches to re-monitor.
ACTIVE_BATCH_STATUSES = frozenset({BATCH_STATUS_PENDING, BATCH_STATUS_INPROGRESS})

# Per-sample passfail status vocabulary.
# DRAGEN's `passfail.json` writes `"Success"` / `"Failed"`.
# Adding a normalisation here to not have to change all downstream code.
CANONICAL_PASSFAIL_SUCCESS = 'Success'
CANONICAL_PASSFAIL_FAIL = 'Fail'
_PASSFAIL_STATUS_NORMALISATION = {
    'Success': CANONICAL_PASSFAIL_SUCCESS,
    'Fail': CANONICAL_PASSFAIL_FAIL,
    'Failed': CANONICAL_PASSFAIL_FAIL,
}


def validate_error_strategy(value: str, *, context: str) -> None:
    if value not in ALLOWED_ERROR_STRATEGIES:
        raise ValueError(
            f'{context}: error_strategy must be one of {sorted(ALLOWED_ERROR_STRATEGIES)}, got {value!r}.',
        )


def normalise_passfail_status(value: str, *, context: str) -> str:
    """Map a raw passfail status onto the canonical `"Success"` / `"Fail"`.

    Raises `ValueError` on any status outside the recognised input set.
    """
    try:
        return _PASSFAIL_STATUS_NORMALISATION[value]
    except KeyError:
        raise ValueError(
            f'{context}: passfail status must be one of {sorted(_PASSFAIL_STATUS_NORMALISATION)}, got {value!r}.',
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
        if version != SCHEMA_VERSION:
            raise ValueError(
                f'BatchesFile schema_version mismatch in {self.path}: '
                f'file has {version}, code expects {SCHEMA_VERSION}',
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

    def write(self) -> None:
        """Single-PUT atomic write — GCS object PUTs are atomic per object.

        `cpg_utils.Path.open('w')` uploads on close in a single PUT, so readers
        see either the old content or the new, never partial. No `.tmp` sidecar
        is needed; the previous tmp+rename pattern was broken on GCS (rename =
        copy+delete) and is intentionally removed here.
        """
        payload = {
            'schema_version': SCHEMA_VERSION,
            'batch_size': self.batch_size,
            'n_batches': len(self.batches),
            'batches': self.batches,
        }
        with self.path.open('w') as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)

    def record_pipeline_submission(
        self,
        batch_index: int,
        pipeline_id: str,
        ar_guid: str,
        user_reference: str,
    ) -> None:
        b = self.batches[batch_index]
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
        self.batches[batch_index]['status'] = status

    def record_passfail(self, batch_index: int, passfail: dict[str, str]) -> None:
        self.batches[batch_index]['passfail'] = {
            sg: normalise_passfail_status(status, context=f'record_passfail(batch_index={batch_index}, sg={sg!r})')
            for sg, status in passfail.items()
        }
        self.batches[batch_index]['passfail_seen'] = True

    def record_analysis_output_folder_fid(self, batch_index: int, fid: str) -> None:
        self.batches[batch_index]['analysis_output_folder_fid'] = fid

    def record_fastq_list_fid(self, batch_index: int, fid: str) -> None:
        self.batches[batch_index]['fastq_list_fid'] = fid

    def record_cram_fids(self, batch_index: int, fids: list[str]) -> None:
        self.batches[batch_index]['cram_fids'] = list(fids)

    def record_error_strategy(self, batch_index: int, error_strategy: str) -> None:
        validate_error_strategy(error_strategy, context=f'record_error_strategy(batch_index={batch_index})')
        self.batches[batch_index]['error_strategy'] = error_strategy

    def mark_sgs_retried(self, source_batch_idx: int, sg_names: list[str]) -> None:
        """Record that these SGs from `source_batch_idx` have been pulled into a retry batch.

        Per-SG audit trail complementing the batch-level `has_been_retried` flag.
        `has_been_retried` is the "no second retry" action gate: it flips True
        as soon as ANY of this batch's SGs has been pulled into a retry,
        because only one retry pass is allowed per cohort (so the source batch
        has used up its retry allowance regardless of how many SGs were
        involved).
        """
        b = self.batches[source_batch_idx]
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

    def failed_sg_names(self) -> list[str]:
        """SGs marked Fail across all batches (via passfail.json or batch-level FAILED).

        CANCELLED is NOT a failure — `cancel_cohort_run=true` is user-initiated
        and should not count against the 5% threshold or any "failure" report.
        Call `cancelled_sg_names()` for cancellation reporting.

        Deduplicated across batches so an SG that fails in both gen=0 and
        gen=1 counts once. The 5%-threshold check uses `len(failed_sg_names())`
        and must not be artificially tripped by retried failures.
        """
        seen: set[str] = set()
        failed: list[str] = []
        for b in self.batches:
            if b['status'] == BATCH_STATUS_FAILED and b['passfail'] is None:
                candidates: list[str] = list(b['sg_names'])
            elif b['passfail']:
                candidates = [sg for sg, status in b['passfail'].items() if status == CANONICAL_PASSFAIL_FAIL]
            else:
                continue
            for sg in candidates:
                if sg not in seen:
                    seen.add(sg)
                    failed.append(sg)
        return failed

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
        """SGs explicitly marked Success in any non-CANCELLED batch's passfail.

        Asymmetric with `failed_sg_names` by design: success requires positive
        confirmation from `passfail.json`, whereas batch-level FAILED implies
        Fail for every SG in the batch. An SG only appears here once its
        batch's `passfail_seen` is True.

        CANCELLED batches are excluded so a batch that records passfail then
        is cancelled reports only through `cancelled_sg_names()` — this matches
        `failed_sg_names`'s exclusion of CANCELLED and prevents the
        resume-after-cancel guard from double-counting them.
        """
        successful: list[str] = []
        for b in self.batches:
            if b['status'] == BATCH_STATUS_CANCELLED:
                continue
            if b['passfail']:
                successful.extend(sg for sg, status in b['passfail'].items() if status == CANONICAL_PASSFAIL_SUCCESS)
        return successful

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
