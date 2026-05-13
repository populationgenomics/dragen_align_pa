from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    import cpg_utils

SCHEMA_VERSION = 1


@dataclass
class Batch:
    """Internal target representing a batch of SGs for the unified DRAGEN pipeline.

    Not a cpg-flow target type — only `.name` is consumed by `manage_ica_pipeline_loop`.
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
) -> list[Batch]:
    """Partition a cohort's SGs into deterministic batches.

    SGs are sorted lexicographically before chunking so re-runs with the same
    cohort produce the same batch assignment.
    """
    if not sg_names:
        raise ValueError(f'Cannot chunk empty cohort {cohort_name}')
    if batch_size < 1:
        raise ValueError(f'batch_size must be >= 1, got {batch_size}')

    sorted_sgs = sorted(sg_names)
    batches: list[Batch] = []
    for i in range(0, len(sorted_sgs), batch_size):
        batches.append(
            Batch(
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
    responsible for the fallback — see Task 7).

    Note on `retry_generation` / `has_been_retried` / `retried_sgs`:
    - `retry_generation = 0`: initial batch.
    - `retry_generation = 1`: spawned by `add_retry_batch` from failed SGs.
    - `retried_sgs`: which of *this* batch's `sg_names` have been pulled into a
      retry batch. Maintained by `mark_sgs_retried`. The spec requires both
      batch-level and per-SG retry tracking (spec §6 line 304); `retried_sgs`
      is the per-SG audit record.
    - `has_been_retried` is the action gate (`_build_retry_batches` skips
      batches where it's True). `mark_sgs_retried` flips it True on the first
      retry of any SG from this batch — the spec's "single retry only" rule
      applies at the batch level, so partial retries still consume the batch's
      retry allowance. Retry batches have `has_been_retried = True` set at
      creation so a hypothetical second retry pass short-circuits.
    - Resume uses `retry_generation` + `status` to decide what to re-monitor
      (NOT `has_been_retried`) so in-flight retry batches survive a crash.

    Note on atomic writes:
    - GCS object PUTs are atomic at the object level — `cpg_utils.Path.open('w')`
      uploads the file in a single PUT on close, so readers either see the old
      content or the new content, never a partial. We do NOT use a `.tmp`
      sidecar + rename: that pattern is broken on GCS (rename = copy+delete,
      not atomic) and unnecessary given the single-PUT guarantee.
    - Multi-file writes (batches.json + per-SG state files) are not atomic
      across files. The commit order is documented in Task 15: per-SG state
      files first (best-effort projections), then batches.json (the commit
      point). On crash between writes, the batches file still shows the batch
      as PENDING/INPROGRESS, the next orchestrator pass re-submits, and the
      per-SG state files get overwritten with the new pipeline_id.

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

    def initialise(self, batch_size: int, batches: list[Batch]) -> None:
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
    def _new_batch_entry(b: Batch, *, retry_generation: int, error_strategy: str = 'auto') -> dict[str, Any]:
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
            'status': 'PENDING',
            'passfail': None,
            'passfail_seen': False,
            # Retry batches pre-set has_been_retried=True so a second retry pass short-circuits.
            'has_been_retried': retry_generation > 0,
            'error_strategy': error_strategy,
        }

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
        self.batch_size = data['batch_size']
        self.batches = data['batches']
        # Migrate old rows that lack `retried_sgs` (defensive: tests / fixtures may load
        # files written by older code paths). Don't touch other fields.
        for b in self.batches:
            b.setdefault('retried_sgs', [])

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
        b['status'] = 'INPROGRESS'

    def record_status(self, batch_index: int, status: str) -> None:
        self.batches[batch_index]['status'] = status

    def record_passfail(self, batch_index: int, passfail: dict[str, str]) -> None:
        self.batches[batch_index]['passfail'] = dict(passfail)
        self.batches[batch_index]['passfail_seen'] = True

    def record_analysis_output_folder_fid(self, batch_index: int, fid: str) -> None:
        self.batches[batch_index]['analysis_output_folder_fid'] = fid

    def record_fastq_list_fid(self, batch_index: int, fid: str) -> None:
        self.batches[batch_index]['fastq_list_fid'] = fid

    def record_cram_fids(self, batch_index: int, fids: list[str]) -> None:
        self.batches[batch_index]['cram_fids'] = list(fids)

    def record_error_strategy(self, batch_index: int, error_strategy: str) -> None:
        self.batches[batch_index]['error_strategy'] = error_strategy

    def mark_sgs_retried(self, source_batch_idx: int, sg_names: list[str]) -> None:
        """Record that these SGs from `source_batch_idx` have been pulled into a retry batch.

        Per-SG audit trail mandated by spec §6 line 304 ("mark them and their
        SGs has_been_retried=true"). The batch-level `has_been_retried` flag is
        the "no second retry" action gate: it flips True as soon as ANY of this
        batch's SGs has been pulled into a retry, because the spec allows only
        a single retry pass per cohort (so the source batch has used up its
        retry allowance regardless of how many SGs were involved).
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
        new_index = len(self.batches)
        # The cohort_name on Batch is not written into the entry; the entry only
        # carries batch_index + sg_names. Pass an empty string to keep `_new_batch_entry`
        # signature uniform.
        seed = Batch(cohort_name='', batch_index=new_index, sg_names=list(sg_names))
        self.batches.append(
            self._new_batch_entry(seed, retry_generation=1, error_strategy=error_strategy),
        )
        return new_index

    def failed_sg_names(self) -> list[str]:
        """SGs marked Fail across all batches (via passfail.json or batch-level FAILED).

        CANCELLED is NOT a failure — `cancel_cohort_run=true` is user-initiated
        and should not count against the 5% threshold or any "failure" report.
        Call `cancelled_sg_names()` for cancellation reporting.
        """
        failed: list[str] = []
        for b in self.batches:
            if b['status'] == 'FAILED' and b['passfail'] is None:
                failed.extend(b['sg_names'])
                continue
            if b['passfail']:
                failed.extend(sg for sg, status in b['passfail'].items() if status == 'Fail')
        return failed

    def cancelled_sg_names(self) -> list[str]:
        """SGs in batches marked CANCELLED (by user `cancel_cohort_run`).

        Like `failed_sg_names`, may double-count an SG that appears in both
        an initial (gen=0) and a retry (gen=1) batch if BOTH were cancelled —
        a degenerate scenario in practice (retries are spawned only from
        failures, and cancel only fires on PENDING/INPROGRESS batches).
        Callers that need unique SGs should wrap in `set(...)`.
        """
        return [
            sg
            for b in self.batches
            if b['status'] == 'CANCELLED'
            for sg in b['sg_names']
        ]

    def successful_sg_names(self) -> list[str]:
        """SGs explicitly marked Success in any non-CANCELLED batch's passfail.

        Asymmetric with `failed_sg_names` by design: success requires positive
        confirmation from `passfail.json`, whereas batch-level FAILED implies
        Fail for every SG in the batch. An SG only appears here once its
        batch's `passfail_seen` is True.

        CANCELLED batches are excluded for symmetry with `failed_sg_names` —
        if a batch records passfail then is cancelled, those SGs report only
        through `cancelled_sg_names()` so the resume-after-cancel guard
        doesn't double-count them.
        """
        successful: list[str] = []
        for b in self.batches:
            if b['status'] == 'CANCELLED':
                continue
            if b['passfail']:
                successful.extend(sg for sg, status in b['passfail'].items() if status == 'Success')
        return successful

    def find_batch_for_sg(self, sg_name: str) -> dict[str, Any] | None:
        """Return the most recent (highest batch_index) batch containing `sg_name`.

        SGs may appear in both an initial batch (`retry_generation=0`) and a
        retry batch (`retry_generation=1`). The retry batch is the source of
        truth for path resolution because its `pipeline_id` / `user_reference`
        are what the per-SG state file points at after the retry write.
        """
        match: dict[str, Any] | None = None
        for b in self.batches:
            if sg_name in b['sg_names']:
                match = b  # keep iterating; later entries override earlier
        return match
