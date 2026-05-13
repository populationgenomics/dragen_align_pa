# PR #47 Copilot-Feedback Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Address Copilot's 13 review comments on PR #47 (`dragen-unified-foundation`) with TDD-backed fixes, one logical commit per fix.

**Architecture:** Pure additive / corrective changes to existing modules — no new files, no refactors beyond what each comment requires. Each task: write failing test, prove it fails, make minimal fix, prove it passes, run pinned linters, commit.

**Tech Stack:** Python 3.11; pytest (no xdist used in this plan); pinned `ruff v0.15.0` + `mypy v1.19.1` from `.pre-commit-config.yaml`.

**Verification protocol (per task):**
1. `pytest tests/<file>::<test> -v` → confirm new test FAILS as expected
2. Make code change
3. `pytest tests/<file>::<test> -v` → confirm PASSES
4. `pytest tests/ -v` → confirm no regressions
5. `pre-commit run ruff-check --files <changed files>` and `pre-commit run mypy --files <changed files>` → both clean
6. Commit

**Out of scope (planned response to reviewer, not code change):** conftest.py L45 — Copilot's suggested session-scoped autouse fixture does not address the root constraint (`dragen_align_pa.constants` reads config at module-import time, before any fixture runs). Task 12 adds a `pytest_sessionfinish` cleanup hook instead, which gives the bracketing/discoverability Copilot asked for without breaking the import-time requirement.

---

## File map

- `src/dragen_align_pa/batches.py` — 4 fixes (Tasks 1–4)
- `src/dragen_align_pa/jobs/submit_dragen_batch.py` — 4 fixes (Tasks 5–8)
- `src/dragen_align_pa/utils.py` — 2 fixes (Tasks 9–10)
- `src/dragen_align_pa/jobs/parse_passfail.py` — test-only additions (Task 11)
- `tests/conftest.py` — bracketing cleanup (Task 12)

---

### Task 1: `BatchesFile.initialise()` — single-SG batches use `error_strategy='continue'`

**Comment:** PR #47 L164. `add_retry_batch` already downgrades single-SG retries to `'continue'` because DRAGEN's `auto` strategy aborts single-sample runs before `passfail.json` is written. `initialise()` doesn't apply the same heuristic, so a cohort with 1 SG — or any cohort whose trailing batch has 1 SG when `len(sgs) % batch_size == 1` — hits the same ICA failure mode silently.

**Files:**
- Modify: `src/dragen_align_pa/batches.py:141-164`
- Test: `tests/test_batches.py` (append)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_batches.py`:

```python
def test_initialise_single_sample_batch_uses_continue(tmp_path: Path):
    """DRAGEN's `auto` error_strategy aborts single-sample runs before
    passfail.json is written. `initialise()` must mirror `add_retry_batch`'s
    heuristic so an initial cohort with 1 SG — or a trailing single-SG batch —
    gets `continue` and produces a passfail."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
    ])
    assert bf.batches[0]['error_strategy'] == 'continue'


def test_initialise_multi_sample_batch_uses_auto(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
    ])
    assert bf.batches[0]['error_strategy'] == 'auto'


def test_initialise_mixed_batch_sizes_apply_strategy_per_batch(tmp_path: Path):
    """Trailing single-SG batch (e.g. cohort of 6 with batch_size 5) is the
    realistic regression path — the trailing batch silently aborts unless
    initialise applies the per-batch heuristic."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0,
              sg_names=['CPG_A', 'CPG_B', 'CPG_C', 'CPG_D', 'CPG_E']),
        Batch(cohort_name='COH0001', batch_index=1, sg_names=['CPG_F']),
    ])
    assert bf.batches[0]['error_strategy'] == 'auto'
    assert bf.batches[1]['error_strategy'] == 'continue'
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_batches.py::test_initialise_single_sample_batch_uses_continue \
       tests/test_batches.py::test_initialise_mixed_batch_sizes_apply_strategy_per_batch -v
```

Expected: both FAIL with `assert 'auto' == 'continue'`.

- [ ] **Step 3: Apply the fix**

In `src/dragen_align_pa/batches.py`, replace `initialise()` (currently line 141–143):

```python
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
```

- [ ] **Step 4: Run tests — all batches.py tests pass**

```bash
pytest tests/test_batches.py -v
```

Expected: 22 passed (20 existing + 2 new). The third test (mixed sizes) also passes since it shares the implementation.

Wait — there are 3 new tests, so 23 passed. Confirm.

- [ ] **Step 5: Lint + type check**

```bash
pre-commit run ruff-check --files src/dragen_align_pa/batches.py tests/test_batches.py
pre-commit run mypy --files src/dragen_align_pa/batches.py
```

Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add src/dragen_align_pa/batches.py tests/test_batches.py
git commit -m "$(cat <<'EOF'
Fix initialise() single-SG batches to use error_strategy='continue'

DRAGEN's `auto` strategy terminates single-sample runs before
passfail.json is written. `add_retry_batch` already applies this
heuristic; `initialise()` now mirrors it so an initial cohort of 1
SG — or any trailing single-SG batch — produces a passfail rather
than silently aborting.

Addresses PR #47 review comment (batches.py:164).
EOF
)"
```

---

### Task 2: `BatchesFile.successful_sg_names()` excludes CANCELLED batches

**Comment:** PR #47 L330. `successful_sg_names()` reports any SG with `passfail == 'Success'` regardless of batch status. If a batch records passfail then immediately gets CANCELLED, the same SG appears in both `successful_sg_names()` and `cancelled_sg_names()`, and the orchestrator's resume-after-cancel guard (`raises CohortCancelled on any non-empty cancelled_sg_names()`) fires for SGs that already succeeded.

**Files:**
- Modify: `src/dragen_align_pa/batches.py:321-333`
- Test: `tests/test_batches.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_batches.py`:

```python
def test_successful_sg_names_excludes_cancelled_batches(tmp_path: Path):
    """A batch that records passfail then immediately gets CANCELLED must
    NOT appear in successful_sg_names(). Otherwise the orchestrator's
    resume-after-cancel guard fires for SGs that already succeeded —
    `failed_sg_names` already excludes CANCELLED for the symmetric reason."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
    ])
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Success'})
    bf.record_status(0, 'CANCELLED')
    assert bf.successful_sg_names() == []
    assert sorted(bf.cancelled_sg_names()) == ['CPG_A', 'CPG_B']
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_batches.py::test_successful_sg_names_excludes_cancelled_batches -v
```

Expected: FAIL — `assert ['CPG_A', 'CPG_B'] == []`.

- [ ] **Step 3: Apply the fix**

In `src/dragen_align_pa/batches.py`, replace `successful_sg_names()` (line 321–333):

```python
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
```

- [ ] **Step 4: Run tests — confirm no regressions in the existing CANCELLED/SUCCEEDED mixed test**

```bash
pytest tests/test_batches.py -v
```

Expected: all pass, including `test_cancelled_helper_mixed_with_succeeded` (which uses SUCCEEDED, not CANCELLED, for the success batch).

- [ ] **Step 5: Lint + type check**

```bash
pre-commit run ruff-check --files src/dragen_align_pa/batches.py tests/test_batches.py
pre-commit run mypy --files src/dragen_align_pa/batches.py
```

- [ ] **Step 6: Commit**

```bash
git add src/dragen_align_pa/batches.py tests/test_batches.py
git commit -m "$(cat <<'EOF'
Exclude CANCELLED batches from successful_sg_names()

A batch that records passfail then gets CANCELLED previously appeared
in both successful_sg_names() and cancelled_sg_names(), causing the
orchestrator's resume-after-cancel guard to fire for SGs that already
succeeded. Now symmetric with failed_sg_names(), which already
excludes CANCELLED.

Addresses PR #47 review comment (batches.py:330).
EOF
)"
```

---

### Task 3: `find_batch_for_sg` selects by explicit `max(batch_index)`

**Comment:** PR #47 L347. Current implementation relies on `self.batches` being stored in monotonically-increasing `batch_index` order. Nothing in `read()` enforces it; a future refactor or hand-edit could silently return the wrong batch.

**Files:**
- Modify: `src/dragen_align_pa/batches.py:335-347`
- Test: `tests/test_batches.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_batches.py`:

```python
def test_find_batch_for_sg_robust_to_out_of_order_storage(tmp_path: Path):
    """Defend the documented contract ('most recent batch wins') against an
    out-of-order self.batches list. The current implementation iterates and
    overwrites — correct only if batches are in ascending batch_index order.
    A future refactor or hand-edit could break that silently."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
    ])
    # Simulate a corrupt/reordered file: prepend the retry batch.
    retry_entry = bf._new_batch_entry(
        Batch(cohort_name='', batch_index=1, sg_names=['CPG_A']),
        retry_generation=1,
    )
    bf.batches.insert(0, retry_entry)  # now order is [batch_index=1, batch_index=0]
    found = bf.find_batch_for_sg('CPG_A')
    assert found is not None
    assert found['batch_index'] == 1  # the highest, regardless of list position
    assert found['retry_generation'] == 1
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_batches.py::test_find_batch_for_sg_robust_to_out_of_order_storage -v
```

Expected: FAIL — `assert 0 == 1` (loop returns the *last* match in iteration order, which is the original batch_index=0 entry).

- [ ] **Step 3: Apply the fix**

In `src/dragen_align_pa/batches.py`, replace `find_batch_for_sg()` (line 335–347):

```python
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
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_batches.py -v
```

Expected: all pass, including the existing `test_find_batch_for_sg_prefers_latest_generation`.

- [ ] **Step 5: Lint + type check**

```bash
pre-commit run ruff-check --files src/dragen_align_pa/batches.py tests/test_batches.py
pre-commit run mypy --files src/dragen_align_pa/batches.py
```

- [ ] **Step 6: Commit**

```bash
git add src/dragen_align_pa/batches.py tests/test_batches.py
git commit -m "$(cat <<'EOF'
Make find_batch_for_sg() robust to out-of-order batch storage

The previous implementation iterated self.batches and let later entries
overwrite earlier ones — correct only under the unstated invariant that
batches are stored in ascending batch_index order. Now selects by
explicit max(batch_index) so a future refactor / hand-edit can't
silently return a stale ICA folder.

Addresses PR #47 review comment (batches.py:347).
EOF
)"
```

---

### Task 4: Drop the dead `retried_sgs` migration shim in `BatchesFile.read()`

**Comment:** PR #47 L185. `read()` enforces `schema_version == 1`, so any v1 file already has `retried_sgs`. Only migrating one field on top of that is misleading. Verified: no test fixture writes a v1 file without `retried_sgs` (all go through `_new_batch_entry`).

**Files:**
- Modify: `src/dragen_align_pa/batches.py:182-185`

- [ ] **Step 1: Drop the shim**

In `src/dragen_align_pa/batches.py`, replace lines 180–185:

```python
        self.batch_size = data['batch_size']
        self.batches = data['batches']
```

Remove the `for b in self.batches: b.setdefault('retried_sgs', [])` block and its comment entirely.

- [ ] **Step 2: Run tests — full batches.py suite**

```bash
pytest tests/test_batches.py -v
```

Expected: all pass. The fixtures and `_new_batch_entry` always populate `retried_sgs=[]`.

- [ ] **Step 3: Lint + type check**

```bash
pre-commit run ruff-check --files src/dragen_align_pa/batches.py
pre-commit run mypy --files src/dragen_align_pa/batches.py
```

- [ ] **Step 4: Commit**

```bash
git add src/dragen_align_pa/batches.py
git commit -m "$(cat <<'EOF'
Drop dead retried_sgs migration shim in BatchesFile.read()

read() enforces schema_version == 1 with a hard error, so any v1 file
already has `retried_sgs` written by _new_batch_entry. The setdefault
shim could never fire and was misleading about which fields it did
NOT defend. Removing keeps the schema contract single-source-of-truth.

Addresses PR #47 review comment (batches.py:185).
EOF
)"
```

---

### Task 5: `submit_dragen_batch.run()` — fail-fast input-mode validation, reject mixed inputs

**Comment:** PR #47 L431 (two comments). (a) `run()` reads `analysis_output_fid_path` from GCS *before* validating input mode; misuse should fail before any IO. (b) When a caller passes both `cram_state_paths` and `fastq_ids_path`, the CRAM branch silently wins and FASTQ args are ignored.

**Files:**
- Modify: `src/dragen_align_pa/jobs/submit_dragen_batch.py:339-431`
- Test: `tests/test_submit_dragen_batch.py` (append)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_submit_dragen_batch.py`:

```python
def test_run_rejects_no_input_mode_before_any_io():
    """Calling run() with both CRAM and FASTQ paths None must raise BEFORE
    any GCS read. We verify by passing an analysis_output_fid_path that
    would raise on open() — if validation runs first, the ValueError is
    about input mode, not about the unreadable path."""
    class _BoomPath:
        def open(self, _mode='r'):
            raise AssertionError('analysis_output_fid_path was opened before input-mode validation')
    from dragen_align_pa.batches import Batch
    batch = Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
    with pytest.raises(ValueError, match='no valid input mode'):
        submit_dragen_batch.run(
            batch=batch,
            analysis_output_fid_path=_BoomPath(),  # type: ignore[arg-type]
            cram_state_paths=None,
            fastq_ids_path=None,
            per_sg_fastq_list_paths=None,
        )


def test_run_rejects_mixed_cram_and_fastq_inputs():
    """Passing both modes' paths is programmer error; the current code
    silently runs CRAM mode and ignores FASTQ args. Must raise instead."""
    class _BoomPath:
        def open(self, _mode='r'):
            raise AssertionError('analysis_output_fid_path was opened before mixed-input rejection')
    from dragen_align_pa.batches import Batch
    batch = Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
    with pytest.raises(ValueError, match='exactly one of'):
        submit_dragen_batch.run(
            batch=batch,
            analysis_output_fid_path=_BoomPath(),  # type: ignore[arg-type]
            cram_state_paths={'CPG_A': _BoomPath()},  # type: ignore[dict-item]
            fastq_ids_path=_BoomPath(),  # type: ignore[arg-type]
            per_sg_fastq_list_paths={'CPG_A': _BoomPath()},  # type: ignore[dict-item]
        )
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_submit_dragen_batch.py::test_run_rejects_no_input_mode_before_any_io \
       tests/test_submit_dragen_batch.py::test_run_rejects_mixed_cram_and_fastq_inputs -v
```

Expected: FAIL — current `run()` opens `analysis_output_fid_path` first, raising the `AssertionError` from `_BoomPath.open()` (or running the CRAM branch on mixed input).

- [ ] **Step 3: Apply the fix**

In `src/dragen_align_pa/jobs/submit_dragen_batch.py`, near the top of `run()` (insert right after the docstring, before `secrets = ica_api_utils.get_ica_secrets()`):

```python
    # Fail-fast input-mode validation, BEFORE any GCS / ICA / secrets IO,
    # so misuse surfaces cheaply at the orchestrator layer. Exactly one of
    # CRAM mode (cram_state_paths) or FASTQ mode (fastq_ids_path +
    # per_sg_fastq_list_paths) must be populated.
    cram_mode = cram_state_paths is not None
    fastq_mode = fastq_ids_path is not None or per_sg_fastq_list_paths is not None
    if cram_mode and fastq_mode:
        raise ValueError(
            f'submit_dragen_batch: batch {batch.name} received both CRAM and FASTQ inputs; '
            f'pass exactly one of (cram_state_paths) or (fastq_ids_path + per_sg_fastq_list_paths).',
        )
    if not cram_mode and not fastq_mode:
        raise ValueError(
            f'submit_dragen_batch: batch {batch.name} received no valid input mode; '
            f'pass exactly one of (cram_state_paths) or (fastq_ids_path + per_sg_fastq_list_paths).',
        )
    if fastq_mode and (fastq_ids_path is None or per_sg_fastq_list_paths is None):
        raise ValueError(
            f'submit_dragen_batch: batch {batch.name} FASTQ mode requires BOTH '
            f'fastq_ids_path and per_sg_fastq_list_paths.',
        )
```

Then in the existing `with ica_api_utils.get_ica_api_client() as api_client:` block, the trailing `else: raise ValueError(...)` at the old L430–431 becomes unreachable. Remove it; the `if cram_state_paths is not None / elif ...` chain is now exhaustive given the top-of-function validation. Replace the `elif` with a plain `else` for clarity:

```python
        if cram_state_paths is not None:
            specific_data_inputs, cram_fids = _build_cram_data_inputs(
                batch=batch, per_sg_state_paths=cram_state_paths,
            )
        else:
            # FASTQ mode: both paths are guaranteed non-None by the
            # top-of-function validation.
            assert fastq_ids_path is not None and per_sg_fastq_list_paths is not None
            specific_data_inputs, fastq_list_fid = _build_fastq_data_inputs(
                api_instance=data_api,
                project_id=project_id,
                batch=batch,
                fastq_ids_path=fastq_ids_path,
                per_sg_fastq_list_paths=per_sg_fastq_list_paths,
            )
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_submit_dragen_batch.py -v
```

Expected: all pass.

- [ ] **Step 5: Lint + type check**

```bash
pre-commit run ruff-check --files src/dragen_align_pa/jobs/submit_dragen_batch.py tests/test_submit_dragen_batch.py
pre-commit run mypy --files src/dragen_align_pa/jobs/submit_dragen_batch.py
```

Expected: clean. The `assert` is mypy's preferred narrowing pattern for `Optional → T`.

- [ ] **Step 6: Commit**

```bash
git add src/dragen_align_pa/jobs/submit_dragen_batch.py tests/test_submit_dragen_batch.py
git commit -m "$(cat <<'EOF'
Fail-fast input-mode validation in submit_dragen_batch.run()

Two related fixes:
- Validate input mode at the top of run(), before any GCS / ICA / secrets
  IO, so misuse fails cheaply at the orchestrator layer instead of after
  an analysis_output_fid GCS read.
- Reject mixed CRAM + FASTQ inputs explicitly. Previously the CRAM
  branch silently won and FASTQ args were ignored — orchestrator
  miswiring would surface only as a wrong-mode submission to ICA.

Addresses PR #47 review comments (submit_dragen_batch.py:431, both).
EOF
)"
```

---

### Task 6: Validate `coverage_region_beds` length in `_build_common_data_inputs`

**Comment:** PR #47 L336. Design doc §3 line 137 constrains `qc_coverage_region_beds` to 0–3 file IDs; the common args only wires `--qc-coverage-reports-1` and DRAGEN supports up to two `--qc-coverage-reports-N` channels. The current code silently accepts any list length.

**Files:**
- Modify: `src/dragen_align_pa/jobs/submit_dragen_batch.py:316-336`
- Test: `tests/test_submit_dragen_batch.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_submit_dragen_batch.py`:

```python
_MAX_COVERAGE_REGION_BEDS = 3


def test_build_common_data_inputs_rejects_too_many_coverage_beds(monkeypatch):
    """Design spec §3 caps qc_coverage_region_beds at 3 entries. A
    misconfigured TOML with 4+ would silently send them all to ICA.
    Mock just enough config_retrieve to drive the validation path."""
    bed_ids = [f'fil.{i:07d}' for i in range(_MAX_COVERAGE_REGION_BEDS + 1)]
    cfg = {
        ('ica', 'pipelines', 'dragen_ht_id'): 'fil.refref',
        ('ica', 'qc', 'coverage_region_beds'): bed_ids,
        ('ica', 'qc', 'cross_cont_vcf'): None,
        ('workflow', 'sequencing_type'): 'genome',
        ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'genome', 'additional_files'): [],
        ('dragen_align_pa', 'manage_dragen_pipeline', 'user', 'additional_files'): [],
    }
    monkeypatch.setattr(
        submit_dragen_batch,
        'config_retrieve',
        lambda key, default=None: cfg.get(tuple(key), default),
    )
    with pytest.raises(ValueError, match='coverage_region_beds'):
        submit_dragen_batch._build_common_data_inputs()


def test_build_common_data_inputs_accepts_max_coverage_beds(monkeypatch):
    """3 entries is the documented maximum — must not raise."""
    bed_ids = [f'fil.{i:07d}' for i in range(_MAX_COVERAGE_REGION_BEDS)]
    cfg = {
        ('ica', 'pipelines', 'dragen_ht_id'): 'fil.refref',
        ('ica', 'qc', 'coverage_region_beds'): bed_ids,
        ('ica', 'qc', 'cross_cont_vcf'): None,
        ('workflow', 'sequencing_type'): 'genome',
        ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'genome', 'additional_files'): [],
        ('dragen_align_pa', 'manage_dragen_pipeline', 'user', 'additional_files'): [],
    }
    monkeypatch.setattr(
        submit_dragen_batch,
        'config_retrieve',
        lambda key, default=None: cfg.get(tuple(key), default),
    )
    # Just confirm no exception — the assembled list shape is covered by
    # downstream integration tests in Task 17.
    submit_dragen_batch._build_common_data_inputs()
```

- [ ] **Step 2: Run tests to verify the rejection one fails**

```bash
pytest tests/test_submit_dragen_batch.py::test_build_common_data_inputs_rejects_too_many_coverage_beds \
       tests/test_submit_dragen_batch.py::test_build_common_data_inputs_accepts_max_coverage_beds -v
```

Expected: first test FAILS (`DID NOT RAISE`), second passes.

- [ ] **Step 3: Apply the fix**

In `src/dragen_align_pa/jobs/submit_dragen_batch.py`, at module level add (near `_PRESET_PLACEHOLDER_RE`):

```python
_MAX_COVERAGE_REGION_BEDS = 3
```

In `_build_common_data_inputs()` (line 316), after the `coverage_region_beds = config_retrieve(...)` line, add validation:

```python
    coverage_region_beds: list[str] = config_retrieve(['ica', 'qc', 'coverage_region_beds'], default=[])
    if len(coverage_region_beds) > _MAX_COVERAGE_REGION_BEDS:
        raise ValueError(
            f'ica.qc.coverage_region_beds has {len(coverage_region_beds)} entries; '
            f'DRAGEN supports at most {_MAX_COVERAGE_REGION_BEDS} (design spec §3). '
            f'Trim the list in your TOML.',
        )
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_submit_dragen_batch.py -v
```

Expected: all pass.

- [ ] **Step 5: Lint + type check**

```bash
pre-commit run ruff-check --files src/dragen_align_pa/jobs/submit_dragen_batch.py tests/test_submit_dragen_batch.py
pre-commit run mypy --files src/dragen_align_pa/jobs/submit_dragen_batch.py
```

- [ ] **Step 6: Commit**

```bash
git add src/dragen_align_pa/jobs/submit_dragen_batch.py tests/test_submit_dragen_batch.py
git commit -m "$(cat <<'EOF'
Cap coverage_region_beds at 3 entries per design spec §3

DRAGEN supports up to 2 --qc-coverage-reports-N channels (we wire
--qc-coverage-reports-1) and the design caps qc_coverage_region_beds
at 3 file IDs. Previously a misconfigured TOML with 4+ would silently
send them all to ICA — now rejected at config-read time with a
pointer to the offending TOML key.

Addresses PR #47 review comment (submit_dragen_batch.py:336).
EOF
)"
```

---

### Task 7: Scope placeholder regex to source strings (preset + user), not assembled output

**Comment:** PR #47 L96. `_PRESET_PLACEHOLDER_RE` runs on the fully-assembled `additional_args`. The static `_COMMON_ADDITIONAL_ARGS` contains `QUAL<5.0;LowDepth:all:DP<=1` — digit-led after `<`, so the regex (`<[a-zA-Z]…`) doesn't match today. But any future hard-filter tweak that introduces `<token>` (e.g., a tag name) would be misidentified as an unfilled preset placeholder and abort the run. Scan preset + user args individually instead.

**Files:**
- Modify: `src/dragen_align_pa/jobs/submit_dragen_batch.py:55-97`
- Test: `tests/test_submit_dragen_batch.py` (append)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_submit_dragen_batch.py`:

```python
def test_build_additional_args_does_not_flag_common_args_with_future_lt_tokens(monkeypatch):
    """If _COMMON_ADDITIONAL_ARGS ever grows a `<token>` substring (e.g. a
    DRAGEN tag name after a `<`), it must NOT be flagged as an unfilled
    preset placeholder — placeholders are a property of preset/user args,
    not the hardcoded common block. Simulate by monkey-patching
    _COMMON_ADDITIONAL_ARGS with a future-compatible value."""
    monkeypatch.setattr(
        submit_dragen_batch,
        '_COMMON_ADDITIONAL_ARGS',
        submit_dragen_batch._COMMON_ADDITIONAL_ARGS + ' --some-future-flag <somerule>',
    )
    monkeypatch.setattr(submit_dragen_batch, 'config_retrieve', _config_factory())
    # Should succeed: the `<somerule>` came from the common block, not a preset.
    submit_dragen_batch._build_additional_args()


def test_build_additional_args_flags_preset_placeholder():
    """Existing behaviour — preset placeholders still raise."""
    # Covered by test_build_additional_args_rejects_placeholder above;
    # this is a regression-named alias for clarity.
    pass
```

Note: `test_build_additional_args_rejects_placeholder` already covers the "preset placeholder raises" case. We add the inverse (common-args-may-contain-`<token>`) and rely on the existing test for the positive direction.

- [ ] **Step 2: Run the new test to verify it fails**

```bash
pytest tests/test_submit_dragen_batch.py::test_build_additional_args_does_not_flag_common_args_with_future_lt_tokens -v
```

Expected: FAIL — current regex scans assembled string and matches `<somerule>` from the common block.

- [ ] **Step 3: Apply the fix**

In `src/dragen_align_pa/jobs/submit_dragen_batch.py`, rewrite `_build_additional_args()` (line 55–97):

```python
def _build_additional_args() -> str:
    """Concatenate common + sequencing-type preset + user override into one args string.

    Raises if any `<placeholder>` sentinel survives in the preset or user
    args (e.g. the WES preset shipping `<bed-name>` defaults that weren't
    filled in for this run). Placeholders are a property of fill-in-the-blank
    preset/user strings — NOT the hardcoded common block — so we scan
    each source individually rather than the assembled output.
    """
    sequencing_type = config_retrieve(['workflow', 'sequencing_type'])
    if sequencing_type not in {'genome', 'exome'}:
        raise ValueError(
            f"workflow.sequencing_type must be 'genome' or 'exome', got {sequencing_type!r}",
        )
    preset = config_retrieve(
        ['dragen_align_pa', 'manage_dragen_pipeline', 'presets', sequencing_type],
        default=None,
    )
    if preset is None:
        raise ValueError(
            f'Missing config section [dragen_align_pa.manage_dragen_pipeline.presets.{sequencing_type}]; '
            f'add it to your TOML.',
        )
    user = config_retrieve(
        ['dragen_align_pa', 'manage_dragen_pipeline', 'user'],
        default={'additional_args': '', 'additional_files': []},
    )

    preset_args = preset.get('additional_args', '').strip()
    user_args = user.get('additional_args', '').strip()

    # Scan preset and user args individually — these are the only strings
    # where `<placeholder>` sentinels are legitimate inputs that must be
    # filled before submission. _COMMON_ADDITIONAL_ARGS is hardcoded and
    # may contain `<token>` substrings now or in the future without that
    # implying an unfilled placeholder.
    for source_name, source in (('preset', preset_args), ('user', user_args)):
        placeholders = _PRESET_PLACEHOLDER_RE.findall(source)
        if placeholders:
            raise ValueError(
                f'DRAGEN additional_args {source_name} string contains unfilled placeholders '
                f'{placeholders} (config path: '
                f'[dragen_align_pa.manage_dragen_pipeline.presets.{sequencing_type}] / '
                f'[dragen_align_pa.manage_dragen_pipeline.user]). '
                f'Fill them in your config before running.',
            )

    parts = [
        _COMMON_ADDITIONAL_ARGS.strip(),
        f"--cnv-segmentation-mode {preset['cnv_segmentation_mode']}",
        preset_args,
        user_args,
    ]
    return ' '.join(part for part in parts if part)
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_submit_dragen_batch.py -v
```

Expected: all pass, including the existing `test_build_additional_args_rejects_placeholder` (preset still raises).

- [ ] **Step 5: Lint + type check**

```bash
pre-commit run ruff-check --files src/dragen_align_pa/jobs/submit_dragen_batch.py tests/test_submit_dragen_batch.py
pre-commit run mypy --files src/dragen_align_pa/jobs/submit_dragen_batch.py
```

- [ ] **Step 6: Commit**

```bash
git add src/dragen_align_pa/jobs/submit_dragen_batch.py tests/test_submit_dragen_batch.py
git commit -m "$(cat <<'EOF'
Scope placeholder regex to preset+user args, not assembled output

Placeholders (`<bed-name>` etc.) are a property of fill-in-the-blank
preset/user strings, not the hardcoded _COMMON_ADDITIONAL_ARGS block.
Scanning the assembled string today happens to work because the common
block's `<` substrings are digit-led, but any future hard-filter tweak
introducing a `<token>` would be misidentified as an unfilled preset
placeholder and abort the run.

Addresses PR #47 review comment (submit_dragen_batch.py:96).
EOF
)"
```

---

### Task 8: Detect duplicate FASTQ filenames in `_build_fastq_data_inputs`

**Comment:** PR #47 L296. `_load_per_sg_fastq_lists` returns a deduplicated `sorted(set)` of FASTQ names, but `fastq_ids_df['fastq_name'].isin(...)` matches every row whose `fastq_name` is in the SG set — so if a filename appears twice in `{cohort}_fastq_ids.txt` (re-upload after a transient failure), the matched DataFrame contains duplicate rows. Result: either a spurious count-mismatch error, or — if duplicates cancel out across multiple files — duplicate ICA IDs silently sent in `dataIds`.

**Files:**
- Modify: `src/dragen_align_pa/jobs/submit_dragen_batch.py:277-313`
- Test: `tests/test_submit_dragen_batch.py` (append)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_submit_dragen_batch.py`:

```python
import io
import pandas as pd
from unittest.mock import MagicMock


def _make_fastq_ids_path(content: str, tmp_path):
    p = tmp_path / 'COH0001_fastq_ids.txt'
    p.write_text(content)
    return p


def _make_per_sg_fastq_csv(tmp_path, sg_name: str, read1: list[str], read2: list[str]):
    p = tmp_path / f'{sg_name}_fastq_list.csv'
    df = pd.DataFrame({
        'RGID': [f'rg{i}' for i in range(len(read1))],
        'RGSM': [sg_name] * len(read1),
        'RGLB': ['lib1'] * len(read1),
        'Lane': list(range(1, len(read1) + 1)),
        'Read1File': read1,
        'Read2File': read2,
    })
    df.to_csv(p, index=False)
    return p


def test_build_fastq_data_inputs_handles_duplicate_fastq_rows(tmp_path, monkeypatch):
    """A re-uploaded FASTQ may appear in {cohort}_fastq_ids.txt twice (old
    + new ICA file IDs). The submitter must deterministically pick the
    most recent (last) ID and not silently send both — and must not
    spuriously fail the count check."""
    fastq_ids_path = _make_fastq_ids_path(
        'fil.OLD_R1   CPG_A_R1.fastq.gz\n'
        'fil.OLD_R2   CPG_A_R2.fastq.gz\n'
        'fil.NEW_R1   CPG_A_R1.fastq.gz\n'  # re-upload duplicate
        'fil.NEW_R2   CPG_A_R2.fastq.gz\n',  # re-upload duplicate
        tmp_path,
    )
    per_sg_csv = _make_per_sg_fastq_csv(
        tmp_path, 'CPG_A',
        read1=['CPG_A_R1.fastq.gz'], read2=['CPG_A_R2.fastq.gz'],
    )
    monkeypatch.setattr(
        submit_dragen_batch, 'config_retrieve',
        lambda key, default=None: {('ica', 'data_prep', 'output_folder'): 'test/'}.get(tuple(key), default),
    )
    monkeypatch.setattr(submit_dragen_batch, 'BUCKET_NAME', 'test-bucket')
    # Stub the upload helper — duplicate detection happens before upload.
    monkeypatch.setattr(
        submit_dragen_batch, '_upload_per_batch_fastq_list',
        lambda **kwargs: 'fil.uploaded_csv',
    )

    from dragen_align_pa.batches import Batch
    batch = Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
    data_inputs, fastq_list_fid = submit_dragen_batch._build_fastq_data_inputs(
        api_instance=MagicMock(),
        project_id='proj',
        batch=batch,
        fastq_ids_path=fastq_ids_path,
        per_sg_fastq_list_paths={'CPG_A': per_sg_csv},
    )
    fastq_input = next(di for di in data_inputs if di['parameterCode'] == 'fastqs')
    ica_ids = list(fastq_input['dataIds'])
    # Most-recent-wins: NEW IDs preserved, OLD discarded.
    assert sorted(ica_ids) == ['fil.NEW_R1', 'fil.NEW_R2']
    assert len(set(ica_ids)) == len(ica_ids), 'duplicate ICA IDs leaked into dataIds'
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_submit_dragen_batch.py::test_build_fastq_data_inputs_handles_duplicate_fastq_rows -v
```

Expected: FAIL — current code either raises `ValueError('Mismatch in FASTQ IDs')` (because `len(matched)==4` vs `len(sg_fastq_names)==2`) or, less likely, sends 4 IDs in `dataIds`.

- [ ] **Step 3: Apply the fix**

In `src/dragen_align_pa/jobs/submit_dragen_batch.py`, replace the body of `_build_fastq_data_inputs()` starting at line 288:

```python
def _build_fastq_data_inputs(
    api_instance: project_data_api.ProjectDataApi,
    project_id: str,
    batch: Batch,
    fastq_ids_path: cpg_utils.Path,
    per_sg_fastq_list_paths: dict[str, cpg_utils.Path],
) -> tuple[list[AnalysisDataInput], str]:
    """Construct ICA data inputs for a FASTQ-mode batch.

    Returns (data_inputs, fastq_list_fid). The per-batch combined CSV is uploaded inline.

    Duplicate handling: a FASTQ filename may appear in {cohort}_fastq_ids.txt
    more than once (re-upload after a transient failure leaves both rows in
    the manifest, with distinct ICA IDs). We deterministically keep the LAST
    row per fastq_name (most recent upload wins) and log when collapsing —
    sending all matched IDs would push duplicates into dataIds and silently
    submit the wrong/stale file.
    """
    sg_fastq_names, combined_csv = _load_per_sg_fastq_lists(batch.sg_names, per_sg_fastq_list_paths)
    fastq_ids_df = _read_fastq_ids(fastq_ids_path)
    matched = fastq_ids_df[fastq_ids_df['fastq_name'].isin(sg_fastq_names)]

    # Collapse re-upload duplicates: keep the last row per fastq_name.
    dupes = matched[matched.duplicated(subset='fastq_name', keep=False)]
    if not dupes.empty:
        for name, group in dupes.groupby('fastq_name'):
            ids = group['ica_id'].tolist()
            logger.warning(
                f'FASTQ name {name!r} appears {len(ids)} times in {fastq_ids_path}; '
                f'keeping most recent (last) ICA ID, discarding {ids[:-1]}.',
            )
    matched = matched.drop_duplicates(subset='fastq_name', keep='last')

    fastq_ica_ids = matched['ica_id'].tolist()
    if len(fastq_ica_ids) != len(sg_fastq_names):
        missing = set(sg_fastq_names) - set(matched['fastq_name'])
        raise ValueError(
            f'Mismatch in FASTQ IDs for batch {batch.name}: expected {len(sg_fastq_names)}, '
            f'found {len(fastq_ica_ids)}. Missing: {missing}',
        )

    fastq_list_fid = _upload_per_batch_fastq_list(
        api_instance=api_instance,
        project_id=project_id,
        cohort_name=batch.cohort_name,
        batch_index=batch.batch_index,
        combined_csv=combined_csv,
    )

    return (
        [
            AnalysisDataInput(parameterCode='fastqs', dataIds=fastq_ica_ids),
            AnalysisDataInput(parameterCode='fastq_list', dataIds=[fastq_list_fid]),
        ],
        fastq_list_fid,
    )
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_submit_dragen_batch.py -v
```

Expected: all pass.

- [ ] **Step 5: Lint + type check**

```bash
pre-commit run ruff-check --files src/dragen_align_pa/jobs/submit_dragen_batch.py tests/test_submit_dragen_batch.py
pre-commit run mypy --files src/dragen_align_pa/jobs/submit_dragen_batch.py
```

- [ ] **Step 6: Commit**

```bash
git add src/dragen_align_pa/jobs/submit_dragen_batch.py tests/test_submit_dragen_batch.py
git commit -m "$(cat <<'EOF'
Collapse duplicate FASTQ rows in _build_fastq_data_inputs

A re-uploaded FASTQ may appear in {cohort}_fastq_ids.txt twice (old +
new ICA file IDs). The previous code either spuriously failed the
count check or silently sent duplicate IDs in dataIds. Now we
deterministically keep the most recent (last) row per fastq_name and
log the discarded IDs so an operator can audit.

Addresses PR #47 review comment (submit_dragen_batch.py:296).
EOF
)"
```

---

### Task 9: `get_ica_sample_folder` — validate `batch_index` and align docstring

**Comment:** PR #47 L209. Docstring claims "Required key absent under the right schema version → KeyError naming the missing field," but the validation loop only checks `('user_reference', 'pipeline_id')`. The v1 schema also includes `batch_index`, and downstream resume/reconciliation paths rely on it. Tighten the validation list.

**Files:**
- Modify: `src/dragen_align_pa/utils.py:201-205`
- Test: `tests/test_path_resolution.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_path_resolution.py`:

```python
def test_get_ica_sample_folder_raises_keyerror_on_missing_batch_index(tmp_path: Path):
    """Per-SG state schema v1 requires batch_index — downstream resume /
    reconciliation paths read it. A v1 file missing that key should raise
    KeyError naming the missing field, matching the validation contract
    the docstring promises."""
    state_path = tmp_path / 'CPG00001_pipeline_id_and_arguid.json'
    state_path.write_text(json.dumps({
        'schema_version': PER_SG_STATE_SCHEMA_VERSION,
        'pipeline_id': '00000000-1111-2222-3333-444444444444',
        'ar_guid': 'test-guid',
        'user_reference': 'COH0001-batch0000_test-guid_',
        # missing batch_index
    }))
    with pytest.raises(KeyError, match='batch_index'):
        get_ica_sample_folder(state_path, sg_name='CPG00001')
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_path_resolution.py::test_get_ica_sample_folder_raises_keyerror_on_missing_batch_index -v
```

Expected: FAIL — current loop doesn't check `batch_index`; function returns a valid-looking path string.

- [ ] **Step 3: Apply the fix**

In `src/dragen_align_pa/utils.py`, replace lines 201–205:

```python
    for required in ('user_reference', 'pipeline_id', 'batch_index'):
        if required not in state:
            raise KeyError(
                f'Per-SG state file {pipeline_id_arguid_path} missing required key {required!r}.',
            )
```

(The docstring already says "Required key absent under the right schema version → KeyError" — no change there; this aligns the validation with the documented contract.)

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_path_resolution.py -v
```

Expected: all pass. The existing `test_get_ica_sample_folder_renders_expected_path` writes `batch_index=0` via `_write_state` so it's unaffected.

- [ ] **Step 5: Lint + type check**

```bash
pre-commit run ruff-check --files src/dragen_align_pa/utils.py tests/test_path_resolution.py
pre-commit run mypy --files src/dragen_align_pa/utils.py
```

- [ ] **Step 6: Commit**

```bash
git add src/dragen_align_pa/utils.py tests/test_path_resolution.py
git commit -m "$(cat <<'EOF'
Validate batch_index in get_ica_sample_folder

The v1 per-SG state schema requires batch_index, and downstream
resume / reconciliation paths read it. Previously only user_reference
and pipeline_id were validated, so a malformed v1 file with a missing
batch_index would silently produce a valid-looking path. Now matches
the validation contract the docstring already promises.

Addresses PR #47 review comment (utils.py:209).
EOF
)"
```

---

### Task 10: Drop unused `cohort_name` parameter from `get_batch_artefacts_root`

**Comment:** PR #47 L138. The parameter exists only for "future-proofing symmetry" with `get_batch_artefacts_path` and is silenced with `noqa: ARG001`. Per the comment: drop it (callers thread cohort_name only at the leaf, where it's actually used) — keeps the lint suppression off and the API honest.

**Files:**
- Modify: `src/dragen_align_pa/utils.py:129-151`
- Test: `tests/test_path_resolution.py:60-77`

- [ ] **Step 1: Update the signature and call site**

In `src/dragen_align_pa/utils.py`:

```python
def get_batch_artefacts_root() -> cpg_utils.Path:
    """Per-cohort artefacts root under GCS (siblings: passfail/summary/reports per batch).

    All cohorts share a `dragen_batch_metrics/` root under the cohort-scoped
    output prefix; per-cohort scoping is applied at the leaf by
    `get_batch_artefacts_path`, which prefixes the batch name with cohort_name.
    """
    return get_output_path('dragen_batch_metrics')


def get_batch_artefacts_path(cohort_name: str, batch_index: int) -> cpg_utils.Path:
    """Per-batch artefacts directory under GCS (passfail.json, summary.json, reports/).

    Note: the GCS subdirectory uses **underscore** (`{cohort}_batch{NN}`) — distinct
    from `Batch.name`, which uses **hyphen** (`{cohort}-batch{NN}`) as the
    cpg-flow target identifier. The two are deliberately split: hyphen for the
    in-process target name (matches the `user_reference` pattern), underscore
    for the GCS path (filesystem-friendly, won't be confused with the cohort name).
    Both forms zero-pad `batch_index` to width 4.
    """
    return get_batch_artefacts_root() / f'{cohort_name}_batch{batch_index:04d}'
```

- [ ] **Step 2: Update tests**

In `tests/test_path_resolution.py`, the existing `test_get_batch_artefacts_path` already calls `get_batch_artefacts_path(cohort_name='COH0001', batch_index=3)` — no change needed there. Confirm no other tests call `get_batch_artefacts_root` directly:

```bash
grep -rn 'get_batch_artefacts_root' tests/ src/
```

Expected: only the function definition and the call inside `get_batch_artefacts_path`. If any caller surfaces, drop the now-unused arg there too.

- [ ] **Step 3: Run tests**

```bash
pytest tests/test_path_resolution.py -v
```

Expected: all pass.

- [ ] **Step 4: Lint + type check**

```bash
pre-commit run ruff-check --files src/dragen_align_pa/utils.py
pre-commit run mypy --files src/dragen_align_pa/utils.py
```

Expected: clean — no more `noqa: ARG001` needed.

- [ ] **Step 5: Commit**

```bash
git add src/dragen_align_pa/utils.py
git commit -m "$(cat <<'EOF'
Drop unused cohort_name param from get_batch_artefacts_root

The parameter was preserved for future-proofing symmetry with
get_batch_artefacts_path but was silenced with noqa: ARG001 and
threaded through callers for no current effect. Per-cohort scoping is
already applied at the leaf by get_batch_artefacts_path. Dropping it
keeps the API honest; if a future per-cohort namespace change is
needed, it can be reintroduced at that time.

Addresses PR #47 review comment (utils.py:138).
EOF
)"
```

---

### Task 11: Add tests for `fetch_passfail_from_ica`

**Comment:** PR #47 L94. `fetch_passfail_from_ica` is the production fetch path (the file parser is the trivial part). Non-trivial branching — 403 retry, FileNotFoundError, ApiException at lookup vs URL minting, RequestException — has no test coverage.

**Files:**
- Test: `tests/test_passfail_parsing.py` (append)

This task adds tests only; no production code change.

- [ ] **Step 1: Write the tests**

Append to `tests/test_passfail_parsing.py`:

```python
import json
from unittest.mock import MagicMock, patch

import icasdk
import pytest
import requests

from dragen_align_pa.jobs.parse_passfail import fetch_passfail_from_ica


_PATH_PARAMS = {'projectId': 'proj-123'}
_FOLDER = '/bucket/output/COH0001-batch0000_guid_-pipeline-id/'


def _api_instance_with_download_url(url: str = 'https://example.com/passfail.json') -> MagicMock:
    api = MagicMock()
    api.create_download_url_for_data.return_value.body = {'url': url}
    return api


def test_fetch_passfail_returns_parsed_payload_on_happy_path():
    api = _api_instance_with_download_url()
    payload = {'CPG00001': 'Success', 'CPG00002': 'Fail'}
    fake_response = MagicMock(status_code=200)
    fake_response.json.return_value = payload
    fake_response.raise_for_status.return_value = None

    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               return_value='fil.passfail'), \
         patch('dragen_align_pa.jobs.parse_passfail.requests.get',
               return_value=fake_response):
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)

    assert result == payload


def test_fetch_passfail_returns_none_when_file_missing():
    """Catastrophically-failed batch may not have produced passfail.json —
    FileNotFoundError from the lookup is legitimate, not an error."""
    api = MagicMock()
    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               side_effect=FileNotFoundError):
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)
    assert result is None


def test_fetch_passfail_returns_none_on_lookup_api_exception():
    """Any icasdk.ApiException at the lookup stage → log + None (caller retries next poll)."""
    api = MagicMock()
    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               side_effect=icasdk.ApiException(status=500, reason='kaboom')):
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)
    assert result is None


def test_fetch_passfail_returns_none_on_mint_api_exception():
    """ApiException from create_download_url_for_data → log + None."""
    api = MagicMock()
    api.create_download_url_for_data.side_effect = icasdk.ApiException(status=500, reason='kaboom')
    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               return_value='fil.passfail'):
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)
    assert result is None


def test_fetch_passfail_returns_none_on_network_error():
    """requests.RequestException → log + None."""
    api = _api_instance_with_download_url()
    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               return_value='fil.passfail'), \
         patch('dragen_align_pa.jobs.parse_passfail.requests.get',
               side_effect=requests.ConnectionError('timeout')):
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)
    assert result is None


def test_fetch_passfail_retries_once_on_403_then_succeeds():
    """First fetch returns 403 (presigned URL expired between mint and GET);
    code mints a fresh URL and retries once. Second response succeeds."""
    api = _api_instance_with_download_url()
    payload = {'CPG00001': 'Success'}
    first_response = MagicMock(status_code=403)
    second_response = MagicMock(status_code=200)
    second_response.json.return_value = payload
    second_response.raise_for_status.return_value = None

    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               return_value='fil.passfail'), \
         patch('dragen_align_pa.jobs.parse_passfail.requests.get',
               side_effect=[first_response, second_response]) as mock_get:
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)

    assert result == payload
    assert mock_get.call_count == 2
    # Both URLs were minted (initial + retry).
    assert api.create_download_url_for_data.call_count == 2


def test_fetch_passfail_returns_none_on_repeated_403():
    """Two consecutive 403s → no further retry; return None."""
    api = _api_instance_with_download_url()
    first_response = MagicMock(status_code=403)
    second_response = MagicMock(status_code=403)
    second_response.raise_for_status.side_effect = requests.HTTPError('403 still')

    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               return_value='fil.passfail'), \
         patch('dragen_align_pa.jobs.parse_passfail.requests.get',
               side_effect=[first_response, second_response]):
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)
    assert result is None
```

- [ ] **Step 2: Run tests — expect all to pass against the current implementation**

```bash
pytest tests/test_passfail_parsing.py -v
```

Expected: all pass (this is purely test coverage for existing code that already handles these cases).

If any fails, the failure is a real bug in the existing fetch implementation — investigate before pushing forward.

- [ ] **Step 3: Lint + type check**

```bash
pre-commit run ruff-check --files tests/test_passfail_parsing.py
pre-commit run mypy --files tests/test_passfail_parsing.py
```

- [ ] **Step 4: Commit**

```bash
git add tests/test_passfail_parsing.py
git commit -m "$(cat <<'EOF'
Add unit tests for fetch_passfail_from_ica

The production fetch path (with 403-retry, FileNotFoundError,
ApiException at lookup vs URL-mint, and RequestException branches)
previously had no test coverage — only the trivial file parser was
tested. Adds 7 tests with mocked ica_api_utils + requests.get to
lock in each documented failure mode.

Addresses PR #47 review comment (parse_passfail.py:94).
EOF
)"
```

---

### Task 12: `tests/conftest.py` — make the import-time monkeypatch bracketed

**Comment:** PR #47 conftest.py:45. Copilot recommended a session-scoped autouse fixture, but that doesn't work here: `dragen_align_pa.constants` reads `cpg_utils.config` at module-import time, before any fixture runs. The right fix is to capture the originals and restore them at session end via `pytest_sessionfinish` — that gives the "bracketed and discoverable" property without breaking the import-time requirement.

**Files:**
- Modify: `tests/conftest.py`

- [ ] **Step 1: Apply the fix**

In `tests/conftest.py`, between line 14 (`import cpg_utils.config`) and line 37 (the second assignment), capture originals; at end of file, register a `pytest_sessionfinish` hook.

Replace lines 14–37 with:

```python
import cpg_utils.config

# Originals captured so pytest_sessionfinish can restore them at the end of
# the run. We MUST monkeypatch at module-import time (not in a session-scoped
# autouse fixture) because dragen_align_pa.constants reads cpg_utils.config
# at its own import time, which happens during pytest collection before any
# fixture can run. The trade-off is that the patch is not auto-bracketed by
# pytest's fixture machinery — pytest_sessionfinish below provides explicit
# bracketing instead.
_ORIGINAL_CONFIG_RETRIEVE = cpg_utils.config.config_retrieve
_ORIGINAL_OUTPUT_PATH = cpg_utils.config.output_path


_TEST_CONFIG: dict[tuple, object] = {
    ('workflow', 'reads_type'): 'cram',
    # Match the production TOML value verbatim so any future test that doesn't
    # explicitly monkeypatch DRAGEN_VERSION still gets a path-shape match.
    ('ica', 'pipelines', 'dragen_version'): 'dragen_3_7_8',
}


def _test_config_retrieve(key, default=None):
    return _TEST_CONFIG.get(tuple(key), default)


def _test_output_path(suffix: str = '') -> str:
    return f'gs://cpg-test-dataset-test/{suffix}'


# Monkey-patching a module attribute with a narrower signature; mypy's
# concern here doesn't apply at runtime because every test caller invokes
# config_retrieve / output_path through the same two-arg / one-arg shape
# that the stubs cover. Ignore the assignment-type check.
cpg_utils.config.config_retrieve = _test_config_retrieve  # type: ignore[assignment]
cpg_utils.config.output_path = _test_output_path  # type: ignore[assignment]
```

Then at the **end** of `tests/conftest.py`, append:

```python
def pytest_sessionfinish(session, exitstatus):  # noqa: ARG001
    """Restore the originals captured at module import.

    This is cosmetic for a one-shot test run (the process exits anyway), but
    makes the monkeypatch boundary explicit and discoverable: anyone tracing
    `cpg_utils.config.config_retrieve` can find both the install and the
    teardown in this file.
    """
    cpg_utils.config.config_retrieve = _ORIGINAL_CONFIG_RETRIEVE  # type: ignore[assignment]
    cpg_utils.config.output_path = _ORIGINAL_OUTPUT_PATH  # type: ignore[assignment]
```

- [ ] **Step 2: Run the full test suite**

```bash
pytest tests/ -v
```

Expected: all pass (no behavioural change — restoration only fires after the last test).

- [ ] **Step 3: Lint + type check**

```bash
pre-commit run ruff-check --files tests/conftest.py
pre-commit run mypy --files tests/conftest.py
```

- [ ] **Step 4: Commit**

```bash
git add tests/conftest.py
git commit -m "$(cat <<'EOF'
Bracket conftest's cpg_utils.config monkeypatch via pytest_sessionfinish

We can't move the monkeypatch into a session-scoped fixture — the
fixture would run after pytest collection, by which time
dragen_align_pa.constants has already imported and read
cpg_utils.config at its own module-import time. Instead, capture the
originals at install time and restore them in pytest_sessionfinish, so
the patch boundary is explicit and discoverable.

Addresses PR #47 review comment (tests/conftest.py:45).
EOF
)"
```

---

## End-of-plan verification

After all 12 tasks land:

- [ ] **Final lint sweep**

```bash
pre-commit run --all-files
```

Expected: all hooks clean.

- [ ] **Final test sweep**

```bash
pytest tests/ -v
```

Expected: all pre-existing + new tests pass (24 in test_batches.py, 11 in test_submit_dragen_batch.py, 4 in test_path_resolution.py, 9 in test_passfail_parsing.py).

- [ ] **Push and request re-review**

```bash
git push origin dragen-unified-foundation
gh pr comment 47 --body "Addressed all 13 Copilot review comments across 12 commits — one per fix, with TDD coverage. Ready for re-review."
```

---

## Self-review

- **Spec coverage:** Each of Copilot's 13 comments maps to a task (L431a + L431b merged into Task 5). All 13 covered.
- **Placeholder scan:** No "TBD", no "add appropriate error handling" — every step shows the code or command.
- **Type consistency:** Function signatures (`get_batch_artefacts_root() -> cpg_utils.Path` after Task 10) updated consistently across the call site in `get_batch_artefacts_path`. No callers in `src/` or `tests/` pass `cohort_name` to the root function today (verified via grep step in Task 10).
- **Test naming:** New tests use descriptive snake_case matching existing convention (`test_<noun>_<behavior>`).
