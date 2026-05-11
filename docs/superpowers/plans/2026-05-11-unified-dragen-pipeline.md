# Unified DRAGEN ICA Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the two ICA pipelines (`ica.pipelines.cram`, `ica.pipelines.fastq`) with the unified `DRAGEN378-custom-unified-F2-v1` pipeline (ID `18a4baab-a12f-415d-ba8e-10b5bf6834d0`), switch from per-sequencing-group submissions to batched per-cohort submissions, and add per-sample retry via `passfail.json`.

**Architecture:** Targeted refactor on `dragen-unified-dev` branch. The existing cohort-submitter → per-SG-download stage skeleton is preserved. `ManageDragenPipeline` is rewritten to chunk SGs into batches of N (default 5) and submit one ICA analysis per batch. Per-SG state files extend in place to carry batch identity. Downloads resolve their batch via a shared helper. A new `Batch` target class plus an `on_succeeded` callback lets the existing `manage_ica_pipeline_loop` host batches without disturbing MLR's per-SG usage. Per-sample retry is orchestrated outside the loop in `manage_dragen_pipeline.py`.

**Tech Stack:** Python 3.11, `cpg-flow`, `icasdk`, `cpg_utils`, `loguru`, `pandas`, `pytest`. ICA pipeline submission via `create_nextflow_analysis`.

**Reference:** Full design at `docs/superpowers/specs/2026-05-11-unified-dragen-pipeline-design.md` — read before starting.

---

## File structure overview

**Created files**

- `tests/__init__.py` — empty (test package marker)
- `tests/conftest.py` — pytest fixtures (`demo_bundle`)
- `tests/fixtures/generate_demo_bundle.py` — synthetic ICA-output bundle generator
- `tests/test_batches.py` — `Batch` / `chunk_sgs_into_batches` / `BatchesFile` unit tests
- `tests/test_path_resolution.py` — `get_ica_sample_folder` / `get_batch_artefacts_path` unit tests
- `tests/test_passfail_parsing.py` — `passfail.json` parsing unit tests
- `src/dragen_align_pa/batches.py` — `Batch` dataclass, batching algorithm, `{cohort}_batches.json` I/O
- `src/dragen_align_pa/jobs/submit_dragen_batch.py` — per-batch ICA submission (replaces `run_align_genotype_with_dragen.py`); also performs the in-memory FASTQ-list concatenation + upload
- `src/dragen_align_pa/jobs/parse_passfail.py` — downloads + parses `passfail.json`, updates `BatchesFile`
- `src/dragen_align_pa/jobs/download_batch_artefacts.py` — downloads per-batch `passfail.json`, `summary.json`, `reports/`

**Modified files**

- `.gitignore` — ignore real and synthetic demo-bundle dirs
- `pyproject.toml` — `testpaths` from `['test']` → `['tests']`
- `config/dragen_align_pa_defaults.toml` — add `[ica.dragen]`, drop deprecated keys
- `src/dragen_align_pa/utils.py` — add `get_ica_sample_folder`, `get_batch_artefacts_path`
- `src/dragen_align_pa/jobs/ica_pipeline_manager.py` — widen `ProcessingTarget`, add `on_succeeded` callback
- `src/dragen_align_pa/jobs/manage_dragen_pipeline.py` — full rewrite (batching, retry, threshold over SGs)
- `src/dragen_align_pa/stages.py` — multiple stages rewired (see Tasks 16–22)
- `README.md`, `README_developer.md`, `README_lead.md` — describe batched submission + new stage
- `workflow_dag.dot`, `workflow_dag.svg` — DAG reflects new stage layout

**Deleted files**

- `src/dragen_align_pa/jobs/run_align_genotype_with_dragen.py` (replaced by `submit_dragen_batch.py`)
- `src/dragen_align_pa/jobs/upload_fastq_file_list.py` (upload moved into submitter)

**PR-checkpoint markers** (where a working-state commit could be tagged for review)
- After **Task 14c** — core data types, helpers, config, loop generalization, submitter + passfail + orchestration tests (no stage rewiring yet — old pipeline still runnable)
- After **Task 23** — new pipeline end-to-end runnable, old stages removed
- After **Task 24** — docs + DAG refreshed

---

## Task 1: Add `.gitignore` entries for demo bundle data

**Files:**
- Modify: `.gitignore` (append)

- [ ] **Step 1: Read the existing .gitignore tail to find a sensible insertion point**

Run: `tail -20 .gitignore`

- [ ] **Step 2: Append the two entries**

Append the following to the bottom of `.gitignore`:

```
# Real ICA bundle downloads (never commit)
/ica-demo-bundle/

# Synthetic fixture output (script default)
/tests/fixtures/ica-demo-bundle/
```

- [ ] **Step 3: Verify no untracked demo-bundle dirs exist that need explicit cleanup**

Run: `git status --short | grep ica-demo-bundle`
Expected: empty (we deleted the real one earlier)

- [ ] **Step 4: Commit**

```bash
git add .gitignore
git commit -m "Ignore real and synthetic ICA demo-bundle directories"
```

---

## Task 2: Switch pytest testpaths to `tests/` and create the package marker

**Files:**
- Modify: `pyproject.toml:76`
- Create: `tests/__init__.py`

- [ ] **Step 1: Update `pyproject.toml`**

Edit `pyproject.toml`. Find:

```toml
[tool.pytest.ini_options]
testpaths = ['test']
```

Replace with:

```toml
[tool.pytest.ini_options]
testpaths = ['tests']
```

- [ ] **Step 2: Create `tests/__init__.py`**

Create the file with empty content.

- [ ] **Step 3: Verify pytest discovers no tests (yet)**

Run: `pytest -q`
Expected: exit code 5 (no tests collected) with message "no tests ran". This confirms the testpaths config is wired but the dir is empty.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml tests/__init__.py
git commit -m "Add tests/ directory and update pytest testpaths"
```

---

## Task 3: Create the synthetic demo-bundle generator (Python)

**Files:**
- Create: `tests/fixtures/__init__.py` (empty — package marker so `from tests.fixtures...` works)
- Create: `tests/fixtures/generate_demo_bundle.py`

- [ ] **Step 1: Create the package marker**

Create `tests/fixtures/__init__.py` with empty content.

- [ ] **Step 2: Create the generator module**

Create `tests/fixtures/generate_demo_bundle.py`:

```python
"""Generate a synthetic ICA analysis-output folder matching the layout that
DRAGEN378-custom-unified-F2-v1 produces. Files are empty stubs; only the
tiny JSON manifests at the batch root carry real content. Use as a unit-test
fixture. Do NOT commit the generated output.

Can be imported (`generate_demo_bundle(...)`) or run as a script.
"""

import argparse
import json
from pathlib import Path

# Defaults match the production user_reference convention from Task 13:
# `f'{batch.name}_{ar_guid}_'` — ends with `_`. The analysis-folder path
# below uses `{user_reference}-{pipeline_id}` to match `get_ica_sample_folder`
# in utils.py (production), so the bundle layout is a faithful fixture.
DEFAULT_USER_REFERENCE = 'COH0001-batch0000_test-guid_'
DEFAULT_PIPELINE_ID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
DEFAULT_SAMPLES = ('CPG00001', 'CPG00002')

PER_SAMPLE_FILES = (
    '{sample}.cram',
    '{sample}.cram.crai',
    '{sample}.cram.md5sum',
    '{sample}.hard-filtered.gvcf.gz',
    '{sample}.hard-filtered.gvcf.gz.tbi',
    '{sample}.hard-filtered.gvcf.gz.md5sum',
    '{sample}.sv.vcf.gz',
    '{sample}.sv.vcf.gz.tbi',
    '{sample}.cnv_metrics.csv',
    '{sample}.sv_metrics.csv',
    '{sample}.mapping_metrics.csv',
    '{sample}.vc_metrics.csv',
    '{sample}.wgs_coverage_metrics.csv',
    '{sample}.target_bed_coverage_metrics.csv',
    '{sample}.fragment_length_hist.csv',
    '{sample}-replay.json',
)


def generate_demo_bundle(
    output_root: Path,
    samples: tuple[str, ...] = DEFAULT_SAMPLES,
    user_reference: str = DEFAULT_USER_REFERENCE,
    pipeline_id: str = DEFAULT_PIPELINE_ID,
    failed_samples: tuple[str, ...] = (),
) -> Path:
    """Materialise the synthetic bundle. Returns the analysis directory path."""
    failed_set = set(failed_samples)
    # Production convention (matches `get_ica_sample_folder` in utils.py):
    # `{user_reference}-{pipeline_id}`. Because `user_reference` ends with `_`,
    # the resulting folder name is `…_-{pipeline_id}/`.
    analysis_dir = output_root / 'analysis' / f'{user_reference}-{pipeline_id}'

    (analysis_dir / 'reports' / 'report_files' / 'samples').mkdir(parents=True, exist_ok=True)
    (analysis_dir / 'ica_logs' / 'analysis').mkdir(parents=True, exist_ok=True)
    (analysis_dir / 'ica_logs' / 'work').mkdir(parents=True, exist_ok=True)

    (analysis_dir / '_tags.json').write_text(
        json.dumps({'system.iap.timestamp': '2026-05-11T00:00:00Z', 'system.iap.tes': ''}),
    )

    passfail = {s: ('Fail' if s in failed_set else 'Success') for s in samples}
    (analysis_dir / 'passfail.json').write_text(json.dumps(passfail, indent=4))

    (analysis_dir / 'summary.json').write_text(
        json.dumps(
            {
                'num_samples_total': len(samples),
                'num_samples_completed': len(samples) - len(failed_set),
                'num_samples_failed': len(failed_set),
            },
            indent=4,
        ),
    )

    for sample in samples:
        sample_dir = analysis_dir / sample
        (sample_dir / 'logs').mkdir(parents=True, exist_ok=True)
        (sample_dir / 'sv' / 'workspace').mkdir(parents=True, exist_ok=True)
        (sample_dir / 'sv' / 'results').mkdir(parents=True, exist_ok=True)
        for pattern in PER_SAMPLE_FILES:
            (sample_dir / pattern.format(sample=sample)).touch()

    return analysis_dir


def _cli() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output_root', nargs='?', default='./tests/fixtures/ica-demo-bundle')
    parser.add_argument('--samples', nargs='+', default=list(DEFAULT_SAMPLES))
    parser.add_argument('--failed', nargs='+', default=[])
    parser.add_argument('--user-reference', default=DEFAULT_USER_REFERENCE)
    parser.add_argument('--pipeline-id', default=DEFAULT_PIPELINE_ID)
    args = parser.parse_args()

    path = generate_demo_bundle(
        output_root=Path(args.output_root),
        samples=tuple(args.samples),
        user_reference=args.user_reference,
        pipeline_id=args.pipeline_id,
        failed_samples=tuple(args.failed),
    )
    print(f'Generated: {path}')


if __name__ == '__main__':
    _cli()
```

- [ ] **Step 3: Smoke-test the generator**

Run:
```bash
python -m tests.fixtures.generate_demo_bundle /tmp/demo-bundle-smoke --samples CPG_A CPG_B
```

Expected: prints `Generated: /tmp/demo-bundle-smoke/analysis/test-WGS-2samples-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`.

Verify:
```bash
ls /tmp/demo-bundle-smoke/analysis/test-WGS-2samples-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/
cat /tmp/demo-bundle-smoke/analysis/test-WGS-2samples-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/passfail.json
rm -rf /tmp/demo-bundle-smoke
```

`passfail.json` should show both samples as "Success".

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/__init__.py tests/fixtures/generate_demo_bundle.py
git commit -m "Add Python synthetic ICA demo-bundle fixture generator"
```

---

## Task 4: Create the `demo_bundle` pytest fixture

**Files:**
- Create: `tests/conftest.py`

- [ ] **Step 1: Create `tests/conftest.py`**

```python
from pathlib import Path

import pytest

from tests.fixtures.generate_demo_bundle import generate_demo_bundle

DEMO_USER_REFERENCE = 'COH0001-batch0000_test-guid_'
DEMO_PIPELINE_ID = '00000000-1111-2222-3333-444444444444'
DEMO_SAMPLES = ('CPG00001', 'CPG00002')


@pytest.fixture
def demo_bundle(tmp_path: Path) -> Path:
    """Materialise a synthetic ICA analysis output bundle under tmp_path."""
    return generate_demo_bundle(
        output_root=tmp_path,
        samples=DEMO_SAMPLES,
        user_reference=DEMO_USER_REFERENCE,
        pipeline_id=DEMO_PIPELINE_ID,
    )


@pytest.fixture
def demo_bundle_with_failure(tmp_path: Path) -> Path:
    """Materialise a synthetic bundle where CPG00002 is marked Fail."""
    return generate_demo_bundle(
        output_root=tmp_path,
        samples=DEMO_SAMPLES,
        user_reference=DEMO_USER_REFERENCE,
        pipeline_id=DEMO_PIPELINE_ID,
        failed_samples=('CPG00002',),
    )
```

- [ ] **Step 2: Smoke-test the fixture wiring**

Create a temporary test in `tests/test_smoke.py`:

```python
def test_demo_bundle_materialises(demo_bundle):
    assert demo_bundle.is_dir()
    assert (demo_bundle / 'passfail.json').is_file()
    assert (demo_bundle / 'CPG00001' / 'CPG00001.cram').is_file()
```

Run: `pytest tests/test_smoke.py -v`
Expected: PASS.

- [ ] **Step 3: Remove the smoke test**

Delete `tests/test_smoke.py`. Real tests for these fixtures live in later tasks.

- [ ] **Step 4: Commit**

```bash
git add tests/conftest.py
git commit -m "Add demo_bundle pytest fixture wired to fixture generator"
```

---

## Task 5: Create the `Batch` dataclass

**Files:**
- Create: `src/dragen_align_pa/batches.py`
- Test: `tests/test_batches.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_batches.py`:

```python
from dragen_align_pa.batches import Batch


def test_batch_name_zero_padded_four_digits():
    b = Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
    assert b.name == 'COH0001-batch0000'


def test_batch_name_four_digit_index():
    b = Batch(cohort_name='COH0001', batch_index=12, sg_names=[])
    assert b.name == 'COH0001-batch0012'


def test_batch_name_handles_large_index():
    """Width 4 supports up to 9999 batches (= 49995 SGs at batch_size 5) without lex-sort
    breakage. Beyond that, names overflow the field but stay sortable for adjacent ranges."""
    b = Batch(cohort_name='COH0001', batch_index=1234, sg_names=[])
    assert b.name == 'COH0001-batch1234'
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `pytest tests/test_batches.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dragen_align_pa.batches'`.

- [ ] **Step 3: Implement `Batch`**

Create `src/dragen_align_pa/batches.py`:

```python
from dataclasses import dataclass


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
```

- [ ] **Step 4: Run the test to confirm it passes**

Run: `pytest tests/test_batches.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/dragen_align_pa/batches.py tests/test_batches.py
git commit -m "Add Batch dataclass for unified DRAGEN pipeline submissions"
```

---

## Task 6: Add `chunk_sgs_into_batches` and `BatchesFile`

**Files:**
- Modify: `src/dragen_align_pa/batches.py`
- Modify: `tests/test_batches.py`

- [ ] **Step 1: Write the failing tests for chunking**

Append to `tests/test_batches.py`:

```python
from pathlib import Path

import pytest

from dragen_align_pa.batches import BatchesFile, chunk_sgs_into_batches


def test_chunk_into_two_batches_with_remainder():
    sgs = ['CPG_A', 'CPG_B', 'CPG_C', 'CPG_D', 'CPG_E', 'CPG_F', 'CPG_G']
    batches = chunk_sgs_into_batches(cohort_name='COH0001', sg_names=sgs, batch_size=5)
    assert len(batches) == 2
    assert batches[0].batch_index == 0
    assert batches[0].sg_names == ['CPG_A', 'CPG_B', 'CPG_C', 'CPG_D', 'CPG_E']
    assert batches[1].batch_index == 1
    assert batches[1].sg_names == ['CPG_F', 'CPG_G']


def test_chunk_sorts_lexicographically():
    sgs = ['CPG_C', 'CPG_A', 'CPG_B']
    batches = chunk_sgs_into_batches(cohort_name='COH0001', sg_names=sgs, batch_size=5)
    assert batches[0].sg_names == ['CPG_A', 'CPG_B', 'CPG_C']


def test_chunk_rejects_empty_cohort():
    with pytest.raises(ValueError, match='cohort'):
        chunk_sgs_into_batches(cohort_name='COH0001', sg_names=[], batch_size=5)


def test_chunk_rejects_non_positive_batch_size():
    with pytest.raises(ValueError, match='batch_size'):
        chunk_sgs_into_batches(cohort_name='COH0001', sg_names=['CPG_A'], batch_size=0)
```

- [ ] **Step 2: Run to confirm failure**

Run: `pytest tests/test_batches.py -v`
Expected: ImportError for `chunk_sgs_into_batches` and `BatchesFile`.

- [ ] **Step 3: Implement `chunk_sgs_into_batches`**

Append to `src/dragen_align_pa/batches.py`:

```python
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
```

- [ ] **Step 4: Write the failing tests for `BatchesFile`**

Append to `tests/test_batches.py`:

```python
def test_batches_file_roundtrip(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
    ])
    bf.write()

    loaded = BatchesFile(path=path)
    loaded.read()
    assert loaded.batch_size == 5
    assert len(loaded.batches) == 1
    assert loaded.batches[0]['sg_names'] == ['CPG_A', 'CPG_B']
    assert loaded.batches[0]['status'] == 'PENDING'


def test_batches_file_record_pipeline_id(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
    ])
    bf.write()
    bf.record_pipeline_submission(batch_index=0, pipeline_id='abc', ar_guid='xyz', user_reference='COH0001-batch0000_xyz_')
    bf.write()

    loaded = BatchesFile(path=path)
    loaded.read()
    assert loaded.batches[0]['pipeline_id'] == 'abc'
    assert loaded.batches[0]['ar_guid'] == 'xyz'
    assert loaded.batches[0]['user_reference'] == 'COH0001-batch0000_xyz_'


def test_batches_file_record_passfail(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
    ])
    bf.record_passfail(batch_index=0, passfail={'CPG_A': 'Success', 'CPG_B': 'Fail'})
    bf.write()

    loaded = BatchesFile(path=path)
    loaded.read()
    assert loaded.failed_sg_names() == ['CPG_B']
    assert loaded.successful_sg_names() == ['CPG_A']
    assert loaded.batches[0]['passfail_seen'] is True


def test_batches_file_record_cram_fids(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
    ])
    bf.record_cram_fids(batch_index=0, fids=['fil.aaa', 'fil.bbb'])
    bf.write()

    loaded = BatchesFile(path=path)
    loaded.read()
    assert loaded.batches[0]['cram_fids'] == ['fil.aaa', 'fil.bbb']


def test_batches_file_rejects_old_schema_version(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    path.write_text('{"schema_version": 0, "batch_size": 5, "n_batches": 0, "batches": []}')
    bf = BatchesFile(path=path)
    with pytest.raises(ValueError, match='schema_version mismatch'):
        bf.read()


def test_batches_file_rejects_missing_top_level_keys(tmp_path: Path):
    """Truncated / hand-edited files should raise a friendly error, not bare KeyError."""
    path = tmp_path / 'COH0001_batches.json'
    path.write_text('{"schema_version": 1}')
    bf = BatchesFile(path=path)
    with pytest.raises(ValueError, match='missing required key'):
        bf.read()


def test_batches_file_write_persists(tmp_path: Path):
    """Single-PUT atomic write — GCS object PUT is atomic per object."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
    ])
    bf.write()
    assert path.is_file()
    # No .tmp sidecar exists — we use a direct write, not tmp+rename.
    assert not (tmp_path / 'COH0001_batches.json.tmp').exists()


def test_add_retry_batch_single_sample_uses_continue(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
    ])
    new_idx = bf.add_retry_batch(sg_names=['CPG_B'])
    assert new_idx == 1
    assert bf.batches[1]['retry_generation'] == 1
    assert bf.batches[1]['has_been_retried'] is True
    assert bf.batches[1]['error_strategy'] == 'continue'
    assert bf.batches[1]['sg_names'] == ['CPG_B']


def test_add_retry_batch_multi_sample_uses_auto(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
    ])
    bf.add_retry_batch(sg_names=['CPG_A', 'CPG_B'])
    assert bf.batches[1]['error_strategy'] == 'auto'


def test_mark_sgs_retried_tracks_per_sg(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
    ])
    bf.mark_sgs_retried(source_batch_idx=0, sg_names=['CPG_B'])
    # `retried_sgs` accrues per-SG; `has_been_retried` is the action gate and
    # flips True on the first retry (no second-retry allowed regardless of count).
    assert bf.batches[0]['retried_sgs'] == ['CPG_B']
    assert bf.batches[0]['has_been_retried'] is True
    bf.mark_sgs_retried(source_batch_idx=0, sg_names=['CPG_A'])
    assert sorted(bf.batches[0]['retried_sgs']) == ['CPG_A', 'CPG_B']


def test_failed_excludes_cancelled_and_cancelled_helper_returns_them(tmp_path: Path):
    """Round-4 policy: `cancel_cohort_run` is user-initiated and must NOT
    inflate the 5% threshold. `failed_sg_names` excludes CANCELLED; a
    sibling `cancelled_sg_names` reports them separately."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
    ])
    bf.record_status(0, 'CANCELLED')
    assert bf.failed_sg_names() == []
    assert sorted(bf.cancelled_sg_names()) == ['CPG_A', 'CPG_B']


def test_cancelled_helper_mixed_with_succeeded(tmp_path: Path):
    """Regression-proof Option 2's resume-after-cancel guard: in a mixed
    SUCCEEDED + CANCELLED batches file, `cancelled_sg_names()` returns ONLY
    the CANCELLED batches' SGs, and `successful_sg_names()` / `failed_sg_names()`
    return cleanly without leaking. The orchestrator guard reads
    `cancelled_sg_names()` and raises `CohortCancelled` on any non-empty
    result — Option 2's terminal-on-any-CANCELLED semantic depends on this
    helper distinguishing CANCELLED from SUCCEEDED correctly even when both
    co-exist in the same batches file."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        Batch(cohort_name='COH0001', batch_index=1, sg_names=['CPG_C', 'CPG_D']),
    ])
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Success'})
    bf.record_status(0, 'SUCCEEDED')
    bf.record_status(1, 'CANCELLED')
    assert sorted(bf.cancelled_sg_names()) == ['CPG_C', 'CPG_D']
    assert sorted(bf.successful_sg_names()) == ['CPG_A', 'CPG_B']
    # Failed list excludes CANCELLED — guard logic at run()'s tail relies on this
    # being the failure-only signal so the 5% threshold doesn't conflate user
    # cancellations with pipeline failures.
    assert bf.failed_sg_names() == []


def test_find_batch_for_sg_prefers_latest_generation(tmp_path: Path):
    """SGs may appear in both an initial batch (gen=0) and a retry batch (gen=1).
    The most recent batch is the relevant one for path resolution."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
    ])
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Fail'})
    bf.add_retry_batch(sg_names=['CPG_B'])
    found = bf.find_batch_for_sg('CPG_B')
    assert found is not None
    assert found['retry_generation'] == 1
    assert found['batch_index'] == 1
```

- [ ] **Step 5: Implement `BatchesFile`**

Append to `src/dragen_align_pa/batches.py`:

```python
import json
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1


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
                    "has_been_retried": bool,          # True once any SG from this batch has been retried (action gate); pre-set True at creation for retry_generation=1 batches
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

    def __init__(self, path: 'cpg_utils.Path | Path'):
        self.path = path
        self.batch_size: int = 0
        self.batches: list[dict[str, Any]] = []

    def initialise(self, batch_size: int, batches: list[Batch]) -> None:
        self.batch_size = batch_size
        self.batches = [self._new_batch_entry(b, retry_generation=0) for b in batches]

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
        """SGs explicitly marked Success in any batch's passfail.

        Asymmetric with `failed_sg_names` by design: success requires positive
        confirmation from `passfail.json`, whereas batch-level FAILED implies
        Fail for every SG in the batch. An SG only appears here once its
        batch's `passfail_seen` is True.
        """
        successful: list[str] = []
        for b in self.batches:
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
```

- [ ] **Step 6: Run all batch tests**

Run: `pytest tests/test_batches.py -v`
Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/dragen_align_pa/batches.py tests/test_batches.py
git commit -m "Add chunk_sgs_into_batches and BatchesFile state-file helper"
```

---

## Task 7: Add per-SG state file helper + `get_ica_sample_folder`

**Files:**
- Modify: `src/dragen_align_pa/utils.py`
- Create: `tests/test_path_resolution.py`

- [ ] **Step 1: Write the failing test against the demo bundle**

Create `tests/test_path_resolution.py`:

```python
import json
from pathlib import Path

from dragen_align_pa.utils import PER_SG_STATE_SCHEMA_VERSION, get_ica_sample_folder


def _write_state(path: Path, **fields) -> None:
    payload = {
        'schema_version': PER_SG_STATE_SCHEMA_VERSION,
        'pipeline_id': '00000000-1111-2222-3333-444444444444',
        'ar_guid': 'test-guid',
        'user_reference': 'COH0001-batch0000_test-guid_',
        'batch_index': 0,
    }
    payload.update(fields)
    path.write_text(json.dumps(payload))


def test_get_ica_sample_folder_renders_expected_path(tmp_path: Path, monkeypatch):
    state_path = tmp_path / 'CPG00001_pipeline_id_and_arguid.json'
    _write_state(state_path)

    def fake_config_retrieve(key, default=None):
        if key == ['ica', 'data_prep', 'output_folder']:
            return 'test-dragen-378'
        return default

    monkeypatch.setattr('dragen_align_pa.utils.config_retrieve', fake_config_retrieve)
    monkeypatch.setattr('dragen_align_pa.utils.BUCKET_NAME', 'cpg-test-dataset-test')

    result = get_ica_sample_folder(state_path, sg_name='CPG00001')
    assert result == '/cpg-test-dataset-test/test-dragen-378/COH0001-batch0000_test-guid_-00000000-1111-2222-3333-444444444444/CPG00001/'


def test_get_ica_sample_folder_rejects_old_schema(tmp_path: Path):
    """Per-SG state files written by older code (no `schema_version`) must
    raise a friendly error so a resume after a deploy doesn't silently
    misresolve paths."""
    state_path = tmp_path / 'CPG00001_pipeline_id_and_arguid.json'
    state_path.write_text(json.dumps({
        'pipeline_id': 'abc',
        'ar_guid': 'xyz',
        # missing schema_version, user_reference, batch_index
    }))
    import pytest
    with pytest.raises(ValueError, match='schema_version'):
        get_ica_sample_folder(state_path, sg_name='CPG00001')


def test_get_ica_sample_folder_raises_on_missing_state(tmp_path: Path):
    import pytest
    with pytest.raises(FileNotFoundError):
        get_ica_sample_folder(tmp_path / 'does-not-exist.json', sg_name='CPG00001')
```

Note: the leading `-` between `user_reference` and `pipeline_id` follows the existing convention because `user_reference` ends with `_` (see Section 4 of the design doc).

- [ ] **Step 2: Run to confirm failure**

Run: `pytest tests/test_path_resolution.py -v`
Expected: ImportError for `get_ica_sample_folder`.

- [ ] **Step 3: Add `get_ica_sample_folder` (and import `BUCKET_NAME`) in `utils.py`**

Open `src/dragen_align_pa/utils.py`. Near the top with the existing imports, add:

```python
import json
```

at the top, and update the `from cpg_utils.config` line to include `config_retrieve`:

```python
from cpg_utils.config import config_retrieve, get_access_level, get_driver_image, output_path
```

Add a new import line:

```python
from dragen_align_pa.constants import BUCKET_NAME, DRAGEN_VERSION
```

(replacing the existing `from dragen_align_pa.constants import DRAGEN_VERSION`).

Then add this function below `get_output_path`:

```python
PER_SG_STATE_SCHEMA_VERSION = 1


def get_ica_sample_folder(pipeline_id_arguid_path: cpg_utils.Path, sg_name: str) -> str:
    """Resolve the ICA folder containing a single SG's batch output.

    Reads the per-SG state file (extended schema with `schema_version`,
    `user_reference`, `pipeline_id`, `batch_index`) and constructs:
        /{bucket}/{output_folder}/{user_reference}-{pipeline_id}/{sg_name}/

    Failure modes:
    - State file missing → `FileNotFoundError` (resume the orchestrator with
      `monitor_previous=true` to repopulate from `{cohort}_batches.json`).
    - State file lacks `schema_version` or has the wrong value →
      `ValueError`. The file was written by an older code path. A vanilla
      rerun of `ManageDragenPipeline` only rewrites per-SG files for batches
      whose status is PENDING/INPROGRESS — files for SUCCEEDED batches are
      left alone. To force rewriting under the new schema:
        (a) Rerun the cohort with `force_resubmit=true` (deletes batches.json
            + every per-SG state file, then re-batches and re-submits), OR
        (b) Manually delete the offending per-SG file so the next resume pass
            re-reads from `{cohort}_batches.json` (the authoritative source).
    - Required key absent under the right schema version → `KeyError`
      naming the missing field.
    - State file present, schema valid, BUT the SG's batch was CANCELLED →
      this helper returns a syntactically valid path that points at an
      ABORTED ICA analysis. The helper has no awareness of batch status.
      In practice this branch is unreachable from production code because
      the orchestrator-level resume-after-cancel guard in
      `manage_dragen_pipeline.run()` raises `CohortCancelled` on ANY
      remaining CANCELLED batches, halting the cohort before downstream
      Download stages run. If a future caller bypasses that guard, the
      subsequent ICA call would fail with "analysis not found".

    Note: per-SG state files are derived projections of `{cohort}_batches.json`
    (see Task 6 BatchesFile docstring for the recovery contract).
    """
    with pipeline_id_arguid_path.open('r') as fh:
        state = json.load(fh)
    version = state.get('schema_version', 0)
    if version != PER_SG_STATE_SCHEMA_VERSION:
        raise ValueError(
            f'Per-SG state file {pipeline_id_arguid_path} has schema_version {version}; '
            f'code expects {PER_SG_STATE_SCHEMA_VERSION}. Rerun the cohort with '
            f'force_resubmit=true (or manually delete the file) to rewrite it under '
            f'the new schema.',
        )
    for required in ('user_reference', 'pipeline_id'):
        if required not in state:
            raise KeyError(
                f'Per-SG state file {pipeline_id_arguid_path} missing required key {required!r}.',
            )
    user_reference = state['user_reference']
    pipeline_id = state['pipeline_id']
    output_folder = config_retrieve(['ica', 'data_prep', 'output_folder'])
    return f'/{BUCKET_NAME}/{output_folder}/{user_reference}-{pipeline_id}/{sg_name}/'
```

- [ ] **Step 4: Run the test**

Run: `pytest tests/test_path_resolution.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dragen_align_pa/utils.py tests/test_path_resolution.py
git commit -m "Add get_ica_sample_folder helper for resolving per-SG ICA paths"
```

---

## Task 8: Add `get_batch_artefacts_path` GCS helper

**Files:**
- Modify: `src/dragen_align_pa/utils.py`
- Modify: `tests/test_path_resolution.py`

- [ ] **Step 1: Add the test**

Append to `tests/test_path_resolution.py`:

```python
from dragen_align_pa.utils import get_batch_artefacts_path


def test_get_batch_artefacts_path(monkeypatch):
    monkeypatch.setattr('dragen_align_pa.utils.DRAGEN_VERSION', 'dragen_3_7_8')

    captured = {}

    def fake_output_path(suffix, category=None):
        captured['suffix'] = suffix
        captured['category'] = category
        return f'gs://test-bucket/{suffix}'

    monkeypatch.setattr('dragen_align_pa.utils.output_path', fake_output_path)

    result = get_batch_artefacts_path(cohort_name='COH0001', batch_index=3)
    assert str(result) == 'gs://test-bucket/ica/dragen_3_7_8/output/dragen_batch_metrics/COH0001_batch0003'
    # `get_batch_artefacts_root` builds the prefix; the per-batch path is
    # constructed by trailing `/{batch_name}` from the cpg_utils.Path operator.
    assert captured['suffix'] == 'ica/dragen_3_7_8/output/dragen_batch_metrics'
```

- [ ] **Step 2: Run to confirm failure**

Run: `pytest tests/test_path_resolution.py::test_get_batch_artefacts_path -v`
Expected: ImportError for `get_batch_artefacts_path`.

- [ ] **Step 3: Implement `get_batch_artefacts_path` and the sibling root helper**

In `src/dragen_align_pa/utils.py`, below `get_output_path`, add (these
delegate to `get_output_path` so the `ica/{DRAGEN_VERSION}/output/` prefix is
constructed in one place and any future change lands there):

```python
def get_batch_artefacts_root(cohort_name: str) -> cpg_utils.Path:  # noqa: ARG001
    """Per-cohort artefacts root under GCS (siblings: passfail/summary/reports per batch).

    The cohort name is currently unused in path construction (all cohorts share
    a `dragen_batch_metrics/` root under the cohort-scoped output prefix), but
    it is in the signature so callers can pass it consistently with
    `get_batch_artefacts_path` and so a future per-cohort namespace change has
    a single place to land.
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
    return get_batch_artefacts_root(cohort_name) / f'{cohort_name}_batch{batch_index:04d}'
```

- [ ] **Step 4: Run**

Run: `pytest tests/test_path_resolution.py -v`
Expected: 4 passed (3 from Task 7 + 1 from Task 8).

- [ ] **Step 5: Commit**

```bash
git add src/dragen_align_pa/utils.py tests/test_path_resolution.py
git commit -m "Add get_batch_artefacts_path GCS helper for per-batch outputs"
```

---

## Task 9: Update config schema in `dragen_align_pa_defaults.toml`

**Files:**
- Modify: `config/dragen_align_pa_defaults.toml`

- [ ] **Step 1: Replace the `[ica.pipelines]`, `[ica.qc]` sections and add `[ica.dragen]`**

Open `config/dragen_align_pa_defaults.toml`. Apply these surgical edits:

Find:

```toml
[ica.qc]
cross_cont_vcf = 'fil.1a7a2d0442854127e5d608da2935b1b0'
coverage_region_1 = 'fil.ad60897bc97e4646f28c08da33ba0a20'
coverage_region_2 = 'fil.c4ade57f5ffe4baff28d08da33ba0a20'
```

Replace with:

```toml
[ica.qc]
cross_cont_vcf = 'fil.1a7a2d0442854127e5d608da2935b1b0'
coverage_region_beds = [
    'fil.ad60897bc97e4646f28c08da33ba0a20',
    'fil.c4ade57f5ffe4baff28d08da33ba0a20',
]
```

Find:

```toml
# The pipeline ID is from ICA and is specific to the exact pipeline being run. This should not need to be changed.
[ica.pipelines]
dragen_version = 'dragen_3_7_8'
# F2 instance CRAM pipeline
cram = 'cbac3d1f-737f-44f2-9a40-f7f2589b5fad'

# F2 instance Fastq pipeline
fastq = '393e2423-3c0d-42f2-aa83-910a48a9c32c'
# Dragen hash table for 3.7.8
dragen_ht_id = 'fil.854d49a151a24edae5d708da2935b1b0'

#md5sum pipeline
md5_pipeline_id = "e767f290-bc60-4281-b11d-6a65c9791253"

# Chunk size for the new streaming MD5 pipeline
[ica.pipelines.md5]
chunk_size = "100"
```

Replace with:

```toml
# The pipeline ID is from ICA and is specific to the exact pipeline being run. This should not need to be changed.
[ica.pipelines]
dragen_version = 'dragen_3_7_8'
# Dragen hash table for 3.7.8
dragen_ht_id = 'fil.854d49a151a24edae5d708da2935b1b0'

#md5sum pipeline
md5_pipeline_id = "e767f290-bc60-4281-b11d-6a65c9791253"

# Chunk size for the new streaming MD5 pipeline
[ica.pipelines.md5]
chunk_size = "100"

# Unified DRAGEN378 pipeline (single pipeline for CRAM/FASTQ, WGS/WES)
[ica.dragen]
pipeline_id = "18a4baab-a12f-415d-ba8e-10b5bf6834d0"
batch_size = 5

# WGS-only flags. Empty preset is fine; cnv_segmentation_mode is divergent vs WES.
[ica.dragen.presets.genome]
cnv_segmentation_mode = "SLM"
additional_args = "--cnv-enable-self-normalization true"
additional_files = []

# WES-only flags. Fill in WES BED + PoN file IDs per cohort.
[ica.dragen.presets.exome]
cnv_segmentation_mode = "HSLM"
additional_args = "--sv-exome true --sv-call-regions-bed <bed-name> --vc-target-bed <bed-name> --cnv-target-bed <bed-name> --cnv-target-factor-threshold 5 --cnv-enable-self-normalization false"
additional_files = []

# Per-run user override hooks. Appended after the preset values.
[ica.dragen.user]
additional_args = ""
additional_files = []
```

- [ ] **Step 2: Verify TOML parses cleanly**

Run: `python -c "import tomllib; tomllib.loads(open('config/dragen_align_pa_defaults.toml','rb').read().decode())"`
Expected: no output (no error).

- [ ] **Step 3: Verify removed keys are gone**

Spec §2 ("Removed keys") requires these to be absent. The block-rewrite above
should drop them, but enforce it explicitly so a future re-order doesn't
silently regress:

```bash
python - <<'PY'
import tomllib

with open('config/dragen_align_pa_defaults.toml', 'rb') as fh:
    cfg = tomllib.load(fh)

removed = [
    ('ica', 'pipelines', 'cram'),
    ('ica', 'pipelines', 'fastq'),
    ('ica', 'qc', 'coverage_region_1'),
    ('ica', 'qc', 'coverage_region_2'),
]
present = []
for key_path in removed:
    node = cfg
    try:
        for k in key_path:
            node = node[k]
        present.append('.'.join(key_path))
    except (KeyError, TypeError):
        pass
assert not present, f'Removed keys still present: {present}'
print('removed-keys check ok')
PY
```

Expected: prints `removed-keys check ok`.

- [ ] **Step 4: Verify a zero-entry `coverage_region_beds` parses (spec allows 0–3 entries)**

```bash
python - <<'PY'
import tomllib

# A user-overlay TOML may legitimately set this to []. Confirm the parser tolerates it.
sample = """
[ica.qc]
cross_cont_vcf = 'fil.x'
coverage_region_beds = []
"""
parsed = tomllib.loads(sample)
assert parsed['ica']['qc']['coverage_region_beds'] == []
print('empty coverage_region_beds parses ok')
PY
```

Expected: prints `empty coverage_region_beds parses ok`.

- [ ] **Step 5: Commit**

```bash
git add config/dragen_align_pa_defaults.toml
git commit -m "Update config schema for unified DRAGEN pipeline (batch, presets, coverage_region_beds)"
```

---

## Task 10: Generalize `manage_ica_pipeline_loop` to accept `Batch` targets + `on_succeeded` callback

**Files:**
- Modify: `src/dragen_align_pa/jobs/ica_pipeline_manager.py` (imports + signature + docstring + helper + three callback-firing sites in the SUCCEEDED / CANCELLED / FAILED_FINAL branches)

- [ ] **Step 1: Widen `ProcessingTarget` typealias**

Open `src/dragen_align_pa/jobs/ica_pipeline_manager.py`. Find:

```python
from dragen_align_pa.jobs import cancel_ica_pipeline_run, monitor_dragen_pipeline
from dragen_align_pa.utils import delete_pipeline_id_file

ProcessingTarget: TypeAlias = Cohort | SequencingGroup
```

Replace with:

```python
from dragen_align_pa.batches import Batch
from dragen_align_pa.jobs import cancel_ica_pipeline_run, monitor_dragen_pipeline
from dragen_align_pa.utils import delete_pipeline_id_file

ProcessingTarget: TypeAlias = Cohort | SequencingGroup | Batch
```

- [ ] **Step 2: Add `on_succeeded` and `on_status_change` parameters**

In the same file, find the function signature:

```python
def manage_ica_pipeline_loop(  # noqa: PLR0915
    targets_to_process: Sequence[ProcessingTarget],
    outputs: dict[str, cpg_utils.Path],
    pipeline_name: str,
    is_mlr_pipeline: bool,
    success_file_key_template: str,
    pipeline_id_file_key_template: str,
    error_log_key: str,
    submit_function_factory: Callable[[str], Callable[[], str]],
    allow_retry: bool,
    sleep_time_seconds: int,
) -> None:
```

Replace with:

```python
def manage_ica_pipeline_loop(  # noqa: PLR0915
    targets_to_process: Sequence[ProcessingTarget],
    outputs: dict[str, cpg_utils.Path],
    pipeline_name: str,
    is_mlr_pipeline: bool,
    success_file_key_template: str,
    pipeline_id_file_key_template: str,
    error_log_key: str,
    submit_function_factory: Callable[[str], Callable[[], str]],
    allow_retry: bool,
    sleep_time_seconds: int,
    on_succeeded: Callable[[MonitoredTarget], None] | None = None,
    on_status_change: Callable[[MonitoredTarget, PipelineStatus], None] | None = None,
) -> None:
```

Note: the loop continues to manage its own per-target `_pipeline_id` file
(read on resume, written at submit, deleted on cancel/abort/fail). That
file lives at `outputs['{target_name}_pipeline_id']` — under DRAGEN this is
`{batch_name}_pipeline_id.json` (registered by Task 16's `expected_outputs`),
distinct from the orchestrator-managed per-SG state file at
`outputs['{sg_name}_pipeline_id_and_arguid']`. The loop and the orchestrator
write to different files; no clobber risk exists.

- [ ] **Step 3: Update the docstring**

In the same function, add this paragraph at the end of the existing docstring (just before the closing triple-quote):

```
        on_succeeded: Optional callback invoked when a target transitions to
                      SUCCEEDED. The callback runs FIRST; only after it returns
                      successfully is `target.set_status(SUCCEEDED)` and the
                      success marker file written. If the callback raises, the
                      target stays INPROGRESS, the next poll cycle re-fires the
                      callback. This makes the SUCCEEDED transition transactional:
                      the orchestrator never persists "the batch finished" if
                      post-success bookkeeping (e.g. downloading passfail.json)
                      failed.

                      Per-batch `error_strategy` is plumbed via the
                      `submit_function_factory` closure: the DRAGEN orchestrator
                      builds a factory that captures the per-batch error_strategy
                      recorded in `{cohort}_batches.json`, so retry batches with
                      `error_strategy='continue'` submit correctly without
                      changing the factory's `Callable[[str], Callable[[], str]]`
                      signature. MLR's factory ignores this dimension entirely.

                      Note: `on_succeeded` receives the `MonitoredTarget` wrapper,
                      not the wrapped `Batch` / `SequencingGroup`. Access
                      `monitored.target.sg_names` (for `Batch`) to reach the
                      underlying domain object.

                      MLR omits `on_succeeded` — its behaviour is unchanged.

        on_status_change: Optional notification callback invoked when a target
                      reaches a TERMINAL non-success status (`FAILED_FINAL` or
                      `CANCELLED`). Fires AFTER `target.set_status(new_status)`,
                      so it's pure notification — exceptions are caught and
                      logged but do not roll back the transition. DRAGEN uses
                      this to mirror the loop's in-memory terminal status into
                      `{cohort}_batches.json` (without this, the batches file
                      would forever say `INPROGRESS` for batches that ICA
                      reported as failed (mapped to `PipelineStatus.FAILED_FINAL`)
                      or that the operator cancelled via `cancel_cohort_run`,
                      breaking the per-sample retry
                      path's `elif b['status'] == 'FAILED':` branch).
                      `SUCCEEDED` is intentionally NOT routed through this
                      callback — that transition is transactional and goes
                      through `on_succeeded` instead.

                      MLR omits this callback (default `None`); its in-memory
                      target state is sufficient because MLR has no equivalent
                      cohort-level state file.
```

- [ ] **Step 4: Wire up the callback at the SUCCEEDED transition (transactional)**

In the same function, find the SUCCEEDED handler:

```python
                elif pipeline_status == 'SUCCEEDED':
                    target.set_status(new_status=PipelineStatus.SUCCEEDED)
                    logger.info(f'{pipeline_name} pipeline {target.pipeline_id} has succeeded for {target_name}')
                    pipeline_success_file = outputs[
                        success_file_key_template.format(target_name=target_name)
                    ]
                    with pipeline_success_file.open('w') as success_file:
                        success_file.write(
                            f'ICA {pipeline_name} pipeline {target.pipeline_id} has succeeded for {target_name}.'
                        )
```

Replace with:

```python
                elif pipeline_status == 'SUCCEEDED':
                    # Transactional SUCCEEDED transition: run the post-success callback
                    # FIRST (e.g. fetch passfail.json + persist into the cohort batches
                    # file). Only when it returns cleanly do we mark the target SUCCEEDED
                    # and write the success marker. If the callback raises, leave state
                    # as INPROGRESS so the next poll cycle re-fires it — this prevents
                    # divergent state where the loop has set SUCCEEDED but the caller's
                    # side-state (e.g. batches.json) was not updated.
                    if on_succeeded is not None:
                        try:
                            on_succeeded(target)
                        except Exception as exc:  # noqa: BLE001
                            logger.error(
                                f'on_succeeded callback failed for {target_name} '
                                f'(pipeline {target.pipeline_id}): {exc}. '
                                f'Leaving status INPROGRESS so the next poll re-fires.',
                            )
                            continue
                    target.set_status(new_status=PipelineStatus.SUCCEEDED)
                    logger.info(f'{pipeline_name} pipeline {target.pipeline_id} has succeeded for {target_name}')
                    pipeline_success_file = outputs[
                        success_file_key_template.format(target_name=target_name)
                    ]
                    with pipeline_success_file.open('w') as success_file:
                        success_file.write(
                            f'ICA {pipeline_name} pipeline {target.pipeline_id} has succeeded for {target_name}.'
                        )
```

- [ ] **Step 5: Fire `on_status_change` at the three terminal non-success transitions**

The loop's existing per-target state-file management (the
`{target_name}_pipeline_id` file at `outputs[pipeline_id_file_key_template]`)
is left unchanged. That file is the loop's own resume marker — distinct from
the orchestrator's per-SG `_pipeline_id_and_arguid` state file. The two live
at different `outputs` keys and never collide; both DRAGEN and MLR rely on
the loop managing the per-target file as today.

This step only adds the `on_status_change` firing — no existing loop I/O is
gated, because the loop's per-target `_pipeline_id` file and the orchestrator's
per-SG `_pipeline_id_and_arguid` file live at distinct `outputs` keys (Task 16
registers them separately). They can't collide.

The new callback is a notification hook — DRAGEN uses it to sync the loop's
in-memory terminal status into `{cohort}_batches.json` so the per-sample
retry path's `elif b['status'] == 'FAILED':` branch becomes reachable and so
a subsequent resume correctly skips terminal batches.

Add a helper inside `manage_ica_pipeline_loop`, above the main
`while not all(is_finished(target) for target in monitored_targets):` loop
(alongside the existing `is_finished` helper):

```python
    def _fire_status_change(target: MonitoredTarget, new_status: PipelineStatus) -> None:
        """Best-effort notification of a terminal transition. Exceptions are
        logged and swallowed — the in-memory transition has already happened
        and we do NOT want to roll it back. Distinct from `on_succeeded`
        (which is transactional for the SUCCEEDED transition)."""
        if on_status_change is None:
            return
        try:
            on_status_change(target, new_status)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                f'on_status_change callback failed for {target.name} '
                f'(new_status={new_status.name}, pipeline {target.pipeline_id}): {exc}. '
                f'Continuing — in-memory transition stands.',
            )
```

Find the cancel path (around `ica_pipeline_manager.py:169-176`):

```python
            # Cancel a pipeline if requested
            if config_retrieve(key=['ica', 'management', 'cancel_cohort_run'], default=False) and target.pipeline_id:
                logger.info(f'Cancelling {pipeline_name} pipeline run: {target.pipeline_id} for {target_name}')
                cancel_ica_pipeline_run.run(
                    ica_pipeline_id=target.pipeline_id,
                    is_mlr=is_mlr_pipeline,
                )
                delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))
                target.set_status(PipelineStatus.CANCELLED)
```

Replace with (adds the callback fire after the status transition):

```python
            # Cancel a pipeline if requested
            if config_retrieve(key=['ica', 'management', 'cancel_cohort_run'], default=False) and target.pipeline_id:
                logger.info(f'Cancelling {pipeline_name} pipeline run: {target.pipeline_id} for {target_name}')
                cancel_ica_pipeline_run.run(
                    ica_pipeline_id=target.pipeline_id,
                    is_mlr=is_mlr_pipeline,
                )
                delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))
                target.set_status(PipelineStatus.CANCELLED)
                _fire_status_change(target, PipelineStatus.CANCELLED)
```

Find the ABORTING/ABORTED branch (around `ica_pipeline_manager.py:210-214`):

```python
                elif pipeline_status in ['ABORTING', 'ABORTED']:
                    logger.info(f'{pipeline_name} pipeline {target.pipeline_id} has been cancelled for {target_name}.')
                    target.set_status(new_status=PipelineStatus.CANCELLED)
                    target.pipeline_id = None
                    delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))
```

Replace with:

```python
                elif pipeline_status in ['ABORTING', 'ABORTED']:
                    logger.info(f'{pipeline_name} pipeline {target.pipeline_id} has been cancelled for {target_name}.')
                    target.set_status(new_status=PipelineStatus.CANCELLED)
                    target.pipeline_id = None
                    delete_pipeline_id_file(pipeline_id_file=str(pipeline_id_arguid_file))
                    _fire_status_change(target, PipelineStatus.CANCELLED)
```

Find the FAILED-final branch (around the existing `else` after `if target.allow_retry and not target.has_been_retried:`):

```python
                    else:
                        target.set_status(PipelineStatus.FAILED_FINAL)
                        logger.error(
                            f'{target_name} failed {pipeline_name} pipeline {target.pipeline_id} and '
                            f'retry is not allowed or already attempted.'
```

Replace with:

```python
                    else:
                        target.set_status(PipelineStatus.FAILED_FINAL)
                        _fire_status_change(target, PipelineStatus.FAILED_FINAL)
                        logger.error(
                            f'{target_name} failed {pipeline_name} pipeline {target.pipeline_id} and '
                            f'retry is not allowed or already attempted.'
```

`FAILED_RETRYING` is intentionally NOT fired — the target stays in-flight,
not terminal.

- [ ] **Step 6: Verify import works**

Run: `python -c "from dragen_align_pa.jobs.ica_pipeline_manager import manage_ica_pipeline_loop, ProcessingTarget; print(ProcessingTarget)"`
Expected: prints the union including `Batch`.

- [ ] **Step 7: Verify MLR call site still works**

Open `src/dragen_align_pa/jobs/manage_dragen_mlr.py` and confirm the call to
`manage_ica_pipeline_loop` does not pass `on_succeeded` or `on_status_change`
— both should continue using their defaults (`None`), so MLR's existing
per-SG state-file management is preserved bit-for-bit.

Run: `python -c "import dragen_align_pa.jobs.manage_dragen_mlr"` (this imports but does not execute)
Expected: no errors.

- [ ] **Step 8: Commit**

```bash
git add src/dragen_align_pa/jobs/ica_pipeline_manager.py
git commit -m "Generalize manage_ica_pipeline_loop to accept Batch targets and on_succeeded callback"
```

---

## Task 11: Create `submit_dragen_batch.py` — common helpers and parameter assembly

**Files:**
- Create: `src/dragen_align_pa/jobs/submit_dragen_batch.py`

- [ ] **Step 1: Sketch the module skeleton and the common-args constant**

Create `src/dragen_align_pa/jobs/submit_dragen_batch.py`:

```python
"""Submit one batch of SGs to the unified DRAGEN ICA pipeline.

This replaces the per-SG submission logic in the old `run_align_genotype_with_dragen.py`.
The new pipeline (`DRAGEN378-custom-unified-F2-v1`, id 18a4baab-…) accepts a list of
samples per analysis and is configured via top-level parameters + an `additional_args`
string. Per-sample retry is orchestrated by the caller, not here.
"""

import io
import json
import re

import cpg_utils
import pandas as pd
import requests
from cpg_utils.config import config_retrieve, try_get_ar_guid
from icasdk.apis.tags import project_analysis_api, project_data_api
from icasdk.model.analysis_data_input import AnalysisDataInput
from icasdk.model.analysis_parameter_input import AnalysisParameterInput
from icasdk.model.analysis_tag import AnalysisTag
from icasdk.model.create_nextflow_analysis import CreateNextflowAnalysis
from icasdk.model.nextflow_analysis_input import NextflowAnalysisInput
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.batches import Batch
from dragen_align_pa.constants import BUCKET_NAME

# DRAGEN flags that don't depend on input type (CRAM vs FASTQ) or sequencing type (WGS vs WES).
# Sourced from the production CRAM-mode preset in the legacy submitter — anything WGS/WES-divergent
# is instead carried in [ica.dragen.presets.{genome,exome}] in config.
_COMMON_ADDITIONAL_ARGS = (
    "--read-trimmers polyg "
    "--soft-read-trimmers none "
    "--vc-hard-filter 'DRAGENHardQUAL:all:QUAL<5.0;LowDepth:all:DP<=1' "
    '--vc-frd-max-effective-depth 40 '
    '--vc-enable-joint-detection true '
    '--qc-coverage-ignore-overlaps true '
    '--qc-coverage-count-soft-clipped-bases true '
    '--qc-coverage-reports-1 cov_report,cov_report '
    "--qc-coverage-filters-1 'mapq<1,bq<0,mapq<1,bq<0' "
    '--vc-gvcf-gq-bands 10 20 30 40 '
    '--vc-emit-ref-confidence GVCF '
    '--vc-enable-vcf-output false '
    '--enable-map-align-output true '
    '--enable-duplicate-marking true '
    '--enable-cyp2d6 true '
    '--repeat-genotype-enable true '
)


_PRESET_PLACEHOLDER_RE = re.compile(r'<[a-zA-Z][a-zA-Z0-9_-]*>')


def _build_additional_args() -> str:
    """Concatenate common + sequencing-type preset + user override into one args string.

    Raises if any `<placeholder>` sentinel survives in the final string (e.g. the
    WES preset shipping `<bed-name>` defaults that weren't filled in for this run).
    """
    sequencing_type = config_retrieve(['workflow', 'sequencing_type'])
    if sequencing_type not in {'genome', 'exome'}:
        raise ValueError(
            f"workflow.sequencing_type must be 'genome' or 'exome', got {sequencing_type!r}",
        )
    preset = config_retrieve(['ica', 'dragen', 'presets', sequencing_type], default=None)
    if preset is None:
        raise ValueError(
            f'Missing config section [ica.dragen.presets.{sequencing_type}]; add it to your TOML.',
        )
    user = config_retrieve(['ica', 'dragen', 'user'], default={'additional_args': '', 'additional_files': []})

    # Join with explicit spaces. Empty parts are filtered out before the join so
    # we don't end up with double spaces when a preset or user override is "".
    parts = [
        _COMMON_ADDITIONAL_ARGS.strip(),
        f"--cnv-segmentation-mode {preset['cnv_segmentation_mode']}",
        preset.get('additional_args', '').strip(),
        user.get('additional_args', '').strip(),
    ]
    assembled = ' '.join(part for part in parts if part)

    placeholders = _PRESET_PLACEHOLDER_RE.findall(assembled)
    if placeholders:
        raise ValueError(
            f"DRAGEN additional_args contains unfilled placeholders {placeholders} from "
            f"[ica.dragen.presets.{sequencing_type}]. Fill them in your config before running.",
        )
    return assembled


def _build_top_level_parameters(error_strategy: str = 'auto') -> list[AnalysisParameterInput]:
    """Top-level pipeline parameters sent on every run.

    `error_strategy` is overridable so the orchestrator can pass `continue` for
    single-sample retry batches (where the default `auto` would terminate).
    """
    return [
        AnalysisParameterInput(code='enable_map_align', value='true'),
        AnalysisParameterInput(code='output_format', value='CRAM'),
        AnalysisParameterInput(code='enable_variant_caller', value='true'),
        AnalysisParameterInput(code='enable_sv', value='true'),
        AnalysisParameterInput(code='enable_cnv', value='true'),
        AnalysisParameterInput(code='dragen_reports', value='false'),
        AnalysisParameterInput(code='error_strategy', value=error_strategy),
        AnalysisParameterInput(code='additional_args', value=_build_additional_args()),
    ]
```

- [ ] **Step 2: Verify the module imports**

Run: `python -c "from dragen_align_pa.jobs.submit_dragen_batch import _build_top_level_parameters; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 3: Commit**

```bash
git add src/dragen_align_pa/jobs/submit_dragen_batch.py
git commit -m "Add submit_dragen_batch.py skeleton with shared parameter builders"
```

---

## Task 12: Add CRAM-mode and FASTQ-mode data-input builders in `submit_dragen_batch.py`

**Files:**
- Modify: `src/dragen_align_pa/jobs/submit_dragen_batch.py`

- [ ] **Step 1: Append CRAM-mode helper**

Append to `submit_dragen_batch.py`:

```python
def _build_cram_data_inputs(
    batch: Batch,
    per_sg_state_paths: dict[str, cpg_utils.Path],
) -> tuple[list[AnalysisDataInput], list[str]]:
    """Construct ICA data inputs for a CRAM-mode batch.

    `per_sg_state_paths[sg_name]` points at `{sg}_fids.json` (output of UploadDataToIca),
    each containing `{'cram_fid': 'fil.…'}`. We pass the list of CRAM file IDs in one batch.

    Returns `(data_inputs, cram_fids)` so the caller can persist `cram_fids` into the
    BatchesFile entry for audit / future cleanup.
    """
    cram_fids: list[str] = []
    for sg_name in batch.sg_names:
        state_path = per_sg_state_paths[sg_name]
        with state_path.open('r') as fh:
            sg_state = json.load(fh)
        if 'cram_fid' not in sg_state:
            raise ValueError(f"Missing 'cram_fid' in {state_path}")
        cram_fids.append(sg_state['cram_fid'])

    # Resolve the configured CRAM-reference folder ID. Two-step lookup matches today's
    # convention: `ica.cram_references.old_cram_reference` points at a key in
    # `[ica.cram_references]` (e.g. "dragmap" or "gatk") whose value is the folder ID.
    selected_ref: str | None = config_retrieve(
        ['ica', 'cram_references', 'old_cram_reference'], default=None,
    )
    if not selected_ref:
        raise ValueError(
            'Config missing ica.cram_references.old_cram_reference — cannot select a CRAM '
            'reference folder for batch submission. Set it to the name of an entry under '
            '[ica.cram_references] (e.g. "dragmap" or "gatk").',
        )
    cram_reference_id: str | None = config_retrieve(
        ['ica', 'cram_references', selected_ref], default=None,
    )
    if not cram_reference_id:
        raise ValueError(
            f'Config ica.cram_references.{selected_ref} is unset — '
            f'add the ICA folder ID for the {selected_ref!r} CRAM reference.',
        )

    return (
        [
            AnalysisDataInput(parameterCode='crams', dataIds=cram_fids),
            AnalysisDataInput(parameterCode='cram_reference', dataIds=[cram_reference_id]),
        ],
        cram_fids,
    )
```

- [ ] **Step 2: Append FASTQ-mode helper**

Append to `submit_dragen_batch.py`:

```python
def _read_fastq_ids(fastq_ids_path: cpg_utils.Path) -> pd.DataFrame:
    """Reads `{cohort}_fastq_ids.txt` (two whitespace-separated columns: ICA id, FASTQ name)."""
    with fastq_ids_path.open() as fh:
        return pd.read_csv(
            fh,
            sep=r'\s+',
            header=None,
            names=['ica_id', 'fastq_name'],
            dtype={'ica_id': str, 'fastq_name': str},
        )


def _load_per_sg_fastq_lists(
    sg_names: list[str],
    per_sg_fastq_list_paths: dict[str, cpg_utils.Path],
) -> tuple[list[str], pd.DataFrame]:
    """Loads per-SG FASTQ list CSVs (output of MakeFastqFileList) and returns:
    - the concatenated DataFrame (one CSV per batch),
    - the union of all FASTQ filenames referenced across those CSVs.

    Raises:
        ValueError: if any per-SG CSV has a different column header than the
                    first one (`pd.concat` would otherwise silently fill the
                    missing columns with NaN and produce a malformed batch CSV).
        ValueError: if a SG's CSV has zero data rows — the batch would silently
                    omit that SG's reads and surface only as a passfail Fail
                    much later.
    """
    fastq_filenames: set[str] = set()
    frames: list[pd.DataFrame] = []
    expected_columns: list[str] | None = None
    required_columns = {'Read1File', 'Read2File'}
    for sg_name in sg_names:
        path = per_sg_fastq_list_paths[sg_name]
        with path.open() as fh:
            df = pd.read_csv(fh)
        if expected_columns is None:
            expected_columns = list(df.columns)
            # Schema sanity-check on the first CSV: if MakeFastqFileList ever
            # renames its required columns, this surfaces immediately rather
            # than as a `KeyError` on `df['Read1File']` later.
            missing_required = required_columns - set(expected_columns)
            if missing_required:
                raise ValueError(
                    f'FASTQ list for SG {sg_name} at {path} is missing required '
                    f'columns {missing_required} (got {expected_columns}). '
                    f'MakeFastqFileList must emit Read1File / Read2File.',
                )
        elif list(df.columns) != expected_columns:
            raise ValueError(
                f'FASTQ list header mismatch for SG {sg_name} in {path}: '
                f'expected {expected_columns}, got {list(df.columns)}. '
                f'All per-SG CSVs must share the same column shape.',
            )
        if df.empty:
            raise ValueError(
                f'FASTQ list for SG {sg_name} at {path} has zero data rows; '
                f'the combined batch CSV would silently omit this SG.',
            )
        frames.append(df)
        fastq_filenames.update(df['Read1File'].tolist())
        fastq_filenames.update(df['Read2File'].tolist())
    combined = pd.concat(frames, ignore_index=True)
    return sorted(fastq_filenames), combined


def _upload_per_batch_fastq_list(
    api_instance: project_data_api.ProjectDataApi,
    project_id: str,
    cohort_name: str,
    batch_index: int,
    combined_csv: pd.DataFrame,
) -> str:
    """Materialise the per-batch FASTQ list CSV in-memory and upload it to ICA.

    Filename pattern: `{cohort}_batch{NN}_fastq_list.csv`.
    Returns the ICA file ID of the uploaded CSV.
    """
    file_name = f'{cohort_name}_batch{batch_index:04d}_fastq_list.csv'
    folder_path = f'/{BUCKET_NAME}/{config_retrieve(["ica", "data_prep", "output_folder"])}/{cohort_name}'

    file_id, file_status = ica_utils.create_upload_object_id(
        api_instance=api_instance,
        path_params={'projectId': project_id},
        sg_name=cohort_name,
        file_name=file_name,
        folder_path=folder_path,
        object_type='FILE',
    )

    if file_status == 'AVAILABLE':
        logger.info(f"FASTQ list {file_name} is 'AVAILABLE'. Skipping upload.")
        return file_id

    upload_url: str = api_instance.create_upload_url_for_data(
        path_params={'projectId': project_id, 'dataId': file_id},
    ).body['url']

    buffer = io.BytesIO()
    combined_csv.to_csv(buffer, index=False)
    buffer.seek(0)

    response = requests.put(url=upload_url, data=buffer, timeout=300)
    response.raise_for_status()
    logger.info(f'Uploaded per-batch FASTQ list {file_name} (file ID {file_id})')
    return file_id


def _build_fastq_data_inputs(
    api_instance: project_data_api.ProjectDataApi,
    project_id: str,
    batch: Batch,
    fastq_ids_path: cpg_utils.Path,
    per_sg_fastq_list_paths: dict[str, cpg_utils.Path],
) -> tuple[list[AnalysisDataInput], str]:
    """Construct ICA data inputs for a FASTQ-mode batch.

    Returns (data_inputs, fastq_list_fid). The per-batch combined CSV is uploaded inline.
    """
    sg_fastq_names, combined_csv = _load_per_sg_fastq_lists(batch.sg_names, per_sg_fastq_list_paths)
    fastq_ids_df = _read_fastq_ids(fastq_ids_path)
    matched = fastq_ids_df[fastq_ids_df['fastq_name'].isin(sg_fastq_names)]
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

- [ ] **Step 2 (verify): Module still imports**

Run: `python -c "from dragen_align_pa.jobs.submit_dragen_batch import _build_cram_data_inputs, _build_fastq_data_inputs; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 3: Commit**

```bash
git add src/dragen_align_pa/jobs/submit_dragen_batch.py
git commit -m "Add CRAM-mode and FASTQ-mode data-input builders to submit_dragen_batch"
```

---

## Task 13: Add the public `submit_dragen_batch.run` entrypoint

**Files:**
- Modify: `src/dragen_align_pa/jobs/submit_dragen_batch.py`

- [ ] **Step 1: Append the public entrypoint**

Append to `submit_dragen_batch.py`:

```python
def _build_common_data_inputs() -> list[AnalysisDataInput]:
    dragen_ht_id: str = config_retrieve(['ica', 'pipelines', 'dragen_ht_id'])
    coverage_region_beds: list[str] = config_retrieve(['ica', 'qc', 'coverage_region_beds'], default=[])
    cross_cont_vcf: str | None = config_retrieve(['ica', 'qc', 'cross_cont_vcf'], default=None)

    preset_files = config_retrieve(
        ['ica', 'dragen', 'presets', config_retrieve(['workflow', 'sequencing_type']), 'additional_files'],
        default=[],
    )
    user_files = config_retrieve(['ica', 'dragen', 'user', 'additional_files'], default=[])
    additional_files = list(preset_files) + list(user_files)

    inputs: list[AnalysisDataInput] = [AnalysisDataInput(parameterCode='ref_tar', dataIds=[dragen_ht_id])]
    if coverage_region_beds:
        inputs.append(AnalysisDataInput(parameterCode='qc_coverage_region_beds', dataIds=coverage_region_beds))
    if cross_cont_vcf:
        inputs.append(AnalysisDataInput(parameterCode='qc_cross_cont_vcf', dataIds=[cross_cont_vcf]))
    if additional_files:
        inputs.append(AnalysisDataInput(parameterCode='additional_files', dataIds=additional_files))
    return inputs


def run(
    batch: Batch,
    analysis_output_fid_path: cpg_utils.Path,
    cram_state_paths: dict[str, cpg_utils.Path] | None,
    fastq_ids_path: cpg_utils.Path | None,
    per_sg_fastq_list_paths: dict[str, cpg_utils.Path] | None,
    error_strategy: str = 'auto',
    ar_guid_override: str | None = None,
) -> dict[str, str | list[str]]:
    """Submit one batch to the unified DRAGEN pipeline.

    Returns a dict containing:
        pipeline_id: ICA analysis ID (str)
        ar_guid: analysis-runner GUID (str)
        user_reference: the user_reference assembled for this batch (str)
        error_strategy: the value submitted to ICA (str)
        fastq_list_fid: only set in FASTQ mode (str)
        cram_fids: only set in CRAM mode (list[str])

    Caller is responsible for persisting the result into the cohort batches file.

    Persistence boundary (caller contract):

    1. `run` does NOT touch state files. It returns the submission identity to
       the caller. If `run` raises (network blip, ICA 5xx, etc.), no state has
       been written and the caller can safely retry.
    2. If `submit_nextflow_analysis` returns successfully, an ICA analysis
       exists. If the caller then crashes BEFORE persisting per-SG state files
       + `{cohort}_batches.json`, the next orchestrator pass sees status
       PENDING and re-submits — generating a new pipeline_id and orphaning
       the previous analysis (it will eventually be cleaned up by ICA's
       retention policy). This is intentional: we prefer at-least-once
       submission with idempotent reconciliation over a multi-write
       transaction.
    3. The caller (`manage_dragen_pipeline.py::_build_submit_callable`)
       persists in this order: per-SG state files first (best-effort
       projections of batches.json) → `batches.json` (the commit point).
       See Task 15 for the rationale.
    """
    secrets = ica_api_utils.get_ica_secrets()
    project_id: str = secrets['projectID']

    with analysis_output_fid_path.open('r') as fh:
        analysis_output_fid: str = json.load(fh)['analysis_output_fid']

    if ar_guid_override is not None:
        # `force_resubmit` lifts the prior batch's AR GUID out of per-SG state
        # so the new submission keeps the same ICA folder identity (spec §4 line 213).
        # `is not None` (not truthiness) so an empty-string override raises clearly
        # rather than silently falling through to the env GUID.
        if not ar_guid_override:
            raise ValueError(
                f'ar_guid_override was empty string for batch {batch.name}; '
                f'callers should pass None to use the env GUID.',
            )
        ar_guid = ar_guid_override
        logger.info(f'Reusing preserved AR GUID for batch {batch.name}: {ar_guid}')
    else:
        ar_guid = try_get_ar_guid()
        if not ar_guid:
            raise RuntimeError(
                'try_get_ar_guid() returned None/empty — analysis-runner GUID is missing from env. '
                'This breaks ICA folder naming and per-SG state files. Refusing to submit.',
            )
    user_reference = f'{batch.name}_{ar_guid}_'

    pipeline_id_config: str = config_retrieve(['ica', 'dragen', 'pipeline_id'])
    user_tags: list[str] = config_retrieve(['ica', 'tags', 'user_tags'])
    technical_tags: list[str] = config_retrieve(['ica', 'tags', 'technical_tags'])
    reference_tags: list[str] = config_retrieve(['ica', 'tags', 'reference_tags'])

    with ica_api_utils.get_ica_api_client() as api_client:
        analysis_api = project_analysis_api.ProjectAnalysisApi(api_client)
        data_api = project_data_api.ProjectDataApi(api_client)

        common_data_inputs = _build_common_data_inputs()
        fastq_list_fid: str | None = None
        cram_fids: list[str] | None = None

        if cram_state_paths is not None:
            specific_data_inputs, cram_fids = _build_cram_data_inputs(
                batch=batch, per_sg_state_paths=cram_state_paths,
            )
        elif fastq_ids_path is not None and per_sg_fastq_list_paths is not None:
            specific_data_inputs, fastq_list_fid = _build_fastq_data_inputs(
                api_instance=data_api,
                project_id=project_id,
                batch=batch,
                fastq_ids_path=fastq_ids_path,
                per_sg_fastq_list_paths=per_sg_fastq_list_paths,
            )
        else:
            raise ValueError(f'submit_dragen_batch: no valid input mode for batch {batch.name}')

        body = CreateNextflowAnalysis(
            userReference=user_reference,
            pipelineId=pipeline_id_config,
            tags=AnalysisTag(
                technicalTags=technical_tags,
                userTags=user_tags,
                referenceTags=reference_tags,
            ),
            outputParentFolderId=analysis_output_fid,
            analysisInput=NextflowAnalysisInput(
                inputs=common_data_inputs + specific_data_inputs,
                parameters=_build_top_level_parameters(error_strategy=error_strategy),
            ),
        )
        analysis_id = ica_api_utils.submit_nextflow_analysis(
            api_instance=analysis_api,
            path_params={'projectId': project_id},
            body=body,
        )

    logger.info(f'Submitted DRAGEN batch {batch.name} → ICA analysis {analysis_id}')
    result: dict[str, str | list[str]] = {
        'pipeline_id': analysis_id,
        'ar_guid': ar_guid,
        'user_reference': user_reference,
        'error_strategy': error_strategy,
    }
    if fastq_list_fid is not None:
        result['fastq_list_fid'] = fastq_list_fid
    if cram_fids is not None:
        result['cram_fids'] = cram_fids
    return result
```

- [ ] **Step 2: Smoke-check imports**

Run: `python -c "from dragen_align_pa.jobs.submit_dragen_batch import run; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 3: Commit**

```bash
git add src/dragen_align_pa/jobs/submit_dragen_batch.py
git commit -m "Add submit_dragen_batch.run public entrypoint"
```

---

## Task 14: Add `parse_passfail.py` to download + parse `passfail.json` per batch

**Files:**
- Create: `src/dragen_align_pa/jobs/parse_passfail.py`
- Create: `tests/test_passfail_parsing.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_passfail_parsing.py`:

```python
import json
from pathlib import Path

from dragen_align_pa.jobs.parse_passfail import parse_passfail_file


def test_parse_passfail_all_success(demo_bundle: Path):
    result = parse_passfail_file(demo_bundle / 'passfail.json')
    assert result == {'CPG00001': 'Success', 'CPG00002': 'Success'}


def test_parse_passfail_with_failure(demo_bundle_with_failure: Path):
    result = parse_passfail_file(demo_bundle_with_failure / 'passfail.json')
    assert result == {'CPG00001': 'Success', 'CPG00002': 'Fail'}
```

- [ ] **Step 2: Run to confirm failure**

Run: `pytest tests/test_passfail_parsing.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement `parse_passfail.py`**

Create `src/dragen_align_pa/jobs/parse_passfail.py`:

```python
"""Download and parse the per-batch `passfail.json` from ICA.

`passfail.json` is at the batch's analysis-output root and maps
`sample_id → "Success" | "Fail"`. In our pipeline `sample_id` == `sg_name`:
- FASTQ mode: `MakeFastqFileList` writes RGSM = SG name in every row.
- CRAM mode: the original CRAM's RG SM tag is preserved through the unified
  pipeline's input handling (validated in design doc §7 "Open items deferred
  to implementation" and confirmed during the small-cohort validation runs
  in Task 25 step V3 — if a CRAM cohort surfaces RGSM != sg_name, the
  defensive filter in `_on_succeeded` warns and drops the unexpected keys
  before they reach the retry path).
"""

import json
from pathlib import Path

import cpg_utils
import icasdk
import requests
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils


def parse_passfail_file(path: Path | cpg_utils.Path) -> dict[str, str]:
    """Load a passfail.json file from disk and return the {sample_id: status} mapping."""
    with path.open('r') as fh:
        return json.load(fh)


def fetch_passfail_from_ica(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    ica_folder_path: str,
) -> dict[str, str] | None:
    """Fetch passfail.json from an ICA folder and parse it in-memory.

    Returns None if passfail.json is not present in the folder OR if any
    transient ICA/network error prevents the fetch — callers (the
    transactional `on_succeeded` in Task 15) treat None as "retry next poll".
    The file is small (KB-scale), so we never stage to GCS or disk.

    Failure handling:
    - `FileNotFoundError` from the lookup → return None (legitimate: a
      catastrophically-failed batch may not have produced passfail.json).
    - `icasdk.ApiException` from lookup, URL minting, or any other ICA call
      → log a warning and return None.
    - `requests.RequestException` from the GET (network, timeout, etc.) →
      log a warning and return None.
    - `requests.HTTPError` with 403 → presigned URL expired between
      minting and reading; mint a fresh URL once and retry. Any second
      403 → log and return None.
    """
    try:
        file_id = ica_api_utils.find_file_id_by_name(
            api_instance=api_instance,
            path_parameters=path_parameters,
            parent_folder_path=ica_folder_path,
            file_name='passfail.json',
        )
    except FileNotFoundError:
        return None
    except icasdk.ApiException as e:
        logger.warning(f'ICA API error finding passfail.json in {ica_folder_path}: {e}')
        return None

    def _mint_and_fetch() -> requests.Response:
        url_response = api_instance.create_download_url_for_data(
            path_params=path_parameters | {'dataId': file_id},
        )
        download_url: str = url_response.body['url']
        return requests.get(download_url, timeout=60)

    try:
        response = _mint_and_fetch()
        if response.status_code == 403:
            # Presigned URL expired between minting and reading; mint a fresh one.
            logger.warning(
                f'passfail.json presigned URL returned 403 for {ica_folder_path}; re-minting and retrying once.',
            )
            response = _mint_and_fetch()
        response.raise_for_status()
    except icasdk.ApiException as e:
        logger.warning(f'ICA API error minting download URL for passfail.json in {ica_folder_path}: {e}')
        return None
    except requests.RequestException as e:
        logger.warning(f'Network error fetching passfail.json from {ica_folder_path}: {e}')
        return None

    logger.info(f'Fetched passfail.json from {ica_folder_path}')
    return response.json()
```

Note: `find_file_id_by_name` scopes its lookup to `parent_folder_path`, which
the caller (`_on_succeeded`) sets to the batch's specific analysis-output
folder — so a stray `passfail.json` from another batch can never match.

- [ ] **Step 4: Run the tests**

Run: `pytest tests/test_passfail_parsing.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/dragen_align_pa/jobs/parse_passfail.py tests/test_passfail_parsing.py
git commit -m "Add parse_passfail.py for reading per-batch DRAGEN passfail.json"
```

## Task 14b: Unit tests for placeholder detection in additional_args

**Files:**
- Create: `tests/test_submit_dragen_batch.py`

- [ ] **Step 1: Write tests**

Create `tests/test_submit_dragen_batch.py`:

```python
import pytest

from dragen_align_pa.jobs import submit_dragen_batch


def _config_factory(sequencing_type='genome', preset_args='', user_args=''):
    """Returns a fake `config_retrieve` that exposes the bits `_build_additional_args` needs."""
    cfg = {
        ('workflow', 'sequencing_type'): sequencing_type,
        ('ica', 'dragen', 'presets', sequencing_type): {
            'cnv_segmentation_mode': 'SLM' if sequencing_type == 'genome' else 'HSLM',
            'additional_args': preset_args,
            'additional_files': [],
        },
        ('ica', 'dragen', 'user'): {'additional_args': user_args, 'additional_files': []},
    }

    def fake_retrieve(key, default=None):
        return cfg.get(tuple(key), default)

    return fake_retrieve


def test_build_additional_args_genome(monkeypatch):
    monkeypatch.setattr(submit_dragen_batch, 'config_retrieve', _config_factory())
    result = submit_dragen_batch._build_additional_args()
    assert '--cnv-segmentation-mode SLM' in result


def test_build_additional_args_includes_hardcoded_common(monkeypatch):
    """Regression guard: every common DRAGEN flag from spec §2 must appear
    in the assembled output. A future refactor that drops one of these would
    silently produce wrong CRAMs/gVCFs."""
    monkeypatch.setattr(submit_dragen_batch, 'config_retrieve', _config_factory())
    result = submit_dragen_batch._build_additional_args()
    for required_flag in (
        '--read-trimmers polyg',
        '--soft-read-trimmers none',
        '--vc-frd-max-effective-depth 40',
        '--vc-enable-joint-detection true',
        '--qc-coverage-ignore-overlaps true',
        '--qc-coverage-count-soft-clipped-bases true',
        '--qc-coverage-reports-1 cov_report,cov_report',
        '--vc-gvcf-gq-bands 10 20 30 40',
        '--vc-emit-ref-confidence GVCF',
        '--vc-enable-vcf-output false',
        '--enable-map-align-output true',
        '--enable-duplicate-marking true',
        '--enable-cyp2d6 true',
        '--repeat-genotype-enable true',
    ):
        assert required_flag in result, f'Missing required hardcoded flag: {required_flag!r}'


def test_build_additional_args_user_appended_last(monkeypatch):
    monkeypatch.setattr(
        submit_dragen_batch, 'config_retrieve',
        _config_factory(preset_args='--cnv-enable-self-normalization true', user_args='--foo bar'),
    )
    result = submit_dragen_batch._build_additional_args()
    # User args appended *after* preset args.
    assert result.index('--cnv-enable-self-normalization') < result.index('--foo bar')


def test_build_additional_args_rejects_placeholder(monkeypatch):
    monkeypatch.setattr(
        submit_dragen_batch, 'config_retrieve',
        _config_factory(sequencing_type='exome', preset_args='--sv-call-regions-bed <bed-name>'),
    )
    with pytest.raises(ValueError, match='placeholder'):
        submit_dragen_batch._build_additional_args()


def test_build_additional_args_rejects_missing_preset(monkeypatch):
    monkeypatch.setattr(
        submit_dragen_batch, 'config_retrieve',
        lambda key, default=None: 'genome' if key == ['workflow', 'sequencing_type'] else default,
    )
    with pytest.raises(ValueError, match=r'Missing config section'):
        submit_dragen_batch._build_additional_args()


def test_build_additional_args_rejects_invalid_sequencing_type(monkeypatch):
    monkeypatch.setattr(submit_dragen_batch, 'config_retrieve', lambda key, default=None: 'transcriptome')
    with pytest.raises(ValueError, match='must be'):
        submit_dragen_batch._build_additional_args()
```

- [ ] **Step 2: Run**

Run: `pytest tests/test_submit_dragen_batch.py -v`
Expected: all 6 tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_submit_dragen_batch.py
git commit -m "Add unit tests for submit_dragen_batch additional_args assembly + placeholder detection"
```

---

## Task 14c: Unit tests for retry-batch orchestration

**Files:**
- Create: `tests/test_manage_dragen_pipeline.py`

- [ ] **Step 1: Write the tests**

Create `tests/test_manage_dragen_pipeline.py`:

```python
from pathlib import Path

from dragen_align_pa.batches import Batch, BatchesFile
from dragen_align_pa.jobs.manage_dragen_pipeline import _build_retry_batches


def _make_file(tmp_path: Path, batches: list[Batch]) -> BatchesFile:
    bf = BatchesFile(path=tmp_path / 'COH0001_batches.json')
    bf.initialise(batch_size=5, batches=batches)
    bf.write()
    return bf


def test_retry_batches_empty_when_no_failures(tmp_path: Path):
    bf = _make_file(tmp_path, [Batch('COH0001', 0, ['CPG_A', 'CPG_B'])])
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Success'})
    new = _build_retry_batches(cohort_name='COH0001', batches_file=bf, batch_size=5)
    assert new == []


def test_retry_batches_from_passfail_failure(tmp_path: Path):
    bf = _make_file(tmp_path, [Batch('COH0001', 0, ['CPG_A', 'CPG_B'])])
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Fail'})
    new = _build_retry_batches(cohort_name='COH0001', batches_file=bf, batch_size=5)
    assert len(new) == 1
    assert new[0].sg_names == ['CPG_B']
    assert new[0].batch_index == 1
    assert bf.batches[0]['has_been_retried'] is True
    # Retry batch entry is appended, pre-marked has_been_retried=True and retry_generation=1.
    assert bf.batches[1]['retry_generation'] == 1
    assert bf.batches[1]['has_been_retried'] is True


def test_retry_batches_single_sample_uses_continue_strategy(tmp_path: Path):
    bf = _make_file(tmp_path, [Batch('COH0001', 0, ['CPG_A', 'CPG_B'])])
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Fail'})
    _build_retry_batches(cohort_name='COH0001', batches_file=bf, batch_size=5)
    assert bf.batches[1]['error_strategy'] == 'continue'


def test_retry_batches_multi_sample_keeps_auto(tmp_path: Path):
    bf = _make_file(tmp_path, [
        Batch('COH0001', 0, ['CPG_A', 'CPG_B']),
        Batch('COH0001', 1, ['CPG_C', 'CPG_D']),
    ])
    bf.record_passfail(0, {'CPG_A': 'Fail', 'CPG_B': 'Fail'})
    bf.record_passfail(1, {'CPG_C': 'Fail', 'CPG_D': 'Success'})
    _build_retry_batches(cohort_name='COH0001', batches_file=bf, batch_size=5)
    # 3 retry SGs (CPG_A, CPG_B, CPG_C) chunked into one batch of 3 → error_strategy='auto'.
    assert bf.batches[2]['error_strategy'] == 'auto'
    assert bf.batches[2]['sg_names'] == ['CPG_A', 'CPG_B', 'CPG_C']


def test_retry_batches_skips_already_retried(tmp_path: Path):
    """A retry batch's own fails are NOT eligible for a second retry."""
    bf = _make_file(tmp_path, [Batch('COH0001', 0, ['CPG_A', 'CPG_B'])])
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Fail'})
    first = _build_retry_batches('COH0001', bf, 5)
    assert len(first) == 1
    # Simulate the retry batch failing too.
    bf.record_passfail(1, {'CPG_B': 'Fail'})
    second = _build_retry_batches('COH0001', bf, 5)
    assert second == []  # single-retry invariant


def test_retry_marks_source_sgs_retried(tmp_path: Path):
    """`mark_sgs_retried` records the per-SG audit trail on the source batch
    (spec §6 line 304 — both batch-level and per-SG)."""
    bf = _make_file(tmp_path, [Batch('COH0001', 0, ['CPG_A', 'CPG_B'])])
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Fail'})
    _build_retry_batches('COH0001', bf, 5)
    assert bf.batches[0]['retried_sgs'] == ['CPG_B']


def test_retry_handles_sg_in_two_batches(tmp_path: Path):
    """After a retry, the same SG name appears in both the initial batch
    (gen=0) and the retry batch (gen=1). `find_batch_for_sg` must return the
    retry batch — that's where per-SG path resolution should aim."""
    bf = _make_file(tmp_path, [Batch('COH0001', 0, ['CPG_A', 'CPG_B'])])
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Fail'})
    _build_retry_batches('COH0001', bf, 5)
    found = bf.find_batch_for_sg('CPG_B')
    assert found is not None
    assert found['retry_generation'] == 1
    assert found['batch_index'] == 1


def test_retry_batches_whole_batch_failed_keeps_auto(tmp_path: Path):
    """When the source batch is FAILED at ICA-level with N>1 SGs and no
    `passfail.json`, the retry batch keeps `error_strategy='auto'` because
    it's still a multi-sample run (only single-sample retries need `continue`)."""
    bf = _make_file(tmp_path, [Batch('COH0001', 0, ['CPG_A', 'CPG_B', 'CPG_C'])])
    bf.record_status(0, 'FAILED')
    new = _build_retry_batches('COH0001', bf, 5)
    assert len(new) == 1
    assert new[0].sg_names == ['CPG_A', 'CPG_B', 'CPG_C']
    assert bf.batches[1]['error_strategy'] == 'auto'


def test_retry_batches_treats_cancelled_as_terminal(tmp_path: Path):
    """`cancel_cohort_run=true` marks batches CANCELLED — that is the user's
    intent (spec §4 line 214). `_build_retry_batches` must NOT re-submit them."""
    bf = _make_file(tmp_path, [Batch('COH0001', 0, ['CPG_A', 'CPG_B'])])
    bf.record_status(0, 'CANCELLED')
    new = _build_retry_batches('COH0001', bf, 5)
    assert new == []


def test_harvest_ar_guids_from_per_sg_state(tmp_path: Path):
    """Harvest helper reads `{batch_index: ar_guid}` and `{batch_index: {sg, ...}}`
    out of per-SG state files. Used by `force_resubmit` BEFORE deletion."""
    from dragen_align_pa.jobs.manage_dragen_pipeline import _harvest_ar_guids_from_per_sg_state

    state_dir = tmp_path
    (state_dir / 'CPG_A_pipeline_id_and_arguid.json').write_text(
        '{"schema_version": 1, "pipeline_id": "p1", "ar_guid": "guid-batch-0", '
        '"user_reference": "COH-batch0000_guid-batch-0_", "batch_index": 0}',
    )
    (state_dir / 'CPG_B_pipeline_id_and_arguid.json').write_text(
        '{"schema_version": 1, "pipeline_id": "p1", "ar_guid": "guid-batch-0", '
        '"user_reference": "COH-batch0000_guid-batch-0_", "batch_index": 0}',
    )
    paths = {
        'CPG_A_pipeline_id_and_arguid': state_dir / 'CPG_A_pipeline_id_and_arguid.json',
        'CPG_B_pipeline_id_and_arguid': state_dir / 'CPG_B_pipeline_id_and_arguid.json',
    }
    harvested, membership = _harvest_ar_guids_from_per_sg_state(['CPG_A', 'CPG_B'], paths)
    assert harvested == {0: 'guid-batch-0'}
    assert membership == {0: {'CPG_A', 'CPG_B'}}


def test_force_resubmit_deletes_state_and_returns_harvest(tmp_path: Path, monkeypatch):
    """Spec §4 line 213: `force_resubmit=true` deletes batches.json + all
    per-SG state files AND preserves AR GUIDs lifted from per-SG state.
    Exercise `_handle_management_flags` end-to-end."""
    from dragen_align_pa.jobs.manage_dragen_pipeline import _handle_management_flags

    batches_path = tmp_path / 'COH0001_batches.json'
    batches_path.write_text('{"schema_version": 1, "batch_size": 5, "n_batches": 0, "batches": []}')
    sg_a_state = tmp_path / 'CPG_A_pipeline_id_and_arguid.json'
    sg_b_state = tmp_path / 'CPG_B_pipeline_id_and_arguid.json'
    for p in (sg_a_state, sg_b_state):
        p.write_text(
            '{"schema_version": 1, "pipeline_id": "p1", "ar_guid": "guid-0", '
            '"user_reference": "COH-batch0000_guid-0_", "batch_index": 0}',
        )

    def fake_config_retrieve(key, default=None):
        cfg = {
            ('ica', 'management', 'force_resubmit'): True,
            ('ica', 'management', 'monitor_previous'): False,
            ('ica', 'management', 'cancel_cohort_run'): False,
        }
        return cfg.get(tuple(key), default)

    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_pipeline.config_retrieve', fake_config_retrieve,
    )

    outputs = {
        'CPG_A_pipeline_id_and_arguid': sg_a_state,
        'CPG_B_pipeline_id_and_arguid': sg_b_state,
    }
    harvested, membership = _handle_management_flags(
        cohort_name='COH0001',
        batches_file_path=batches_path,
        outputs=outputs,
        sg_names=['CPG_A', 'CPG_B'],
    )
    assert harvested == {0: 'guid-0'}
    assert membership == {0: {'CPG_A', 'CPG_B'}}
    assert not batches_path.exists(), 'batches.json should be deleted'
    assert not sg_a_state.exists(), 'per-SG state should be deleted'
    assert not sg_b_state.exists()


def test_cancel_cohort_run_raises_cohort_cancelled(tmp_path: Path, monkeypatch):
    """`cancel_cohort_run=true` is terminal — raises CohortCancelled so run()
    short-circuits past retry-building and threshold-checking."""
    from dragen_align_pa.jobs.manage_dragen_pipeline import (
        CohortCancelled, _handle_management_flags,
    )
    # No batches file → "no in-flight state" branch still raises CohortCancelled.
    def fake_config_retrieve(key, default=None):
        cfg = {
            ('ica', 'management', 'force_resubmit'): False,
            ('ica', 'management', 'monitor_previous'): False,
            ('ica', 'management', 'cancel_cohort_run'): True,
        }
        return cfg.get(tuple(key), default)
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_pipeline.config_retrieve', fake_config_retrieve,
    )
    import pytest
    with pytest.raises(CohortCancelled):
        _handle_management_flags(
            cohort_name='COH0001',
            batches_file_path=tmp_path / 'does-not-exist.json',
            outputs={},
            sg_names=['CPG_A'],
        )


def test_cancel_cohort_run_preserves_per_sg_state(tmp_path: Path, monkeypatch):
    """Round-4 directive: cancel must NOT delete per-SG state files (the
    versioned state file is the single source of per-SG truth; preserving it
    keeps AR GUIDs available for a future `force_resubmit` to harvest).

    Symmetric to `test_force_resubmit_deletes_state_and_returns_harvest`,
    which asserts the OPPOSITE for force_resubmit (deletion is necessary
    for clean state).
    """
    from dragen_align_pa.jobs.manage_dragen_pipeline import (
        CohortCancelled, _handle_management_flags,
    )

    batches_path = tmp_path / 'COH0001_batches.json'
    # Seed a batches file with one INPROGRESS batch (no pipeline_id so the
    # cancel path doesn't try to call ICA — keeps the test hermetic).
    batches_path.write_text(
        '{"schema_version": 1, "batch_size": 5, "n_batches": 1, "batches": ['
        '{"batch_index": 0, "retry_generation": 0, "sg_names": ["CPG_A"], '
        '"retried_sgs": [], "user_reference": "COH-batch0000_g_", '
        '"pipeline_id": null, "ar_guid": "g", "analysis_output_folder_fid": null, '
        '"fastq_list_fid": null, "cram_fids": null, "status": "PENDING", '
        '"passfail": null, "passfail_seen": false, "has_been_retried": false, '
        '"error_strategy": "auto"}]}',
    )
    sg_state = tmp_path / 'CPG_A_pipeline_id_and_arguid.json'
    sg_state.write_text(
        '{"schema_version": 1, "pipeline_id": "p1", "ar_guid": "g", '
        '"user_reference": "COH-batch0000_g_", "batch_index": 0}',
    )

    def fake_config_retrieve(key, default=None):
        cfg = {
            ('ica', 'management', 'force_resubmit'): False,
            ('ica', 'management', 'monitor_previous'): False,
            ('ica', 'management', 'cancel_cohort_run'): True,
        }
        return cfg.get(tuple(key), default)

    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_pipeline.config_retrieve', fake_config_retrieve,
    )

    outputs = {'CPG_A_pipeline_id_and_arguid': sg_state}
    import pytest
    with pytest.raises(CohortCancelled):
        _handle_management_flags(
            cohort_name='COH0001',
            batches_file_path=batches_path,
            outputs=outputs,
            sg_names=['CPG_A'],
        )

    # Batches file is updated to reflect CANCELLED status.
    bf = BatchesFile(path=batches_path)
    bf.read()
    assert bf.batches[0]['status'] == 'CANCELLED'
    # Per-SG state file is PRESERVED — that's the round-4 directive.
    assert sg_state.exists(), 'cancel_cohort_run must not delete per-SG state files'


def test_threshold_breached_just_above_5pct(tmp_path: Path):
    """20 SGs, 2 Fail → 10% → above threshold."""
    from dragen_align_pa.jobs.manage_dragen_pipeline import _threshold_breached

    sg_names = [f'CPG{i:05d}' for i in range(20)]
    bf = _make_file(tmp_path, [Batch('COH0001', 0, sg_names)])
    bf.record_passfail(0, {**{sg: 'Success' for sg in sg_names[:18]}, sg_names[18]: 'Fail', sg_names[19]: 'Fail'})
    n_failed = len(bf.failed_sg_names())
    assert n_failed == 2
    assert _threshold_breached(n_failed=n_failed, n_total=len(sg_names))


def test_threshold_breached_exactly_at_5pct(tmp_path: Path):
    """Spec §6 line 312 uses strict `>`: 20 SGs / 1 Fail = 5.0% → NOT raise.
    Boundary semantics matter when N is small."""
    from dragen_align_pa.jobs.manage_dragen_pipeline import _threshold_breached

    sg_names = [f'CPG{i:05d}' for i in range(20)]
    bf = _make_file(tmp_path, [Batch('COH0001', 0, sg_names)])
    bf.record_passfail(0, {**{sg: 'Success' for sg in sg_names[:19]}, sg_names[19]: 'Fail'})
    n_failed = len(bf.failed_sg_names())
    assert n_failed == 1
    assert not _threshold_breached(n_failed=n_failed, n_total=len(sg_names))


def test_threshold_breached_just_above_5pct_small_n(tmp_path: Path):
    """19 SGs, 1 Fail = 5.26% → above threshold."""
    from dragen_align_pa.jobs.manage_dragen_pipeline import _threshold_breached

    sg_names = [f'CPG{i:05d}' for i in range(19)]
    bf = _make_file(tmp_path, [Batch('COH0001', 0, sg_names)])
    bf.record_passfail(0, {**{sg: 'Success' for sg in sg_names[:18]}, sg_names[18]: 'Fail'})
    n_failed = len(bf.failed_sg_names())
    assert n_failed == 1
    assert _threshold_breached(n_failed=n_failed, n_total=len(sg_names))
```

- [ ] **Step 2: Run**

Run: `pytest tests/test_manage_dragen_pipeline.py -v`
Expected: all tests pass (count grows as the file accumulates: retry
orchestration, force_resubmit + cancel coverage of `_handle_management_flags`,
and the threshold-boundary tests share this file).

- [ ] **Step 3: Commit**

```bash
git add tests/test_manage_dragen_pipeline.py
git commit -m "Add unit tests for manage_dragen_pipeline orchestration (retry, force_resubmit, cancel, threshold)"
```

---

> **PR checkpoint A** — Core data types, helpers, config, loop generalization, submitter + passfail parsing, and orchestration tests are all in place. Old pipeline still runnable because the old `ManageDragenPipeline` stage hasn't been touched yet. Reasonable point to merge to `dragen-unified-dev` for review before continuing.

---

## Task 15: Rewrite `manage_dragen_pipeline.run` for batched submission + per-sample retry

**Files:**
- Modify: `src/dragen_align_pa/jobs/manage_dragen_pipeline.py` (full rewrite)

This is the biggest single change in the migration. Read the existing file before editing — most of it gets replaced.

- [ ] **Step 1: Replace the whole file**

Open `src/dragen_align_pa/jobs/manage_dragen_pipeline.py`. Replace the entire contents with:

```python
"""Orchestrate the unified DRAGEN pipeline across a cohort.

Responsibilities:
- Chunk the cohort into deterministic batches (Batch dataclass).
- Persist `{cohort}_batches.json` plus per-SG state files (extended schema).
- Submit batches via `submit_dragen_batch.run`, monitored by the shared
  `manage_ica_pipeline_loop` (now generic over Batch targets).
- After the first pass completes, read passfail across all batches; if any SGs
  are marked Fail and have not been retried, form retry batches and run the
  loop a second time. Single retry only.
- Apply the 5% threshold over sequencing groups (not over batches).
"""

import json
from collections.abc import Callable
from typing import Literal

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils
from dragen_align_pa.batches import Batch, BatchesFile, chunk_sgs_into_batches
from dragen_align_pa.constants import BUCKET_NAME
from dragen_align_pa.jobs import cancel_ica_pipeline_run, submit_dragen_batch
from dragen_align_pa.jobs.ica_pipeline_manager import (
    MonitoredTarget,
    PipelineStatus,
    manage_ica_pipeline_loop,
)
from dragen_align_pa.jobs.parse_passfail import fetch_passfail_from_ica


class CohortCancelled(RuntimeError):
    """Raised when `cancel_cohort_run=true` has terminated the cohort.

    Distinct exception type so cpg-flow / operators can distinguish a
    user-initiated cancellation from a true pipeline failure (e.g. the
    threshold-breach `RuntimeError`). Both halt the stage and skip downstream
    work via cpg-flow's required-stage propagation; only the message differs.
    """


def _build_submit_callable(
    batch: Batch,
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
        # If `force_resubmit` pre-seeded an AR GUID on this batch entry, reuse it.
        ar_guid_override = entry.get('ar_guid')
        result = submit_dragen_batch.run(
            batch=batch,
            analysis_output_fid_path=analysis_output_fid_path,
            cram_state_paths=cram_state_paths,
            fastq_ids_path=fastq_ids_path,
            per_sg_fastq_list_paths=per_sg_fastq_list_paths,
            error_strategy=error_strategy,
            ar_guid_override=ar_guid_override,
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
        _persist_per_sg_state_for_batch(outputs, batch, {
            'pipeline_id': pipeline_id_v,
            'ar_guid': ar_guid_v,
            'user_reference': user_reference_v,
        })
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
    batch: Batch,
    submission_result: dict[str, str],
) -> None:
    """Write per-SG state files for one batch immediately on submission.

    Schema must match the version validated by `get_ica_sample_folder` (Task 7).
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
            # `expected_outputs` undercount — downstream per-SG download stages
            # will fail to resolve the path. Surface this loudly rather than
            # silently dropping the SG's state file.
            logger.warning(
                f'Per-SG state output {key!r} not declared in outputs for batch '
                f'{batch.name}; downstream download stages for this SG will fail. '
                f'Check ManageDragenPipeline.expected_outputs.',
            )
            continue
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


def _harvest_ar_guids_from_per_sg_state(
    sg_names: list[str],
    outputs: dict[str, cpg_utils.Path],
) -> tuple[dict[int, str], dict[int, set[str]]]:
    """Read per-SG state files and return ({batch_index: ar_guid}, {batch_index: {sg_name, ...}}).

    Used by `force_resubmit` to lift AR GUIDs out of per-SG state before
    deleting both the batches file and the per-SG files (spec §4 line 213:
    "preserves AR GUID per batch (lifted up from per-SG)"). SGs in the same
    original batch share an AR GUID by construction, so the mapping is
    well-defined even if a few state files are missing.

    The second return is the SG membership that was associated with each old
    batch_index. The caller compares it to the new partition's sg_names so it
    can warn when membership has drifted (cohort changed between runs) —
    positional AR-GUID reuse is still applied in that case (the audit-trail
    identity is preserved), but the warning makes the drift visible.
    """
    harvested: dict[int, str] = {}
    membership: dict[int, set[str]] = {}
    for sg_name in sg_names:
        key = f'{sg_name}_pipeline_id_and_arguid'
        path = outputs.get(key)
        if path is None or not path.exists():
            continue
        try:
            with path.open('r') as fh:
                state = json.load(fh)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f'Could not harvest AR GUID from {path}: {e}')
            continue
        batch_index = state.get('batch_index')
        ar_guid = state.get('ar_guid')
        if batch_index is None or not ar_guid:
            continue
        # Earlier entries win; subsequent SGs in the same batch should agree.
        harvested.setdefault(batch_index, ar_guid)
        membership.setdefault(batch_index, set()).add(sg_name)
    return harvested, membership


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
    batches_by_name: dict[str, Batch],
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
        # `get_ica_sample_folder` in utils.py — same separator). The hyphen is
        # required: user_reference ends in `_`, so the resulting folder name
        # is `…_-{pipeline_id}/`.
        analysis_folder_name = f'{batch_entry["user_reference"]}-{batch_entry["pipeline_id"]}'
        ica_parent = f'/{BUCKET_NAME}/{config_retrieve(["ica", "data_prep", "output_folder"])}/'
        ica_folder = f'{ica_parent}{analysis_folder_name}/'

        secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
        path_parameters = {'projectId': secrets['projectID']}

        passfail = None
        folder_fid: str | None = None
        try:
            with ica_api_utils.get_ica_api_client() as api_client:
                api_instance = project_data_api.ProjectDataApi(api_client)
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
            # RAISE (don't `return`): Task 10's transactional callback catches
            # this, logs it, and leaves the loop's per-target status INPROGRESS.
            # batches.json also stays INPROGRESS (we never called record_status).
            # Next poll cycle re-fires on_succeeded. If we silently `return`ed
            # here, the loop would think the callback succeeded and set
            # SUCCEEDED — diverging from batches.json which would still show
            # INPROGRESS.
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
            batches_file.record_passfail(batch.batch_index, {sg: 'Fail' for sg in batch.sg_names})
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
                filtered[sg] = 'Fail'
            batches_file.record_passfail(batch.batch_index, filtered)
        if folder_fid is not None:
            batches_file.record_analysis_output_folder_fid(batch.batch_index, folder_fid)
        batches_file.record_status(batch.batch_index, 'SUCCEEDED')
        batches_file.write()

    return _on_succeeded


def _on_status_change_factory(
    batches_file: BatchesFile,
    batches_by_name: dict[str, Batch],
) -> Callable[[MonitoredTarget, PipelineStatus], None]:
    """Mirror the loop's terminal non-success transitions into `{cohort}_batches.json`.

    Without this, the loop's in-memory `FAILED_FINAL` / `CANCELLED` transitions
    never propagate to the batches file, leaving entries stuck at INPROGRESS
    forever. The downstream consequences are:
    - `_build_retry_batches`'s `elif b['status'] == 'FAILED':` branch becomes
      unreachable (whole-batch infrastructure failure can't trigger a retry).
    - A subsequent resume's `initial_batches` filter (`status in {PENDING,
      INPROGRESS}`) re-picks-up the dead batch and the loop polls a long-
      aborted ICA analysis.

    `SUCCEEDED` is intentionally NOT routed through this callback — the
    transactional `_on_succeeded_factory` already records SUCCEEDED via
    `batches_file.record_status(idx, 'SUCCEEDED')`.
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
            batches_file.record_status(batch.batch_index, 'FAILED')
        elif new_status == PipelineStatus.CANCELLED:
            batches_file.record_status(batch.batch_index, 'CANCELLED')
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
) -> list[Batch]:
    """Form retry batches from per-sample failures across batches.

    Only retries batches with `retry_generation == 0` (the initial cohort batches).
    Uses `BatchesFile.add_retry_batch` to append retry entries and
    `BatchesFile.mark_sgs_retried` to record per-SG audit trail on the source
    batches (spec §6 line 304). Retry batches are created with
    `has_been_retried=True` and `error_strategy` defaulting to `continue` for
    single-sample batches, so a hypothetical second retry pass short-circuits —
    enforcing the "single retry only" spec invariant.

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
                if status == 'Fail':
                    sg_to_source[sg] = b['batch_index']
        elif b['status'] == 'FAILED':
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
        cohort_name=cohort_name, sg_names=eligible, batch_size=batch_size,
    )

    new_batches: list[Batch] = []
    source_to_retried: dict[int, list[str]] = {}
    for pseudo in pseudo_batches:
        new_index = batches_file.add_retry_batch(sg_names=pseudo.sg_names)
        new_batches.append(
            Batch(cohort_name=cohort_name, batch_index=new_index, sg_names=list(pseudo.sg_names)),
        )
        for sg in pseudo.sg_names:
            source_to_retried.setdefault(sg_to_source[sg], []).append(sg)

    for source_idx, sg_names in source_to_retried.items():
        batches_file.mark_sgs_retried(source_batch_idx=source_idx, sg_names=sg_names)
    batches_file.write()
    return new_batches


def _build_loop_outputs_for_batches(
    batches: list[Batch],
    outputs: dict[str, cpg_utils.Path],
) -> dict[str, cpg_utils.Path]:
    """Subset of `outputs` keyed by batch name, as the shared loop expects.

    Raises if any batch's expected_outputs entries are missing — that means
    `ManageDragenPipeline.expected_outputs` undercounted `max_batches` and the
    shared loop would fail later in a more confusing way.

    Common cause: `batch_size` was lowered between a force_resubmit and the
    next orchestrator run, so the resumed batches partition exceeds the new
    `max_batches = 2 * ceil(N / batch_size)`. The fix is to delete
    `{cohort}_batches.json` (force_resubmit deletes it; manual delete also
    works) so the re-batch uses the current `batch_size`.
    """
    keys: dict[str, cpg_utils.Path] = {}
    for b in batches:
        success_key = f'{b.name}_success'
        pid_key = f'{b.name}_pipeline_id'
        if success_key not in outputs or pid_key not in outputs:
            raise KeyError(
                f'Missing expected_outputs entries for batch {b.name}. '
                f'This usually means batch_size was lowered between runs. '
                f'Rerun with force_resubmit=true (or delete the cohort\'s '
                f'batches.json manually) so the cohort is re-batched under '
                f'the current batch_size, then retry.',
            )
        keys[success_key] = outputs[success_key]
        keys[pid_key] = outputs[pid_key]
    return keys


def _handle_management_flags(
    cohort_name: str,
    batches_file_path: cpg_utils.Path,
    outputs: dict[str, cpg_utils.Path],
    sg_names: list[str],
) -> tuple[dict[int, str], dict[int, set[str]]]:
    """Apply `force_resubmit` / `monitor_previous` / `cancel_cohort_run` BEFORE
    constructing the BatchesFile.

    Returns `({batch_index: ar_guid}, {batch_index: {sg_name, ...}})`:
    - The AR-GUID map is empty unless `force_resubmit` harvested some.
    - The membership map carries the old per-batch SG set so the caller can
      warn when the new partition diverges (cohort membership changed).

    Raises `CohortCancelled` (terminal) if `cancel_cohort_run=true` —
    short-circuits `run()` so it doesn't fall into retry-building or
    threshold-checking.

    Deliberate spec drift (design doc §4 line 214): the original spec said
    `cancel_cohort_run=true` "deletes per-SG state files". We preserve them
    instead — the versioned per-SG state file is the single source of truth
    and a subsequent `force_resubmit=true` (the only sanctioned recovery
    path) needs the preserved AR GUIDs in order to honour spec §4 line 213's
    AR-GUID preservation requirement. The design doc has been updated to
    match; this docstring records the drift for reviewers cross-checking
    the original spec text.

    Semantics (spec §4 lines 211–215):
    - `monitor_previous=true`: raises if the batches file is missing.
    - `force_resubmit=true`: harvests per-batch AR GUIDs from the existing
      per-SG state files (the authoritative source — the user may have changed
      cohort membership, so positional mapping by `batch_index` is what gets
      re-used), then DELETES both `{cohort}_batches.json` and the per-SG state
      files. The caller re-batches the cohort from scratch and passes the
      harvested AR GUIDs into the new submissions.
    - `cancel_cohort_run=true`: for each batch with status PENDING/INPROGRESS,
      calls the ICA abort API if a `pipeline_id` is known, then marks the
      batch CANCELLED in the file. **Per-SG state files are NOT deleted** —
      the versioned state file is the single source of per-SG truth and we
      avoid deleting it unless we have no choice. Preserving them keeps the
      AR GUIDs available for a future `force_resubmit=true` to harvest. The
      function raises `CohortCancelled` to terminate the run cleanly; a
      subsequent run will only get clean state via `force_resubmit=true`
      (which IS allowed to delete, since deletion is the only way to clear
      stale pointers to aborted ICA analyses).

    Precedence (contract C8): if both `force_resubmit` and `monitor_previous`
    are set, `force_resubmit` wins and `monitor_previous` is logged as ignored.
    `force_resubmit` and `cancel_cohort_run` together: undefined; we raise.
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
        logger.warning(
            f'Cohort {cohort_name}: both force_resubmit and monitor_previous are set; '
            f'force_resubmit wins and monitor_previous is ignored.',
        )
        monitor_previous = False

    if monitor_previous and not batches_file_path.exists():
        raise FileNotFoundError(
            f'monitor_previous=true but {batches_file_path} does not exist — nothing to resume.',
        )

    if force_resubmit:
        had_prior_state = batches_file_path.exists() or any(
            outputs.get(f'{sg}_pipeline_id_and_arguid', None) is not None
            and outputs[f'{sg}_pipeline_id_and_arguid'].exists()
            for sg in sg_names
        )
        if had_prior_state:
            logger.warning(
                f'force_resubmit=true for cohort {cohort_name}: harvesting AR GUIDs and '
                f'deleting batches file + per-SG state.',
            )
        else:
            logger.info(
                f'force_resubmit=true for cohort {cohort_name} but no prior state exists; '
                f'proceeding as a fresh submission.',
            )
        preserved_ar_guids, old_membership = _harvest_ar_guids_from_per_sg_state(
            sg_names=sg_names, outputs=outputs,
        )
        if batches_file_path.exists():
            batches_file_path.unlink()
        for sg_name in sg_names:
            key = f'{sg_name}_pipeline_id_and_arguid'
            if key in outputs and outputs[key].exists():
                outputs[key].unlink()
        return preserved_ar_guids, old_membership

    if cancel_cohort_run and not batches_file_path.exists():
        logger.warning(
            f'cancel_cohort_run=true for cohort {cohort_name} but no batches file exists — '
            f'nothing to cancel. Exiting cleanly.',
        )
        raise CohortCancelled(
            f'Cohort {cohort_name} cancelled by user request '
            f'(cancel_cohort_run=true; no in-flight state to abort).',
        )

    if cancel_cohort_run and batches_file_path.exists():
        logger.warning(f'cancel_cohort_run=true for cohort {cohort_name}: aborting in-flight batches.')
        existing = BatchesFile(path=batches_file_path)
        existing.read()
        n_aborted = 0
        for b in existing.batches:
            if b['status'] not in {'PENDING', 'INPROGRESS'}:
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
            b['status'] = 'CANCELLED'
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
            f'Rerun with force_resubmit=true to start a fresh submission '
            f'(AR GUIDs from the preserved per-SG state will be reused).',
        )

    return {}, {}


def run(
    cohort: Cohort,
    outputs: dict[str, cpg_utils.Path],
    cram_state_paths: dict[str, cpg_utils.Path] | None,
    fastq_ids_path: cpg_utils.Path | None,
    per_sg_fastq_list_paths: dict[str, cpg_utils.Path] | None,
    analysis_output_fid_path: cpg_utils.Path,
) -> None:
    """Build batches, submit them, retry per-sample failures once, enforce 5% threshold."""
    batch_size: int = config_retrieve(['ica', 'dragen', 'batch_size'], default=5)
    sg_names = [sg.name for sg in cohort.get_sequencing_groups()]
    if not sg_names:
        raise ValueError(f'Cohort {cohort.name} has no sequencing groups.')
    batches_file_path: cpg_utils.Path = outputs[f'{cohort.name}_batches']

    # `_handle_management_flags` raises CohortCancelled on `cancel_cohort_run=true`;
    # we let it propagate so cpg-flow marks the stage failed and downstream stages
    # skip. For force_resubmit it returns `(preserved_ar_guids, old_membership)`
    # so we can pre-seed AR GUIDs on the freshly-batched cohort and warn if
    # membership drifted.
    preserved_ar_guids, old_membership = _handle_management_flags(
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
            Batch(cohort_name=cohort.name, batch_index=b['batch_index'], sg_names=b['sg_names'])
            for b in batches_file.batches
            if b['retry_generation'] == 0 and b['status'] in {'PENDING', 'INPROGRESS'}
        ]
    else:
        # Fresh cohort, or post-`force_resubmit` re-batching. Cohort membership may
        # have changed (SGs added/removed) since the original submission, so we
        # always re-batch from the *current* cohort SG list rather than reusing
        # the prior `sg_names` partitions.
        initial_batches = chunk_sgs_into_batches(
            cohort_name=cohort.name,
            sg_names=sg_names,
            batch_size=batch_size,
        )
        batches_file.initialise(batch_size=batch_size, batches=initial_batches)
        # Pre-seed any AR GUIDs lifted by `force_resubmit` so the new submissions
        # reuse the cohort's existing submission identity where positional mapping
        # exists. New batches (or batches without a positional match) get a fresh
        # AR GUID minted at submit time inside `submit_dragen_batch.run`.
        for batch_entry in batches_file.batches:
            new_index = batch_entry['batch_index']
            preserved = preserved_ar_guids.get(new_index)
            if preserved:
                batch_entry['ar_guid'] = preserved
                # Warn if the new batch's SG membership differs from the old
                # batch that originally held this AR GUID — positional reuse
                # then maps the AR GUID to a different sample set (audit
                # confusion, not a correctness bug per spec §4 line 213).
                old_set = old_membership.get(new_index)
                new_set = set(batch_entry['sg_names'])
                if old_set is not None and old_set != new_set:
                    added = new_set - old_set
                    removed = old_set - new_set
                    logger.warning(
                        f'force_resubmit: batch {new_index} reuses AR GUID {preserved!r} '
                        f'but membership has drifted (added={sorted(added)}, '
                        f'removed={sorted(removed)}). The AR GUID will be associated '
                        f'with the new membership in this run.',
                    )
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
        loop_outputs = _build_loop_outputs_for_batches(initial_batches, outputs)
        # allow_retry=False: the shared loop's whole-target retry plus its 5%-of-targets
        # threshold are bypassed for DRAGEN. Retry + threshold logic is owned by this
        # orchestrator (per-sample retry over the cohort), not by the loop (which would
        # over-trigger on small batch counts).
        manage_ica_pipeline_loop(
            targets_to_process=initial_batches,
            outputs=loop_outputs | {f'{cohort.name}_errors': outputs[f'{cohort.name}_errors']},
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
        Batch(cohort_name=cohort.name, batch_index=b['batch_index'], sg_names=b['sg_names'])
        for b in batches_file.batches
        if b['retry_generation'] == 1 and b['status'] in {'PENDING', 'INPROGRESS'}
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

        retry_loop_outputs = _build_loop_outputs_for_batches(retry_batches, outputs)
        manage_ica_pipeline_loop(
            targets_to_process=retry_batches,
            outputs=retry_loop_outputs | {f'{cohort.name}_errors': outputs[f'{cohort.name}_errors']},
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
        # Persist errors.log to the stage's declared output before raising, so the
        # cohort run produces a durable error artefact (spec §6 line 312).
        errors_path = outputs[f'{cohort.name}_errors']
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
```

- [ ] **Step 2: Smoke-check imports**

Run: `python -c "from dragen_align_pa.jobs.manage_dragen_pipeline import run; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 3: Commit**

```bash
git add src/dragen_align_pa/jobs/manage_dragen_pipeline.py
git commit -m "Rewrite manage_dragen_pipeline.run for batched submission with per-sample retry"
```

---

## Task 16: Update `ManageDragenPipeline` stage in `stages.py`

> **Execution order note:** Task 16 and Task 17 must be applied together in the **same commit** — or Task 17 first, then Task 16 — because Task 16's `queue_jobs` calls `inputs.as_path(target=cohort, stage=PrepareIcaForDragenAnalysis)` which expects the simplified output shape introduced by Task 17. The cleanest approach is to stage both edits and make one commit with both changes; the commit message can be "Switch ManageDragenPipeline and PrepareIcaForDragenAnalysis to batched / single-output shapes". The Step-5 commit in this task is therefore deferred — commit happens at the end of Task 17 instead.

**Files:**
- Modify: `src/dragen_align_pa/stages.py` (the `ManageDragenPipeline` class)

- [ ] **Step 1: Replace the `ManageDragenPipeline` class**

Open `src/dragen_align_pa/stages.py`. Find the `ManageDragenPipeline` class (look for `@stage(required_stages=[PrepareIcaForDragenAnalysis, UploadDataToIca, UploadFastqFileList, MakeFastqFileList, FastqIntakeQc,],)` followed by `class ManageDragenPipeline(CohortStage):`).

Replace the entire class definition with:

```python
@stage(
    required_stages=[
        PrepareIcaForDragenAnalysis,
        UploadDataToIca,
        MakeFastqFileList,
        FastqIntakeQc,
    ],
)
class ManageDragenPipeline(CohortStage):
    """Submit cohort batches to the unified DRAGEN pipeline and monitor them."""

    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        results: dict[str, cpg_utils.Path] = {
            f'{cohort.name}_errors': get_pipeline_path(filename=f'{cohort.name}_errors.log'),
            f'{cohort.name}_batches': get_pipeline_path(filename=f'{cohort.name}_batches.json'),
        }

        # Per-SG state files (extended schema; consumed by download stages).
        for sg in cohort.get_sequencing_groups():
            results[f'{sg.name}_pipeline_id_and_arguid'] = get_pipeline_path(
                filename=f'{sg.name}_pipeline_id_and_arguid.json',
            )

        # Per-batch success + pipeline-id files used by the shared loop.
        # Batch count isn't known until submission, so generate enough keys for
        # twice the cohort size (initial + retry batches with batch_size>=1).
        sg_names = cohort.get_sequencing_group_ids()
        batch_size = config_retrieve(['ica', 'dragen', 'batch_size'], default=5)
        max_batches = 2 * ((len(sg_names) + batch_size - 1) // batch_size)
        for i in range(max_batches):
            name = f'{cohort.name}-batch{i:04d}'
            results[f'{name}_success'] = get_pipeline_path(filename=f'{name}_pipeline_success.json')
            results[f'{name}_pipeline_id'] = get_pipeline_path(filename=f'{name}_pipeline_id.json')

        return results

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput:
        outputs = self.expected_outputs(cohort=cohort)

        cram_state_paths: dict[str, cpg_utils.Path] | None = None
        fastq_ids_path: cpg_utils.Path | None = None
        per_sg_fastq_list_paths: dict[str, cpg_utils.Path] | None = None

        if READS_TYPE == 'cram':
            cram_state_paths = inputs.as_path_by_target(stage=UploadDataToIca)
        elif READS_TYPE == 'fastq':
            fastq_ids_path = inputs.as_path(target=cohort, stage=FastqIntakeQc, key='fastq_ids_outpath')
            per_sg_fastq_list_paths = inputs.as_dict(target=cohort, stage=MakeFastqFileList)

        analysis_output_fid_path: cpg_utils.Path = inputs.as_path(
            target=cohort, stage=PrepareIcaForDragenAnalysis,
        )

        job: PythonJob = initialise_python_job(
            job_name=f'Manage Dragen pipeline runs for cohort: {cohort.name}',
            target=cohort,
            tool_name='Dragen',
        )
        job.image(image=get_driver_image())

        job.call(
            manage_dragen_pipeline.run,
            cohort=cohort,
            outputs=outputs,
            cram_state_paths=cram_state_paths,
            fastq_ids_path=fastq_ids_path,
            per_sg_fastq_list_paths=per_sg_fastq_list_paths,
            analysis_output_fid_path=analysis_output_fid_path,
        )

        return self.make_outputs(target=cohort, data=outputs, jobs=job)  # pyright: ignore[reportArgumentType]
```

- [ ] **Step 2: Verify import**

Run: `python -c "from dragen_align_pa import stages; print('ok')"`
Expected: prints `ok`. The combined commit happens at the end of Task 17 — do not commit here standalone.

---

## Task 17: Simplify `PrepareIcaForDragenAnalysis` to one cohort-level output

> **Note: `DeleteDataInIca` is intentionally left broken on this branch.**
> The spec (§1 line 33, §7 line 491) defers the `DeleteDataInIca` rewrite to a
> follow-up PR. That stage reads `inputs.as_dict(target=cohort, stage=PrepareIcaForDragenAnalysis)`
> in `stages.py`, which assumed the old multi-output shape. After this task,
> `PrepareIcaForDragenAnalysis` returns a single `cpg_utils.Path`, so
> `DeleteDataInIca`'s stage-graph wiring will fail at construction time. That's
> acceptable on `dragen-unified-dev`: production runs continue on `main` until
> the migration is complete (spec §7 line 333), and the follow-up PR will
> rewrite `DeleteDataInIca` for the batched layout. Do NOT add a half-fix here
> — leaving it broken is the cleanest signal that the rewrite is still owed.

**Files:**
- Modify: `src/dragen_align_pa/stages.py` (the `PrepareIcaForDragenAnalysis` class)
- Modify: `src/dragen_align_pa/jobs/prepare_ica_for_analysis.py`

- [ ] **Step 1: Read the existing `prepare_ica_for_analysis.py`**

Read `src/dragen_align_pa/jobs/prepare_ica_for_analysis.py` to understand the per-SG iteration before replacing it.

- [ ] **Step 2: Replace `prepare_ica_for_analysis.run`**

Open `src/dragen_align_pa/jobs/prepare_ica_for_analysis.py` and replace the `run` function with:

```python
import json
from typing import Literal

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.constants import BUCKET_NAME


def run(cohort: Cohort, output: cpg_utils.Path) -> None:
    """Create (or find) a single ICA folder for the cohort's pipeline outputs.

    The new unified pipeline writes per-batch analysis folders directly under
    this parent folder, so we no longer need per-SG parent folders.
    """
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    project_id = secrets['projectID']
    output_folder = config_retrieve(['ica', 'data_prep', 'output_folder'])
    folder_path = f'/{BUCKET_NAME}/{output_folder}'

    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        folder_id, status = ica_utils.create_upload_object_id(
            api_instance=api_instance,
            path_params={'projectId': project_id},
            sg_name=cohort.name,
            file_name=cohort.name,
            folder_path=folder_path,
            object_type='FOLDER',
        )
    logger.info(f'Cohort output folder for {cohort.name} (status {status}): {folder_id}')

    with output.open('w') as fh:
        json.dump({'analysis_output_fid': folder_id}, fh)
```

If the existing `create_upload_object_id` signature doesn't quite match this usage for folders (the function exists at `ica_utils.py:25` and was previously called per-SG), inspect it and adapt the call to whatever shape the helper expects. Goal: one cohort-level folder, no per-SG iteration.

- [ ] **Step 3: Update the `PrepareIcaForDragenAnalysis` stage**

In `stages.py`, replace the `PrepareIcaForDragenAnalysis` class definition with:

```python
@stage()
class PrepareIcaForDragenAnalysis(CohortStage):
    """Create a single cohort-level analysis output folder on ICA."""

    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:  # pyright: ignore[reportIncompatibleMethodOverride]
        return get_prep_path(filename=f'{cohort.name}_analysis_output_fid.json')

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput:  # noqa: ARG002
        output = self.expected_outputs(cohort=cohort)

        job: PythonJob = initialise_python_job(
            job_name='PrepareIcaForDragenAnalysis',
            target=cohort,
            tool_name='ICA',
        )
        job.image(image=get_driver_image())
        job.call(prepare_ica_for_analysis.run, cohort=cohort, output=output)

        return self.make_outputs(target=cohort, data=output, jobs=job)
```

- [ ] **Step 4: Smoke-check imports**

Run: `python -c "from dragen_align_pa import stages; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 5: Combined commit (Tasks 16 + 17)**

Tasks 16 and 17 are committed together because Task 16's `ManageDragenPipeline.queue_jobs` calls `inputs.as_path(target=cohort, stage=PrepareIcaForDragenAnalysis)` which only works once Task 17 has simplified `PrepareIcaForDragenAnalysis` to return a single path.

```bash
git add src/dragen_align_pa/stages.py src/dragen_align_pa/jobs/prepare_ica_for_analysis.py
git commit -m "Switch ManageDragenPipeline to batched submission + simplify PrepareIcaForDragenAnalysis to a single cohort-level output"
```

---

## Task 18: Update `DownloadCramFromIca` to use `get_ica_sample_folder`

**Files:**
- Modify: `src/dragen_align_pa/stages.py` (the `DownloadCramFromIca` class)
- Modify: `src/dragen_align_pa/jobs/download_specific_files_from_ica.py` (to accept a pre-resolved ICA folder path)

- [ ] **Step 1: Update the job to accept a pre-resolved folder path**

Open `src/dragen_align_pa/jobs/download_specific_files_from_ica.py`. The current `run` function reconstructs the ICA folder from the pipeline-id file using the old per-SG path convention. Update it to:

- Accept a parameter `ica_folder_path: str` instead of recomputing the path.
- Drop the `pipeline_id_arguid_path` parameter from the public signature (or accept both for a transition period — but prefer cleanly replacing it).

Specifically, change `run`'s body so it uses the supplied `ica_folder_path` directly, removing the inline path-construction code.

- [ ] **Step 2: Update `DownloadCramFromIca`'s `queue_jobs`**

In `stages.py`, find `DownloadCramFromIca.queue_jobs`. Replace the body so the call to `download_specific_files_from_ica.run` uses the new `ica_folder_path` parameter computed via `get_ica_sample_folder`:

```python
    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(sequencing_group=sequencing_group)

        pipeline_id_arguid_path: cpg_utils.Path = inputs.as_dict(
            target=get_multicohort().get_cohorts()[0],
            stage=ManageDragenPipeline,
        )[f'{sequencing_group.name}_pipeline_id_and_arguid']

        ica_download_job: PythonJob = initialise_python_job(
            job_name='DownloadCramFromIca',
            target=sequencing_group,
            tool_name='ICA-Python',
        )
        ica_download_job.image(image=get_driver_image())
        ica_download_job.storage('8Gi')
        ica_download_job.memory('8Gi')
        ica_download_job.spot(is_spot=False)

        cram_spec: FileTypeSpec = FileTypeSpec(
            gcs_prefix='cram',
            data_suffix='cram',
            index_suffix='cram.crai',
            md5_suffix='md5sum',
        )

        ica_download_job.call(
            _resolve_then_download,
            sequencing_group=sequencing_group,
            file_spec=cram_spec,
            pipeline_id_arguid_path=pipeline_id_arguid_path,
            gcs_output_dir=outputs['cram'].parent,
        )

        return self.make_outputs(target=sequencing_group, data=outputs, jobs=ica_download_job)
```

- [ ] **Step 3: Add the `_resolve_then_download` helper near the top of `stages.py`**

In `stages.py`, after the imports block, add a private helper that wraps the download call:

```python
def _resolve_then_download(
    sequencing_group: SequencingGroup,
    file_spec: FileTypeSpec,
    pipeline_id_arguid_path: cpg_utils.Path,
    gcs_output_dir: cpg_utils.Path,
) -> None:
    """Look up the SG's batched ICA folder, then invoke the existing downloader."""
    from dragen_align_pa.utils import get_ica_sample_folder  # noqa: PLC0415

    ica_folder = get_ica_sample_folder(pipeline_id_arguid_path, sequencing_group.name)
    download_specific_files_from_ica.run(
        sequencing_group=sequencing_group,
        file_spec=file_spec,
        ica_folder_path=ica_folder,
        gcs_output_dir=gcs_output_dir,
    )
```

- [ ] **Step 4: Smoke-check imports**

Run: `python -c "from dragen_align_pa import stages; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 5: Commit**

```bash
git add src/dragen_align_pa/stages.py src/dragen_align_pa/jobs/download_specific_files_from_ica.py
git commit -m "Update DownloadCramFromIca to resolve batch path via get_ica_sample_folder"
```

---

## Task 19: Update `DownloadGvcfFromIca` and `DownloadMlrGvcfFromIca` similarly

> **Execution order note:** Task 19 and Task 19b must be applied together in
> the **same commit** (or with 19b first). Task 19 wires
> `DownloadMlrGvcfFromIca` to resolve its ICA folder via `ManageDragenPipeline`'s
> state on the assumption that MLR writes its outputs into the parent DRAGEN
> batch folder — but that assumption is only true after Task 19b's path-
> construction rewrite in `manage_dragen_mlr.py`. Committing 19 alone leaves
> the download stage pointed at a folder MLR hasn't written into yet.

**Files:**
- Modify: `src/dragen_align_pa/stages.py` (the `DownloadGvcfFromIca` and `DownloadMlrGvcfFromIca` classes)

- [ ] **Step 1: Update `DownloadGvcfFromIca.queue_jobs`**

In `stages.py`, replace the body of `DownloadGvcfFromIca.queue_jobs` with the same pattern as `DownloadCramFromIca` — i.e. call `_resolve_then_download` with the gVCF spec. Keep the existing `base_gvcf_spec` definition.

```python
        ica_download_job.call(
            _resolve_then_download,
            sequencing_group=sequencing_group,
            file_spec=base_gvcf_spec,
            pipeline_id_arguid_path=pipeline_id_arguid_path,
            gcs_output_dir=outputs['gvcf'].parent,
        )
```

- [ ] **Step 2: Update `DownloadMlrGvcfFromIca.queue_jobs`**

Under the new design (see Task 19b), MLR writes its outputs into the **same per-SG subfolder of the parent DRAGEN batch folder** as the original Dragen outputs. That means `DownloadMlrGvcfFromIca` resolves its ICA folder via `get_ica_sample_folder` — the SAME way as `DownloadCramFromIca` and `DownloadGvcfFromIca` — using the Dragen per-SG state file (not the MLR state file).

In `DownloadMlrGvcfFromIca.queue_jobs`, read `pipeline_id_arguid_path` from `ManageDragenPipeline`'s outputs (NOT from `ManageDragenMlr`'s), then call `_resolve_then_download` with the recal-gVCF spec. The dependency on `ManageDragenMlr` stays — it's needed for stage ordering (we must wait for MLR to finish), but the path is resolved through the Dragen state.

```python
    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(sequencing_group=sequencing_group)

        # Resolve through the Dragen per-SG state file because MLR outputs now live
        # in the parent Dragen batch folder per SG (see Task 19b).
        pipeline_id_arguid_path: cpg_utils.Path = inputs.as_dict(
            target=get_multicohort().get_cohorts()[0],
            stage=ManageDragenPipeline,
        )[f'{sequencing_group.name}_pipeline_id_and_arguid']

        ica_download_job: PythonJob = initialise_python_job(
            job_name='DownloadMlrGvcfFromIca',
            target=sequencing_group,
            tool_name='ICA-Python',
        )
        ica_download_job.image(image=get_driver_image())
        ica_download_job.storage('8Gi')
        ica_download_job.memory('8Gi')
        ica_download_job.spot(is_spot=False)

        recal_gvcf_spec: FileTypeSpec = FileTypeSpec(
            gcs_prefix='recal_gvcf',
            data_suffix='hard-filtered.recal.gvcf.gz',
            index_suffix='hard-filtered.recal.gvcf.gz.tbi',
            md5_suffix='md5',
        )

        ica_download_job.call(
            _resolve_then_download,
            sequencing_group=sequencing_group,
            file_spec=recal_gvcf_spec,
            pipeline_id_arguid_path=pipeline_id_arguid_path,
            gcs_output_dir=outputs['gvcf'].parent,
        )

        return self.make_outputs(target=sequencing_group, data=outputs, jobs=ica_download_job)
```

No separate `_resolve_then_download_mlr` helper is needed.

- [ ] **Step 3: Smoke-check imports**

Run: `python -c "from dragen_align_pa import stages; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 4: Stage the edits; commit happens at the end of Task 19b**

Tasks 19 and 19b ship as a single commit (per the combined-commit note at the
top of Task 19). Do not commit standalone here — Task 19's stage rewiring
points `DownloadMlrGvcfFromIca` at a folder MLR only writes into after
Task 19b's path-construction rewrite.

```bash
git add src/dragen_align_pa/stages.py  # stage but don't commit yet
```

---

## Task 19b: Update `manage_dragen_mlr.py` to write outputs into the parent batch folder

Under the legacy per-SG layout, MLR wrote outputs to its own per-SG analysis folder. Under the new batched layout, MLR writes into the **per-SG subfolder of the parent DRAGEN batch's analysis folder** so all outputs for an SG (CRAM, base gVCF, recal gVCF, metrics) live together — making future cleanup and audit far simpler.

> **Ordering invariant (relied on by both MLR and the per-SG downloads):**
> When an SG is included in a retry batch, `ManageDragenPipeline` overwrites
> its per-SG state file with the retry batch's identifiers (pipeline_id,
> user_reference, batch_index). cpg-flow's `required_stages` declarations
> ensure `ManageDragenPipeline` finishes — including any retry pass — before
> MLR or any `Download*FromIca` stage runs. So the state file MLR reads is
> always the latest-generation entry, and there is no race between retry
> writes and MLR reads.

**Files:**
- Modify: `src/dragen_align_pa/jobs/manage_dragen_mlr.py`

The change is in two spots: the `output_folder_url` passed to `popgen-cli`, and the `ica_base_folder` used to locate the parent Dragen run's CRAM + gVCF inputs.

- [ ] **Step 1: Read the existing `_submit_mlr_run` and `_create_submit_callable`**

Open `src/dragen_align_pa/jobs/manage_dragen_mlr.py`. Locate `_submit_mlr_run` (around line 122) and `_create_submit_callable` (around line 201) for context.

- [ ] **Step 2: Replace the path-construction logic in `_submit_mlr_run`**

Find the block at the top of `_submit_mlr_run`:

```python
    with pipeline_id_arguid_path.open() as pid_arguid_fhandle:
        data: dict[str, str] = json.load(pid_arguid_fhandle)
        pipeline_id = data['pipeline_id']
        ar_guid = f'_{data["ar_guid"]}_'

    batch_tmpdir = os.environ.get('BATCH_TMPDIR', '/io')
    ica_base_folder = (
        f'/{BUCKET_NAME}/{ica_analysis_output_folder}/{sg_name}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name}/'
    )
```

Replace with:

```python
    with pipeline_id_arguid_path.open() as pid_arguid_fhandle:
        # No type annotation — `schema_version`/`batch_index` are ints while
        # `pipeline_id`/`ar_guid`/`user_reference` are strings, so a single
        # `dict[str, str]` annotation would lie. Let json.load's return type
        # stand (a dict mixing str/int/None values).
        data = json.load(pid_arguid_fhandle)
        # Validate the extended schema written by ManageDragenPipeline.
        # Pre-migration state files have neither `schema_version` nor
        # `user_reference` and would otherwise raise a bare `KeyError` mid-job.
        # Schema kept symmetric with `get_ica_sample_folder` (utils.py): same
        # `schema_version` check, same explicit required-key loop, so the
        # error messages an operator sees are identical regardless of which
        # consumer surfaces the bad file.
        schema_version = data.get('schema_version', 0)
        if schema_version != 1:
            raise ValueError(
                f'Per-SG state file {pipeline_id_arguid_path} has schema_version '
                f'{schema_version}; MLR expects 1 (extended schema with '
                f'`user_reference` / `batch_index`). Rerun the cohort with '
                f'force_resubmit=true (or manually delete the file) to '
                f'rewrite it under the new schema.',
            )
        for required in ('pipeline_id', 'user_reference'):
            if required not in data:
                raise KeyError(
                    f'Per-SG state file {pipeline_id_arguid_path} missing required '
                    f'key {required!r} under schema_version=1.',
                )
        pipeline_id = data['pipeline_id']
        user_reference = data['user_reference']  # extended schema, set by ManageDragenPipeline

    batch_tmpdir = os.environ.get('BATCH_TMPDIR', '/io')
    # MLR outputs land inside the parent Dragen batch's per-SG subfolder so all
    # outputs for an SG live together.
    ica_base_folder = (
        f'/{BUCKET_NAME}/{ica_analysis_output_folder}/{user_reference}-{pipeline_id}/{sg_name}/'
    )
```

- [ ] **Step 3: Update the `output_folder_url` construction**

Further down in `_submit_mlr_run`, find:

```python
        # --- 4. Build and run the popgen-cli command ---
        output_folder_url = f'{output_prefix}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name}'
```

Replace with:

```python
        # --- 4. Build and run the popgen-cli command ---
        output_folder_url = f'{output_prefix}/{user_reference}-{pipeline_id}/{sg_name}'
```

The `output_prefix` from `_create_submit_callable` is `ica://{dragen_align_project}/{BUCKET_NAME}/{output_folder}/{sg_name}` — note this still has a trailing `/{sg_name}` segment from the legacy layout. Update `_create_submit_callable` to drop that trailing segment:

Find:

```python
        output_prefix: str = (
            f'ica://{dragen_align_project}/{BUCKET_NAME}/'
            f'{config_retrieve(["ica", "data_prep", "output_folder"])}/{sg_name}'
        )
```

Replace with:

```python
        output_prefix: str = (
            f'ica://{dragen_align_project}/{BUCKET_NAME}/'
            f'{config_retrieve(["ica", "data_prep", "output_folder"])}'
        )
```

Net result: MLR's `output_folder_url` becomes `ica://{dragen_align_project}/{BUCKET_NAME}/{output_folder}/{user_reference}-{pipeline_id}/{sg_name}`, which is identical to the per-SG ICA folder that `get_ica_sample_folder` resolves to (without the trailing `/`).

- [ ] **Step 4: Smoke-check imports**

Run: `python -c "from dragen_align_pa.jobs import manage_dragen_mlr; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 5: Combined commit (Tasks 19 + 19b)**

Task 19's `DownloadMlrGvcfFromIca` rewiring (now in the index from Task 19's
Step 4) and Task 19b's MLR path-construction rewrite ship as one commit —
they're coupled at runtime: the download stage looks for MLR outputs in the
parent batch folder, which only holds true after this MLR-side change lands.

```bash
git add src/dragen_align_pa/jobs/manage_dragen_mlr.py
git status --short  # expect: M stages.py + M manage_dragen_mlr.py
git commit -m "Update DownloadMlrGvcfFromIca + MLR write path for batched layout"
```

---

## Task 20: Update `DownloadDataFromIca` to skip batch-root artefacts

**Files:**
- Modify: `src/dragen_align_pa/stages.py` (the `DownloadDataFromIca` class)
- Modify: `src/dragen_align_pa/jobs/download_ica_pipeline_outputs.py`

- [ ] **Step 1: Read `download_ica_pipeline_outputs.py`**

Read `src/dragen_align_pa/jobs/download_ica_pipeline_outputs.py` to understand what it currently downloads.

- [ ] **Step 2: Add an `ica_folder_path` parameter to its `run`**

Update the `run` function signature to accept `ica_folder_path: str` instead of recomputing the path internally. Restrict its download to **the contents of `{ica_folder_path}/`** which now corresponds to a single sample's subfolder (under the new batch layout). It must NOT recurse upward into the batch-root artefacts (`passfail.json`, `summary.json`, `reports/`) — those are handled by the new `DownloadBatchArtefactsFromIca` stage.

> **Note on the file-type filter:** `ica_utils.list_and_filter_ica_files`
> excludes files ending in `.cram`, `.cram.crai`, `.gvcf.gz`, `.gvcf.gz.tbi`.
> The recal-gVCFs (`*.hard-filtered.recal.gvcf.gz[.tbi]`) end in `.gvcf.gz` /
> `.gvcf.gz.tbi`, so they are also excluded by this filter — they are
> downloaded only by `DownloadMlrGvcfFromIca` (Task 19), not duplicated here.
> No filter change is needed.

- [ ] **Step 3: Update `DownloadDataFromIca.queue_jobs`**

In `stages.py`, modify the body so it calls into the new signature, using the same `pipeline_id_arguid_path` + `get_ica_sample_folder` pattern:

```python
    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput:
        outputs: cpg_utils.Path = self.expected_outputs(sequencing_group=sequencing_group)

        pipeline_id_arguid_path: cpg_utils.Path = inputs.as_dict(
            target=get_multicohort().get_cohorts()[0],
            stage=ManageDragenPipeline,
        )[f'{sequencing_group.name}_pipeline_id_and_arguid']

        ica_download_job: PythonJob = initialise_python_job(
            job_name='Download ICA bulk data',
            target=sequencing_group,
            tool_name='ICA-Python',
        )
        ica_download_job.image(image=get_driver_image())
        ica_download_job.spot(is_spot=False)
        ica_download_job.memory(memory='8Gi')

        ica_download_job.call(
            _resolve_then_download_bulk,
            sequencing_group=sequencing_group,
            pipeline_id_arguid_path=pipeline_id_arguid_path,
        )

        return self.make_outputs(target=sequencing_group, data=outputs, jobs=ica_download_job)
```

Add the helper near the other resolve helpers in `stages.py`:

```python
def _resolve_then_download_bulk(
    sequencing_group: SequencingGroup,
    pipeline_id_arguid_path: cpg_utils.Path,
) -> None:
    from dragen_align_pa.utils import get_ica_sample_folder  # noqa: PLC0415

    ica_folder = get_ica_sample_folder(pipeline_id_arguid_path, sequencing_group.name)
    download_ica_pipeline_outputs.run(
        sequencing_group=sequencing_group,
        ica_folder_path=ica_folder,
    )
```

- [ ] **Step 4: Smoke-check imports**

Run: `python -c "from dragen_align_pa import stages; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 5: Commit**

```bash
git add src/dragen_align_pa/stages.py src/dragen_align_pa/jobs/download_ica_pipeline_outputs.py
git commit -m "Update DownloadDataFromIca to download per-sample artefacts only"
```

---

## Task 21: Create `DownloadBatchArtefactsFromIca` cohort stage + job

**Files:**
- Create: `src/dragen_align_pa/jobs/download_batch_artefacts.py`
- Modify: `src/dragen_align_pa/stages.py` (add the new stage)

- [ ] **Step 1: Create the job module**

Stream all per-batch artefacts directly from ICA to GCS using the existing `ica_utils.stream_ica_file_to_gcs` helper — no local intermediate, no icav2 CLI. `passfail.json` and `summary.json` go in the batch's GCS folder; `reports/` is enumerated via `list_and_filter_ica_files` and each file streamed.

Create `src/dragen_align_pa/jobs/download_batch_artefacts.py`:

```python
"""Mirror per-batch artefacts (passfail.json, summary.json, reports/) from ICA to GCS.

One run per cohort; iterates over every successfully-submitted batch in the
batches file. Files are streamed directly from ICA to GCS via the existing
`ica_utils.stream_ica_file_to_gcs` helper — no local staging, no icav2 CLI.
"""

import json
from typing import Literal

import cpg_utils
import icasdk
from cpg_utils.config import config_retrieve
from google.cloud import storage
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.constants import BUCKET_NAME


def _stream_named_file(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    parent_folder: str,
    file_name: str,
    gcs_bucket: storage.Bucket,
    gcs_prefix: str,
) -> None:
    """Find one named file in `parent_folder` and stream it to `gcs_prefix/file_name`.

    Logs and returns silently if the file is not present (passfail.json/summary.json
    may legitimately be absent on a catastrophically-failed batch).

    Also tolerates transient `icasdk.ApiException` from the lookup — symmetric
    with the `reports/` enumeration's `try/except` below. We don't want a
    single transient API error to abort the whole batch-artefacts run mid-loop
    after partial GCS writes; missed files are observable via the absence of
    GCS objects and can be re-fetched by re-running the stage.
    """
    try:
        file_id = ica_api_utils.find_file_id_by_name(
            api_instance=api_instance,
            path_parameters=path_parameters,
            parent_folder_path=parent_folder,
            file_name=file_name,
        )
    except FileNotFoundError:
        logger.warning(f'{file_name} not present in {parent_folder}; skipping.')
        return
    except icasdk.ApiException as e:
        logger.warning(
            f'ICA API error while looking up {file_name} in {parent_folder}: {e}; skipping.',
        )
        return

    ica_utils.stream_ica_file_to_gcs(
        api_instance=api_instance,
        path_parameters=path_parameters,
        file_id=file_id,
        file_name=file_name,
        gcs_bucket=gcs_bucket,
        gcs_prefix=gcs_prefix,
        expected_md5_hash=None,
    )


def run(
    batches_file_path: cpg_utils.Path,
    gcs_output_root: cpg_utils.Path,
    marker_path: cpg_utils.Path,
) -> None:
    """For each successfully-submitted batch, mirror passfail.json/summary.json/reports/.

    Writes `marker_path` on completion so the stage has a deterministic expected_output.
    """
    with batches_file_path.open('r') as fh:
        data = json.load(fh)

    output_folder = config_retrieve(['ica', 'data_prep', 'output_folder'])
    cohort_name = batches_file_path.name.replace('_batches.json', '')

    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(BUCKET_NAME)
    # `gcs_output_root` is a `cpg_utils.Path` like `gs://{BUCKET}/ica/{ver}/output/dragen_batch_metrics`;
    # `gcs_prefix` for `stream_ica_file_to_gcs` must be relative to the bucket.
    # Assert the expected bucket prefix rather than silently `removeprefix`-ing —
    # if `output_path` ever returns a different bucket (test override, future
    # `category` redirect), `removeprefix` would be a no-op and we'd write
    # objects under a path like `gs://other-bucket/...` inside `BUCKET_NAME`.
    expected_prefix = f'gs://{BUCKET_NAME}/'
    gcs_output_str = str(gcs_output_root)
    if not gcs_output_str.startswith(expected_prefix):
        raise ValueError(
            f'gcs_output_root {gcs_output_str!r} does not start with expected '
            f'bucket prefix {expected_prefix!r}; refusing to derive a relative '
            f'GCS prefix that would land objects in the wrong bucket.',
        )
    base_prefix = gcs_output_str.removeprefix(expected_prefix)

    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    path_parameters = {'projectId': secrets['projectID']}

    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        for batch_entry in data['batches']:
            if not batch_entry.get('pipeline_id'):
                continue

            ica_folder = (
                f'/{BUCKET_NAME}/{output_folder}/'
                f'{batch_entry["user_reference"]}-{batch_entry["pipeline_id"]}/'
            )
            batch_name = f'{cohort_name}_batch{batch_entry["batch_index"]:04d}'
            gcs_prefix = f'{base_prefix}/{batch_name}'

            for name in ('passfail.json', 'summary.json'):
                _stream_named_file(
                    api_instance=api_instance,
                    path_parameters=path_parameters,
                    parent_folder=ica_folder,
                    file_name=name,
                    gcs_bucket=gcs_bucket,
                    gcs_prefix=gcs_prefix,
                )

            # `reports/` — enumerate every file under the subfolder and stream each.
            # The folder may be absent on a catastrophically-failed batch
            # (e.g. single-sample retry that aborted before producing reports);
            # treat that as a non-fatal warning so the rest of the batches
            # continue.
            reports_folder = f'{ica_folder}reports/'
            try:
                report_files = ica_utils.list_and_filter_ica_files(
                    api_instance=api_instance,
                    path_parameters=path_parameters,
                    base_ica_folder_path=reports_folder,
                )
            except icasdk.ApiException as e:
                logger.warning(
                    f'Batch {batch_name}: reports/ folder not enumerable at '
                    f'{reports_folder} ({e}); skipping.',
                )
                report_files = []

            for report_name, report_id in report_files:
                ica_utils.stream_ica_file_to_gcs(
                    api_instance=api_instance,
                    path_parameters=path_parameters,
                    file_id=report_id,
                    file_name=report_name,
                    gcs_bucket=gcs_bucket,
                    gcs_prefix=f'{gcs_prefix}/reports',
                    expected_md5_hash=None,
                )

    with marker_path.open('w') as fh:
        fh.write(f'Downloaded batch artefacts for {cohort_name}\n')
```

Note: `ica_utils.list_and_filter_ica_files` (`ica_utils.py:208`) currently filters out CRAM/gVCF — verify before using it for `reports/`. If its filter rules don't match, write a thin sibling helper `list_ica_files(api_instance, path_parameters, folder_path)` that lists all files without filtering and call that instead.

- [ ] **Step 2: Add the `DownloadBatchArtefactsFromIca` stage**

In `stages.py`, add a new stage definition immediately after `DownloadDataFromIca`:

```python
@stage(required_stages=[ManageDragenPipeline, DownloadDataFromIca])
class DownloadBatchArtefactsFromIca(CohortStage):
    """One-shot per-batch download of passfail.json / summary.json / reports/."""

    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:  # pyright: ignore[reportIncompatibleMethodOverride]
        # Sibling marker in the dragen_batch_metrics/ root. Use the shared helper
        # (Task 8) so the `ica/{DRAGEN_VERSION}/output/` prefix is built in
        # exactly one place — no double-prefixing.
        return get_batch_artefacts_root(cohort.name) / f'{cohort.name}_artefacts_done.txt'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput:
        batches_file_path: cpg_utils.Path = inputs.as_dict(target=cohort, stage=ManageDragenPipeline)[
            f'{cohort.name}_batches'
        ]
        gcs_output_root = get_batch_artefacts_root(cohort.name)
        marker_path = self.expected_outputs(cohort=cohort)

        job: PythonJob = initialise_python_job(
            job_name='DownloadBatchArtefactsFromIca',
            target=cohort,
            tool_name='ICA-Python',
        )
        job.image(image=get_driver_image())
        job.memory('4Gi')
        job.spot(is_spot=False)
        job.call(
            download_batch_artefacts.run,
            batches_file_path=batches_file_path,
            gcs_output_root=gcs_output_root,
            marker_path=marker_path,
        )

        return self.make_outputs(target=cohort, data=marker_path, jobs=job)
```

- [ ] **Step 3: Add the import for `download_batch_artefacts` at the top of `stages.py`**

In the `from dragen_align_pa.jobs import (...)` block, add `download_batch_artefacts,` in alphabetical order. Also import `get_batch_artefacts_root` from `dragen_align_pa.utils` alongside the other utils imports.

- [ ] **Step 4: Smoke-check imports**

Run: `python -c "from dragen_align_pa import stages; print('ok')"`
Expected: prints `ok`.

> Note: `DeleteDataInIca.required_stages` is **not** updated here. The
> `DeleteDataInIca` rewrite is deferred to a follow-up PR (see Task 17's
> note); on this branch it remains broken from the `PrepareIcaForDragenAnalysis`
> simplification and should not be touched piecemeal.

- [ ] **Step 5: Commit**

```bash
git add src/dragen_align_pa/stages.py src/dragen_align_pa/jobs/download_batch_artefacts.py
git commit -m "Add DownloadBatchArtefactsFromIca cohort stage for per-batch passfail/summary/reports"
```

---

## Task 22: Remove `UploadFastqFileList` stage and obsolete module

**Files:**
- Modify: `src/dragen_align_pa/stages.py` (delete the `UploadFastqFileList` class and its references)
- Delete: `src/dragen_align_pa/jobs/upload_fastq_file_list.py`

- [ ] **Step 1: Delete the stage class**

In `stages.py`, delete the entire `@stage(...)` + `class UploadFastqFileList(CohortStage): …` block.

- [ ] **Step 2: Drop the import**

In the `from dragen_align_pa.jobs import (…)` block, remove `upload_fastq_file_list,`.

- [ ] **Step 3: Delete the job module**

Run: `git rm src/dragen_align_pa/jobs/upload_fastq_file_list.py`

- [ ] **Step 4: Smoke-check imports**

Run: `python -c "from dragen_align_pa import stages; print('ok')"`
Expected: prints `ok`. If any other module imports `upload_fastq_file_list`, fix those imports.

- [ ] **Step 5: Commit**

`git rm` in Step 3 already staged the deletion; `git add` here stages the
`stages.py` edits. Verify both changes are staged before committing.

```bash
git add src/dragen_align_pa/stages.py
git status --short  # expect: D upload_fastq_file_list.py + M stages.py
git commit -m "Remove obsolete UploadFastqFileList stage (per-batch upload moved into submitter)"
```

---

## Task 23: Delete `run_align_genotype_with_dragen.py`

**Files:**
- Delete: `src/dragen_align_pa/jobs/run_align_genotype_with_dragen.py`

- [ ] **Step 1: Confirm no consumers**

Run: `grep -rn "run_align_genotype_with_dragen" src/`
Expected: only the file itself and nothing else.

- [ ] **Step 2: Delete the file**

Run: `git rm src/dragen_align_pa/jobs/run_align_genotype_with_dragen.py`

- [ ] **Step 3: Smoke-check imports**

Run: `python -c "from dragen_align_pa import stages; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 4: Commit**

```bash
git commit -m "Delete obsolete run_align_genotype_with_dragen.py (replaced by submit_dragen_batch.py)"
```

---

## Task 24: Update READMEs and workflow DAG

**Files:**
- Modify: `README.md` — describe batched submission, new stage, removed stage, config keys
- Modify: `README_developer.md` — update stage list and config docs
- Modify: `README_lead.md` — update pipeline overview
- Modify: `workflow_dag.dot` — replace per-SG pipeline submission with batched flow + add `DownloadBatchArtefactsFromIca` node
- Regenerate: `workflow_dag.svg` from the updated `.dot`

- [ ] **Step 1: Edit `README.md`**

Update the "Pipeline Overview" section to describe:
- The unified pipeline (single ID), CRAM+FASTQ, WGS+WES
- Batching (`ica.dragen.batch_size`, default 5)
- Per-sample retry behaviour via `passfail.json`
- The new `DownloadBatchArtefactsFromIca` stage and its `dragen_batch_metrics/` GCS path
- New config sections (`[ica.dragen]`, presets) — note that the WES preset needs PoN/BED file IDs filled in per cohort

- [ ] **Step 2: Edit `README_developer.md`**

Update the stages list to reflect:
- `UploadFastqFileList` removed
- `ManageDragenPipeline` now batches
- `DownloadBatchArtefactsFromIca` added
- All downloads now resolve via `get_ica_sample_folder`

Also update the config section to document the new keys (`[ica.dragen]`, `[ica.dragen.presets.genome|exome]`, `[ica.dragen.user]`, `[ica.qc].coverage_region_beds`).

- [ ] **Step 3: Edit `README_lead.md`**

Update the pipeline scope paragraph to mention the unified pipeline and remove the implicit reference to two separate pipelines.

- [ ] **Step 4: Update `workflow_dag.dot`**

Edit the `.dot` to:
- Remove the `UploadFastqFileList` node
- Add a `DownloadBatchArtefactsFromIca` node
- Wire `DownloadBatchArtefactsFromIca` as a successor of `ManageDragenPipeline` + predecessor of `DeleteDataInIca`
- Update any per-SG / per-batch labels for `ManageDragenPipeline` to reflect "(C)\n[ICA batched]"

- [ ] **Step 5: Regenerate `workflow_dag.svg`**

Run: `dot -Tsvg workflow_dag.dot -o workflow_dag.svg`
Expected: no errors. (If `graphviz` is not installed, install it: `brew install graphviz` on macOS.)

- [ ] **Step 6: Commit**

```bash
git add README.md README_developer.md README_lead.md workflow_dag.dot workflow_dag.svg
git commit -m "Update README and workflow DAG for unified DRAGEN pipeline migration"
```

> **PR checkpoint B** — End-to-end pipeline (minus `DeleteDataInIca` rewrite, which is intentionally deferred per Section 7 of the design doc) is wired and documented. Ready for the post-merge validation runs in Task 25.

---

## Task 25: Post-merge validation checklist

Mirrors Section 7 of the design doc. These steps are NOT executed during the implementation pass — they are the validation gates before promoting the migration from `dragen-unified-dev` to `main`. Each item maps to a tracked check (PR comment, test script output, or smoke run).

- [ ] **V1. Unit-test path construction against the demo bundle**
  - Run `pytest tests/test_path_resolution.py tests/test_batches.py tests/test_passfail_parsing.py tests/test_submit_dragen_batch.py tests/test_manage_dragen_pipeline.py -v`
  - Expected: all green.

- [ ] **V2. Dry-run a small cohort**
  - Author a new `local_test/<dataset>-unified-test.toml` with the new `[ica.dragen]` schema (the existing `local_test/*.toml` files reference removed keys — create fresh ones).
  - Run with `--dry_run`. Verify stage-graph construction does not fail on import or `expected_outputs` errors.

- [ ] **V3. Small real cohort (2 SGs, single batch, FASTQ mode)**
  - End-to-end FASTQ run with one batch of two SGs.
  - Verify CRAM + gVCF in per-SG GCS paths.
  - Verify `passfail.json` / `summary.json` / `reports/` under `dragen_batch_metrics/{cohort}_batch0000/`.
  - Verify Metamist registration per SG via the existing decorators (`analysis_type='cram'`, `analysis_type='gvcf'`).

- [ ] **V4. Two-batch cohort (7 SGs, batch_size=5)**
  - Verify two ICA submissions, both finish, downloads land per-SG, batch artefacts land per-batch.
  - Verify MLR outputs (`*.hard-filtered.recal.gvcf.gz`) live in the same per-SG subfolder as the Dragen outputs.

- [ ] **V5. Forced-failure cohort**
  - Poison one SG's input so the pipeline marks it `Fail` in `passfail.json`.
  - Verify a retry batch is formed containing just that SG with `error_strategy=continue`.
  - Verify final state in `{cohort}_batches.json` reflects the retry outcome.

- [ ] **V6. Resume test**
  - Kill the analysis-runner job mid-flight after the first batch is submitted.
  - Re-launch with `monitor_previous=true`.
  - Verify monitoring resumes from the batches file and the second batch (or retry batch) progresses.

- [ ] **V7. Correctness comparison against the existing 3.2.x pipeline**
  - Pick one cohort already processed by `main`.
  - Rerun under `dragen-unified-dev`.
  - Bytewise-compare gVCFs (after sorting headers if order differs); `samtools quickcheck` + header diff on CRAMs; diff Metamist analyses.
  - Diff goes in the merge-to-main PR description.

> Only after V1–V7 all pass should `dragen-unified-dev` be merged to `main`.

---

## Self-review checklist (run before declaring complete)

- [ ] Spec coverage: every section of `docs/superpowers/specs/2026-05-11-unified-dragen-pipeline-design.md` has a corresponding task (or is explicitly deferred — only `DeleteDataInIca` rewrite is in that category).
- [ ] No placeholders: search the plan for "TBD", "TODO", "implement later" — should return zero results.
- [ ] Type/method consistency: `Batch.name`, `BatchesFile` API, `get_ica_sample_folder` signature, `submit_dragen_batch.run` signature, `manage_dragen_pipeline.run` signature, `on_succeeded` callback signature — all referenced consistently across tasks.
- [ ] Each task has at least one bash command + expected output for verification, except where the task is purely documentation.

## Things explicitly out of scope (handle in follow-up PRs)

- `DeleteDataInIca` rewrite for batched outputs (Section 7 of design doc; deferred).
- `local_test/*.toml` examples refresh — current examples reference the old `ica.pipelines.cram`/`fastq` keys. Per agreement, new test configs will be authored in Task 25 (V2).
- Pipeline version bump (likely → 4.0.0).

## Known limitations documented in the plan but acknowledged as accepted simplifications

- **Batch-name width is fixed at `:04d`** (not variable `max(2, len(str(N-1)))` as the spec text originally suggested) — supports up to 9999 batches (≈ 50000 SGs at `batch_size=5`). Spec text updated to match.
- **`BUCKET_NAME` is imported at module top in `submit_dragen_batch`, `manage_dragen_pipeline`, `download_batch_artefacts`** — tests that need to patch the bucket name must patch each consumer module, not just `dragen_align_pa.utils`. Tests in this plan rely on the helper functions being called from `utils` (via `get_ica_sample_folder`), where a single patch site works.
