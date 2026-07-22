import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from dragen_align_pa.batches import BatchesFile, IcaBatch, chunk_sgs_into_batches


def test_batches_module_imports_without_a_loaded_config():
    """M5 regression: `batches.py` must import without a CPG config present. It
    pulls its constants from the config-free `batch_constants` module, not from
    `ica_constants.py` (which reads the analysis config at import time). Run in a fresh
    subprocess with `CPG_CONFIG_PATH` cleared so no config is available — the
    in-process `conftest` monkeypatch does not reach it."""
    # pyproject's `pythonpath=['src']` only applies to the pytest process, not
    # this child, so put `src` on the child's PYTHONPATH to match — otherwise the
    # test would need the package installed and fail with ModuleNotFoundError.
    src_dir = Path(__file__).resolve().parent.parent / 'src'
    env = {k: v for k, v in os.environ.items() if k != 'CPG_CONFIG_PATH'}
    env['PYTHONPATH'] = os.pathsep.join(p for p in (str(src_dir), env.get('PYTHONPATH', '')) if p)
    result = subprocess.run(
        [sys.executable, '-c', 'import dragen_align_pa.batches'],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, f'batches failed to import without config:\n{result.stderr}'


def test_batch_name_zero_padded_four_digits():
    b = IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
    assert b.name == 'COH0001-batch0000'


def test_batch_name_four_digit_index():
    b = IcaBatch(cohort_name='COH0001', batch_index=12, sg_names=[])
    assert b.name == 'COH0001-batch0012'


def test_batch_name_handles_large_index():
    """Width 4 supports up to 9999 batches (= 49995 SGs at batch_size 5) without lex-sort
    breakage. Beyond that, names overflow the field but stay sortable for adjacent ranges."""
    b = IcaBatch(cohort_name='COH0001', batch_index=1234, sg_names=[])
    assert b.name == 'COH0001-batch1234'


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


def test_batches_file_roundtrip(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
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
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
        ],
    )
    bf.write()
    bf.record_pipeline_submission(
        batch_index=0,
        pipeline_id='abc',
        ar_guid='xyz',
        user_reference='COH0001-batch0000_xyz_',
    )
    bf.write()

    loaded = BatchesFile(path=path)
    loaded.read()
    assert loaded.batches[0]['pipeline_id'] == 'abc'
    assert loaded.batches[0]['ar_guid'] == 'xyz'
    assert loaded.batches[0]['user_reference'] == 'COH0001-batch0000_xyz_'


def test_batches_file_record_passfail(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    bf.record_passfail(batch_index=0, passfail={'CPG_A': 'Success', 'CPG_B': 'Fail'})
    bf.write()

    loaded = BatchesFile(path=path)
    loaded.read()
    assert loaded.failed_sg_names() == ['CPG_B']
    assert loaded.successful_sg_names() == ['CPG_A']
    assert loaded.batches[0]['passfail_seen'] is True


def test_record_passfail_normalises_dragen_failed_status(tmp_path: Path):
    """DRAGEN writes `"Failed"`; record_passfail normalises it to canonical `"Fail"`
    so `failed_sg_names` (and the retry path) pick the sample up."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    bf.record_passfail(batch_index=0, passfail={'CPG_A': 'Success', 'CPG_B': 'Failed'})
    assert bf.batches[0]['passfail'] == {'CPG_A': 'Success', 'CPG_B': 'Fail'}
    assert bf.failed_sg_names() == ['CPG_B']
    assert bf.successful_sg_names() == ['CPG_A']


def test_read_migrates_legacy_failed_passfail_value(tmp_path: Path):
    """A batches.json written by an earlier build stored DRAGEN's raw `"Failed"`
    verbatim in an already-SUCCEEDED batch. read() must re-normalise it to `"Fail"`
    so a resume doesn't silently drop the sample from failed_sg_names / the retry
    path (the straddling-deploy blind spot)."""
    path = tmp_path / 'COH0001_batches.json'
    legacy = {
        'schema_version': 1,
        'batch_size': 5,
        'n_batches': 1,
        'batches': [
            {
                'batch_index': 0,
                'retry_generation': 0,
                'sg_names': ['CPG_A', 'CPG_B'],
                'retried_sgs': [],
                'user_reference': None,
                'pipeline_id': None,
                'ar_guid': None,
                'analysis_output_folder_fid': None,
                'fastq_list_fid': None,
                'cram_fids': None,
                'status': 'SUCCEEDED',
                'passfail': {'CPG_A': 'Success', 'CPG_B': 'Failed'},  # legacy raw value
                'passfail_seen': True,
                'has_been_retried': False,
                'error_strategy': 'auto',
            },
        ],
    }
    path.write_text(json.dumps(legacy))

    bf = BatchesFile(path=path)
    bf.read()
    assert bf.batches[0]['passfail'] == {'CPG_A': 'Success', 'CPG_B': 'Fail'}
    assert bf.failed_sg_names() == ['CPG_B']


def test_record_passfail_raises_on_unknown_status(tmp_path: Path):
    """An unrecognised status raises loudly rather than silently recording a value
    that matches neither `== 'Fail'` nor `== 'Success'`."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    with pytest.raises(ValueError, match=r'passfail status must be one of'):
        bf.record_passfail(batch_index=0, passfail={'CPG_A': 'Success', 'CPG_B': 'PASS'})


def test_batches_file_record_cram_fids(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
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


def test_batches_file_rejects_missing_per_batch_keys(tmp_path: Path):
    """A truncated / hand-edited per-batch entry must surface at read() with
    a clear message naming the missing key, not as a bare KeyError much
    later from `failed_sg_names()` / `find_batch_for_sg()` etc."""
    path = tmp_path / 'COH0001_batches.json'
    payload = {
        'schema_version': 1,
        'batch_size': 5,
        'batches': [
            {
                'batch_index': 0,
                'retry_generation': 0,
                'sg_names': ['CPG_A'],
                # missing 'status', 'passfail', 'passfail_seen', etc.
            },
        ],
    }
    path.write_text(json.dumps(payload))
    bf = BatchesFile(path=path)
    with pytest.raises(ValueError, match='status'):
        bf.read()


def test_batches_file_write_persists(tmp_path: Path):
    """Single-PUT atomic write — GCS object PUT is atomic per object."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
        ],
    )
    bf.write()
    assert path.is_file()
    # No .tmp sidecar exists — we use a direct write, not tmp+rename.
    assert not (tmp_path / 'COH0001_batches.json.tmp').exists()


def test_add_retry_batch_single_sample_uses_continue(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    new_idx = bf.add_retry_batch(sg_names=['CPG_B'])
    assert new_idx == 1
    assert bf.batches[1]['retry_generation'] == 1
    assert bf.batches[1]['has_been_retried'] is True
    assert bf.batches[1]['error_strategy'] == 'continue'
    assert bf.batches[1]['sg_names'] == ['CPG_B']


def test_add_retry_batch_multi_sample_uses_auto(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    bf.add_retry_batch(sg_names=['CPG_A', 'CPG_B'])
    assert bf.batches[1]['error_strategy'] == 'auto'


def test_add_retry_batch_rejects_invalid_error_strategy(tmp_path: Path):
    """ICA only accepts {auto, continue, terminate}. A typo (e.g. trailing
    whitespace, wrong case) must surface here as a clear ValueError, not as
    an obscure ICA pipeline-parameter rejection at submission time."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
        ],
    )
    with pytest.raises(ValueError, match='error_strategy'):
        bf.add_retry_batch(sg_names=['CPG_A'], error_strategy='CONTINUE')


def test_record_error_strategy_rejects_invalid_value(tmp_path: Path):
    """record_error_strategy() writes the value verbatim into batches.json
    and downstream submission. Reject anything outside the allowed set."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
        ],
    )
    with pytest.raises(ValueError, match='error_strategy'):
        bf.record_error_strategy(batch_index=0, error_strategy='continue ')  # trailing whitespace


def test_failed_sg_names_dedupes_across_retry_generations(tmp_path: Path):
    """An SG that fails in gen=0 (initial) AND gen=1 (retry) must appear
    once in failed_sg_names(). The orchestrator's completion-marker failure
    count uses `len(failed_sg_names())`; double-counting would inflate the
    reported failure count on cohorts with retries."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    # Gen 0 fails CPG_A.
    bf.record_passfail(0, {'CPG_A': 'Fail', 'CPG_B': 'Success'})
    bf.record_status(0, 'FAILED')
    # Retry batch (gen 1) re-runs CPG_A and it fails again.
    bf.add_retry_batch(sg_names=['CPG_A'])
    bf.record_passfail(1, {'CPG_A': 'Fail'})
    bf.record_status(1, 'FAILED')

    assert bf.failed_sg_names() == ['CPG_A'], f'CPG_A should appear once; got {bf.failed_sg_names()}'


def test_failed_sg_names_excludes_sg_recovered_by_retry(tmp_path: Path):
    """An SG that fails in gen=0 but SUCCEEDS in its gen=1 retry must NOT count
    as failed: `failed_sg_names()` resolves each SG to its latest generation.
    The orchestrator raises when `failed_sg_names()` is non-empty after the
    retry pass, so a recovered SG lingering here would abort a run the retry
    actually saved."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    # Gen 0: CPG_B fails, CPG_A succeeds.
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Fail'})
    bf.record_status(0, 'SUCCEEDED')
    # Gen 1 retry re-runs CPG_B and it succeeds.
    bf.add_retry_batch(sg_names=['CPG_B'])
    bf.record_passfail(1, {'CPG_B': 'Success'})
    bf.record_status(1, 'SUCCEEDED')

    assert bf.failed_sg_names() == [], f'CPG_B recovered on retry; got {bf.failed_sg_names()}'
    assert sorted(bf.successful_sg_names()) == ['CPG_A', 'CPG_B']


def test_failed_sg_names_whole_batch_failure_recovered_by_retry(tmp_path: Path):
    """A batch-level FAILED (no passfail) marks every SG Fail, but a later retry
    that succeeds those SGs wins: whole-batch infrastructure failure recovered on
    retry must not count as failed."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    # Gen 0: whole batch fails at ICA level (no passfail produced).
    bf.record_status(0, 'FAILED')
    # Gen 1 retry re-runs both and they succeed.
    bf.add_retry_batch(sg_names=['CPG_A', 'CPG_B'])
    bf.record_passfail(1, {'CPG_A': 'Success', 'CPG_B': 'Success'})
    bf.record_status(1, 'SUCCEEDED')

    assert bf.failed_sg_names() == [], f'both SGs recovered on retry; got {bf.failed_sg_names()}'


def test_failed_sg_names_counts_failed_batch_with_empty_passfail(tmp_path: Path):
    """A FAILED batch whose passfail is an empty dict (hand-edited/older file) counts
    as a whole-batch failure — resolution keys on falsy passfail, not `is None`, so it
    is never silently dropped."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    bf.record_status(0, 'FAILED')
    bf.batch_entry(0)['passfail'] = {}  # not the None the writers produce

    assert bf.failed_sg_names() == ['CPG_A', 'CPG_B']


def test_success_at_any_generation_beats_later_failure(tmp_path: Path):
    """force_retry can reconcile an original batch to SUCCEEDED after a superseding
    retry has already failed (GCS-drift recovery). The confirmed Success must win:
    the SG is not resubmitted, and path resolution points at the successful batch."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])],
    )
    # Gen 1 retry, spawned while GCS thought batch 0 failed, genuinely failed.
    bf.add_retry_batch(sg_names=['CPG_A'])
    bf.record_status(1, 'FAILED')  # whole-batch fail, passfail stays None
    # Reconcile then finds batch 0 actually SUCCEEDED at ICA.
    bf.record_passfail(0, {'CPG_A': 'Success'})
    bf.record_status(0, 'SUCCEEDED')

    assert bf.successful_sg_names() == ['CPG_A']
    assert bf.failed_sg_names() == []
    found = bf.find_batch_for_sg('CPG_A')
    assert found is not None
    assert found['batch_index'] == 0  # the successful generation, not the failed retry


def test_batch_entry_resolves_by_index_not_position(tmp_path: Path):
    """record_* / clear_passfail resolve by batch_index via `batch_entry`, not list
    position — so a batches list stored out of order (hand-edit / future reorder)
    mutates the right entry rather than a positional neighbour."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch('COH0001', 0, ['CPG_A']),
            IcaBatch('COH0001', 1, ['CPG_B']),
        ],
    )
    bf.batches.reverse()  # position 0 now holds batch_index 1, and vice versa

    bf.record_status(0, 'FAILED')

    assert bf.batch_entry(0)['status'] == 'FAILED'  # batch_index 0 mutated
    assert bf.batch_entry(1)['status'] == 'PENDING'  # batch_index 1 untouched
    with pytest.raises(KeyError, match='batch_index=2'):
        bf.batch_entry(2)


def test_successful_sg_names_resolves_latest_generation(tmp_path: Path):
    """`successful_sg_names` is latest-generation-aware and symmetric with
    `failed_sg_names`: an SG whose gen-0 outcome was Fail but whose gen-1 retry
    Succeeded appears in successful (and NOT in failed), counted once."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Fail'})
    bf.record_status(0, 'SUCCEEDED')
    bf.add_retry_batch(sg_names=['CPG_B'])  # gen-1 retry, index 1
    bf.record_passfail(1, {'CPG_B': 'Success'})
    bf.record_status(1, 'SUCCEEDED')

    assert sorted(bf.successful_sg_names()) == ['CPG_A', 'CPG_B']
    assert bf.successful_sg_names().count('CPG_B') == 1  # not double-counted across generations
    assert bf.failed_sg_names() == []


def test_record_status_rejects_invalid_status(tmp_path: Path):
    """`failed_sg_names`, `cancelled_sg_names`, `successful_sg_names` compare
    against literal status strings. A typo from a future caller (e.g. the
    orchestrator's `FAILED_FINAL` leaking through unmapped) would silently
    produce a batch that is neither successful, failed, nor cancelled —
    breaking the failure/success counts and the resume-after-cancel guard."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
        ],
    )
    with pytest.raises(ValueError, match='FAILED_FINAL'):
        bf.record_status(batch_index=0, status='FAILED_FINAL')


def test_mark_sgs_retried_tracks_per_sg(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    bf.mark_sgs_retried(source_batch_idx=0, sg_names=['CPG_B'])
    # `retried_sgs` accrues per-SG; `has_been_retried` is the action gate and
    # flips True on the first retry (no second-retry allowed regardless of count).
    assert bf.batches[0]['retried_sgs'] == ['CPG_B']
    assert bf.batches[0]['has_been_retried'] is True
    bf.mark_sgs_retried(source_batch_idx=0, sg_names=['CPG_A'])
    assert sorted(bf.batches[0]['retried_sgs']) == ['CPG_A', 'CPG_B']


def test_failed_excludes_cancelled_and_cancelled_helper_returns_them(tmp_path: Path):
    """Round-4 policy: `cancel_cohort_run` is user-initiated and must NOT
    be counted as a failure. `failed_sg_names` excludes CANCELLED; a
    sibling `cancelled_sg_names` reports them separately."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
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
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
            IcaBatch(cohort_name='COH0001', batch_index=1, sg_names=['CPG_C', 'CPG_D']),
        ],
    )
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Success'})
    bf.record_status(0, 'SUCCEEDED')
    bf.record_status(1, 'CANCELLED')
    assert sorted(bf.cancelled_sg_names()) == ['CPG_C', 'CPG_D']
    assert sorted(bf.successful_sg_names()) == ['CPG_A', 'CPG_B']
    # Failed list excludes CANCELLED — guard logic at run()'s tail relies on this
    # being the failure-only signal so the failure count doesn't conflate user
    # cancellations with pipeline failures.
    assert bf.failed_sg_names() == []


def test_find_batch_for_sg_prefers_latest_generation(tmp_path: Path):
    """SGs may appear in both an initial batch (gen=0) and a retry batch (gen=1).
    The most recent batch is the relevant one for path resolution."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Fail'})
    bf.add_retry_batch(sg_names=['CPG_B'])
    found = bf.find_batch_for_sg('CPG_B')
    assert found is not None
    assert found['retry_generation'] == 1
    assert found['batch_index'] == 1


def test_initialise_single_sample_batch_uses_continue(tmp_path: Path):
    """DRAGEN's `auto` error_strategy aborts single-sample runs before
    passfail.json is written. `initialise()` must mirror `add_retry_batch`'s
    heuristic so an initial cohort with 1 SG — or a trailing single-SG batch —
    gets `continue` and produces a passfail."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
        ],
    )
    assert bf.batches[0]['error_strategy'] == 'continue'


def test_initialise_multi_sample_batch_uses_auto(tmp_path: Path):
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    assert bf.batches[0]['error_strategy'] == 'auto'


def test_initialise_mixed_batch_sizes_apply_strategy_per_batch(tmp_path: Path):
    """Trailing single-SG batch (e.g. cohort of 6 with batch_size 5) is the
    realistic regression path — the trailing batch silently aborts unless
    initialise applies the per-batch heuristic."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B', 'CPG_C', 'CPG_D', 'CPG_E']),
            IcaBatch(cohort_name='COH0001', batch_index=1, sg_names=['CPG_F']),
        ],
    )
    assert bf.batches[0]['error_strategy'] == 'auto'
    assert bf.batches[1]['error_strategy'] == 'continue'


def test_successful_sg_names_excludes_cancelled_batches(tmp_path: Path):
    """A batch that records passfail then immediately gets CANCELLED must
    NOT appear in successful_sg_names(). Otherwise the orchestrator's
    resume-after-cancel guard fires for SGs that already succeeded —
    `failed_sg_names` already excludes CANCELLED for the symmetric reason."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A', 'CPG_B']),
        ],
    )
    bf.record_passfail(0, {'CPG_A': 'Success', 'CPG_B': 'Success'})
    bf.record_status(0, 'CANCELLED')
    assert bf.successful_sg_names() == []
    assert sorted(bf.cancelled_sg_names()) == ['CPG_A', 'CPG_B']


def test_find_batch_for_sg_robust_to_out_of_order_storage(tmp_path: Path):
    """Defend the documented contract ('most recent batch wins') against an
    out-of-order self.batches list. The previous implementation iterated
    and overwrote, which was correct only if batches were stored in
    ascending batch_index order. A future refactor or hand-edit could
    break that silently."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(
        batch_size=5,
        batches=[
            IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
        ],
    )
    # Simulate a corrupt/reordered file: prepend the retry batch.
    retry_entry = bf._new_batch_entry(
        IcaBatch(cohort_name='', batch_index=1, sg_names=['CPG_A']),
        retry_generation=1,
    )
    bf.batches.insert(0, retry_entry)  # now order is [batch_index=1, batch_index=0]
    found = bf.find_batch_for_sg('CPG_A')
    assert found is not None
    assert found['batch_index'] == 1  # the highest, regardless of list position
    assert found['retry_generation'] == 1
