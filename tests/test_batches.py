from pathlib import Path

import pytest

from dragen_align_pa.batches import Batch, BatchesFile, chunk_sgs_into_batches


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


def test_add_retry_batch_rejects_invalid_error_strategy(tmp_path: Path):
    """ICA only accepts {auto, continue, terminate}. A typo (e.g. trailing
    whitespace, wrong case) must surface here as a clear ValueError, not as
    an obscure ICA pipeline-parameter rejection at submission time."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
    ])
    with pytest.raises(ValueError, match='error_strategy'):
        bf.add_retry_batch(sg_names=['CPG_A'], error_strategy='CONTINUE')


def test_record_error_strategy_rejects_invalid_value(tmp_path: Path):
    """record_error_strategy() writes the value verbatim into batches.json
    and downstream submission. Reject anything outside the allowed set."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
    ])
    with pytest.raises(ValueError, match='error_strategy'):
        bf.record_error_strategy(batch_index=0, error_strategy='continue ')  # trailing whitespace


def test_record_status_rejects_invalid_status(tmp_path: Path):
    """`failed_sg_names`, `cancelled_sg_names`, `successful_sg_names` compare
    against literal status strings. A typo from a future caller (e.g. the
    orchestrator's `FAILED_FINAL` leaking through unmapped) would silently
    produce a batch that is neither successful, failed, nor cancelled —
    breaking the 5%-threshold check and the resume-after-cancel guard."""
    path = tmp_path / 'COH0001_batches.json'
    bf = BatchesFile(path=path)
    bf.initialise(batch_size=5, batches=[
        Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A']),
    ])
    with pytest.raises(ValueError, match='FAILED_FINAL'):
        bf.record_status(batch_index=0, status='FAILED_FINAL')


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


def test_find_batch_for_sg_robust_to_out_of_order_storage(tmp_path: Path):
    """Defend the documented contract ('most recent batch wins') against an
    out-of-order self.batches list. The previous implementation iterated
    and overwrote, which was correct only if batches were stored in
    ascending batch_index order. A future refactor or hand-edit could
    break that silently."""
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
