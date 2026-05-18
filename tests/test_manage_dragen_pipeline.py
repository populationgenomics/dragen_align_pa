"""Unit tests for the manage_dragen_pipeline orchestrator helpers.

Covers the algorithmic helpers: _build_retry_batches (per-sample retry
discipline + single-retry invariant + CANCELLED-as-terminal),
_harvest_ar_guids_from_per_sg_state, _handle_management_flags
(force_resubmit harvest+delete, cancel preserves per-SG state),
_threshold_breached (5% boundary semantics).

Integration glue around the shared monitor loop (_on_succeeded_factory,
_on_status_change_factory, _build_submit_callable) is left for end-to-end
validation — those helpers' behaviours depend on cpg-flow runtime state
that can't reasonably be mocked here.
"""

import json
from pathlib import Path

import pytest

from dragen_align_pa.batches import BatchesFile, IcaBatch
from dragen_align_pa.jobs.manage_dragen_pipeline import (
    CohortCancelled,
    _build_retry_batches,
    _handle_management_flags,
    _harvest_ar_guids_from_per_sg_state,
    _threshold_breached,
)


def _make_file(tmp_path: Path, batches: list[IcaBatch]) -> BatchesFile:
    bf = BatchesFile(path=tmp_path / 'COH0001_batches.json')
    bf.initialise(batch_size=5, batches=batches)
    bf.write()
    return bf


def test_retry_batches_empty_when_no_failures(tmp_path: Path):
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B'])])
    bf.record_passfail(0, {'SYN_A': 'Success', 'SYN_B': 'Success'})
    new = _build_retry_batches(cohort_name='COH0001', batches_file=bf, batch_size=5)
    assert new == []


def test_retry_batches_from_passfail_failure(tmp_path: Path):
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B'])])
    bf.record_passfail(0, {'SYN_A': 'Success', 'SYN_B': 'Fail'})
    new = _build_retry_batches(cohort_name='COH0001', batches_file=bf, batch_size=5)
    assert len(new) == 1
    assert new[0].sg_names == ['SYN_B']
    assert new[0].batch_index == 1
    assert bf.batches[0]['has_been_retried'] is True
    # Retry batch entry is appended, pre-marked has_been_retried=True and retry_generation=1.
    assert bf.batches[1]['retry_generation'] == 1
    assert bf.batches[1]['has_been_retried'] is True


def test_retry_batches_single_sample_uses_continue_strategy(tmp_path: Path):
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B'])])
    bf.record_passfail(0, {'SYN_A': 'Success', 'SYN_B': 'Fail'})
    _build_retry_batches(cohort_name='COH0001', batches_file=bf, batch_size=5)
    assert bf.batches[1]['error_strategy'] == 'continue'


def test_retry_batches_multi_sample_keeps_auto(tmp_path: Path):
    bf = _make_file(tmp_path, [
        IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B']),
        IcaBatch('COH0001', 1, ['SYN_C', 'SYN_D']),
    ])
    bf.record_passfail(0, {'SYN_A': 'Fail', 'SYN_B': 'Fail'})
    bf.record_passfail(1, {'SYN_C': 'Fail', 'SYN_D': 'Success'})
    _build_retry_batches(cohort_name='COH0001', batches_file=bf, batch_size=5)
    # 3 retry SGs (SYN_A, SYN_B, SYN_C) chunked into one batch of 3 → error_strategy='auto'.
    assert bf.batches[2]['error_strategy'] == 'auto'
    assert bf.batches[2]['sg_names'] == ['SYN_A', 'SYN_B', 'SYN_C']


def test_retry_batches_skips_already_retried(tmp_path: Path):
    """A retry batch's own fails are NOT eligible for a second retry."""
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B'])])
    bf.record_passfail(0, {'SYN_A': 'Success', 'SYN_B': 'Fail'})
    first = _build_retry_batches('COH0001', bf, 5)
    assert len(first) == 1
    # Simulate the retry batch failing too.
    bf.record_passfail(1, {'SYN_B': 'Fail'})
    second = _build_retry_batches('COH0001', bf, 5)
    assert second == []  # single-retry invariant


def test_retry_marks_source_sgs_retried(tmp_path: Path):
    """`mark_sgs_retried` records the per-SG audit trail on the source batch
    (spec §6 line 304 — both batch-level and per-SG)."""
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B'])])
    bf.record_passfail(0, {'SYN_A': 'Success', 'SYN_B': 'Fail'})
    _build_retry_batches('COH0001', bf, 5)
    assert bf.batches[0]['retried_sgs'] == ['SYN_B']


def test_retry_handles_sg_in_two_batches(tmp_path: Path):
    """After a retry, the same SG name appears in both the initial batch
    (gen=0) and the retry batch (gen=1). `find_batch_for_sg` must return the
    retry batch — that's where per-SG path resolution should aim."""
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B'])])
    bf.record_passfail(0, {'SYN_A': 'Success', 'SYN_B': 'Fail'})
    _build_retry_batches('COH0001', bf, 5)
    found = bf.find_batch_for_sg('SYN_B')
    assert found is not None
    assert found['retry_generation'] == 1
    assert found['batch_index'] == 1


def test_retry_batches_whole_batch_failed_keeps_auto(tmp_path: Path):
    """When the source batch is FAILED at ICA-level with N>1 SGs and no
    `passfail.json`, the retry batch keeps `error_strategy='auto'` because
    it's still a multi-sample run (only single-sample retries need `continue`)."""
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B', 'SYN_C'])])
    bf.record_status(0, 'FAILED')
    new = _build_retry_batches('COH0001', bf, 5)
    assert len(new) == 1
    assert new[0].sg_names == ['SYN_A', 'SYN_B', 'SYN_C']
    assert bf.batches[1]['error_strategy'] == 'auto'


def test_retry_batches_treats_cancelled_as_terminal(tmp_path: Path):
    """`cancel_cohort_run=true` marks batches CANCELLED — that is the user's
    intent (spec §4 line 214). `_build_retry_batches` must NOT re-submit them."""
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B'])])
    bf.record_status(0, 'CANCELLED')
    new = _build_retry_batches('COH0001', bf, 5)
    assert new == []


def test_harvest_ar_guids_from_per_sg_state(tmp_path: Path):
    """Harvest helper reads `{batch_index: ar_guid}` and `{batch_index: {sg, ...}}`
    out of per-SG state files. Used by `force_resubmit` BEFORE deletion."""
    state_dir = tmp_path
    (state_dir / 'SYN_A_pipeline_id_and_arguid.json').write_text(
        '{"schema_version": 1, "pipeline_id": "p1", "ar_guid": "guid-batch-0", '
        '"user_reference": "COH-batch0000_guid-batch-0_", "batch_index": 0}',
    )
    (state_dir / 'SYN_B_pipeline_id_and_arguid.json').write_text(
        '{"schema_version": 1, "pipeline_id": "p1", "ar_guid": "guid-batch-0", '
        '"user_reference": "COH-batch0000_guid-batch-0_", "batch_index": 0}',
    )
    paths = {
        'SYN_A_pipeline_id_and_arguid': state_dir / 'SYN_A_pipeline_id_and_arguid.json',
        'SYN_B_pipeline_id_and_arguid': state_dir / 'SYN_B_pipeline_id_and_arguid.json',
    }
    harvested, membership = _harvest_ar_guids_from_per_sg_state(['SYN_A', 'SYN_B'], paths)
    assert harvested == {0: 'guid-batch-0'}
    assert membership == {0: {'SYN_A', 'SYN_B'}}


def test_harvest_raises_on_state_file_missing_required_keys(tmp_path: Path):
    """A state file that parses but is missing `batch_index` / `ar_guid`
    raises rather than silently being skipped. force_resubmit relies on
    harvesting every existing state file; silently skipping would mint a
    fresh AR GUID and sever the billing audit trail without a log signal."""
    bad_path = tmp_path / 'SYN_A_pipeline_id_and_arguid.json'
    bad_path.write_text('{}')  # parses but no fields
    paths = {'SYN_A_pipeline_id_and_arguid': bad_path}

    with pytest.raises(KeyError, match='missing required keys'):
        _harvest_ar_guids_from_per_sg_state(['SYN_A'], paths)


def test_harvest_raises_when_every_state_file_unparseable(tmp_path: Path):
    """If every per-SG state file fails to parse, harvested ends up empty.
    Raise rather than returning {} — force_resubmit would otherwise mint
    fresh AR GUIDs across the whole cohort without surfacing the divergence."""
    a_path = tmp_path / 'SYN_A_pipeline_id_and_arguid.json'
    b_path = tmp_path / 'SYN_B_pipeline_id_and_arguid.json'
    a_path.write_text('not json {')
    b_path.write_text('also not json')
    paths = {
        'SYN_A_pipeline_id_and_arguid': a_path,
        'SYN_B_pipeline_id_and_arguid': b_path,
    }

    with pytest.raises(RuntimeError, match='harvested zero AR GUIDs'):
        _harvest_ar_guids_from_per_sg_state(['SYN_A', 'SYN_B'], paths)


def test_harvest_returns_empty_when_no_state_files_on_disk(tmp_path: Path):
    """If no per-SG state files exist on disk at all (truly fresh cohort,
    or the harvest helper was called outside the `had_prior_state` guard),
    return empty dicts rather than raising. The empty-harvest-with-no-state
    case is handled by the caller's `had_prior_state` check, not here."""
    paths = {
        'SYN_A_pipeline_id_and_arguid': tmp_path / 'SYN_A_pipeline_id_and_arguid.json',
    }
    harvested, membership = _harvest_ar_guids_from_per_sg_state(['SYN_A'], paths)
    assert harvested == {}
    assert membership == {}


def test_force_resubmit_deletes_state_and_returns_harvest(tmp_path: Path, monkeypatch):
    """Spec §4 line 213: `force_resubmit=true` deletes batches.json + all
    per-SG state files AND preserves AR GUIDs lifted from per-SG state.
    Exercise `_handle_management_flags` end-to-end."""
    batches_path = tmp_path / 'COH0001_batches.json'
    batches_path.write_text('{"schema_version": 1, "batch_size": 5, "n_batches": 0, "batches": []}')
    sg_a_state = tmp_path / 'SYN_A_pipeline_id_and_arguid.json'
    sg_b_state = tmp_path / 'SYN_B_pipeline_id_and_arguid.json'
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
        'SYN_A_pipeline_id_and_arguid': sg_a_state,
        'SYN_B_pipeline_id_and_arguid': sg_b_state,
    }
    harvested, membership = _handle_management_flags(
        cohort_name='COH0001',
        batches_file_path=batches_path,
        outputs=outputs,
        sg_names=['SYN_A', 'SYN_B'],
    )
    assert harvested == {0: 'guid-0'}
    assert membership == {0: {'SYN_A', 'SYN_B'}}
    assert not batches_path.exists(), 'batches.json should be deleted'
    assert not sg_a_state.exists(), 'per-SG state should be deleted'
    assert not sg_b_state.exists()


def test_force_resubmit_no_prior_state_raises(tmp_path: Path, monkeypatch):
    """force_resubmit is the destructive flag — running it against a cohort
    with no prior state should not silently fall through to a fresh
    submission (would burn money on an ICA run the user didn't intend).
    Raise so the user un-sets force_resubmit explicitly."""
    batches_path = tmp_path / 'COH0001_batches.json'  # does not exist

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

    with pytest.raises(RuntimeError, match='no prior state'):
        _handle_management_flags(
            cohort_name='COH0001', batches_file_path=batches_path,
            outputs={}, sg_names=['SYN_A'],
        )


def test_force_resubmit_and_monitor_previous_raises(tmp_path: Path, monkeypatch):
    """Both flags set simultaneously is contradictory — force_resubmit is
    destructive (delete state + fresh submission); monitor_previous resumes
    an in-flight run. Pick one."""
    batches_path = tmp_path / 'COH0001_batches.json'
    batches_path.write_text('{"schema_version": 1, "batch_size": 5, "n_batches": 0, "batches": []}')

    def fake_config_retrieve(key, default=None):
        cfg = {
            ('ica', 'management', 'force_resubmit'): True,
            ('ica', 'management', 'monitor_previous'): True,
            ('ica', 'management', 'cancel_cohort_run'): False,
        }
        return cfg.get(tuple(key), default)

    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_pipeline.config_retrieve', fake_config_retrieve,
    )

    with pytest.raises(ValueError, match='mutually exclusive'):
        _handle_management_flags(
            cohort_name='COH0001', batches_file_path=batches_path,
            outputs={}, sg_names=[],
        )


def test_force_resubmit_deletes_completion_marker(tmp_path: Path, monkeypatch):
    """If a completion marker from a previous successful run is sitting in
    the outputs dict, force_resubmit deletes it. Otherwise the marker
    would advertise this cohort as 'complete' even after wiping the state
    underneath it, and cpg-flow would skip the re-submission entirely."""
    batches_path = tmp_path / 'COH0001_batches.json'
    batches_path.write_text('{"schema_version": 1, "batch_size": 5, "n_batches": 0, "batches": []}')
    complete_marker = tmp_path / 'COH0001_pipeline_complete.json'
    complete_marker.write_text('{"cohort_name": "COH0001"}')

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
        'COH0001_pipeline_complete': complete_marker,
    }
    _handle_management_flags(
        cohort_name='COH0001', batches_file_path=batches_path, outputs=outputs, sg_names=[],
    )
    assert not complete_marker.exists()


def test_cancel_cohort_run_raises_cohort_cancelled(tmp_path: Path, monkeypatch):
    """`cancel_cohort_run=true` is terminal — raises CohortCancelled so run()
    short-circuits past retry-building and threshold-checking."""
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
    with pytest.raises(CohortCancelled):
        _handle_management_flags(
            cohort_name='COH0001',
            batches_file_path=tmp_path / 'does-not-exist.json',
            outputs={},
            sg_names=['SYN_A'],
        )


def test_cancel_cohort_run_preserves_per_sg_state(tmp_path: Path, monkeypatch):
    """Round-4 directive: cancel must NOT delete per-SG state files (the
    versioned state file is the single source of per-SG truth; preserving it
    keeps AR GUIDs available for a future `force_resubmit` to harvest).

    Symmetric to `test_force_resubmit_deletes_state_and_returns_harvest`,
    which asserts the OPPOSITE for force_resubmit (deletion is necessary
    for clean state).
    """
    batches_path = tmp_path / 'COH0001_batches.json'
    # Seed a batches file with one PENDING batch (no pipeline_id so the
    # cancel path doesn't try to call ICA — keeps the test hermetic).
    seed_payload = {
        'schema_version': 1,
        'batch_size': 5,
        'n_batches': 1,
        'batches': [
            {
                'batch_index': 0,
                'retry_generation': 0,
                'sg_names': ['SYN_A'],
                'retried_sgs': [],
                'user_reference': 'COH-batch0000_g_',
                'pipeline_id': None,
                'ar_guid': 'g',
                'analysis_output_folder_fid': None,
                'fastq_list_fid': None,
                'cram_fids': None,
                'status': 'PENDING',
                'passfail': None,
                'passfail_seen': False,
                'has_been_retried': False,
                'error_strategy': 'auto',
            },
        ],
    }
    batches_path.write_text(json.dumps(seed_payload))
    sg_state = tmp_path / 'SYN_A_pipeline_id_and_arguid.json'
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

    outputs = {'SYN_A_pipeline_id_and_arguid': sg_state}
    with pytest.raises(CohortCancelled):
        _handle_management_flags(
            cohort_name='COH0001',
            batches_file_path=batches_path,
            outputs=outputs,
            sg_names=['SYN_A'],
        )

    # Batches file is updated to reflect CANCELLED status.
    bf = BatchesFile(path=batches_path)
    bf.read()
    assert bf.batches[0]['status'] == 'CANCELLED'
    # Per-SG state file is PRESERVED — that's the round-4 directive.
    assert sg_state.exists(), 'cancel_cohort_run must not delete per-SG state files'


def test_threshold_breached_just_above_5pct(tmp_path: Path):
    """20 SGs, 2 Fail → 10% → above threshold."""
    sg_names = [f'SYN{i:05d}' for i in range(20)]
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, sg_names)])
    bf.record_passfail(
        0,
        {**dict.fromkeys(sg_names[:18], 'Success'), sg_names[18]: 'Fail', sg_names[19]: 'Fail'},
    )
    n_failed = len(bf.failed_sg_names())
    assert n_failed == 2
    assert _threshold_breached(n_failed=n_failed, n_total=len(sg_names))


def test_threshold_breached_exactly_at_5pct(tmp_path: Path):
    """Spec §6 line 312 uses strict `>`: 20 SGs / 1 Fail = 5.0% → NOT raise.
    Boundary semantics matter when N is small."""
    sg_names = [f'SYN{i:05d}' for i in range(20)]
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, sg_names)])
    bf.record_passfail(0, {**dict.fromkeys(sg_names[:19], 'Success'), sg_names[19]: 'Fail'})
    n_failed = len(bf.failed_sg_names())
    assert n_failed == 1
    assert not _threshold_breached(n_failed=n_failed, n_total=len(sg_names))


def test_threshold_breached_just_above_5pct_small_n(tmp_path: Path):
    """19 SGs, 1 Fail = 5.26% → above threshold."""
    sg_names = [f'SYN{i:05d}' for i in range(19)]
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, sg_names)])
    bf.record_passfail(0, {**dict.fromkeys(sg_names[:18], 'Success'), sg_names[18]: 'Fail'})
    n_failed = len(bf.failed_sg_names())
    assert n_failed == 1
    assert _threshold_breached(n_failed=n_failed, n_total=len(sg_names))
