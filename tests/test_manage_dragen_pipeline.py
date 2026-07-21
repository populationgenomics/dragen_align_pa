"""Unit tests for the manage_dragen_pipeline orchestrator helpers.

Covers the algorithmic helpers: _build_retry_batches (per-sample retry
discipline + single-retry invariant + CANCELLED-as-terminal),
_handle_management_flags (force_resubmit clean-slate delete, cancel
preserves per-SG state).

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
)


def _make_file(tmp_path: Path, batches: list[IcaBatch]) -> BatchesFile:
    bf = BatchesFile(path=tmp_path / 'COH0001_batches.json')
    bf.initialise(batch_size=5, batches=batches)
    bf.write()
    return bf


def _set_management_flags(
    monkeypatch,
    *,
    force_resubmit: bool = False,
    monitor_previous: bool = False,
    cancel_cohort_run: bool = False,
) -> None:
    """Stub `config_retrieve` to return the supplied management flag values.

    Every test that exercises `_handle_management_flags` needs to pin all three
    flags; doing it inline produces four near-identical 7-line blocks. This
    helper collapses each to a one-line call.
    """
    cfg = {
        ('ica', 'management', 'force_resubmit'): force_resubmit,
        ('ica', 'management', 'monitor_previous'): monitor_previous,
        ('ica', 'management', 'cancel_cohort_run'): cancel_cohort_run,
    }
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_pipeline.config_retrieve',
        lambda key, default=None: cfg.get(tuple(key), default),
    )


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


def test_retry_batches_from_dragen_failed_status(tmp_path: Path):
    """Regression: DRAGEN's passfail.json writes `"Failed"`, not `"Fail"`.

    A batch of 5 where one sample fails records e.g. `{"CPG_A": "Success", ...,
    "CPG_B": "Failed"}`. Before normalisation at `record_passfail`, the raw
    `"Failed"` matched neither `== 'Fail'` (the retry selection) nor
    `== 'Success'`, so the failed sample was silently dropped — never retried
    and never surfaced by `failed_sg_names`. `record_passfail` now normalises
    `"Failed"` to the canonical `"Fail"`, so the sample enters the retry path.
    """
    bf = _make_file(
        tmp_path,
        [IcaBatch('COH0001', 0, ['CPG_A', 'CPG_B', 'CPG_C', 'CPG_D', 'CPG_E'])],
    )
    bf.record_passfail(
        0,
        {
            'CPG_A': 'Success',
            'CPG_B': 'Failed',
            'CPG_C': 'Success',
            'CPG_D': 'Success',
            'CPG_E': 'Success',
        },
    )
    new = _build_retry_batches(cohort_name='COH0001', batches_file=bf, batch_size=5)
    assert len(new) == 1
    assert new[0].sg_names == ['CPG_B']
    assert bf.failed_sg_names() == ['CPG_B']


def test_retry_batches_single_sample_uses_continue_strategy(tmp_path: Path):
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B'])])
    bf.record_passfail(0, {'SYN_A': 'Success', 'SYN_B': 'Fail'})
    _build_retry_batches(cohort_name='COH0001', batches_file=bf, batch_size=5)
    assert bf.batches[1]['error_strategy'] == 'continue'


def test_retry_batches_multi_sample_keeps_auto(tmp_path: Path):
    bf = _make_file(
        tmp_path,
        [
            IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B']),
            IcaBatch('COH0001', 1, ['SYN_C', 'SYN_D']),
        ],
    )
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


def test_force_resubmit_deletes_state(tmp_path: Path, monkeypatch):
    """`force_resubmit=true` is the clean-slate flag: deletes
    `{cohort}_batches.json` and every per-SG state file so the orchestrator
    re-batches the cohort from scratch with fresh AR GUIDs. No prior state
    is preserved across the boundary."""
    batches_path = tmp_path / 'COH0001_batches.json'
    batches_path.write_text('{"schema_version": 1, "batch_size": 5, "n_batches": 0, "batches": []}')
    sg_a_state = tmp_path / 'SYN_A_pipeline_id_and_arguid.json'
    sg_b_state = tmp_path / 'SYN_B_pipeline_id_and_arguid.json'
    for p in (sg_a_state, sg_b_state):
        p.write_text(
            '{"schema_version": 1, "pipeline_id": "p1", "ar_guid": "guid-0", '
            '"user_reference": "COH-batch0000_guid-0_", "batch_index": 0}',
        )

    _set_management_flags(monkeypatch, force_resubmit=True)

    outputs = {
        'SYN_A_pipeline_id_and_arguid': sg_a_state,
        'SYN_B_pipeline_id_and_arguid': sg_b_state,
    }
    _handle_management_flags(
        cohort_name='COH0001',
        batches_file_path=batches_path,
        outputs=outputs,
        sg_names=['SYN_A', 'SYN_B'],
    )
    assert not batches_path.exists(), 'batches.json should be deleted'
    assert not sg_a_state.exists(), 'per-SG state should be deleted'
    assert not sg_b_state.exists()


def test_force_resubmit_no_prior_state_raises(tmp_path: Path, monkeypatch):
    """force_resubmit is the destructive flag — running it against a cohort
    with no prior state should not silently fall through to a fresh
    submission (would burn money on an ICA run the user didn't intend).
    Raise so the user un-sets force_resubmit explicitly."""
    batches_path = tmp_path / 'COH0001_batches.json'  # does not exist
    _set_management_flags(monkeypatch, force_resubmit=True)

    with pytest.raises(RuntimeError, match='no prior state'):
        _handle_management_flags(
            cohort_name='COH0001',
            batches_file_path=batches_path,
            outputs={},
            sg_names=['SYN_A'],
        )


def test_force_resubmit_and_monitor_previous_raises(tmp_path: Path, monkeypatch):
    """Both flags set simultaneously is contradictory — force_resubmit is
    destructive (delete state + fresh submission); monitor_previous resumes
    an in-flight run. Pick one."""
    batches_path = tmp_path / 'COH0001_batches.json'
    batches_path.write_text('{"schema_version": 1, "batch_size": 5, "n_batches": 0, "batches": []}')
    _set_management_flags(monkeypatch, force_resubmit=True, monitor_previous=True)

    with pytest.raises(ValueError, match='mutually exclusive'):
        _handle_management_flags(
            cohort_name='COH0001',
            batches_file_path=batches_path,
            outputs={},
            sg_names=[],
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
    _set_management_flags(monkeypatch, force_resubmit=True)

    outputs = {
        'COH0001_pipeline_complete': complete_marker,
    }
    _handle_management_flags(
        cohort_name='COH0001',
        batches_file_path=batches_path,
        outputs=outputs,
        sg_names=[],
    )
    assert not complete_marker.exists()


def test_cancel_cohort_run_raises_cohort_cancelled(tmp_path: Path, monkeypatch):
    """`cancel_cohort_run=true` is terminal — raises CohortCancelled so run()
    short-circuits past retry-building."""
    # No batches file → "no in-flight state" branch still raises CohortCancelled.
    _set_management_flags(monkeypatch, cancel_cohort_run=True)
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

    _set_management_flags(monkeypatch, cancel_cohort_run=True)

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


def test_failed_sg_names_reports_single_failure_no_rate_tolerance(tmp_path: Path):
    """The orchestrator raises after the retry pass on ANY residual failure
    (no 5%-rate tolerance). failed_sg_names() is the signal it checks: even a
    single failed SG in a large cohort surfaces, so run() would raise."""
    sg_names = [f'SYN{i:05d}' for i in range(20)]
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, sg_names)])
    # 1/20 = 5% — under the OLD gate this passed; now it's a reported failure.
    bf.record_passfail(0, {**dict.fromkeys(sg_names[:19], 'Success'), sg_names[19]: 'Fail'})
    assert bf.failed_sg_names() == [sg_names[19]]
