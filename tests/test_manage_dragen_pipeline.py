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

import icasdk
import pytest

from dragen_align_pa.batches import BatchesFile, IcaBatch
from dragen_align_pa.jobs.manage_dragen_pipeline import (
    CohortCancelled,
    _build_retry_batches,
    _handle_management_flags,
    _reconcile_batches_with_ica,
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
    force_retry: bool = False,
) -> None:
    """Stub `config_retrieve` to return the supplied management flag values.

    Every test that exercises `_handle_management_flags` needs to pin all the
    flags; doing it inline produces near-identical blocks. This helper collapses
    each to a one-line call.
    """
    cfg = {
        ('ica', 'management', 'force_resubmit'): force_resubmit,
        ('ica', 'management', 'monitor_previous'): monitor_previous,
        ('ica', 'management', 'cancel_cohort_run'): cancel_cohort_run,
        ('ica', 'management', 'force_retry'): force_retry,
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


def _submitted_batch_file(tmp_path: Path, sg_names: list[str], status: str) -> BatchesFile:
    """A one-batch file that reached ICA (has pipeline_id/user_reference) at `status`."""
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, sg_names)])
    bf.record_pipeline_submission(
        batch_index=0,
        pipeline_id='pid-0',
        ar_guid='guid-0',
        user_reference='COH0001-batch0000_guid-0_',
    )
    bf.record_status(0, status)
    return bf


def test_build_retry_batches_force_overrides_single_retry_gate(tmp_path: Path):
    """force=True resubmits a failure that already used its one automatic retry."""
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B'])])
    bf.record_passfail(0, {'SYN_A': 'Success', 'SYN_B': 'Fail'})
    assert len(_build_retry_batches('COH0001', bf, 5)) == 1  # normal retry consumes the gate
    bf.record_passfail(1, {'SYN_B': 'Fail'})  # the retry batch also fails
    assert _build_retry_batches('COH0001', bf, 5) == []  # gate blocks a second normal retry
    forced = _build_retry_batches('COH0001', bf, 5, force=True)
    assert len(forced) == 1
    assert forced[0].sg_names == ['SYN_B']


def test_build_retry_batches_force_skips_already_succeeded_sgs(tmp_path: Path):
    """force mode harvests: an SG that succeeded in any batch is never rerun."""
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B'])])
    bf.record_status(0, 'FAILED')  # gen-0 whole-batch failure (all SGs)
    bf.add_retry_batch(sg_names=['SYN_A', 'SYN_B'])  # gen-1 retry, index 1
    bf.record_passfail(1, {'SYN_A': 'Success', 'SYN_B': 'Fail'})
    forced = _build_retry_batches('COH0001', bf, 5, force=True)
    assert len(forced) == 1
    assert forced[0].sg_names == ['SYN_B']  # SYN_A harvested from the gen-1 success


def test_build_retry_batches_force_skips_sg_with_in_flight_retry(tmp_path: Path):
    """force mode must not resubmit an SG whose retry is still in flight: the resume
    path in run() re-monitors the existing PENDING/INPROGRESS batch, so a fresh retry
    here would run two concurrent ICA analyses for the same SG."""
    bf = _make_file(tmp_path, [IcaBatch('COH0001', 0, ['SYN_A', 'SYN_B'])])
    bf.record_passfail(0, {'SYN_A': 'Success', 'SYN_B': 'Fail'})
    bf.record_status(0, 'SUCCEEDED')
    bf.add_retry_batch(sg_names=['SYN_B'])  # gen-1 retry, index 1
    bf.record_status(1, 'INPROGRESS')  # retry still running at ICA
    assert _build_retry_batches('COH0001', bf, 5, force=True) == []


def test_reconcile_flips_stale_failed_to_succeeded_from_ica(tmp_path: Path, monkeypatch):
    """The reported scenario: GCS says FAILED, ICA says SUCCEEDED with all-Success
    passfail → reconcile to SUCCEEDED and harvest (nothing left to rerun)."""
    bf = _submitted_batch_file(tmp_path, ['SYN_A', 'SYN_B'], status='FAILED')
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_pipeline.monitor_dragen_pipeline.run',
        lambda **_kwargs: 'SUCCEEDED',
    )
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_pipeline._fetch_batch_passfail_and_folder',
        lambda *_args: ({'SYN_A': 'Success', 'SYN_B': 'Success'}, 'fol.x'),
    )
    _reconcile_batches_with_ica('COH0001', bf)
    assert bf.batches[0]['status'] == 'SUCCEEDED'
    assert bf.failed_sg_names() == []
    assert bf.successful_sg_names() == ['SYN_A', 'SYN_B']


def test_reconcile_keeps_failed_when_ica_failed(tmp_path: Path, monkeypatch):
    bf = _submitted_batch_file(tmp_path, ['SYN_A'], status='INPROGRESS')
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_pipeline.monitor_dragen_pipeline.run',
        lambda **_kwargs: 'FAILED',
    )
    _reconcile_batches_with_ica('COH0001', bf)
    assert bf.batches[0]['status'] == 'FAILED'


def test_reconcile_marks_failed_when_ica_analysis_gone(tmp_path: Path, monkeypatch):
    """A gone (404) analysis is marked FAILED so its SGs resubmit fresh."""
    bf = _submitted_batch_file(tmp_path, ['SYN_A'], status='SUCCEEDED')

    def _raise_not_found(**_kwargs):
        raise icasdk.ApiException(status=404)

    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_pipeline.monitor_dragen_pipeline.run',
        _raise_not_found,
    )
    _reconcile_batches_with_ica('COH0001', bf)
    assert bf.batches[0]['status'] == 'FAILED'


def test_reconcile_gone_batch_clears_passfail_so_all_sgs_resubmit(tmp_path: Path, monkeypatch):
    """A gone (404) analysis that previously recorded a passfail must clear it: its
    outputs no longer exist, so every SG — including ones marked Success — resubmits
    fresh rather than being harvested from a stale outcome."""
    bf = _submitted_batch_file(tmp_path, ['SYN_A', 'SYN_B'], status='SUCCEEDED')
    bf.record_passfail(0, {'SYN_A': 'Success', 'SYN_B': 'Fail'})

    def _raise_not_found(**_kwargs):
        raise icasdk.ApiException(status=404)

    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_pipeline.monitor_dragen_pipeline.run',
        _raise_not_found,
    )
    _reconcile_batches_with_ica('COH0001', bf)
    assert bf.batches[0]['status'] == 'FAILED'
    assert bf.batches[0]['passfail'] is None
    assert bf.successful_sg_names() == []  # SYN_A no longer harvested from a gone analysis
    forced = _build_retry_batches('COH0001', bf, 5, force=True)
    assert sorted(sg for b in forced for sg in b.sg_names) == ['SYN_A', 'SYN_B']


def test_reconcile_terminal_failure_clears_stale_passfail(tmp_path: Path, monkeypatch):
    """Symmetric with the 404 branch: an ICA terminal failure clears any recorded
    passfail so the whole batch resubmits rather than harvesting stale Success SGs."""
    bf = _submitted_batch_file(tmp_path, ['SYN_A', 'SYN_B'], status='SUCCEEDED')
    bf.record_passfail(0, {'SYN_A': 'Success', 'SYN_B': 'Fail'})
    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_pipeline.monitor_dragen_pipeline.run',
        lambda **_kwargs: 'ABORTED',
    )
    _reconcile_batches_with_ica('COH0001', bf)
    assert bf.batches[0]['status'] == 'FAILED'
    assert bf.batches[0]['passfail'] is None
    assert bf.successful_sg_names() == []  # SYN_A not harvested from a failed analysis


def test_reconcile_skips_cancelled_batch(tmp_path: Path, monkeypatch):
    """CANCELLED is terminal: reconcile must not query or relabel a cancelled batch,
    even though it still carries a pipeline_id (else its ABORTED status would map to
    FAILED and the cancelled work would be resubmitted)."""
    bf = _submitted_batch_file(tmp_path, ['SYN_A'], status='CANCELLED')

    def _must_not_be_called(**_kwargs):
        raise AssertionError('reconcile queried a CANCELLED batch')

    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_pipeline.monitor_dragen_pipeline.run',
        _must_not_be_called,
    )
    _reconcile_batches_with_ica('COH0001', bf)
    assert bf.batches[0]['status'] == 'CANCELLED'


def test_reconcile_reraises_non_404_ica_error(tmp_path: Path, monkeypatch):
    """A transient/unexpected ICA error must not be swallowed as 'gone'."""
    bf = _submitted_batch_file(tmp_path, ['SYN_A'], status='INPROGRESS')

    def _raise_server_error(**_kwargs):
        raise icasdk.ApiException(status=503)

    monkeypatch.setattr(
        'dragen_align_pa.jobs.manage_dragen_pipeline.monitor_dragen_pipeline.run',
        _raise_server_error,
    )
    with pytest.raises(icasdk.ApiException):
        _reconcile_batches_with_ica('COH0001', bf)


def test_force_retry_requires_existing_batches_file(tmp_path: Path, monkeypatch):
    _set_management_flags(monkeypatch, force_retry=True)
    with pytest.raises(FileNotFoundError, match='force_retry'):
        _handle_management_flags(
            cohort_name='COH0001',
            batches_file_path=tmp_path / 'missing.json',
            outputs={},
            sg_names=['SYN_A'],
        )


def test_force_retry_mutually_exclusive_with_other_flags(tmp_path: Path, monkeypatch):
    _set_management_flags(monkeypatch, force_retry=True, force_resubmit=True)
    existing = tmp_path / 'COH0001_batches.json'
    existing.write_text('{}')
    with pytest.raises(ValueError, match='mutually exclusive'):
        _handle_management_flags(
            cohort_name='COH0001',
            batches_file_path=existing,
            outputs={},
            sg_names=['SYN_A'],
        )
