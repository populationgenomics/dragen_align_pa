"""Tests for the on_succeeded retry-cap helper in ica_pipeline_manager.

The full polling loop has too many side-effecting dependencies to unit-test
in isolation; the SUCCEEDED-branch logic is extracted into a small helper
(`_process_succeeded_transition`) so the attempt-cap behaviour can be
exercised directly.
"""

from collections.abc import Callable

import pytest

from dragen_align_pa.batches import IcaBatch, PassfailStatusError
from dragen_align_pa.jobs.ica_pipeline_manager import (
    MAX_CONSECUTIVE_ON_SUCCEEDED_FAILURES,
    MonitoredTarget,
    PipelineStatus,
    _failed_final_target_names,
    _process_succeeded_transition,
    manage_ica_pipeline_loop,
)


def _make_target() -> MonitoredTarget:
    batch = IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
    t = MonitoredTarget(target=batch, allow_retry=False)
    t.pipeline_id = 'analysis-123'
    t.status = PipelineStatus.INPROGRESS
    return t


def _always_raises(_target: MonitoredTarget) -> None:
    raise RuntimeError('callback boom')


def test_on_succeeded_returns_true_when_callback_succeeds():
    """Happy path: callback completes cleanly → caller proceeds to set SUCCEEDED."""
    t = _make_target()
    proceed = _process_succeeded_transition(
        target=t,
        on_succeeded=lambda _t: None,
        on_status_change=None,
    )
    assert proceed is True
    assert t.on_succeeded_failure_count == 0


def test_on_succeeded_returns_false_and_increments_counter_on_failure():
    """A failed callback below the cap: counter advances; caller continues."""
    t = _make_target()
    proceed = _process_succeeded_transition(
        target=t,
        on_succeeded=_always_raises,
        on_status_change=None,
    )
    assert proceed is False
    assert t.on_succeeded_failure_count == 1
    assert t.status == PipelineStatus.INPROGRESS  # not escalated yet


def test_on_succeeded_propagates_passfail_status_error_immediately():
    """A `PassfailStatusError` is a deterministic data error, not a transient
    callback failure: the helper re-raises it immediately rather than counting it
    toward the cap and escalating the whole batch to FAILED_FINAL (which would cost
    MAX_CONSECUTIVE_ON_SUCCEEDED_FAILURES poll cycles and rerun every SG in the batch)."""
    t = _make_target()

    def _raise_passfail(_target: MonitoredTarget) -> None:
        raise PassfailStatusError('unrecognised passfail status')

    with pytest.raises(PassfailStatusError):
        _process_succeeded_transition(
            target=t,
            on_succeeded=_raise_passfail,
            on_status_change=None,
        )
    assert t.on_succeeded_failure_count == 0  # not counted as a transient failure
    assert t.status == PipelineStatus.INPROGRESS  # not escalated to FAILED_FINAL


def test_on_succeeded_escalates_to_failed_final_after_cap():
    """After MAX_CONSECUTIVE_ON_SUCCEEDED_FAILURES consecutive failures, the
    helper transitions the target to FAILED_FINAL and fires on_status_change.
    Without this cap, a persistently broken callback would spin the polling
    loop forever, hammering ICA on every iteration."""
    t = _make_target()
    fired: list[tuple[str, PipelineStatus]] = []

    def record_status_change(target, new_status):
        fired.append((target.name, new_status))

    for _ in range(MAX_CONSECUTIVE_ON_SUCCEEDED_FAILURES):
        _process_succeeded_transition(
            target=t,
            on_succeeded=_always_raises,
            on_status_change=record_status_change,
        )

    assert t.on_succeeded_failure_count == MAX_CONSECUTIVE_ON_SUCCEEDED_FAILURES
    assert t.status == PipelineStatus.FAILED_FINAL
    assert fired == [(t.name, PipelineStatus.FAILED_FINAL)]


def test_on_succeeded_resets_counter_on_success():
    """A transient failure followed by a success must reset the counter so a
    target that recovers doesn't get penalised by accumulated history."""
    t = _make_target()
    _process_succeeded_transition(target=t, on_succeeded=_always_raises, on_status_change=None)
    _process_succeeded_transition(target=t, on_succeeded=_always_raises, on_status_change=None)
    assert t.on_succeeded_failure_count == 2

    proceed = _process_succeeded_transition(
        target=t,
        on_succeeded=lambda _t: None,
        on_status_change=None,
    )
    assert proceed is True
    assert t.on_succeeded_failure_count == 0


def test_on_succeeded_none_callback_is_a_noop():
    """If no on_succeeded callback is configured, the helper trivially
    returns True (legacy MLR call site)."""
    t = _make_target()
    proceed = _process_succeeded_transition(target=t, on_succeeded=None, on_status_change=None)
    assert proceed is True


def test_on_succeeded_swallows_status_change_callback_failure_during_escalation():
    """on_status_change is best-effort even at the escalation point — if it
    raises, the in-memory FAILED_FINAL transition stands; the helper does
    not roll it back. Mirrors the existing _fire_status_change semantics."""
    t = _make_target()

    def boom(_target, _new_status):
        raise RuntimeError('status callback boom')

    for _ in range(MAX_CONSECUTIVE_ON_SUCCEEDED_FAILURES):
        _process_succeeded_transition(target=t, on_succeeded=_always_raises, on_status_change=boom)

    assert t.status == PipelineStatus.FAILED_FINAL


def test_max_consecutive_on_succeeded_failures_constant_is_sane():
    """Sanity bound on the cap — must be > 0 and not absurd."""
    assert 1 <= MAX_CONSECUTIVE_ON_SUCCEEDED_FAILURES <= 20


def _target_with_status(name_index: int, status: PipelineStatus) -> MonitoredTarget:
    batch = IcaBatch(cohort_name='COH0001', batch_index=name_index, sg_names=['CPG_A'])
    t = MonitoredTarget(target=batch, allow_retry=False)
    t.status = status
    return t


def test_failed_final_target_names_selects_only_failed_final():
    """The loop's abort decision: only FAILED_FINAL targets count as
    unrecoverable failures — SUCCEEDED / INPROGRESS / CANCELLED are excluded.
    A single FAILED_FINAL is enough (no failure-rate tolerance)."""
    targets = [
        _target_with_status(0, PipelineStatus.SUCCEEDED),
        _target_with_status(1, PipelineStatus.INPROGRESS),
        _target_with_status(2, PipelineStatus.CANCELLED),
        _target_with_status(3, PipelineStatus.FAILED_FINAL),
    ]
    assert _failed_final_target_names(targets) == ['COH0001-batch0003']


def test_failed_final_target_names_empty_when_none_failed():
    targets = [
        _target_with_status(0, PipelineStatus.SUCCEEDED),
        _target_with_status(1, PipelineStatus.SUCCEEDED),
    ]
    assert _failed_final_target_names(targets) == []


def test_force_resubmit_and_cancel_together_are_rejected(tmp_path, monkeypatch):
    """`force_resubmit` + `cancel_cohort_run` must raise up front: the resubmit
    cleanup deletes the pipeline-id file before the cancel branch reads it, so the
    ICA abort would never be sent and the analysis would be orphaned."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        'dragen_align_pa.jobs.ica_pipeline_manager.config_retrieve',
        lambda key, default=None: (
            True
            if key in (['ica', 'management', 'force_resubmit'], ['ica', 'management', 'cancel_cohort_run'])
            else default
        ),
    )

    batch = IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
    target_name = batch.name
    outputs = {
        'COH0001_errors': tmp_path / 'errors.log',
        f'{target_name}_pipeline_id': tmp_path / f'{target_name}_pipeline_id.json',
        f'{target_name}_success': tmp_path / f'{target_name}_success.json',
    }

    with pytest.raises(ValueError, match='mutually exclusive'):
        manage_ica_pipeline_loop(
            targets_to_process=[batch],
            outputs=outputs,
            pipeline_name='Dragen',
            is_mlr_pipeline=False,
            success_file_key_template='{target_name}_success',
            pipeline_id_file_key_template='{target_name}_pipeline_id',
            error_log_key='COH0001_errors',
            submit_function_factory=lambda name: pytest.fail(f'must not submit {name}'),  # type: ignore[arg-type]
            allow_retry=False,
            sleep_time_seconds=0,
        )


def test_cancel_marks_never_submitted_target_cancelled_without_submitting(tmp_path, monkeypatch):
    """A PENDING target with no pipeline-id file, under cancel_cohort_run=true, must be
    marked CANCELLED without the loop ever invoking the submit callable. Before this fix,
    the cancel branch required `target.pipeline_id`, so a never-submitted target fell
    through to the else branch and called submit_callable() — wastefully submitting a
    doomed analysis (or, after the prefetch rewrite, raising KeyError against the empty
    inputs dict and aborting the manager mid-cancellation)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        'dragen_align_pa.jobs.ica_pipeline_manager.config_retrieve',
        lambda key, default=None: True if key == ['ica', 'management', 'cancel_cohort_run'] else default,
    )

    batch = IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
    target_name = batch.name

    outputs = {
        'COH0001_errors': tmp_path / 'errors.log',
        f'{target_name}_pipeline_id': tmp_path / f'{target_name}_pipeline_id.json',
        f'{target_name}_success': tmp_path / f'{target_name}_success.json',
    }

    def _submit_function_factory(name: str) -> Callable[[], str]:
        def _submit() -> str:
            pytest.fail(f'submit callable must never be invoked for {name} during cancellation')
            return ''

        return _submit

    with pytest.raises(Exception, match='have been cancelled'):
        manage_ica_pipeline_loop(
            targets_to_process=[batch],
            outputs=outputs,
            pipeline_name='Dragen',
            is_mlr_pipeline=False,
            success_file_key_template='{target_name}_success',
            pipeline_id_file_key_template='{target_name}_pipeline_id',
            error_log_key='COH0001_errors',
            submit_function_factory=_submit_function_factory,
            allow_retry=False,
            sleep_time_seconds=0,
        )
