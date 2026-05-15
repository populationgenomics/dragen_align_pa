"""Tests for the on_succeeded retry-cap helper in ica_pipeline_manager.

The full polling loop has too many side-effecting dependencies to unit-test
in isolation; the SUCCEEDED-branch logic is extracted into a small helper
(`_process_succeeded_transition`) so the attempt-cap behaviour can be
exercised directly.
"""

from dragen_align_pa.batches import Batch
from dragen_align_pa.jobs.ica_pipeline_manager import (
    MAX_CONSECUTIVE_ON_SUCCEEDED_FAILURES,
    MonitoredTarget,
    PipelineStatus,
    _process_succeeded_transition,
)


def _make_target() -> MonitoredTarget:
    batch = Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
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
