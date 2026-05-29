"""Unit tests for the StatusProvider Protocol and ParallelPerIdStatusProvider.

The provider runs the per-cycle ICA fan-out for `manage_ica_pipeline_loop`.
These tests stub `check_ica_pipeline_status` (mocking the SDK is covered
in test_ica_api_utils.py) so the focus stays on:
  - map population on success
  - partial-failure → 'UNKNOWN'
  - cycle wall-clock timeout
  - executor lifecycle
  - log-policy thresholds
"""

import time as time_module
from unittest.mock import MagicMock, patch

from icasdk.exceptions import ApiException

from dragen_align_pa.jobs.ica_status_provider import (
    ParallelPerIdStatusProvider,
    StatusProvider,
)

_ICA_API_UTILS = __import__('dragen_align_pa.ica_api_utils', fromlist=['_'])

_FAKE_API_CLIENT = MagicMock(
    __enter__=MagicMock(return_value=MagicMock()),
    __exit__=MagicMock(return_value=None),
)
_FAKE_SECRETS = {'projectID': 'proj', 'apiKey': 'k'}


def _patch_ica(fake_check):
    """Return a context manager that patches the three ica_api_utils callables."""
    return (
        patch.object(_ICA_API_UTILS, 'check_ica_pipeline_status', side_effect=fake_check),
        patch.object(_ICA_API_UTILS, 'get_ica_api_client', return_value=_FAKE_API_CLIENT),
        patch.object(_ICA_API_UTILS, 'get_ica_secrets', return_value=_FAKE_SECRETS),
    )


def test_status_provider_protocol_surface():
    """The Protocol is the contract the polling loop depends on; assert
    it exposes refresh and get_status. ParallelPerIdStatusProvider is the
    concrete implementation."""
    assert hasattr(StatusProvider, 'refresh')
    assert hasattr(StatusProvider, 'get_status')


def test_parallel_per_id_status_provider_returns_unknown_before_first_refresh():
    """Before refresh has ever been called, every id reads as 'UNKNOWN' —
    the loop's elif chain falls through, no transition that cycle."""
    with ParallelPerIdStatusProvider(concurrency=2, refresh_timeout_seconds=10) as provider:
        assert provider.get_status('any-id') == 'UNKNOWN'


def test_refresh_populates_map_with_per_id_statuses():
    """Happy path: every in-flight id has its status fetched and stored."""
    statuses = {'a': 'INPROGRESS', 'b': 'SUCCEEDED', 'c': 'FAILED'}

    def fake_check(api_instance, path_params):  # noqa: ARG001
        return statuses[path_params['analysisId']]

    check_patch, client_patch, secrets_patch = _patch_ica(fake_check)
    with check_patch, client_patch, secrets_patch, \
            ParallelPerIdStatusProvider(concurrency=4, refresh_timeout_seconds=10) as provider:
        provider.refresh({'a', 'b', 'c'})

        assert provider.get_status('a') == 'INPROGRESS'
        assert provider.get_status('b') == 'SUCCEEDED'
        assert provider.get_status('c') == 'FAILED'


def test_refresh_marks_failed_ids_as_unknown_without_propagating():
    """A subset of workers raises ApiException (exhausted retries from the
    inner wrapper, simulated here as a bare exception). refresh() must
    NOT propagate; affected ids stay absent → get_status returns 'UNKNOWN';
    successful ids still populate."""
    def fake_check(api_instance, path_params):  # noqa: ARG001
        if path_params['analysisId'] == 'broken':
            raise ApiException(status=429, reason='Too Many Requests')
        return 'INPROGRESS'

    check_patch, client_patch, secrets_patch = _patch_ica(fake_check)
    with check_patch, client_patch, secrets_patch, \
            ParallelPerIdStatusProvider(concurrency=4, refresh_timeout_seconds=10) as provider:
        provider.refresh({'good-1', 'good-2', 'broken'})

        assert provider.get_status('good-1') == 'INPROGRESS'
        assert provider.get_status('good-2') == 'INPROGRESS'
        assert provider.get_status('broken') == 'UNKNOWN'


def test_refresh_clears_stale_map_each_cycle():
    """Successive refresh() calls must not leak ids from a previous cycle —
    if an id transitions to terminal and is no longer in_flight, it must
    not still show as INPROGRESS from a stale cycle."""
    def fake_check(api_instance, path_params):  # noqa: ARG001
        return 'INPROGRESS'

    check_patch, client_patch, secrets_patch = _patch_ica(fake_check)
    with check_patch, client_patch, secrets_patch, \
            ParallelPerIdStatusProvider(concurrency=4, refresh_timeout_seconds=10) as provider:
        provider.refresh({'a', 'b'})
        assert provider.get_status('a') == 'INPROGRESS'

        provider.refresh({'b'})  # 'a' is no longer in_flight
        assert provider.get_status('a') == 'UNKNOWN'
        assert provider.get_status('b') == 'INPROGRESS'


def test_refresh_marks_timed_out_ids_as_unknown():
    """A worker hanging past refresh_timeout_seconds must not stall the
    cycle. wait(timeout=...) returns the done set; stragglers are cancelled
    (no-op if already running, but their absence from the map means
    get_status returns 'UNKNOWN' → no transition this cycle, safe)."""
    def slow_check(api_instance, path_params):  # noqa: ARG001
        if path_params['analysisId'] == 'slow':
            time_module.sleep(2.0)  # hangs past the 0.1s cycle timeout
        return 'INPROGRESS'

    check_patch, client_patch, secrets_patch = _patch_ica(slow_check)
    # Tiny timeout so the test runs in <1s.
    with check_patch, client_patch, secrets_patch, \
            ParallelPerIdStatusProvider(concurrency=4, refresh_timeout_seconds=0.1) as provider:
        provider.refresh({'fast-1', 'fast-2', 'slow'})

        assert provider.get_status('fast-1') == 'INPROGRESS'
        assert provider.get_status('fast-2') == 'INPROGRESS'
        assert provider.get_status('slow') == 'UNKNOWN'


def test_close_shuts_down_the_executor():
    """The provider's executor must shut down cleanly when the cohort run
    exits (normal or exception path). The `with` block in
    manage_ica_pipeline_loop relies on this so Hail Batch workers don't
    leak threads."""
    provider = ParallelPerIdStatusProvider(concurrency=2, refresh_timeout_seconds=10)
    assert not provider._executor._shutdown  # type: ignore[attr-defined]

    provider.close()

    assert provider._executor._shutdown  # type: ignore[attr-defined]


def test_refresh_logs_warning_when_some_failures():
    """One summary log per refresh, level WARNING, only when n_failed > 0."""
    def fake_check(api_instance, path_params):  # noqa: ARG001
        if path_params['analysisId'].startswith('broken'):
            raise ApiException(status=429, reason='Too Many Requests')
        return 'INPROGRESS'

    check_patch, client_patch, secrets_patch = _patch_ica(fake_check)
    # 1 in 4 fails → ratio 0.25, well under 0.5 → WARNING (not ERROR).
    with check_patch, client_patch, secrets_patch, \
            ParallelPerIdStatusProvider(concurrency=4, refresh_timeout_seconds=10) as provider, \
            patch('dragen_align_pa.jobs.ica_status_provider.logger') as mock_logger:
        # loguru → caplog requires routing; easiest is asserting the map
        # directly. We verify the summary log fires by patching logger.
        provider.refresh({'good-1', 'good-2', 'good-3', 'broken-1'})

        # WARNING (not ERROR) because failure ratio 0.25 ≤ 0.5.
        assert mock_logger.warning.call_count == 1
        assert mock_logger.error.call_count == 0
        summary = mock_logger.warning.call_args.args[0]
        assert 'n_ok=3' in summary
        assert 'n_failed=1' in summary


def test_refresh_does_not_log_when_all_succeed():
    """No summary log when n_failed == 0 — 12K-target cycles would
    otherwise flood the log every 600s."""
    def fake_check(api_instance, path_params):  # noqa: ARG001
        return 'INPROGRESS'

    check_patch, client_patch, secrets_patch = _patch_ica(fake_check)
    with check_patch, client_patch, secrets_patch, \
            ParallelPerIdStatusProvider(concurrency=4, refresh_timeout_seconds=10) as provider, \
            patch('dragen_align_pa.jobs.ica_status_provider.logger') as mock_logger:
        provider.refresh({'a', 'b', 'c'})

        assert mock_logger.warning.call_count == 0
        assert mock_logger.error.call_count == 0


def test_refresh_escalates_to_error_when_majority_failed():
    """ratio > 0.5 ⇒ ERROR. Signals a cohort-wide ICA outage worth
    paging on, not just a per-id flake."""
    def fake_check(api_instance, path_params):  # noqa: ARG001
        raise ApiException(status=503, reason='Service Unavailable')

    check_patch, client_patch, secrets_patch = _patch_ica(fake_check)
    with check_patch, client_patch, secrets_patch, \
            ParallelPerIdStatusProvider(concurrency=4, refresh_timeout_seconds=10) as provider, \
            patch('dragen_align_pa.jobs.ica_status_provider.logger') as mock_logger:
        provider.refresh({'a', 'b', 'c'})

        assert mock_logger.error.call_count == 1
        assert mock_logger.warning.call_count == 0
