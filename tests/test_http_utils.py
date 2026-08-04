"""Tests for the shared download session.

Every pre-signed-URL fetch previously used the module-level `requests.get`, which
builds a throwaway session per call: one TCP+TLS handshake per file, and no retry
at all (requests' default adapter is `max_retries=0`). A single
`ConnectionResetError` during a handshake therefore killed a whole download job.
"""

from unittest.mock import MagicMock

import pytest
import requests

from dragen_align_pa import http_utils


def test_download_session_is_shared_across_calls():
    """Callers must get the same session, or connection reuse never happens."""
    assert http_utils.download_session() is http_utils.download_session()


def test_download_session_retries_get_on_connection_and_status_errors():
    """The https adapter must carry a real retry budget covering connect, read and
    the retryable statuses — this is what absorbs a handshake-time reset. Exact values are
    asserted because the budgets multiply with the transfer retry: raising one should be a
    deliberate edit here, not a silent change in worst-case job latency."""
    adapter = http_utils.download_session().get_adapter('https://example.com')
    retry = adapter.max_retries

    assert retry.total == 5
    assert retry.connect == 5
    assert retry.status == 5
    # Deliberately low: a read error on a streamed GET is a failed wait for response headers,
    # and each one can burn the whole 600s read budget.
    assert retry.read == 1
    assert 'GET' in retry.allowed_methods
    assert {429, 503}.issubset(set(retry.status_forcelist))
    assert retry.backoff_factor == 2.0


def test_a_servers_retry_after_is_capped():
    """urllib3's own ceiling is 21600s, so an upstream answering with a day would still park a
    worker for six hours."""
    retry = http_utils.download_session().get_adapter('https://example.com').max_retries
    response = MagicMock()
    response.headers = {'Retry-After': '86400'}

    assert retry.get_retry_after(response) == http_utils._MAX_RETRY_AFTER_SECONDS


def test_a_short_retry_after_is_honoured_unchanged():
    """Capping must not override a server asking for a reasonable wait."""
    retry = http_utils.download_session().get_adapter('https://example.com').max_retries
    response = MagicMock()
    response.headers = {'Retry-After': '5'}

    assert retry.get_retry_after(response) == 5


def test_backoff_is_jittered():
    """~500 concurrent per-SG jobs taking a 429 from one endpoint must not retry in lockstep and
    re-create the spike they are backing off from."""
    retry = http_utils.download_session().get_adapter('https://example.com').max_retries
    exhausted = retry.increment(method='GET', error=ConnectionError('boom')).increment(
        method='GET',
        error=ConnectionError('boom'),
    )

    waits = {exhausted.get_backoff_time() for _ in range(50)}

    assert len(waits) > 1
    assert min(waits) >= 4.0  # the un-jittered backoff for this attempt; jitter only adds


def test_the_first_retry_is_not_delayed_by_jitter():
    """urllib3 answers the first retry immediately, which is right for a one-off reset; jitter
    must not turn that into a wait."""
    retry = http_utils.download_session().get_adapter('https://example.com').max_retries

    assert retry.increment(method='GET', error=ConnectionError('boom')).get_backoff_time() == 0


def test_the_retry_after_cap_survives_being_copied_for_the_next_attempt():
    """urllib3 rebuilds the Retry between attempts; a plain `Retry` copy would drop the cap."""
    retry = http_utils.download_session().get_adapter('https://example.com').max_retries

    assert isinstance(retry.new(), http_utils._CappedRetryAfter)


def test_transfer_retrying_is_bounded_by_both_attempts_and_elapsed_time():
    """Attempts alone don't bound wall clock: one attempt can stall for many minutes."""
    stops = http_utils.transfer_retrying('sample.cram').stop.stops

    assert {type(stop).__name__ for stop in stops} == {'stop_after_attempt', 'stop_after_delay'}


def test_default_requests_adapter_does_not_retry():
    """Pins the contrast the shared session exists to fix: a bare session retries nothing."""
    assert requests.Session().get_adapter('https://example.com').max_retries.total == 0


def test_timeouts_are_connect_read_tuples():
    """A scalar timeout applies the read budget to the connect leg too, so a stalled
    connect sits for minutes (observed: 3.5) instead of failing in seconds."""
    assert http_utils.STREAM_TIMEOUT == (30, 600)
    assert http_utils.SMALL_FILE_TIMEOUT == (30, 120)


def _http_error(status: int) -> requests.HTTPError:
    """An HTTPError as `raise_for_status` builds it, carrying its response."""
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(f'{status}', response=response)


@pytest.mark.parametrize('status', [404, 401, 400, 429, 503])
def test_a_status_error_does_not_restart_the_whole_transfer(status):
    """The adapter already spent its status budget. Restarting on top of that re-mints a
    rate-limited URL and re-runs all five attempts: a persistent 429 would cost 20 requests
    and 4 mints per file, aimed at the endpoint we are backing off from."""
    assert http_utils._should_restart_transfer(_http_error(status)) is False


def test_a_403_does_restart_the_whole_transfer():
    """403 is how an expired pre-signed URL reads, and each restart mints a fresh one."""
    assert http_utils._should_restart_transfer(_http_error(403)) is True


@pytest.mark.parametrize(
    'error',
    [
        requests.exceptions.ChunkedEncodingError('truncated'),
        requests.ConnectionError('reset'),
    ],
)
def test_a_dropped_body_still_restarts_the_whole_transfer(error):
    """The truncation signal is a ChunkedEncodingError, which is not an HTTPError, so the
    status carve-out above must not disarm it."""
    assert http_utils._should_restart_transfer(error) is True


def test_an_exhausted_status_budget_does_not_restart_the_whole_transfer():
    """A persistent 429 reaches us as `RetryError`, which inherits from `RequestException` and
    not `HTTPError`, so it slips past the status branch above and lands in the transport one.
    Restarting there re-mints a rate-limited URL to ask the same throttled host again: 20
    requests and 4 mints per file, which is exactly what the status carve-out is for."""
    assert http_utils._should_restart_transfer(requests.exceptions.RetryError('adapter gave up')) is False
