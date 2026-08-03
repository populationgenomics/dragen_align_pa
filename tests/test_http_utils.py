"""Tests for the shared download session.

Every pre-signed-URL fetch previously used the module-level `requests.get`, which
builds a throwaway session per call: one TCP+TLS handshake per file, and no retry
at all (requests' default adapter is `max_retries=0`). A single
`ConnectionResetError` during a handshake therefore killed a whole download job.
"""

from unittest.mock import MagicMock

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
    """urllib3 sleeps a Retry-After verbatim and its backoff ceiling does not apply, so an
    upstream answering with a day would park a worker for a day."""
    retry = http_utils.download_session().get_adapter('https://example.com').max_retries
    response = MagicMock()
    response.headers = {'Retry-After': '86400'}

    assert retry.get_retry_after(response) == http_utils._MAX_RETRY_AFTER_SECONDS  # noqa: SLF001


def test_a_short_retry_after_is_honoured_unchanged():
    """Capping must not override a server asking for a reasonable wait."""
    retry = http_utils.download_session().get_adapter('https://example.com').max_retries
    response = MagicMock()
    response.headers = {'Retry-After': '5'}

    assert retry.get_retry_after(response) == 5


def test_the_cap_survives_being_copied_for_the_next_attempt():
    """urllib3 rebuilds the Retry between attempts; a plain `Retry` copy would drop the cap."""
    retry = http_utils.download_session().get_adapter('https://example.com').max_retries

    assert isinstance(retry.new(), http_utils._CappedRetryAfter)  # noqa: SLF001


def test_transfer_retrying_is_bounded_by_both_attempts_and_elapsed_time():
    """Attempts alone don't bound wall clock — one attempt can stall for many minutes — and
    nothing else in the pipeline caps job runtime."""
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
