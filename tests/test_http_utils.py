"""Tests for the shared download session.

Every pre-signed-URL fetch previously used the module-level `requests.get`, which
builds a throwaway session per call: one TCP+TLS handshake per file, and no retry
at all (requests' default adapter is `max_retries=0`). A single
`ConnectionResetError` during a handshake therefore killed a whole download job.
"""

import requests

from dragen_align_pa import http_utils


def test_download_session_is_shared_across_calls():
    """Callers must get the same session, or connection reuse never happens."""
    assert http_utils.download_session() is http_utils.download_session()


def test_download_session_retries_get_on_connection_and_status_errors():
    """The https adapter must carry a real retry budget covering connect, read and
    the retryable statuses — this is what absorbs a handshake-time reset."""
    adapter = http_utils.download_session().get_adapter('https://example.com')
    retry = adapter.max_retries

    assert retry.total > 0
    assert retry.connect > 0
    assert retry.read > 0
    assert 'GET' in retry.allowed_methods
    assert {429, 503}.issubset(set(retry.status_forcelist))
    assert retry.backoff_factor > 0


def test_default_requests_adapter_does_not_retry():
    """Pins the contrast the shared session exists to fix: a bare session retries nothing."""
    assert requests.Session().get_adapter('https://example.com').max_retries.total == 0


def test_timeouts_are_connect_read_tuples():
    """A scalar timeout applies the read budget to the connect leg too, so a stalled
    connect sits for minutes; both timeouts must be split."""
    for timeout in (http_utils.STREAM_TIMEOUT, http_utils.SMALL_FILE_TIMEOUT):
        connect_timeout, read_timeout = timeout
        assert connect_timeout < read_timeout
