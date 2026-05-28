"""Tests for ica_api_utils.get_ica_secrets — Secret Manager resilience.

Production hit `google.api_core.exceptions.DeadlineExceeded` (gRPC 504) from
`SECRET_CLIENT.access_secret_version` inside `get_ica_secrets()`, tearing
down monitor jobs mid-run. The secret payload doesn't change during a run,
so the fix is two-fold: cache the result for the process lifetime, and
retry on transient Secret Manager failures.

GAPIC's default retry policy does NOT cover `DeadlineExceeded`, which is
why these blips propagate unhandled.
"""

import json
from unittest.mock import MagicMock

import pytest
from google.api_core import exceptions as gax_exceptions

from dragen_align_pa import ica_api_utils


@pytest.fixture(autouse=True)
def _clear_ica_secrets_cache():
    """`get_ica_secrets` is `@lru_cache(maxsize=1)` and `_secret_client` is
    `@cache`d — clear both between tests so caching from one test doesn't
    bleed into the next."""
    ica_api_utils.get_ica_secrets.cache_clear()
    ica_api_utils._secret_client.cache_clear()
    yield
    ica_api_utils.get_ica_secrets.cache_clear()
    ica_api_utils._secret_client.cache_clear()


@pytest.fixture(autouse=True)
def _instant_retry_sleeps(monkeypatch):
    """Tenacity sleeps between retries — patch time.sleep so the retry tests
    don't take real wall-clock time. The retry logic is verified by call
    counts and final outcomes, not by wait timings."""
    monkeypatch.setattr('tenacity.nap.time.sleep', lambda _seconds: None)


def _fake_access_secret_version_response(payload: dict) -> MagicMock:
    """Build a stand-in for the AccessSecretVersionResponse returned by
    SecretManagerServiceClient.access_secret_version."""
    response = MagicMock()
    response.payload.data = json.dumps(payload).encode('UTF-8')
    return response


def test_get_ica_secrets_returns_parsed_payload(monkeypatch):
    """Happy-path regression: secret JSON → parsed dict."""
    payload = {'projectID': 'proj-abc', 'apiKey': 'key-xyz'}
    mock_client = MagicMock()
    mock_client.access_secret_version.return_value = _fake_access_secret_version_response(payload)
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)

    result = ica_api_utils.get_ica_secrets()

    assert result == payload


def test_get_ica_secrets_caches_across_calls(monkeypatch):
    """The secret doesn't change during a run; calling get_ica_secrets()
    repeatedly (e.g. once per monitor poll, once per batch submission) must
    hit Secret Manager exactly once — otherwise a single transient 504 takes
    down the whole monitor job."""
    payload = {'projectID': 'proj-abc', 'apiKey': 'key-xyz'}
    mock_client = MagicMock()
    mock_client.access_secret_version.return_value = _fake_access_secret_version_response(payload)
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)

    first = ica_api_utils.get_ica_secrets()
    second = ica_api_utils.get_ica_secrets()
    third = ica_api_utils.get_ica_secrets()

    assert first == second == third == payload
    assert mock_client.access_secret_version.call_count == 1, (
        f'expected exactly one RPC; got {mock_client.access_secret_version.call_count}'
    )


def test_get_ica_secrets_retries_on_deadline_exceeded(monkeypatch):
    """The originating production failure: SecretManagerServiceClient raises
    DeadlineExceeded on a transient blip. The retry layer must absorb it and
    return the value on the second attempt."""
    payload = {'projectID': 'proj-abc', 'apiKey': 'key-xyz'}
    mock_client = MagicMock()
    mock_client.access_secret_version.side_effect = [
        gax_exceptions.DeadlineExceeded('Secret Manager 504'),
        _fake_access_secret_version_response(payload),
    ]
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)

    result = ica_api_utils.get_ica_secrets()

    assert result == payload
    assert mock_client.access_secret_version.call_count == 2


def test_get_ica_secrets_retries_on_service_unavailable(monkeypatch):
    """ServiceUnavailable (gRPC UNAVAILABLE / HTTP 503) is the other
    transient class — must also be retried."""
    payload = {'projectID': 'proj-abc', 'apiKey': 'key-xyz'}
    mock_client = MagicMock()
    mock_client.access_secret_version.side_effect = [
        gax_exceptions.ServiceUnavailable('Secret Manager 503'),
        _fake_access_secret_version_response(payload),
    ]
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)

    result = ica_api_utils.get_ica_secrets()

    assert result == payload
    assert mock_client.access_secret_version.call_count == 2


def test_get_ica_secrets_gives_up_after_persistent_failures(monkeypatch):
    """Persistent (non-transient) Secret Manager unavailability eventually
    propagates after the retry budget is exhausted. The caller (cpg-flow
    job) sees a real failure rather than silent infinite retry."""
    mock_client = MagicMock()
    mock_client.access_secret_version.side_effect = gax_exceptions.DeadlineExceeded('persistent 504')
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)

    with pytest.raises(gax_exceptions.DeadlineExceeded):
        ica_api_utils.get_ica_secrets()

    # At least some retries happened — confirms the retry layer is active.
    # The exact retry budget is configured in the production module; we just
    # assert it's > 1 (i.e. more than the initial attempt).
    assert mock_client.access_secret_version.call_count > 1


def test_get_ica_secrets_does_not_retry_non_transient_errors(monkeypatch):
    """A non-Secret-Manager-transient failure (e.g. PermissionDenied,
    NotFound — IAM or config errors) must propagate immediately, not be
    retried. Retrying a permanent error would just hammer Secret Manager
    and delay the real failure signal."""
    mock_client = MagicMock()
    mock_client.access_secret_version.side_effect = gax_exceptions.PermissionDenied('forbidden')
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)

    with pytest.raises(gax_exceptions.PermissionDenied):
        ica_api_utils.get_ica_secrets()

    assert mock_client.access_secret_version.call_count == 1


def test_get_ica_secrets_cache_skips_retry_layer_on_subsequent_calls(monkeypatch):
    """After a successful first call populates the cache, a subsequent call
    must NOT go anywhere near Secret Manager — even if the SECRET_CLIENT is
    swapped for one that would raise. Belt-and-braces against the regression
    Copilot warned about for #8 (loops re-firing callbacks)."""
    payload = {'projectID': 'proj-abc', 'apiKey': 'key-xyz'}
    happy_client = MagicMock()
    happy_client.access_secret_version.return_value = _fake_access_secret_version_response(payload)
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: happy_client)
    first = ica_api_utils.get_ica_secrets()
    assert first == payload

    angry_client = MagicMock()
    angry_client.access_secret_version.side_effect = AssertionError(
        'cache must short-circuit; _secret_client must not be called on cache hit',
    )
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: angry_client)
    second = ica_api_utils.get_ica_secrets()

    assert second == payload
    angry_client.access_secret_version.assert_not_called()


from icasdk.exceptions import ApiException


def _mock_api_with_status(status: int) -> object:
    """Build a stand-in for ProjectAnalysisApi whose get_analysis raises an
    ApiException with the given HTTP status preserved on the exception."""
    api = MagicMock()
    api.get_analysis.side_effect = ApiException(status=status, reason='Too Many Requests')
    return api


def test_check_ica_pipeline_status_preserves_api_exception_status():
    """Regression: the wrapper's old `raise ApiException(f'...{e}') from e`
    pattern clobbered `.status` (the f-string went into the `status=`
    positional, making it a str). Without `.status` preserved as int, any
    downstream retry predicate `e.status in (429, 503)` is dead code.
    The wrapper must propagate the original ApiException intact."""
    api = _mock_api_with_status(429)

    with pytest.raises(ApiException) as exc_info:
        ica_api_utils.check_ica_pipeline_status(
            api_instance=api,
            path_params={'projectId': 'p', 'analysisId': 'a'},
        )

    assert exc_info.value.status == 429, (
        f'expected .status == 429 to survive the wrapper; got {exc_info.value.status!r}'
    )
