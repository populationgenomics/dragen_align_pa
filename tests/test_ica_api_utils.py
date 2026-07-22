"""Tests for ica_api_utils._fetch_ica_secrets — Secret Manager resilience.

Production hit `google.api_core.exceptions.DeadlineExceeded` (gRPC 504) from
`SECRET_CLIENT.access_secret_version` inside `_fetch_ica_secrets()`, tearing
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

from dragen_align_pa import ica_api_utils, ica_utils
from icasdk.exceptions import ApiException


@pytest.fixture(autouse=True)
def _clear_ica_secrets_cache():
    """`_fetch_ica_secrets` is `@lru_cache(maxsize=1)` and `_secret_client` is
    `@cache`d — clear both between tests so caching from one test doesn't
    bleed into the next."""
    ica_api_utils._fetch_ica_secrets.cache_clear()
    ica_api_utils._secret_client.cache_clear()
    yield
    ica_api_utils._fetch_ica_secrets.cache_clear()
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


def test__fetch_ica_secrets_returns_parsed_payload(monkeypatch):
    """Happy-path regression: secret JSON → parsed dict."""
    payload = {'projectID': 'proj-abc', 'apiKey': 'key-xyz'}
    mock_client = MagicMock()
    mock_client.access_secret_version.return_value = _fake_access_secret_version_response(payload)
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)

    result = ica_api_utils._fetch_ica_secrets()

    assert result == payload


def test__fetch_ica_secrets_caches_across_calls(monkeypatch):
    """The secret doesn't change during a run; calling _fetch_ica_secrets()
    repeatedly (e.g. once per monitor poll, once per batch submission) must
    hit Secret Manager exactly once — otherwise a single transient 504 takes
    down the whole monitor job."""
    payload = {'projectID': 'proj-abc', 'apiKey': 'key-xyz'}
    mock_client = MagicMock()
    mock_client.access_secret_version.return_value = _fake_access_secret_version_response(payload)
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)

    first = ica_api_utils._fetch_ica_secrets()
    second = ica_api_utils._fetch_ica_secrets()
    third = ica_api_utils._fetch_ica_secrets()

    assert first == second == third == payload
    assert mock_client.access_secret_version.call_count == 1, (
        f'expected exactly one RPC; got {mock_client.access_secret_version.call_count}'
    )


def test__fetch_ica_secrets_retries_on_deadline_exceeded(monkeypatch):
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

    result = ica_api_utils._fetch_ica_secrets()

    assert result == payload
    assert mock_client.access_secret_version.call_count == 2


def test__fetch_ica_secrets_retries_on_service_unavailable(monkeypatch):
    """ServiceUnavailable (gRPC UNAVAILABLE / HTTP 503) is the other
    transient class — must also be retried."""
    payload = {'projectID': 'proj-abc', 'apiKey': 'key-xyz'}
    mock_client = MagicMock()
    mock_client.access_secret_version.side_effect = [
        gax_exceptions.ServiceUnavailable('Secret Manager 503'),
        _fake_access_secret_version_response(payload),
    ]
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)

    result = ica_api_utils._fetch_ica_secrets()

    assert result == payload
    assert mock_client.access_secret_version.call_count == 2


def test__fetch_ica_secrets_gives_up_after_persistent_failures(monkeypatch):
    """Persistent (non-transient) Secret Manager unavailability eventually
    propagates after the retry budget is exhausted. The caller (cpg-flow
    job) sees a real failure rather than silent infinite retry."""
    mock_client = MagicMock()
    mock_client.access_secret_version.side_effect = gax_exceptions.DeadlineExceeded('persistent 504')
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)

    with pytest.raises(gax_exceptions.DeadlineExceeded):
        ica_api_utils._fetch_ica_secrets()

    # At least some retries happened — confirms the retry layer is active.
    # The exact retry budget is configured in the production module; we just
    # assert it's > 1 (i.e. more than the initial attempt).
    assert mock_client.access_secret_version.call_count > 1


def test__fetch_ica_secrets_does_not_retry_non_transient_errors(monkeypatch):
    """A non-Secret-Manager-transient failure (e.g. PermissionDenied,
    NotFound — IAM or config errors) must propagate immediately, not be
    retried. Retrying a permanent error would just hammer Secret Manager
    and delay the real failure signal."""
    mock_client = MagicMock()
    mock_client.access_secret_version.side_effect = gax_exceptions.PermissionDenied('forbidden')
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)

    with pytest.raises(gax_exceptions.PermissionDenied):
        ica_api_utils._fetch_ica_secrets()

    assert mock_client.access_secret_version.call_count == 1


def test__fetch_ica_secrets_cache_skips_retry_layer_on_subsequent_calls(monkeypatch):
    """After a successful first call populates the cache, a subsequent call
    must NOT go anywhere near Secret Manager — even if the SECRET_CLIENT is
    swapped for one that would raise. Belt-and-braces against the regression
    Copilot warned about for #8 (loops re-firing callbacks)."""
    payload = {'projectID': 'proj-abc', 'apiKey': 'key-xyz'}
    happy_client = MagicMock()
    happy_client.access_secret_version.return_value = _fake_access_secret_version_response(payload)
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: happy_client)
    first = ica_api_utils._fetch_ica_secrets()
    assert first == payload

    angry_client = MagicMock()
    angry_client.access_secret_version.side_effect = AssertionError(
        'cache must short-circuit; _secret_client must not be called on cache hit',
    )
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: angry_client)
    second = ica_api_utils._fetch_ica_secrets()

    assert second == payload
    angry_client.access_secret_version.assert_not_called()


def test_get_ica_api_key_returns_key_when_present(monkeypatch):
    """get_ica_api_key() returns the configured family's key — the default family (conftest:
    `project_root='ourdna'`) authenticates with the base `apiKey`, which the payload carries."""
    payload = {'projectID': 'proj-abc', 'apiKey': 'key-xyz'}
    mock_client = MagicMock()
    mock_client.access_secret_version.return_value = _fake_access_secret_version_response(payload)
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)

    assert ica_api_utils.get_ica_api_key() == 'key-xyz'


def test_get_ica_api_key_raises_when_family_field_missing(monkeypatch):
    """A family whose key field is absent from the secret fails loud once here, naming the
    field — instead of a bare KeyError at the client call site or an `x-api-key: null` written
    by the CLI. Configuring the tenk10k family needs `tenk10k_apiKey`, absent below."""
    payload = {'projectID': 'proj-abc', 'apiKey': 'key-xyz'}  # no tenk10k_apiKey
    mock_client = MagicMock()
    mock_client.access_secret_version.return_value = _fake_access_secret_version_response(payload)
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)
    monkeypatch.setattr(
        'dragen_align_pa.constants.constants_registry.config_retrieve',
        lambda key, default=None: 'tenk10k',  # noqa: ARG005
    )

    with pytest.raises(KeyError, match=r'tenk10k_apiKey'):
        ica_api_utils.get_ica_api_key()


def test_get_ica_api_key_raises_when_field_blank(monkeypatch):
    """An empty-string key value is treated the same as missing, so it must fail here rather
    than producing a silently-broken client. Default family `ourdna` uses `apiKey`."""
    payload = {'projectID': 'proj-abc', 'apiKey': ''}
    mock_client = MagicMock()
    mock_client.access_secret_version.return_value = _fake_access_secret_version_response(payload)
    monkeypatch.setattr(ica_api_utils, '_secret_client', lambda: mock_client)

    with pytest.raises(KeyError, match=r'apiKey'):
        ica_api_utils.get_ica_api_key()


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


def test_submit_nextflow_analysis_preserves_api_exception_status():
    """Same defect as test_check_ica_pipeline_status_preserves_api_exception_status,
    different call site. submit_nextflow_analysis is the natural next target for
    the retry decorator; ensure .status survives so the predicate can match."""
    api = MagicMock()
    api.create_nextflow_analysis.side_effect = ApiException(status=503, reason='Service Unavailable')

    with pytest.raises(ApiException) as exc_info:
        ica_api_utils.submit_nextflow_analysis(
            api_instance=api,
            path_params={'projectId': 'p'},
            body=MagicMock(),
        )

    assert exc_info.value.status == 503


def test_check_ica_pipeline_status_retries_on_429_then_succeeds():
    """A transient 429 must be retried; the second attempt's success
    populates the return value. Without this, a single ICA blip tears
    down the cohort monitor (the originating production symptom)."""
    api = MagicMock()
    succeeding_response = MagicMock()
    succeeding_response.body = {'status': 'INPROGRESS'}
    api.get_analysis.side_effect = [
        ApiException(status=429, reason='Too Many Requests'),
        succeeding_response,
    ]

    result = ica_api_utils.check_ica_pipeline_status(
        api_instance=api,
        path_params={'projectId': 'p', 'analysisId': 'a'},
    )

    assert result == 'INPROGRESS'
    assert api.get_analysis.call_count == 2


def test_check_ica_pipeline_status_retries_on_503_then_succeeds():
    """503 (ICA backend unavailable) is the other transient class the
    retry must absorb."""
    api = MagicMock()
    succeeding_response = MagicMock()
    succeeding_response.body = {'status': 'SUCCEEDED'}
    api.get_analysis.side_effect = [
        ApiException(status=503, reason='Service Unavailable'),
        succeeding_response,
    ]

    result = ica_api_utils.check_ica_pipeline_status(
        api_instance=api,
        path_params={'projectId': 'p', 'analysisId': 'a'},
    )

    assert result == 'SUCCEEDED'
    assert api.get_analysis.call_count == 2


def test_check_ica_pipeline_status_does_not_retry_409():
    """409 is retryable only for create-data (via `ica_retry_create`, behind an
    existence re-check). A status poll goes through the base `ica_retry`, which
    must propagate 409 on the first occurrence rather than retry a non-idempotent
    conflict it cannot recover."""
    api = MagicMock()
    api.get_analysis.side_effect = ApiException(status=409, reason='Conflict')

    with pytest.raises(ApiException) as exc_info:
        ica_api_utils.check_ica_pipeline_status(
            api_instance=api,
            path_params={'projectId': 'p', 'analysisId': 'a'},
        )

    assert exc_info.value.status == 409
    assert api.get_analysis.call_count == 1


def test_check_ica_pipeline_status_does_not_retry_non_transient_status():
    """A 404 (analysis not found) is a real not-retryable error — retrying
    just delays the failure signal. Other non-(429|503) ApiExceptions must
    propagate on the first occurrence."""
    api = MagicMock()
    api.get_analysis.side_effect = ApiException(status=404, reason='Not Found')

    with pytest.raises(ApiException) as exc_info:
        ica_api_utils.check_ica_pipeline_status(
            api_instance=api,
            path_params={'projectId': 'p', 'analysisId': 'a'},
        )

    assert exc_info.value.status == 404
    assert api.get_analysis.call_count == 1


def test_check_ica_pipeline_status_gives_up_after_persistent_429():
    """If every attempt 429s, eventually we surface the original
    ApiException to the caller (the StatusProvider then logs it at DEBUG
    and the id reads as UNKNOWN)."""
    api = MagicMock()
    api.get_analysis.side_effect = ApiException(status=429, reason='Too Many Requests')

    with pytest.raises(ApiException) as exc_info:
        ica_api_utils.check_ica_pipeline_status(
            api_instance=api,
            path_params={'projectId': 'p', 'analysisId': 'a'},
        )

    assert exc_info.value.status == 429
    # Default 10 retries => 11 total attempts (initial + 10).
    assert api.get_analysis.call_count == 11


def test_check_ica_pipeline_status_retry_count_is_configurable(monkeypatch):
    """[ica.retry] max_retries tunes the attempt count at call time, so it can
    be changed via config without rebuilding the image. max_retries=2 => the
    initial attempt + 2 retries = 3 total."""
    monkeypatch.setattr(
        ica_api_utils,
        'config_retrieve',
        lambda key, default=None: 2 if key == ['ica', 'retry', 'max_retries'] else default,
    )
    api = MagicMock()
    api.get_analysis.side_effect = ApiException(status=429, reason='Too Many Requests')

    with pytest.raises(ApiException):
        ica_api_utils.check_ica_pipeline_status(
            api_instance=api,
            path_params={'projectId': 'p', 'analysisId': 'a'},
        )

    assert api.get_analysis.call_count == 3


# --- ica_retry helper: the shared controller all data-plane calls go through ---


def test_ica_retry_retries_transient_then_returns():
    """The public helper retries a 429 and returns the eventual success value,
    so any data-plane SDK call wrapped in it survives a transient blip."""
    call = MagicMock(side_effect=[ApiException(status=429, reason='Too Many Requests'), 'ok'])

    result = ica_api_utils.ica_retry(call, path_params={'projectId': 'p'})

    assert result == 'ok'
    assert call.call_count == 2
    # kwargs must be forwarded to the wrapped call on every attempt.
    assert call.call_args_list[0].kwargs == {'path_params': {'projectId': 'p'}}


def test_ica_retry_does_not_retry_non_transient():
    """A 404 is a real, permanent error — surface it on the first attempt."""
    call = MagicMock(side_effect=ApiException(status=404, reason='Not Found'))

    with pytest.raises(ApiException) as exc_info:
        ica_api_utils.ica_retry(call)

    assert exc_info.value.status == 404
    assert call.call_count == 1


# --- The other data-plane call sites now go through the retry ---


def test_submit_nextflow_analysis_retries_on_429_then_succeeds():
    """Pipeline submission is a data-plane POST; a 429 means the request was
    rejected (not processed), so retrying is safe and must not tear down the run."""
    api = MagicMock()
    succeeding_response = MagicMock()
    succeeding_response.body = {'id': 'ana.123'}
    api.create_nextflow_analysis.side_effect = [
        ApiException(status=429, reason='Too Many Requests'),
        succeeding_response,
    ]

    analysis_id = ica_api_utils.submit_nextflow_analysis(
        api_instance=api,
        path_params={'projectId': 'p'},
        body=MagicMock(),
    )

    assert analysis_id == 'ana.123'
    assert api.create_nextflow_analysis.call_count == 2


def test_check_object_already_exists_retries_on_503_then_succeeds():
    """The existence check is the most-called data-plane endpoint
    (get_project_data_list); it must absorb transient 503s."""
    api = MagicMock()
    succeeding_response = MagicMock()
    succeeding_response.body = {'items': []}
    api.get_project_data_list.side_effect = [
        ApiException(status=503, reason='Service Unavailable'),
        succeeding_response,
    ]

    result = ica_api_utils.check_object_already_exists(
        api_instance=api,
        path_params={'projectId': 'p'},
        file_name='SYN1.cram',
        folder_path='/bucket/folder',
        object_type='FILE',
    )

    assert result is None
    assert api.get_project_data_list.call_count == 2


def test_create_upload_object_id_recovers_object_on_conflict_retry():
    """A 409 on create is retried; because the existence check now sits inside the
    retry boundary, the retry finds the object the conflicting write already landed
    and returns it rather than creating a duplicate."""
    api = MagicMock()
    not_found = MagicMock(body={'items': []})
    now_exists = MagicMock(body={'items': [{'data': {'id': 'fid_recovered', 'details': {'status': 'AVAILABLE'}}}]})
    api.get_project_data_list.side_effect = [not_found, now_exists]
    api.create_data_in_project.side_effect = ApiException(status=409, reason='Conflict')

    object_id, status = ica_utils.create_upload_object_id(
        api_instance=api,
        path_params={'projectId': 'p'},
        folder_name='myfolder',
        file_name='SYN1.cram',
        folder_path='/bucket/folder',
        object_type='FILE',
    )

    assert (object_id, status) == ('fid_recovered', 'AVAILABLE')
    assert api.create_data_in_project.call_count == 1  # not re-created after recovery
    assert api.get_project_data_list.call_count == 2  # re-checked on the retry


def test_submit_nextflow_analysis_does_not_retry_409():
    """MF2: the DRAGEN analysis submit is non-idempotent and has no existence
    re-check, so a 409 must propagate on the first occurrence — retrying it would
    risk a duplicate analysis. Only create-data (via `ica_retry_create`) retries 409."""
    api = MagicMock()
    api.create_nextflow_analysis.side_effect = ApiException(status=409, reason='Conflict')

    with pytest.raises(ApiException) as exc_info:
        ica_api_utils.submit_nextflow_analysis(
            api_instance=api,
            path_params={'projectId': 'p'},
            body=MagicMock(),
        )

    assert exc_info.value.status == 409
    assert api.create_nextflow_analysis.call_count == 1


def test_check_object_already_exists_folder_available_is_returned():
    """An AVAILABLE folder is safe to reuse and is returned as (id, status)."""
    api = MagicMock()
    api.get_project_data_list.return_value = MagicMock(
        body={'items': [{'data': {'id': 'fol.ok', 'details': {'status': 'AVAILABLE'}}}]},
    )

    result = ica_api_utils.check_object_already_exists(
        api_instance=api,
        path_params={'projectId': 'p'},
        file_name='myfolder',
        folder_path='/bucket/parent',
        object_type='FOLDER',
    )

    assert result == ('fol.ok', 'AVAILABLE')


def test_check_object_already_exists_folder_deleting_raises():
    """SF3: a folder in async-DELETING state must not be handed back as 'exists'.
    ICA delete is async, so returning its id would point an upload / analysis
    output at a folder about to vanish — raise loudly instead."""
    api = MagicMock()
    api.get_project_data_list.return_value = MagicMock(
        body={'items': [{'data': {'id': 'fol.doomed', 'details': {'status': 'DELETING'}}}]},
    )

    with pytest.raises(NotImplementedError, match='DELETING'):
        ica_api_utils.check_object_already_exists(
            api_instance=api,
            path_params={'projectId': 'p'},
            file_name='myfolder',
            folder_path='/bucket/parent',
            object_type='FOLDER',
        )
