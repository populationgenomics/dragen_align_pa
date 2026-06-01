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
from unittest.mock import MagicMock, patch

import pytest
from google.api_core import exceptions as gax_exceptions

from dragen_align_pa import ica_api_utils
from icasdk.exceptions import ApiException
from icasdk.model.analysis import Analysis
from icasdk.paths.api_projects_project_id_analyses_analysis_id.put import (
    request_body_analysis,
)


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
    # 5 attempts total (stop_after_attempt(5)).
    assert api.get_analysis.call_count == 5


def test_get_ica_api_client_sets_connection_pool_maxsize(monkeypatch):
    """urllib3.PoolManager defaults maxsize=4. At 16 workers, that
    silently degrades the polling fan-out to ~4 in-flight. The context
    manager must set connection_pool_maxsize from [ica.polling]
    concurrency so the parallel polling design actually parallelises."""
    captured: dict[str, object] = {}

    class _FakeApiClient:
        def __init__(self, configuration):
            captured['pool_maxsize'] = configuration.connection_pool_maxsize

        def __enter__(self):
            return self

        def __exit__(self, *a: object) -> None:
            return None

    monkeypatch.setattr('dragen_align_pa.ica_api_utils.ApiClient', _FakeApiClient)
    monkeypatch.setattr(
        ica_api_utils,
        'get_ica_secrets',
        lambda: {'projectID': 'p', 'apiKey': 'k'},
    )

    with ica_api_utils.get_ica_api_client():
        pass

    # Should match [ica.polling] concurrency from _TEST_CONFIG (16).
    assert captured['pool_maxsize'] == 16


def _stub_ica_api(monkeypatch, api):
    """Wire add_technical_tag to use a mock ProjectAnalysisApi."""
    monkeypatch.setattr(
        ica_api_utils,
        'get_ica_api_client',
        lambda: MagicMock(
            __enter__=MagicMock(return_value=MagicMock()),
            __exit__=MagicMock(return_value=None),
        ),
    )
    monkeypatch.setattr(
        'dragen_align_pa.ica_api_utils.project_analysis_api.ProjectAnalysisApi',
        lambda _client: api,
    )


def _full_analysis_body() -> dict:
    """A minimal-but-complete analysis as ICA's GET returns it.

    `update_analysis` is a full-replace PUT whose body validates against
    the `Analysis` schema (ten required fields), so the helper must PUT
    the whole object back, not a tags-only fragment.
    """
    return {
        'id': 'analysis-id',
        'reference': 'existing-reference',
        'userReference': 'uref',
        'status': 'SUCCEEDED',
        'tenantId': 'tenant-1',
        'ownerId': 'owner-1',
        'timeCreated': '2026-01-01T00:00:00Z',
        'timeModified': '2026-01-01T00:00:00Z',
        'pipeline': {'id': 'pipe-1', 'urn': 'urn:x'},
        'tags': {
            'technicalTags': ['popgen-existing'],
            'userTags': ['user-existing'],
            'referenceTags': [],
        },
    }


def test_add_technical_tag_appends_and_dedupes(monkeypatch):
    """The helper reads the current Analysis, appends the AR-GUID to
    technicalTags (deduped), and PUTs the *complete* object back. It MUST
    NOT clobber tags written by popgen-cli, nor drop other analysis fields."""
    existing_get = MagicMock()
    existing_get.body = _full_analysis_body()
    existing_get.headers = {'ETag': 'etag-abc'}

    api = MagicMock()
    api.get_analysis.return_value = existing_get
    api.update_analysis.return_value = MagicMock(body={'id': 'analysis-id'})
    _stub_ica_api(monkeypatch, api)

    ica_api_utils.add_technical_tag(
        project_id='proj-1',
        analysis_id='analysis-id',
        tag='AR-GUID-12345',
    )

    update_kwargs = api.update_analysis.call_args.kwargs
    body = update_kwargs['body']
    # Full object preserved (not a tags-only fragment).
    assert body['id'] == 'analysis-id'
    assert body['status'] == 'SUCCEEDED'
    assert body['tags']['technicalTags'] == ['popgen-existing', 'AR-GUID-12345']
    assert body['tags']['userTags'] == ['user-existing']
    # If-Match carries the GET's ETag for optimistic concurrency.
    assert update_kwargs['header_params'] == {'If-Match': 'etag-abc'}

    # No duplication if the same tag is already present — verify dedupe:
    existing_get.body = body  # second GET returns what we just PUT
    ica_api_utils.add_technical_tag(
        project_id='proj-1',
        analysis_id='analysis-id',
        tag='AR-GUID-12345',
    )
    # The tag is already present, so no second PUT happens.
    assert api.update_analysis.call_count == 1


def test_add_technical_tag_body_serializes_through_icasdk(monkeypatch):
    """Regression guard for the original defect: the helper used a plain
    dataclass body that failed icasdk's local serialization (Analysis needs
    ten required fields) — every call was a swallowed no-op. Run the body
    the helper builds through the real request-body serializer and assert
    it produces a wire payload carrying the tag."""
    valid_uuid = '12345678-1234-1234-1234-123456789abc'
    ts = '2026-01-01T00:00:00Z'
    analysis = Analysis(
        id=valid_uuid, reference='ref', tenantId=valid_uuid, ownerId=valid_uuid,
        timeCreated=ts, timeModified=ts, status='SUCCEEDED', userReference='uref',
        pipeline={
            'id': valid_uuid, 'urn': 'urn:x', 'timeCreated': ts, 'timeModified': ts,
            'ownerId': valid_uuid, 'tenantId': valid_uuid, 'code': 'c',
            'description': 'd', 'language': 'NEXTFLOW', 'pipelineTags': {'technicalTags': []},
            'analysisStorage': {
                'id': valid_uuid, 'timeCreated': ts, 'timeModified': ts,
                'ownerId': valid_uuid, 'tenantId': valid_uuid, 'name': 'Small',
                'description': 'd',
            },
        },
        tags={'technicalTags': ['popgen'], 'userTags': [], 'referenceTags': []},
    )
    existing_get = MagicMock(body=analysis, headers={'ETag': 'e'})
    api = MagicMock()
    api.get_analysis.return_value = existing_get
    api.update_analysis.return_value = MagicMock()
    _stub_ica_api(monkeypatch, api)

    ica_api_utils.add_technical_tag(
        project_id='p', analysis_id='analysis-id', tag='AR-GUID-999',
    )

    sent_body = api.update_analysis.call_args.kwargs['body']
    serialized = request_body_analysis.serialize(
        sent_body, 'application/vnd.illumina.v3+json',
    )
    assert b'AR-GUID-999' in serialized['body']
    assert b'popgen' in serialized['body']


def test_add_technical_tag_retries_once_on_412(monkeypatch):
    """A 412 (If-Match ETag drift) triggers a single re-read-and-retry."""
    existing_get = MagicMock()
    existing_get.body = _full_analysis_body()
    existing_get.headers = {'ETag': 'etag-abc'}

    api = MagicMock()
    api.get_analysis.return_value = existing_get
    api.update_analysis.side_effect = [
        ApiException(status=412, reason='Precondition Failed'),
        MagicMock(),
    ]
    _stub_ica_api(monkeypatch, api)

    ica_api_utils.add_technical_tag(
        project_id='proj-1', analysis_id='analysis-id', tag='AR-GUID-12345',
    )

    assert api.get_analysis.call_count == 2
    assert api.update_analysis.call_count == 2


def test_add_technical_tag_logs_and_swallows_on_persistent_failure(monkeypatch):
    """Best-effort: on any ApiException, log a warning and return cleanly.
    Correctness of the polling design does not depend on the tag, so a
    failed tag-write must not break MLR submission."""
    api = MagicMock()
    api.get_analysis.side_effect = ApiException(status=500, reason='Server Error')
    _stub_ica_api(monkeypatch, api)

    with patch('dragen_align_pa.ica_api_utils.logger') as mock_logger:
        # Must not raise.
        ica_api_utils.add_technical_tag(
            project_id='proj-1',
            analysis_id='analysis-id',
            tag='AR-GUID-12345',
        )
        assert mock_logger.warning.call_count >= 1
