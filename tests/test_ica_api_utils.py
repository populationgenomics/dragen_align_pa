"""Tests for ica_api_utils ICA-call resilience.

Production tore down the monitor jobs when a single transient ICA 429
(rate limit) propagated unhandled out of `check_ica_pipeline_status`.
The fix retries transient 429/503 with jittered exponential backoff and
stops re-wrapping `ApiException` (which clobbered `.status`, making any
retry predicate dead code).
"""

from unittest.mock import MagicMock

import pytest

from dragen_align_pa import ica_api_utils
from icasdk.exceptions import ApiException


@pytest.fixture(autouse=True)
def _instant_retry_sleeps(monkeypatch):
    """Tenacity sleeps between retries — patch time.sleep so the retry tests
    don't take real wall-clock time. The retry logic is verified by call
    counts and final outcomes, not by wait timings."""
    monkeypatch.setattr('tenacity.nap.time.sleep', lambda _seconds: None)


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
    ApiException to the caller rather than retrying forever."""
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
