"""Tests for ica_utils data-plane resilience.

The originating production symptom (PR follow-up): a transient ICA 429 on
`create_download_url_for_data` inside `stream_ica_file_to_gcs` propagated
unhandled and killed the download job, even though tenacity retries had been
added — because the retry was only wired into `check_ica_pipeline_status`,
never into the download path. These tests pin the download-URL calls to the
shared retry.
"""

from unittest.mock import MagicMock

import pytest

from dragen_align_pa import ica_utils
from icasdk.exceptions import ApiException


@pytest.fixture(autouse=True)
def _instant_retry_sleeps(monkeypatch):
    """Patch tenacity's sleep so retry tests don't burn real wall-clock time."""
    monkeypatch.setattr('tenacity.nap.time.sleep', lambda _seconds: None)


def _streaming_response() -> MagicMock:
    """A requests.get(...) stand-in usable as a context manager that yields one
    chunk of content."""
    resp = MagicMock()
    resp.__enter__.return_value = resp
    resp.iter_content.return_value = [b'chunk']
    return resp


def test_stream_ica_file_to_gcs_retries_download_url_on_429(monkeypatch):
    """A 429 on create_download_url_for_data (the exact production traceback)
    must be retried, not propagated; the second attempt's URL is then streamed."""
    monkeypatch.setattr(ica_utils.requests, 'get', MagicMock(return_value=_streaming_response()))

    api = MagicMock()
    url_response = MagicMock()
    url_response.body = {'url': 'https://signed.example/download'}
    api.create_download_url_for_data.side_effect = [
        ApiException(status=429, reason='Too Many Requests'),
        url_response,
    ]

    ica_utils.stream_ica_file_to_gcs(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        file_id='fil.abc',
        file_name='sample.qc.csv',
        gcs_bucket=MagicMock(),
        gcs_prefix='ica/output',
    )

    assert api.create_download_url_for_data.call_count == 2


def test_stream_ica_file_to_gcs_gives_up_after_persistent_429(monkeypatch):
    """Persistent 429 eventually surfaces the original ApiException to the caller."""
    monkeypatch.setattr(
        ica_utils.ica_api_utils,
        'config_retrieve',
        lambda key, default=None: 2 if key == ['ica', 'retry', 'max_retries'] else default,
    )
    monkeypatch.setattr(ica_utils.requests, 'get', MagicMock(return_value=_streaming_response()))

    api = MagicMock()
    api.create_download_url_for_data.side_effect = ApiException(status=429, reason='Too Many Requests')

    with pytest.raises(ApiException) as exc_info:
        ica_utils.stream_ica_file_to_gcs(
            api_instance=api,
            path_parameters={'projectId': 'p'},
            file_id='fil.abc',
            file_name='sample.qc.csv',
            gcs_bucket=MagicMock(),
            gcs_prefix='ica/output',
        )

    assert exc_info.value.status == 429
    # max_retries=2 => initial attempt + 2 retries = 3 total.
    assert api.create_download_url_for_data.call_count == 3
