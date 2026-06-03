"""Tests for ica_utils data-plane resilience.

A transient ICA 429 on `create_download_url_for_data` inside
`stream_ica_file_to_gcs` previously propagated unhandled and killed the
download job — the retry was only wired into `check_ica_pipeline_status`,
never into the download path. These tests pin the download-URL calls to the
shared `ica_retry` controller.
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
    """A 429 on create_download_url_for_data (the production traceback) must be
    retried, not propagated; the second attempt's URL is then streamed."""
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


def test_stream_ica_file_to_gcs_uses_provided_url_without_minting(monkeypatch):
    """When a pre-minted URL is supplied (batch path), stream must NOT call the
    per-file create_download_url_for_data endpoint at all."""
    monkeypatch.setattr(ica_utils.requests, 'get', MagicMock(return_value=_streaming_response()))
    api = MagicMock()

    ica_utils.stream_ica_file_to_gcs(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        file_id='fil.abc',
        file_name='sample.qc.csv',
        gcs_bucket=MagicMock(),
        gcs_prefix='ica/output',
        download_url='https://signed.example/presigned',
    )

    api.create_download_url_for_data.assert_not_called()
    ica_utils.requests.get.assert_called_once()
    assert ica_utils.requests.get.call_args.args[0] == 'https://signed.example/presigned'


# --- batch_create_download_urls: one API call for a whole folder's URLs ---


def test_batch_create_download_urls_returns_id_to_url_map():
    """The batch endpoint collapses N per-file mints into ONE call and returns
    a {dataId: url} map keyed so callers match URLs to the IDs they hold."""
    api = MagicMock()
    response = MagicMock()
    response.body = {
        'items': [
            {'dataId': 'fil.a', 'url': 'https://u/a'},
            {'dataId': 'fil.b', 'url': 'https://u/b'},
        ],
    }
    api.create_download_urls_for_data.return_value = response

    result = ica_utils.batch_create_download_urls(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        file_ids=['fil.a', 'fil.b'],
    )

    assert result == {'fil.a': 'https://u/a', 'fil.b': 'https://u/b'}
    assert api.create_download_urls_for_data.call_count == 1


def test_batch_create_download_urls_empty_makes_no_call():
    """An empty id list must short-circuit — never hit the API."""
    api = MagicMock()

    result = ica_utils.batch_create_download_urls(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        file_ids=[],
    )

    assert result == {}
    api.create_download_urls_for_data.assert_not_called()


def test_batch_create_download_urls_retries_on_429(monkeypatch):
    """The batch mint is itself a rate-limited POST; it must go through the
    shared ica_retry so a transient 429 is absorbed."""
    monkeypatch.setattr('tenacity.nap.time.sleep', lambda _seconds: None)
    api = MagicMock()
    response = MagicMock()
    response.body = {'items': [{'dataId': 'fil.a', 'url': 'https://u/a'}]}
    api.create_download_urls_for_data.side_effect = [
        ApiException(status=429, reason='Too Many Requests'),
        response,
    ]

    result = ica_utils.batch_create_download_urls(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        file_ids=['fil.a'],
    )

    assert result == {'fil.a': 'https://u/a'}
    assert api.create_download_urls_for_data.call_count == 2
