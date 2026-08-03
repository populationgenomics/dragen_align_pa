"""Tests for ica_utils data-plane resilience.

A transient ICA 429 on `create_download_url_for_data` inside
`stream_ica_file_to_gcs` previously propagated unhandled and killed the
download job — the retry was only wired into `check_ica_pipeline_status`,
never into the download path. These tests pin the download-URL calls to the
shared `ica_retry` controller, the body fetches to the shared retrying
`requests.Session`, and the skip-if-already-in-GCS contract that lets a
part-way-failed stage re-run without re-fetching everything.
"""

import base64
import hashlib
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
import requests

from dragen_align_pa import ica_utils
from icasdk.exceptions import ApiException


@pytest.fixture(autouse=True)
def _instant_retry_sleeps(monkeypatch):
    """Patch tenacity's sleep so retry tests don't burn real wall-clock time."""
    monkeypatch.setattr('tenacity.nap.time.sleep', lambda _seconds: None)


_STREAMED_CHUNK_MD5 = hashlib.md5(b'chunk').hexdigest()  # noqa: S324
"""MD5 of the body `_streaming_response` serves, for tests that stream to completion."""


def _streaming_response() -> MagicMock:
    """A session.get(...) stand-in usable as a context manager that yields one
    chunk of content."""
    resp = MagicMock()
    resp.__enter__.return_value = resp
    resp.iter_content.return_value = [b'chunk']
    resp.headers = {}
    return resp


def _patch_session(monkeypatch) -> MagicMock:
    """Swap the shared download session for a mock and return it."""
    session = MagicMock()
    session.get.return_value = _streaming_response()
    monkeypatch.setattr(ica_utils.http_utils, 'download_session', lambda: session)
    return session


def _empty_bucket() -> MagicMock:
    """A bucket mock that reports nothing is present yet."""
    bucket = MagicMock()
    bucket.get_blob.return_value = None
    return bucket


def _bucket_holding(md5_hex: str | None) -> MagicMock:
    """A bucket mock whose objects already exist, with the given GCS-recorded MD5."""
    bucket = MagicMock()
    existing = MagicMock()
    existing.md5_hash = None if md5_hex is None else base64.b64encode(bytes.fromhex(md5_hex)).decode()
    bucket.get_blob.return_value = existing
    return bucket


def test_stream_ica_file_to_gcs_retries_download_url_on_429(monkeypatch):
    """A 429 on create_download_url_for_data (the production traceback) must be
    retried, not propagated; the second attempt's URL is then streamed."""
    _patch_session(monkeypatch)

    api = MagicMock()
    url_response = MagicMock()
    url_response.body = {'url': 'https://signed.example/download'}
    api.create_download_url_for_data.side_effect = [
        ApiException(status=429, reason='Too Many Requests'),
        url_response,
    ]

    assert (
        ica_utils.stream_ica_file_to_gcs(
            api_instance=api,
            path_parameters={'projectId': 'p'},
            file_id='fil.abc',
            file_name='sample.qc.csv',
            gcs_bucket=_empty_bucket(),
            gcs_prefix='ica/output',
        )
        is True
    )

    assert api.create_download_url_for_data.call_count == 2


def test_stream_ica_file_to_gcs_gives_up_after_persistent_429(monkeypatch):
    """Persistent 429 eventually surfaces the original ApiException to the caller."""
    monkeypatch.setattr(
        ica_utils.ica_api_utils,
        'config_retrieve',
        lambda key, default=None: 2 if key == ['ica', 'retry', 'max_retries'] else default,
    )
    _patch_session(monkeypatch)

    api = MagicMock()
    api.create_download_url_for_data.side_effect = ApiException(status=429, reason='Too Many Requests')

    with pytest.raises(ApiException) as exc_info:
        ica_utils.stream_ica_file_to_gcs(
            api_instance=api,
            path_parameters={'projectId': 'p'},
            file_id='fil.abc',
            file_name='sample.qc.csv',
            gcs_bucket=_empty_bucket(),
            gcs_prefix='ica/output',
        )

    assert exc_info.value.status == 429
    # max_retries=2 => initial attempt + 2 retries = 3 total.
    assert api.create_download_url_for_data.call_count == 3


def test_stream_ica_file_to_gcs_uses_provided_url_without_minting(monkeypatch):
    """When a pre-minted URL is supplied (batch path), stream must NOT call the
    per-file create_download_url_for_data endpoint at all."""
    session = _patch_session(monkeypatch)
    api = MagicMock()

    ica_utils.stream_ica_file_to_gcs(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        file_id='fil.abc',
        file_name='sample.qc.csv',
        gcs_bucket=_empty_bucket(),
        gcs_prefix='ica/output',
        download_url='https://signed.example/presigned',
    )

    api.create_download_url_for_data.assert_not_called()
    session.get.assert_called_once()
    assert session.get.call_args.args[0] == 'https://signed.example/presigned'


# --- skip-if-already-in-GCS ------------------------------------------------------------


def _stream_against(bucket, monkeypatch, *, expected_md5_hash=None, skip_existing=True):
    """Stream one file against `bucket`, returning (result, session)."""
    session = _patch_session(monkeypatch)
    api = MagicMock()
    result = ica_utils.stream_ica_file_to_gcs(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        file_id='fil.abc',
        file_name='sample.cram',
        gcs_bucket=bucket,
        gcs_prefix='ica/output',
        expected_md5_hash=expected_md5_hash,
        download_url='https://signed.example/presigned',
        skip_existing=skip_existing,
    )
    return result, session


def test_stream_skips_a_file_already_in_gcs(monkeypatch):
    """No hash to check: a finalized object means a prior run completed the file,
    so it is neither re-fetched nor re-uploaded."""
    result, session = _stream_against(_bucket_holding(None), monkeypatch)

    assert result is False
    session.get.assert_not_called()


def test_stream_redownloads_a_present_file_when_skip_existing_is_off(monkeypatch):
    """skip_existing=False bypasses the skip so operators can rebuild outputs from ICA."""
    result, session = _stream_against(_bucket_holding(None), monkeypatch, skip_existing=False)

    assert result is True
    session.get.assert_called_once()


def test_stream_skips_when_stored_md5_matches_ica(monkeypatch):
    """With an expected hash the existing object is checked, not merely trusted —
    a match skips a potentially multi-hour CRAM re-download."""
    md5_hex = '0123456789abcdef0123456789abcdef'
    result, session = _stream_against(_bucket_holding(md5_hex), monkeypatch, expected_md5_hash=md5_hex)

    assert result is False
    session.get.assert_not_called()


def test_stream_redownloads_when_stored_md5_differs(monkeypatch):
    """A stored object whose checksum disagrees with ICA is replaced, not trusted."""
    result, session = _stream_against(
        _bucket_holding('0123456789abcdef0123456789abcdef'),
        monkeypatch,
        expected_md5_hash=_STREAMED_CHUNK_MD5,
    )

    assert result is True
    session.get.assert_called_once()


def test_stream_redownloads_when_stored_object_has_no_md5(monkeypatch):
    """An object GCS holds no checksum for cannot be verified, so it is re-fetched."""
    result, session = _stream_against(
        _bucket_holding(None),
        monkeypatch,
        expected_md5_hash=_STREAMED_CHUNK_MD5,
    )

    assert result is True
    session.get.assert_called_once()


# --- whole-transfer retry ---------------------------------------------------------------


def _reset_mid_body() -> MagicMock:
    """A response that dies part-way through the body, as a long-haul CRAM transfer does."""
    resp = MagicMock()
    resp.__enter__.return_value = resp
    resp.iter_content.side_effect = requests.ConnectionError(
        'Connection aborted.',
        ConnectionResetError(104, 'Connection reset by peer'),
    )
    resp.headers = {}
    return resp


def _stream_with_responses(
    monkeypatch,
    responses,
    *,
    download_url='https://signed.example/presigned',
    expected_md5_hash=None,
):
    """Stream one file, serving `responses` in order; returns (result, api, session)."""
    session = MagicMock()
    session.get.side_effect = responses
    monkeypatch.setattr(ica_utils.http_utils, 'download_session', lambda: session)

    api = MagicMock()
    api.create_download_url_for_data.return_value.body = {'url': 'https://signed.example/fresh'}

    result = ica_utils.stream_ica_file_to_gcs(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        file_id='fil.abc',
        file_name='sample.cram',
        gcs_bucket=_empty_bucket(),
        gcs_prefix='ica/output',
        expected_md5_hash=expected_md5_hash,
        download_url=download_url,
    )
    return result, api, session


def test_transfer_restarts_after_a_reset_part_way_through_the_body(monkeypatch):
    """The failure this whole change exists for: a reset mid-body can't be replayed by the
    HTTP adapter, so the file-level retry must re-fetch it."""
    result, _api, session = _stream_with_responses(monkeypatch, [_reset_mid_body(), _streaming_response()])

    assert result is True
    assert session.get.call_count == 2


def test_transfer_retry_mints_a_fresh_url(monkeypatch):
    """A pre-minted batch URL may have expired by the time the retry runs, so the retry
    must not reuse it."""
    _result, api, session = _stream_with_responses(monkeypatch, [_reset_mid_body(), _streaming_response()])

    api.create_download_url_for_data.assert_called_once()
    assert [call.args[0] for call in session.get.call_args_list] == [
        'https://signed.example/presigned',
        'https://signed.example/fresh',
    ]


def test_transfer_gives_up_after_configured_attempts(monkeypatch):
    """Exhausted attempts surface the underlying connection error to the caller."""
    monkeypatch.setattr(
        ica_utils.http_utils,
        'config_retrieve',
        lambda key, default=None: 2 if key == ['ica', 'download', 'max_transfer_attempts'] else default,
    )

    with pytest.raises(requests.ConnectionError):
        _stream_with_responses(monkeypatch, [_reset_mid_body(), _reset_mid_body(), _streaming_response()])


def test_md5_mismatch_is_not_retried(monkeypatch):
    """A checksum mismatch means the bytes were wrong, not that the connection dropped:
    it stays a loud, immediate failure and the bad object is deleted."""
    bucket = _empty_bucket()
    session = MagicMock()
    session.get.return_value = _streaming_response()
    monkeypatch.setattr(ica_utils.http_utils, 'download_session', lambda: session)

    with pytest.raises(ValueError, match='MD5 mismatch'):
        ica_utils.stream_ica_file_to_gcs(
            api_instance=MagicMock(),
            path_parameters={'projectId': 'p'},
            file_id='fil.abc',
            file_name='sample.cram',
            gcs_bucket=bucket,
            gcs_prefix='ica/output',
            expected_md5_hash='ffffffffffffffffffffffffffffffff',
            download_url='https://signed.example/presigned',
        )

    session.get.assert_called_once()
    bucket.blob.return_value.delete.assert_called_once()


# --- truncated transfers -----------------------------------------------------------------


def _sized_response(body: bytes, declared: str | None, encoding: str | None = None) -> MagicMock:
    """A response serving `body` while declaring `declared` as its Content-Length."""
    resp = MagicMock()
    resp.__enter__.return_value = resp
    resp.iter_content.return_value = [body]
    headers = {}
    if declared is not None:
        headers['Content-Length'] = declared
    if encoding is not None:
        headers['Content-Encoding'] = encoding
    resp.headers = headers
    return resp


def test_a_short_body_is_treated_as_a_failed_transfer(monkeypatch):
    """The silent failure mode: urllib3 1.x does not enforce Content-Length, so a clean
    mid-body FIN otherwise finalizes a truncated object that is then skipped forever."""
    short = _sized_response(b'chunk', declared='999999')
    result, _api, session = _stream_with_responses(monkeypatch, [short, _streaming_response()])

    assert result is True
    assert session.get.call_count == 2


def test_a_truncated_transfer_never_finalizes_the_gcs_object(monkeypatch):
    """The check must raise INSIDE the writer's context so BlobWriter.__exit__ terminates
    the resumable session; finalizing a short object is what makes the corruption sticky."""
    session = MagicMock()
    session.get.return_value = _sized_response(b'chunk', declared='999999')
    monkeypatch.setattr(ica_utils.http_utils, 'download_session', lambda: session)
    monkeypatch.setattr(
        ica_utils.http_utils,
        'config_retrieve',
        lambda key, default=None: 1 if key == ['ica', 'download', 'max_transfer_attempts'] else default,
    )
    bucket = _empty_bucket()

    with pytest.raises(requests.RequestException):
        ica_utils.stream_ica_file_to_gcs(
            api_instance=MagicMock(),
            path_parameters={'projectId': 'p'},
            file_id='fil.abc',
            file_name='sample.cram',
            gcs_bucket=bucket,
            gcs_prefix='ica/output',
            download_url='https://signed.example/presigned',
        )

    # __exit__ received the exception, so the writer terminates instead of closing.
    assert bucket.blob.return_value.open.return_value.__exit__.call_args.args[0] is not None


def test_a_complete_body_matching_content_length_is_accepted(monkeypatch):
    """The guard must not fire on a good transfer."""
    exact = _sized_response(b'chunk', declared=str(len(b'chunk')))
    result, _api, session = _stream_with_responses(monkeypatch, [exact])

    assert result is True
    session.get.assert_called_once()


def test_content_length_is_ignored_for_an_encoded_body(monkeypatch):
    """requests decodes a Content-Encoding'd body, so Content-Length describes the encoded
    size and comparing it against decoded bytes would fail every transfer."""
    encoded = _sized_response(b'chunk', declared='999999', encoding='gzip')
    result, _api, session = _stream_with_responses(monkeypatch, [encoded])

    assert result is True
    session.get.assert_called_once()


def test_transfer_is_verified_against_the_expected_md5_after_a_retry(monkeypatch):
    """Guards the hasher-reset invariant: the digest must cover only the winning attempt's
    bytes. Accumulating a failed attempt's bytes too would fail a transfer that succeeded."""
    result, _api, _session = _stream_with_responses(
        monkeypatch,
        [_reset_mid_body(), _streaming_response()],
        expected_md5_hash=_STREAMED_CHUNK_MD5,
    )

    assert result is True


def test_an_http_error_status_is_raised_before_anything_is_written(monkeypatch):
    """Without raise_for_status an S3 error body would be written to GCS as the file."""
    error_response = _streaming_response()
    error_response.raise_for_status.side_effect = requests.HTTPError('404 Not Found')
    session = MagicMock()
    session.get.return_value = error_response
    monkeypatch.setattr(ica_utils.http_utils, 'download_session', lambda: session)
    monkeypatch.setattr(
        ica_utils.http_utils,
        'config_retrieve',
        lambda key, default=None: 1 if key == ['ica', 'download', 'max_transfer_attempts'] else default,
    )
    bucket = _empty_bucket()

    with pytest.raises(requests.HTTPError):
        ica_utils.stream_ica_file_to_gcs(
            api_instance=MagicMock(),
            path_parameters={'projectId': 'p'},
            file_id='fil.abc',
            file_name='sample.cram',
            gcs_bucket=bucket,
            gcs_prefix='ica/output',
            download_url='https://signed.example/presigned',
        )

    bucket.blob.return_value.open.assert_not_called()


# --- run provenance ----------------------------------------------------------------------


def _marker(ica_folder: str, created: datetime) -> MagicMock:
    """An existing provenance marker naming `ica_folder`."""
    blob = MagicMock()
    blob.download_as_bytes.return_value = json.dumps({'ica_folder': ica_folder}).encode()
    blob.time_created = created
    return blob


def test_claiming_a_prefix_keeps_a_marker_from_the_same_ica_run():
    """Resuming a part-way download must not reset ownership, or every file already
    fetched by this run would look like another run's and be re-downloaded."""
    claimed_at = datetime(2026, 8, 1, tzinfo=UTC)
    bucket = MagicMock()
    bucket.get_blob.return_value = _marker('/ica/run-a/', claimed_at)

    owned_since = ica_utils.claim_download_for_run(bucket, 'provenance/sg.json', '/ica/run-a/')

    assert owned_since == claimed_at
    bucket.blob.return_value.upload_from_string.assert_not_called()


def test_claiming_a_prefix_rewrites_a_marker_from_a_different_ica_run():
    """A re-analysis writes new outputs over the same prefix, so ownership resets and
    nothing already there counts as downloaded."""
    bucket = MagicMock()
    bucket.get_blob.return_value = _marker('/ica/run-a/', datetime(2026, 8, 1, tzinfo=UTC))
    new_marker = bucket.blob.return_value
    new_marker.time_created = datetime(2026, 8, 3, tzinfo=UTC)

    owned_since = ica_utils.claim_download_for_run(bucket, 'provenance/sg.json', '/ica/run-b/')

    assert owned_since == datetime(2026, 8, 3, tzinfo=UTC)
    payload = json.loads(new_marker.upload_from_string.call_args.args[0])
    assert payload == {'ica_folder': '/ica/run-b/'}


def test_claiming_an_unmarked_prefix_writes_a_marker():
    """A legacy prefix populated before provenance existed has unknown ownership, so the
    claim starts now and its contents are re-downloaded once."""
    bucket = MagicMock()
    bucket.get_blob.return_value = None
    bucket.blob.return_value.time_created = datetime(2026, 8, 3, tzinfo=UTC)

    assert ica_utils.claim_download_for_run(bucket, 'provenance/sg.json', '/ica/run-b/') == datetime(
        2026, 8, 3, tzinfo=UTC
    )


def test_claiming_raises_when_gcs_reports_no_creation_time():
    """Without a reference instant the skip decision is unsound, so fail rather than guess."""
    bucket = MagicMock()
    bucket.get_blob.return_value = None
    bucket.blob.return_value.time_created = None

    with pytest.raises(ValueError, match='no creation time'):
        ica_utils.claim_download_for_run(bucket, 'provenance/sg.json', '/ica/run-b/')


def test_listing_returns_names_relative_to_prefix_written_since_the_claim():
    """Nested report paths come back in the form ICA lists them; objects predating the
    claim belong to another run and must be omitted."""
    owned_since = datetime(2026, 8, 2, tzinfo=UTC)
    bucket = MagicMock()
    ours, nested, stale = MagicMock(), MagicMock(), MagicMock()
    ours.name, ours.time_created = 'ica/output/a.csv', datetime(2026, 8, 3, tzinfo=UTC)
    nested.name, nested.time_created = 'ica/output/reports/b.html', owned_since
    stale.name, stale.time_created = 'ica/output/old.csv', datetime(2026, 8, 1, tzinfo=UTC)
    bucket.list_blobs.return_value = [ours, nested, stale]

    assert ica_utils.list_gcs_names_written_since(bucket, 'ica/output', owned_since) == {
        'a.csv',
        'reports/b.html',
    }
    bucket.list_blobs.assert_called_once_with(prefix='ica/output/')


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
