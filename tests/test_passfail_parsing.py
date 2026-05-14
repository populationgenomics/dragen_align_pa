from pathlib import Path
from unittest.mock import MagicMock, patch

import icasdk
import requests

from dragen_align_pa.jobs.parse_passfail import fetch_passfail_from_ica, parse_passfail_file


def test_parse_passfail_all_success(demo_bundle: Path):
    result = parse_passfail_file(demo_bundle / 'passfail.json')
    assert result == {'SYN00001': 'Success', 'SYN00002': 'Success'}


def test_parse_passfail_with_failure(demo_bundle_with_failure: Path):
    result = parse_passfail_file(demo_bundle_with_failure / 'passfail.json')
    assert result == {'SYN00001': 'Success', 'SYN00002': 'Fail'}


_PATH_PARAMS = {'projectId': 'proj-123'}
_FOLDER = '/bucket/output/COH0001-batch0000_guid_-pipeline-id/'


def _api_instance_with_download_url(url: str = 'https://example.com/passfail.json') -> MagicMock:
    api = MagicMock()
    api.create_download_url_for_data.return_value.body = {'url': url}
    return api


def test_fetch_passfail_returns_parsed_payload_on_happy_path():
    api = _api_instance_with_download_url()
    payload = {'SYN00001': 'Success', 'SYN00002': 'Fail'}
    fake_response = MagicMock(status_code=200)
    fake_response.json.return_value = payload
    fake_response.raise_for_status.return_value = None

    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               return_value='fil.passfail'), \
         patch('dragen_align_pa.jobs.parse_passfail.requests.get',
               return_value=fake_response):
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)

    assert result == payload


def test_fetch_passfail_returns_none_when_file_missing():
    """Catastrophically-failed batch may not have produced passfail.json —
    FileNotFoundError from the lookup is legitimate, not an error."""
    api = MagicMock()
    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               side_effect=FileNotFoundError):
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)
    assert result is None


def test_fetch_passfail_returns_none_on_lookup_api_exception():
    """Any icasdk.ApiException at the lookup stage -> log + None (caller retries next poll)."""
    api = MagicMock()
    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               side_effect=icasdk.ApiException(status=500, reason='kaboom')):
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)
    assert result is None


def test_fetch_passfail_returns_none_on_mint_api_exception():
    """ApiException from create_download_url_for_data -> log + None."""
    api = MagicMock()
    api.create_download_url_for_data.side_effect = icasdk.ApiException(status=500, reason='kaboom')
    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               return_value='fil.passfail'):
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)
    assert result is None


def test_fetch_passfail_returns_none_on_network_error():
    """requests.RequestException -> log + None."""
    api = _api_instance_with_download_url()
    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               return_value='fil.passfail'), \
         patch('dragen_align_pa.jobs.parse_passfail.requests.get',
               side_effect=requests.ConnectionError('timeout')):
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)
    assert result is None


def test_fetch_passfail_retries_once_on_403_then_succeeds():
    """First fetch returns 403 (presigned URL expired between mint and GET);
    code mints a fresh URL and retries once. Second response succeeds."""
    api = _api_instance_with_download_url()
    payload = {'SYN00001': 'Success'}
    first_response = MagicMock(status_code=403)
    second_response = MagicMock(status_code=200)
    second_response.json.return_value = payload
    second_response.raise_for_status.return_value = None

    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               return_value='fil.passfail'), \
         patch('dragen_align_pa.jobs.parse_passfail.requests.get',
               side_effect=[first_response, second_response]) as mock_get:
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)

    assert result == payload
    assert mock_get.call_count == 2
    # Both URLs were minted (initial + retry).
    assert api.create_download_url_for_data.call_count == 2


def test_fetch_passfail_returns_none_on_repeated_403():
    """Two consecutive 403s -> no further retry; return None."""
    api = _api_instance_with_download_url()
    first_response = MagicMock(status_code=403)
    second_response = MagicMock(status_code=403)
    second_response.raise_for_status.side_effect = requests.HTTPError('403 still')

    with patch('dragen_align_pa.jobs.parse_passfail.ica_api_utils.find_file_id_by_name',
               return_value='fil.passfail'), \
         patch('dragen_align_pa.jobs.parse_passfail.requests.get',
               side_effect=[first_response, second_response]):
        result = fetch_passfail_from_ica(api, _PATH_PARAMS, _FOLDER)
    assert result is None
