"""Unit tests for the in-process popgen_cli MLR submission layer.

popgen_cli's own RetryRunner retries every exception (its submission POST up to
1,000,000 times), so a permanent error like a 401 from a stale MLR config spins
silently until the batch times out. TransientRetryRunner replaces it: transient
ICA errors get the shared bounded backoff, everything else propagates on the
first occurrence. Relies on conftest's autouse `_instant_retry_sleeps`.
"""

import json
import logging
from pathlib import Path

import pytest
import requests
from loguru import logger as loguru_logger

from dragen_align_pa import popgen_mlr


def _http_error(status_code: int, reason: str) -> requests.exceptions.HTTPError:
    """Build a real `requests.exceptions.HTTPError` the way `raise_for_status` does."""
    response = requests.Response()
    response.status_code = status_code
    response.reason = reason
    response.url = 'https://ica.illumina.com/ica/rest/api/projects/x/analysis:cwl'
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        return exc
    raise AssertionError('raise_for_status did not raise')


def test_retry_runner_retries_transient_429_then_succeeds():
    """The real exception shape: `requests.exceptions.HTTPError` from `raise_for_status`."""
    calls = {'n': 0}

    def flaky(suffix: str) -> str:
        calls['n'] += 1
        if calls['n'] == 1:
            raise _http_error(429, 'Too Many Requests')
        return f'ok-{suffix}'

    assert popgen_mlr.TransientRetryRunner().run(flaky, 'x') == 'ok-x'
    assert calls['n'] == 2


def test_retry_runner_retries_transient_503_then_succeeds():
    calls = {'n': 0}

    def flaky() -> str:
        calls['n'] += 1
        if calls['n'] == 1:
            raise _http_error(503, 'Service Unavailable')
        return 'ok'

    assert popgen_mlr.TransientRetryRunner().run(flaky) == 'ok'
    assert calls['n'] == 2


def test_retry_runner_propagates_permanent_http_error_immediately():
    """A 404 HTTPError is not in the retryable status set, so it surfaces on the first attempt."""
    calls = {'n': 0}

    def missing() -> None:
        calls['n'] += 1
        raise _http_error(404, 'Not Found')

    with pytest.raises(requests.exceptions.HTTPError, match='404'):
        popgen_mlr.TransientRetryRunner().run(missing)
    assert calls['n'] == 1


def test_retry_runner_retries_on_message_marker_fallback():
    """Covers exceptions from other layers that only carry the ICA error text, not an HTTPError."""
    calls = {'n': 0}

    def flaky() -> str:
        calls['n'] += 1
        if calls['n'] == 1:
            raise Exception('ICA_API_429: Too many requests')
        return 'ok'

    assert popgen_mlr.TransientRetryRunner().run(flaky) == 'ok'
    assert calls['n'] == 2


def test_retry_runner_propagates_permanent_error_immediately():
    """The stale-MLR-config case: a 401 must surface on the first attempt."""
    calls = {'n': 0}

    def stale_config() -> None:
        calls['n'] += 1
        raise Exception('API request failed: 401 - {"code": "ICA_SEC_002"}')

    with pytest.raises(Exception, match='401'):
        popgen_mlr.TransientRetryRunner().run(stale_config)
    assert calls['n'] == 1


def test_stdlib_logging_is_routed_to_loguru():
    """popgen_cli reports the real submission failure only via logging.warning;
    the bridge must land those lines in the loguru stream the job logs use."""
    records: list[str] = []
    sink_id = loguru_logger.add(lambda message: records.append(str(message)), level='WARNING')
    try:
        popgen_mlr._intercept_popgen_logging()
        logging.getLogger('popgen_cli.utils.utils').warning(
            'submission failed, reason = "API request failed: 401"',
        )
    finally:
        loguru_logger.remove(sink_id)

    assert any('API request failed: 401' in r for r in records)


# --- submit_analysis (in-process replacement for the popgen-cli subprocess) ---

# The keys submit_analysis and the wheel's check_args/make_job actually read; shaped
# like a real `popgen-cli dragen-mlr config` output.
_PROJECT_CONFIG = {
    'ica_api_key': 'PROJECT-KEY',
    'ica_region': {'code': 'use1'},
    'ica_storage_bundle': {'bundleName': 'bundle'},
    'ica_storage_configuration': None,
    'ica_job_project': {'name': 'ourdna-mlr-jobs'},
    'ica_job_project_meta_folder': {'data': {'details': {'path': '/meta/'}}},
    'ica_job_project_jobs_folder': {'data': {'details': {'path': '/jobs/'}}},
    'ica_job_project_config_file': {
        'data': {'id': 'fil.config1', 'details': {'path': '/meta/project_config.json'}},
    },
    'ica_analysis_storage': {'name': 'Small'},
    'ica_analysis_pipeline': {'pipeline': {'code': 'mlr-pipeline'}},
    'ica_analysis_activation_code': {'pipelineBundle': {'name': 'mlr-bundle'}},
}


class _FakeIcaClient:
    def __init__(self, api_key: str, retry_runner=None):
        self.api_key = api_key
        self.retry_runner = retry_runner
        self.cache_config = None
        self.submit_kwargs = None
        self.submit_response = {'id': 'analysis-789'}

    def _set_cache_using_project_config(self, project_config_dict):
        self.cache_config = project_config_dict

    def submit_cwl_analysis(self, **kwargs):
        self.submit_kwargs = kwargs
        return self.submit_response


def _submit_argv(tmp_path: Path) -> list[str]:
    config = tmp_path / 'mlr_config.json'
    config.write_text(json.dumps(_PROJECT_CONFIG))
    return [
        '--input-project-config-file-path', str(config),
        '--output-analysis-json-folder-path', str(tmp_path / 'out'),
        '--run-id', 'SYN00001-mlr',
        '--sample-id', 'SYN00001',
        '--input-ht-folder-url', 'ica://ourdna-dragen-mlr/ref/hashtable/hg38_alt_masked_graph_v2/DRAGEN/9',
        '--output-folder-url', 'ica://ourdna-dragen-align/run-folder/SYN00001/',
        '--input-align-file-url', 'ica://ourdna-dragen-align/run-folder/SYN00001/SYN00001.cram',
        '--input-gvcf-file-url', 'ica://ourdna-dragen-align/run-folder/SYN00001/SYN00001.hard-filtered.gvcf.gz',
        '--analysis-instance-tier', 'economy',
    ]


@pytest.fixture
def fake_ica_clients(monkeypatch) -> list[_FakeIcaClient]:
    created: list[_FakeIcaClient] = []

    def _construct(api_key: str, retry_runner=None) -> _FakeIcaClient:
        client = _FakeIcaClient(api_key, retry_runner)
        created.append(client)
        return client

    monkeypatch.setattr('dragen_align_pa.popgen_mlr.popgen_utils.ICAClient', _construct)
    return created


def test_submit_analysis_returns_id_with_bounded_retries(tmp_path, fake_ica_clients):
    analysis_id = popgen_mlr.submit_analysis(_submit_argv(tmp_path))

    assert analysis_id == 'analysis-789'
    client = fake_ica_clients[0]
    assert client.api_key == 'PROJECT-KEY'
    assert isinstance(client.retry_runner, popgen_mlr.TransientRetryRunner)
    assert client.cache_config == _PROJECT_CONFIG
    kwargs = client.submit_kwargs
    # The outer do-or-die loop is neutered: one pass, no pre-raise sleep.
    assert kwargs['max_retry'] == 1
    assert kwargs['retry_sleep'] == 0
    assert kwargs['project_name'] == 'ourdna-mlr-jobs'
    assert kwargs['project_jobs_folder_path'] == '/jobs/'
    assert kwargs['pipeline_name'] == 'mlr-pipeline'
    assert kwargs['activation_code_name'] == 'mlr-bundle'
    assert kwargs['analysis_storage_name'] == 'Small'
    assert kwargs['analysis_name'] == 'sample-SYN00001-run-SYN00001-mlr'
    assert kwargs['mount_list'] == [{'dataId': 'fil.config1', 'mountPath': 'project_config.json'}]
    assert kwargs['input_json']['job_secret_file'] == {'class': 'File', 'location': 'project_config.json'}
    assert 'job_def_str' in kwargs['input_json']


def test_submit_analysis_raises_when_response_has_no_id(tmp_path, monkeypatch, fake_ica_clients):  # noqa: ARG001
    # Replace the method on the class (instances set submit_response in __init__,
    # so a class attribute would be shadowed and never seen).
    monkeypatch.setattr(_FakeIcaClient, 'submit_cwl_analysis', lambda self, **kwargs: {})

    with pytest.raises(ValueError, match='missing "id"'):
        popgen_mlr.submit_analysis(_submit_argv(tmp_path))


def test_submit_analysis_runs_wheel_validation(tmp_path, fake_ica_clients):
    """check_args stays authoritative: a hash-table URL without the required
    suffix must be rejected before any API interaction."""
    argv = _submit_argv(tmp_path)
    ht_index = argv.index('--input-ht-folder-url') + 1
    argv[ht_index] = 'ica://ourdna-dragen-mlr/ref/wrong-hashtable'

    with pytest.raises(Exception, match='hash table folder URL'):
        popgen_mlr.submit_analysis(argv)
    assert fake_ica_clients == []
