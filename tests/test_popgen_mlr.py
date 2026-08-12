"""Unit tests for the in-process popgen_cli MLR submission layer.

popgen_cli's own RetryRunner retries every exception (its submission POST up to
1,000,000 times), so a permanent error like a 401 from a stale MLR config spins
silently until the batch times out. TransientRetryRunner replaces it: transient
ICA errors get the shared bounded backoff, everything else propagates on the
first occurrence. Relies on conftest's autouse `_instant_retry_sleeps`.

The submit_analysis tests drive the REAL `popgen_cli.utils.utils.ICAClient` with
only `requests.request` stubbed, so the cache lookups, request construction, and
retry integration are all exercised for real; the wheel-parity test compares our
POST byte-for-byte against the wheel's own `submit_job` to catch mirror drift.
"""

import base64
import json
import logging
from pathlib import Path

import popgen_cli.utils.utils as popgen_utils
import pytest
import requests
from loguru import logger as loguru_logger
from popgen_cli.dragen_mlr import submit as popgen_submit

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


@pytest.mark.parametrize(('status', 'reason'), [(502, 'Bad Gateway'), (504, 'Gateway Timeout')])
def test_retry_runner_retries_gateway_errors_then_succeeds(status: int, reason: str):
    """502/504 come from gateways in front of ICA, not the API itself; the old
    subprocess path retried them indefinitely, so treating them as permanent
    would regress resilience."""
    calls = {'n': 0}

    def flaky() -> str:
        calls['n'] += 1
        if calls['n'] == 1:
            raise _http_error(status, reason)
        return 'ok'

    assert popgen_mlr.TransientRetryRunner().run(flaky) == 'ok'
    assert calls['n'] == 2


def test_retry_runner_retries_connection_error_then_succeeds():
    """A TCP reset mid-request must not fail the whole MLR stage on first occurrence."""
    calls = {'n': 0}

    def flaky() -> str:
        calls['n'] += 1
        if calls['n'] == 1:
            raise requests.exceptions.ConnectionError('Connection reset by peer')
        return 'ok'

    assert popgen_mlr.TransientRetryRunner().run(flaky) == 'ok'
    assert calls['n'] == 2


def test_retry_runner_retries_read_timeout_then_succeeds():
    calls = {'n': 0}

    def flaky() -> str:
        calls['n'] += 1
        if calls['n'] == 1:
            raise requests.exceptions.ReadTimeout('Read timed out')
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


def test_logging_bridge_installs_once_and_stays_installed():
    """The bridge mutates process-global logging state; re-invoking it (it used to
    run once per SG submission) must be a no-op, not a remove-and-replace of the
    root handlers each time."""
    popgen_mlr._intercept_popgen_logging()
    first_handler = next(
        h for h in logging.getLogger().handlers if isinstance(h, popgen_mlr._LoguruHandler)
    )
    popgen_mlr._intercept_popgen_logging()

    bridge_handlers = [h for h in logging.getLogger().handlers if isinstance(h, popgen_mlr._LoguruHandler)]
    assert bridge_handlers == [first_handler]  # same instance, not a fresh install


def test_logging_bridge_attributes_the_real_caller():
    """A fixed frame depth only fits one exact call chain; module-level
    `logging.warning(...)` (one frame deeper than `Logger.warning`) must still
    attribute to the caller, so the bridge has to walk out of the logging module."""
    functions: list[str] = []
    sink_id = loguru_logger.add(lambda message: functions.append(message.record['function']), level='WARNING')
    try:
        popgen_mlr._intercept_popgen_logging()
        logging.warning('who logged this?')
    finally:
        loguru_logger.remove(sink_id)

    assert functions == ['test_logging_bridge_attributes_the_real_caller']


# --- submit_analysis (in-process replacement for the popgen-cli subprocess) ---

# The keys submit_analysis (and the wheel's check_args/make_job/cache getters) actually
# read; shaped like a real `popgen-cli dragen-mlr config` output, ids included so the
# real ICAClient's cache lookups resolve without any HTTP GET.
_PROJECT_CONFIG = {
    'ica_api_key': 'PROJECT-KEY',
    'ica_region': {'code': 'use1'},
    'ica_storage_bundle': {'bundleName': 'bundle'},
    'ica_storage_configuration': None,
    'ica_job_project': {'id': 'proj-1', 'name': 'ourdna-mlr-jobs'},
    'ica_job_project_meta_folder': {'data': {'id': 'fol.meta1', 'details': {'path': '/meta/'}}},
    'ica_job_project_jobs_folder': {'data': {'id': 'fol.jobs1', 'details': {'path': '/jobs/'}}},
    'ica_job_project_config_file': {
        'data': {'id': 'fil.config1', 'details': {'path': '/meta/project_config.json'}},
    },
    'ica_analysis_storage': {'id': 'st-1', 'name': 'Small'},
    'ica_analysis_pipeline': {'pipeline': {'id': 'pipe-1', 'code': 'mlr-pipeline'}},
    'ica_analysis_activation_code': {'id': 'ac-1', 'pipelineBundle': {'name': 'mlr-bundle'}},
}

_TAGS = {
    'technicalTags': ['test_technical_tag', 'mlr'],
    'userTags': ['test_user_tags'],
    'referenceTags': ['test_reference_tags'],
}


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


def _response(status_code: int, body: str, reason: str = 'OK') -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.reason = reason
    response.url = 'https://ica.illumina.com/ica/rest/api/projects/proj-1/analysis:cwl'
    response._content = body.encode()
    return response


def _stub_http(monkeypatch, *responses: requests.Response) -> list[dict]:
    """Stub `requests.request` inside popgen_cli; responses are consumed in order
    (the last one repeats). Returns the list of captured calls."""
    calls: list[dict] = []
    remaining = list(responses)

    def _request(method: str, url: str, **kwargs) -> requests.Response:
        calls.append({'method': method, 'url': url, **kwargs})
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    monkeypatch.setattr(popgen_utils.requests, 'request', _request)
    return calls


def test_submit_analysis_posts_once_with_tags_and_returns_id(tmp_path, monkeypatch):
    calls = _stub_http(monkeypatch, _response(201, '{"id": "analysis-789"}'))

    analysis_id = popgen_mlr.submit_analysis(_submit_argv(tmp_path), tags=_TAGS)

    assert analysis_id == 'analysis-789'
    # Everything the POST needs is cache-served from the config JSON: exactly one HTTP call.
    assert len(calls) == 1
    call = calls[0]
    assert call['method'] == 'POST'
    assert call['url'].endswith('/projects/proj-1/analysis:cwl')
    assert call['headers']['Content-Type'] == 'application/vnd.illumina.v4+json'
    assert call['headers']['X-API-Key'] == 'PROJECT-KEY'
    body = json.loads(call['data'])
    assert body['tags'] == _TAGS
    assert body['userReference'] == 'sample-SYN00001-run-SYN00001-mlr'
    assert body['pipelineId'] == 'pipe-1'
    assert body['activationCodeDetailId'] == 'ac-1'
    assert body['analysisStorageId'] == 'st-1'
    assert body['outputParentFolderId'] == 'fol.jobs1'
    analysis_input = body['analysisInput']
    assert analysis_input['mounts'] == [{'dataId': 'fil.config1', 'mountPath': 'project_config.json'}]
    input_json = json.loads(analysis_input['inputJson'])
    assert input_json['job_secret_file'] == {'class': 'File', 'location': 'project_config.json'}
    job_def = json.loads(base64.b64decode(input_json['job_def_str']))
    assert job_def['project_data_list'] == []
    assert job_def['sample_id'] == 'SYN00001'


def test_submit_analysis_post_body_matches_wheel_except_tags(tmp_path, monkeypatch):
    """Drift guard for the deliberate mirror: the wheel's own submit_job and our
    submit_analysis must produce identical POSTs, tags aside."""
    argv = _submit_argv(tmp_path)

    ours_calls = _stub_http(monkeypatch, _response(201, '{"id": "analysis-789"}'))
    popgen_mlr.submit_analysis(list(argv), tags=_TAGS)
    ours = ours_calls[0]

    wheel_args = popgen_submit.parse_args(list(argv))
    popgen_submit.check_args(wheel_args)
    wheel_job = popgen_submit.make_job(wheel_args)
    wheel_calls = _stub_http(monkeypatch, _response(201, '{"id": "analysis-789"}'))
    popgen_submit.submit_job(
        project_config_dict=popgen_utils.read_json(wheel_args.input_project_config_file_path),
        job_config_dict=wheel_job,
        output_analysis_json_folder_path=wheel_args.output_analysis_json_folder_path,
    )
    wheel = wheel_calls[0]

    assert ours['method'] == wheel['method']
    assert ours['url'] == wheel['url']
    assert ours['headers'] == wheel['headers']
    ours_body = json.loads(ours['data'])
    wheel_body = json.loads(wheel['data'])
    assert ours_body.pop('tags') == _TAGS
    assert wheel_body.pop('tags') == {'technicalTags': [], 'userTags': [], 'referenceTags': []}
    assert ours_body == wheel_body


def test_submit_analysis_permanent_error_propagates_with_status_and_body(tmp_path, monkeypatch):
    """The stale-config case end to end: the raised error is the real HTTPError,
    not popgen's generic 'failed to submit analysis' Exception, and only one
    HTTP call is made."""
    calls = _stub_http(
        monkeypatch,
        _response(401, '{"code": "ICA_SEC_002", "message": "stale key"}', reason='Unauthorized'),
    )

    with pytest.raises(requests.exceptions.HTTPError, match='401'):
        popgen_mlr.submit_analysis(_submit_argv(tmp_path), tags=_TAGS)
    assert len(calls) == 1


def test_submit_analysis_retries_transient_429_then_submits(tmp_path, monkeypatch):
    calls = _stub_http(
        monkeypatch,
        _response(429, '{"code": "ICA_API_429"}', reason='Too Many Requests'),
        _response(201, '{"id": "analysis-789"}'),
    )

    analysis_id = popgen_mlr.submit_analysis(_submit_argv(tmp_path), tags=_TAGS)

    assert analysis_id == 'analysis-789'
    assert len(calls) == 2


def test_submit_analysis_raises_when_response_has_no_id(tmp_path, monkeypatch):
    _stub_http(monkeypatch, _response(201, '{}'))

    with pytest.raises(ValueError, match='missing "id"'):
        popgen_mlr.submit_analysis(_submit_argv(tmp_path), tags=_TAGS)


def test_submit_analysis_raises_on_non_json_response(tmp_path, monkeypatch):
    """popgen's `_request_base` returns `response.text` (a str) for a non-JSON 2xx
    body; that must become a clear error, not an AttributeError."""
    _stub_http(monkeypatch, _response(201, 'Created'))

    with pytest.raises(ValueError, match='non-JSON'):
        popgen_mlr.submit_analysis(_submit_argv(tmp_path), tags=_TAGS)


def test_submit_analysis_runs_wheel_validation(tmp_path, monkeypatch):
    """check_args stays authoritative: a hash-table URL without the required
    suffix must be rejected before any API interaction."""
    calls = _stub_http(monkeypatch, _response(201, '{"id": "analysis-789"}'))
    argv = _submit_argv(tmp_path)
    ht_index = argv.index('--input-ht-folder-url') + 1
    argv[ht_index] = 'ica://ourdna-dragen-mlr/ref/wrong-hashtable'

    with pytest.raises(Exception, match='hash table folder URL'):
        popgen_mlr.submit_analysis(argv, tags=_TAGS)
    assert calls == []


def test_submit_analysis_rejects_dry_run(tmp_path, monkeypatch):
    """The wheel's parser accepts --dry-run but this mirror would submit for real;
    fail loudly rather than silently ignore the flag."""
    calls = _stub_http(monkeypatch, _response(201, '{"id": "analysis-789"}'))
    argv = [*_submit_argv(tmp_path), '--dry-run', 'true']

    with pytest.raises(ValueError, match='dry-run'):
        popgen_mlr.submit_analysis(argv, tags=_TAGS)
    assert calls == []
