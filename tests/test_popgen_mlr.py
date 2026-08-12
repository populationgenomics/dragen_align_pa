"""Unit tests for the in-process popgen_cli MLR submission layer.

popgen_cli's own RetryRunner retries every exception (its submission POST up to
1,000,000 times), so a permanent error like a 401 from a stale MLR config spins
silently until the batch times out. TransientRetryRunner replaces it: transient
ICA errors get the shared bounded backoff, everything else propagates on the
first occurrence. Relies on conftest's autouse `_instant_retry_sleeps`.
"""

import logging

import pytest
from loguru import logger as loguru_logger

from dragen_align_pa import popgen_mlr


def test_retry_runner_retries_transient_429_then_succeeds():
    calls = {'n': 0}

    def flaky(suffix: str) -> str:
        calls['n'] += 1
        if calls['n'] == 1:
            # Bare Exception mimics popgen_cli's ICAClient._request_base.
            raise Exception(
                'API request failed: 429 - {"code": "ICA_API_429", "message": "Too many requests"}',
            )
        return f'ok-{suffix}'

    assert popgen_mlr.TransientRetryRunner().run(flaky, 'x') == 'ok-x'
    assert calls['n'] == 2


def test_retry_runner_retries_on_status_prefix_without_body_marker():
    """A 503 whose body is an opaque HTML page still matches via the message prefix."""
    calls = {'n': 0}

    def flaky() -> str:
        calls['n'] += 1
        if calls['n'] == 1:
            raise Exception('API request failed: 503 - <html>Service Unavailable</html>')
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
