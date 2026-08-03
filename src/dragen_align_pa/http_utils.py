"""Shared HTTP transport for fetching ICA pre-signed download URLs.

Every direct-to-URL fetch in this package — streamed file bodies, MD5 lists,
passfail.json — goes through `download_session()` instead of the module-level
`requests.get`, so all of them reuse connections and inherit one transport-level
retry policy.
"""

import functools
from typing import Final

import requests
from cpg_utils.config import config_retrieve
from loguru import logger
from requests.adapters import HTTPAdapter
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
    wait_random_exponential,
)
from urllib3.util.retry import Retry

# (connect, read) rather than a single scalar: a scalar applies the same budget to both,
# so a stalled TCP/TLS connect to Illumina's S3 front end sits for the whole read budget
# (observed: 3.5 minutes) before failing. The connect leg should give up in seconds.
STREAM_TIMEOUT: Final[tuple[int, int]] = (30, 600)
"""Timeouts for streamed file bodies; the read budget is per-chunk, not whole-transfer."""

SMALL_FILE_TIMEOUT: Final[tuple[int, int]] = (30, 120)
"""Timeouts for KB-scale text fetches (MD5 lists, passfail.json)."""

_MAX_HTTP_RETRIES: Final = 5

# 0s, 4s, 8s, 16s, 32s. urllib3 1.x sleeps 0 before the first retry, which suits a one-off
# reset but not a throttling peer; jittered file-level backoff is the tenacity layer's job.
_BACKOFF_FACTOR: Final = 2.0

_RETRYABLE_HTTP_STATUSES: Final = frozenset({429, 500, 502, 503, 504})


# Cached, so a job that downloads 200 files pays one TCP+TLS handshake per host instead of
# 200: `requests.get` builds a throwaway Session per call and closes the connection with it.
# Handshake-time resets are the dominant failure at full per-SG fan-out, so fewer handshakes
# is itself the mitigation; the Retry then absorbs the ones that still land.
@functools.lru_cache(maxsize=1)
def download_session() -> requests.Session:
    """Return the process-wide session used for all pre-signed URL downloads.

    Returns:
        A `requests.Session` whose HTTPS adapter retries connect, read, and
        retryable-status failures on GET with exponential backoff. Read retries
        cover failures up to and including response headers; a reset part-way
        through a response body cannot be retried at this layer, because the
        consumed stream can't be rewound.
    """
    retry = Retry(
        total=_MAX_HTTP_RETRIES,
        connect=_MAX_HTTP_RETRIES,
        read=_MAX_HTTP_RETRIES,
        status=_MAX_HTTP_RETRIES,
        status_forcelist=_RETRYABLE_HTTP_STATUSES,
        allowed_methods=frozenset({'GET', 'HEAD'}),
        backoff_factor=_BACKOFF_FACTOR,
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retry))
    return session


_DEFAULT_MAX_TRANSFER_ATTEMPTS: Final = 4


def _log_transfer_retry(retry_state: RetryCallState) -> None:
    """tenacity `before_sleep` hook: log every scheduled transfer retry."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    sleep = retry_state.next_action.sleep if retry_state.next_action else 0.0
    logger.warning(
        f'Transfer failed ({exc}); retrying (attempt {retry_state.attempt_number}) after {sleep:.1f}s',
    )


# The adapter Retry above can only replay a request that failed before its response body
# started arriving. A reset part-way through a body — the common failure on a multi-GB CRAM
# over a long-haul link — surfaces from `iter_content` with the stream already partly
# consumed and unrewindable, so recovering means restarting the whole transfer from the
# caller. Backoff is fully jittered to desynchronise concurrent per-SG jobs retrying against
# the same throttled endpoint, with a hard floor so a reset is never answered instantly.
def transfer_retrying() -> Retrying:
    """Build the tenacity controller for restarting a whole file transfer.

    Returns:
        A `Retrying` that retries `requests.RequestException` with jittered
        exponential backoff, re-raising the last error once attempts are exhausted.
        Attempt count comes from `[ica.download] max_transfer_attempts`.
    """
    max_attempts = int(
        config_retrieve(['ica', 'download', 'max_transfer_attempts'], default=_DEFAULT_MAX_TRANSFER_ATTEMPTS),
    )
    return Retrying(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(max_attempts),
        wait=wait_random_exponential(multiplier=1, min=2, max=60) + wait_fixed(2),
        before_sleep=_log_transfer_retry,
        reraise=True,
    )
