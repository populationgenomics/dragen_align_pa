"""Shared HTTP transport for fetching ICA pre-signed download URLs.

Every direct-to-URL fetch in this package — streamed file bodies, MD5 lists,
passfail.json — goes through `download_session()` instead of the module-level
`requests.get`, so all of them reuse connections and inherit one transport-level
retry policy.
"""

import functools
import ssl
from typing import Final

import requests
import urllib3.exceptions
from cpg_utils.config import config_retrieve
from loguru import logger
from requests.adapters import HTTPAdapter
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    stop_after_delay,
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

# Deliberately lower than the other budgets. For a streamed GET a "read" error is a failure
# waiting for response *headers*, and each one can burn the whole STREAM_TIMEOUT read budget
# (600s) before it is even counted — 5 of those is the single largest term in the worst-case
# latency of the whole download path.
_MAX_READ_RETRIES: Final = 1

# 0s, 4s, 8s, 16s, 32s. urllib3 sleeps 0 before the first retry, which suits a one-off reset
# but not a throttling peer; the tenacity layers add a hard floor on top.
_BACKOFF_FACTOR: Final = 2.0

# Added as `random() * jitter` seconds on top of each backoff. Without it ~500 concurrent per-SG
# jobs that all take a 429 from the same endpoint retry in lockstep and re-create the spike they
# are backing off from — the same reason both tenacity layers jitter.
_BACKOFF_JITTER: Final = 4.0

_RETRYABLE_HTTP_STATUSES: Final = frozenset({429, 500, 502, 503, 504})

# Whether a mid-body failure reaches us as a `requests` exception depends on the installed
# `requests`: older versions let urllib3's own `SSLError` escape `iter_content` unwrapped, so a
# `[SSL: DECRYPTION_FAILED_OR_BAD_RECORD_MAC]` part-way through a transfer bypassed every
# `except requests.RequestException` in this package and killed the job. Catching all three
# families makes the behaviour independent of which version resolves. `ssl.SSLError` covers a
# raw error escaping without even urllib3 wrapping it.
TRANSPORT_ERRORS: Final = (requests.RequestException, urllib3.exceptions.HTTPError, ssl.SSLError)
"""Exceptions meaning the transfer failed at the network layer, whatever wrapped it."""

# urllib3 caps Retry-After at 21600s of its own accord, so an upstream answering
# `Retry-After: 86400` still parks a worker for six hours, once per retry.
_MAX_RETRY_AFTER_SECONDS: Final = 120


# `backoff_jitter` is a constructor argument (urllib3 2.0+) and is used as one below. Its
# sibling `retry_after_max` would delete this class too, but it only landed in 2.7.0, and a
# floor that tight to save six lines is a worse trade than the override. Collapse this once
# 2.7 is comfortably the norm.
class _CappedRetryAfter(Retry):
    """`Retry` that clamps a server-supplied `Retry-After` to `_MAX_RETRY_AFTER_SECONDS`."""

    def get_retry_after(self, response: object) -> float | None:  # pyright: ignore[reportIncompatibleMethodOverride]
        retry_after = super().get_retry_after(response)  # pyright: ignore[reportArgumentType]
        if retry_after is None:
            return None
        return min(retry_after, _MAX_RETRY_AFTER_SECONDS)


# Cached, so a job that downloads 200 files pays one TCP+TLS handshake per host instead of
# 200: `requests.get` builds a throwaway Session per call and closes the connection with it.
# Handshake-time resets are the dominant failure at full per-SG fan-out, so fewer handshakes
# is itself the mitigation; the Retry then absorbs the ones that still land.
@functools.lru_cache(maxsize=1)
def download_session() -> requests.Session:
    """Return the process-wide session used for all pre-signed URL downloads.

    Read retries cover failures up to and including response headers; a reset part-way
    through a response body cannot be retried at this layer, because the consumed stream
    can't be rewound. `transfer_retrying` covers that case.

    Returns:
        A `requests.Session` whose HTTPS adapter retries connect, read, and
        retryable-status failures on GET with exponential backoff.
    """
    retry = _CappedRetryAfter(
        total=_MAX_HTTP_RETRIES,
        connect=_MAX_HTTP_RETRIES,
        read=_MAX_READ_RETRIES,
        status=_MAX_HTTP_RETRIES,
        status_forcelist=_RETRYABLE_HTTP_STATUSES,
        allowed_methods=frozenset({'GET', 'HEAD'}),
        backoff_factor=_BACKOFF_FACTOR,
        backoff_jitter=_BACKOFF_JITTER,
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retry))
    return session


_DEFAULT_MAX_TRANSFER_ATTEMPTS: Final = 4

# Bounds the retry *window*, not a single attempt: tenacity checks `stop` between attempts, so
# a legitimately slow multi-GB transfer already in flight is never cut off. That is also its
# limit — a peer dribbling one chunk just inside the read timeout never reaches a `stop` check,
# which is what `utils.download_job_timeout_seconds` backstops.
_DEFAULT_MAX_TRANSFER_SECONDS: Final = 7200


def _log_transfer_retry(description: str, max_attempts: int, retry_state: RetryCallState) -> None:
    """tenacity `before_sleep` hook: log every scheduled transfer retry."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    sleep = retry_state.next_action.sleep if retry_state.next_action else 0.0
    logger.warning(
        f'{description}: transfer attempt {retry_state.attempt_number} of {max_attempts} failed '
        f'({exc}); retrying in {sleep:.1f}s',
    )


# The adapter Retry above can only replay a request that failed before its response body
# started arriving. A reset part-way through a body — the common failure on a multi-GB CRAM
# over a long-haul link — surfaces from `iter_content` with the stream already partly
# consumed and unrewindable, so recovering means restarting the whole transfer from the
# caller. Backoff is fully jittered to desynchronise concurrent per-SG jobs retrying against
# the same throttled endpoint, with a hard floor so a reset is never answered instantly.
# describes the *encoded* size — would not match the bytes we count. Only an identity-encoded
# response carries a length we can compare against.
def _declared_content_length(response: requests.Response) -> int | None:
    """Return the body length the server declared, if it is comparable to bytes received.

    Args:
        response: A streamed response whose headers have arrived.

    Returns:
        The `Content-Length` value, or None when it is absent (e.g. chunked transfer
        encoding) or not comparable.
    """
    encoding: str = response.headers.get('Content-Encoding', 'identity').strip().lower()
    if encoding not in ('', 'identity'):
        return None
    declared: str | None = response.headers.get('Content-Length')
    if declared is None:
        return None
    try:
        return int(declared)
    except ValueError:
        logger.warning(f'Ignoring malformed Content-Length header {declared!r}')
        return None


_HTTP_FORBIDDEN: Final = 403


# A status error is not a dropped body, and restarting for one re-mints a rate-limited ICA URL
# only to ask the same throttled host again. The two ways a status reaches us differ:
#   - a status in `_RETRYABLE_HTTP_STATUSES` is retried by the adapter and, once that budget is
#     spent, surfaces as `RetryError`. It inherits from `RequestException`, NOT `HTTPError`, so
#     it needs its own branch — without one a persistent 429 costs 20 requests and 4 mints per
#     file, which is the amplification this predicate exists to prevent.
#   - any other status passes straight through to `raise_for_status()` as `HTTPError`. 403 is
#     the one worth restarting for: it is how an expired pre-signed URL reads, and a fresh mint
#     genuinely fixes it.
# `ChunkedEncodingError` (the truncation signal) is neither, so it still restarts.
def _should_restart_transfer(exc: BaseException) -> bool:
    """tenacity predicate: True for a failure a whole-transfer restart can actually fix."""
    if isinstance(exc, requests.exceptions.RetryError):
        return False
    if isinstance(exc, requests.HTTPError):
        return exc.response is not None and exc.response.status_code == _HTTP_FORBIDDEN
    return isinstance(exc, TRANSPORT_ERRORS)


def transfer_retrying(description: str) -> Retrying:
    """Build the tenacity controller for restarting a whole file transfer.

    Args:
        description: What is being transferred, for the retry log line.

    Returns:
        A `Retrying` that retries `_should_restart_transfer` failures with jittered
        exponential backoff, re-raising the last error once attempts are exhausted.
        Attempts come from `[ica.download] max_transfer_attempts` and the retry window
        from `[ica.download] max_transfer_seconds`, whichever is reached first.
    """
    max_attempts = int(
        config_retrieve(['ica', 'download', 'max_transfer_attempts'], default=_DEFAULT_MAX_TRANSFER_ATTEMPTS),
    )
    max_seconds = float(
        config_retrieve(['ica', 'download', 'max_transfer_seconds'], default=_DEFAULT_MAX_TRANSFER_SECONDS),
    )
    return Retrying(
        retry=retry_if_exception(_should_restart_transfer),
        stop=stop_after_attempt(max_attempts) | stop_after_delay(max_seconds),
        wait=wait_random_exponential(multiplier=1, min=2, max=60) + wait_fixed(2),
        before_sleep=functools.partial(_log_transfer_retry, description, max_attempts),
        reraise=True,
    )
