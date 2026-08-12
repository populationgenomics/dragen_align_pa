"""In-process submission of DRAGEN MLR analyses through the vendored `popgen_cli` package.

Running `popgen-cli dragen-mlr submit` as a subprocess hides failures: its
submission POST sits in a retry loop that also retries permanent errors (400/401)
practically forever, the real reason only ever appears on its stdlib logging
stream, and `capture_output=True` buffers that stream until the process exits —
which it never does. Submitting in-process instead lets us inject a
transient-only retry policy into popgen_cli's `ICAClient` and route its logging
into loguru, so a permanent error (e.g. a stale MLR config JSON) fails the job
loudly and immediately.
"""

import logging
from collections.abc import Callable
from typing import Any

from loguru import logger

from dragen_align_pa import ica_api_utils


def _is_transient_popgen_error(exc: BaseException) -> bool:
    """Tenacity predicate: True when the exception message shows a transient ICA error.

    popgen_cli's `ICAClient._request_base` raises bare `Exception`s shaped
    `API request failed: {status} - {body}`. The status prefix is matched as well
    as the shared body markers, so a 429/503 whose body lacks an ICA error code
    still retries.
    """
    message = str(exc)
    if any(marker in message for marker in ica_api_utils.TRANSIENT_ICA_ERROR_MARKERS):
        return True
    return message.startswith(('API request failed: 429', 'API request failed: 503'))


class TransientRetryRunner:
    """Substitute for popgen_cli's `RetryRunner`, backed by the shared ICA backoff.

    popgen_cli's own runner retries every exception; this one retries only
    transient ICA errors (429/503) and propagates anything else on the first
    occurrence, so a permanent error surfaces with its full response text instead
    of spinning silently.
    """

    def run(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return ica_api_utils.ica_retrying(_is_transient_popgen_error)(func, *args, **kwargs)


class _LoguruHandler(logging.Handler):
    """Forward stdlib logging records into loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        logger.opt(depth=6, exception=record.exc_info).log(level, record.getMessage())


def _intercept_popgen_logging() -> None:
    """Route stdlib logging into loguru.

    popgen_cli reports request failures and submission retries only via the root
    stdlib logger; without this bridge those lines bypass the loguru sinks the
    pipeline's error log monitoring reads.
    """
    logging.basicConfig(handlers=[_LoguruHandler()], level=logging.INFO, force=True)
