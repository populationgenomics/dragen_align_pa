"""Per-cycle ICA pipeline-status fan-out for manage_ica_pipeline_loop.

The provider holds a bounded ThreadPoolExecutor for its lifetime (one
cohort run = hundreds of refresh cycles; recreating the executor per
cycle wastes thread-spawn cost). On each refresh, it dispatches one
`check_ica_pipeline_status` call per in-flight id. Successes populate
an internal map; failures (tenacity exhaustion or cycle timeout) leave
the id absent → reads as 'UNKNOWN' → existing elif chain in
manage_ica_pipeline_loop falls through (no transition this cycle).

NOT thread-safe: caller must serialise refresh()/get_status() (the
polling loop is serial by design).
"""

from __future__ import annotations

import concurrent.futures as cf
import time
from typing import Protocol

from icasdk.apis.tags import project_analysis_api
from loguru import logger

from dragen_align_pa import ica_api_utils


class StatusProvider(Protocol):
    """The contract that manage_ica_pipeline_loop depends on."""

    def refresh(self, in_flight_ids: set[str]) -> None: ...

    def get_status(self, pipeline_id: str) -> str: ...


class ParallelPerIdStatusProvider:
    """Bounded-parallel per-id fan-out.

    Lifetime is owned by the caller via `with` (the executor's threads
    must shut down deterministically when the cohort run exits).
    """

    def __init__(
        self,
        concurrency: int,
        refresh_timeout_seconds: int,
        project_id: str | None = None,
    ) -> None:
        self._concurrency = concurrency
        self._refresh_timeout_s = refresh_timeout_seconds
        self._project_id = project_id  # set by caller for MLR (different project)
        self._status_map: dict[str, str] = {}
        self._executor = cf.ThreadPoolExecutor(
            max_workers=concurrency,
            thread_name_prefix='ica-status',
        )

    def __enter__(self) -> ParallelPerIdStatusProvider:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:  # noqa: ANN001
        self.close()

    def close(self) -> None:
        """Shut down the executor, cancelling any pending futures."""
        self._executor.shutdown(wait=True, cancel_futures=True)

    def refresh(self, in_flight_ids: set[str]) -> None:
        """Populate the status map for in_flight_ids via parallel fetches.

        Failures are absorbed and do not propagate. Affected ids stay
        absent from the map → get_status returns 'UNKNOWN'.
        """
        raise NotImplementedError('Implemented in next task.')

    def get_status(self, pipeline_id: str) -> str:
        """Return the most recently observed status, or 'UNKNOWN' if absent."""
        return self._status_map.get(pipeline_id, 'UNKNOWN')
