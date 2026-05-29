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
from typing import Protocol, Self

from icasdk.apis.tags import project_analysis_api
from loguru import logger

from dragen_align_pa import ica_api_utils

# ratio above which partial-fetch failure is escalated from WARNING to ERROR
_ERROR_THRESHOLD = 0.5


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
        refresh_timeout_seconds: float,
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

    def __enter__(self) -> Self:
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
        self._status_map.clear()
        if not in_flight_ids:
            return

        secrets = ica_api_utils.get_ica_secrets()
        project_id = self._project_id or secrets['projectID']
        t0 = time.monotonic()

        with ica_api_utils.get_ica_api_client() as api_client:
            api_instance = project_analysis_api.ProjectAnalysisApi(api_client)

            def _fetch_one(pid: str) -> tuple[str, str]:
                status = ica_api_utils.check_ica_pipeline_status(
                    api_instance=api_instance,
                    path_params={'projectId': project_id, 'analysisId': pid},
                )
                return pid, status

            futures = {
                self._executor.submit(_fetch_one, pid): pid
                for pid in in_flight_ids
            }
            done, not_done = cf.wait(futures, timeout=self._refresh_timeout_s)
            n_ok = 0
            n_failed = 0
            for fut in done:
                pid = futures[fut]
                try:
                    _id, status = fut.result()
                    self._status_map[pid] = status
                    n_ok += 1
                except Exception as exc:  # noqa: BLE001
                    logger.debug(f'status fetch failed for {pid}: {exc}')
                    n_failed += 1
            for fut in not_done:
                fut.cancel()
                n_failed += 1

        elapsed = time.monotonic() - t0
        if n_failed > 0:
            ratio = n_failed / max(len(in_flight_ids), 1)
            log = logger.error if ratio > _ERROR_THRESHOLD else logger.warning
            log(
                f'ica status refresh: n_ok={n_ok} n_failed={n_failed} '
                f'elapsed={elapsed:.1f}s',
            )

    def get_status(self, pipeline_id: str) -> str:
        """Return the most recently observed status, or 'UNKNOWN' if absent."""
        return self._status_map.get(pipeline_id, 'UNKNOWN')
