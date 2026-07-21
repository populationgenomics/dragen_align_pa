"""Config-free DRAGEN batch / pipeline-management constants.

These are pure literals (batch-file schema version, status vocabulary, passfail
vocabulary, chunking width, ICA status vocabulary). They live here — not in
`constants.py` — because `constants.py` reads the analysis config at import time
(`config_retrieve` / `output_path`), so importing it requires a loaded config.
Keeping this vocabulary in a side-effect-free module lets `batches.py` (the
`{cohort}_batches.json` reader/writer) and the monitor loop import it without a
config, which keeps those modules importable in isolation.
"""

from typing import Final

# Schema version of the per-cohort `{cohort}_batches.json` state file.
BATCHES_SCHEMA_VERSION: Final = 1

# DRAGEN/ICA accepts exactly these `error_strategy` pipeline-parameter values.
ALLOWED_ERROR_STRATEGIES: Final = frozenset({'auto', 'continue', 'terminate'})

# Per-batch status values persisted in `batches.json`. The orchestrator's in-memory
# `PipelineStatus` enum is finer-grained (FAILED_RETRYING / FAILED_FINAL); the
# persistence layer collapses both to `BATCH_STATUS_FAILED`. A typo in a literal is
# a silent lookup miss; a typo in one of these names is a NameError.
BATCH_STATUS_PENDING: Final = 'PENDING'
BATCH_STATUS_INPROGRESS: Final = 'INPROGRESS'
BATCH_STATUS_SUCCEEDED: Final = 'SUCCEEDED'
BATCH_STATUS_FAILED: Final = 'FAILED'
BATCH_STATUS_CANCELLED: Final = 'CANCELLED'
ALLOWED_BATCH_STATUSES: Final = frozenset(
    {
        BATCH_STATUS_PENDING,
        BATCH_STATUS_INPROGRESS,
        BATCH_STATUS_SUCCEEDED,
        BATCH_STATUS_FAILED,
        BATCH_STATUS_CANCELLED,
    },
)
# Statuses meaning "still being worked (or waiting to be)"; the resume paths use
# this to decide which batches to re-monitor.
ACTIVE_BATCH_STATUSES: Final = frozenset({BATCH_STATUS_PENDING, BATCH_STATUS_INPROGRESS})

# Per-sample passfail vocabulary. DRAGEN's `passfail.json` writes `"Success"` /
# `"Failed"`; `batches.record_passfail` normalises at the persistence boundary so
# downstream code only ever compares against the two canonical values below.
# `"Fail"` (not `"Failed"`) is canonical because every existing consumer already
# compared `== 'Fail'`.
CANONICAL_PASSFAIL_SUCCESS: Final = 'Success'
CANONICAL_PASSFAIL_FAIL: Final = 'Fail'
PASSFAIL_STATUS_NORMALISATION: Final = {
    'Success': CANONICAL_PASSFAIL_SUCCESS,
    'Fail': CANONICAL_PASSFAIL_FAIL,
    'Failed': CANONICAL_PASSFAIL_FAIL,
}

# Default batch chunking width (sequencing groups per ICA analysis).
DEFAULT_BATCH_SIZE: Final = 5

# Cap on consecutive `on_succeeded` callback failures before the shared monitor
# loop escalates a target to FAILED_FINAL rather than spinning forever.
MAX_CONSECUTIVE_ON_SUCCEEDED_FAILURES: Final = 5

# Raw ICA analysis status vocabulary (from `check_ica_pipeline_status`) — a
# DISTINCT vocabulary from the persisted `BATCH_STATUS_*` values (they coincide
# only in spelling for SUCCEEDED). `force_retry` reconciliation maps a terminal
# ICA failure status to `BATCH_STATUS_FAILED`. ABORTED is treated as needing a
# rerun (force_retry is an explicit attempt to obtain results).
ICA_STATUS_SUCCEEDED: Final = 'SUCCEEDED'
ICA_TERMINAL_FAILURE_STATUSES: Final = frozenset({'FAILED', 'FAILEDFINAL', 'ABORTED'})

# HTTP status code for a missing ICA resource (analysis expired/deleted).
HTTP_NOT_FOUND: Final = 404
