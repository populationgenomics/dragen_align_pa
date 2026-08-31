#!/usr/bin/env python3


from argparse import ArgumentParser

from cpg_flow.workflow import run_workflow  # type: ignore[ReportUnknownVariableType]

from dragen_align_pa.stages import (  # type: ignore[ReportUnknownVariableType]
    BACKFILL_MODE,
    BackfillGvcfsFromUpload,
    DeleteBackfillUpload,
    DeleteDataInIca,
    SomalierExtract,
)
from dragen_align_pa.validator import validate_configuration


def cli_main():
    # CLI entrypoint
    parser = ArgumentParser()
    parser.add_argument('--dry_run', action='store_true', help='Dry run')
    args = parser.parse_args()

    # The backfill graph is fixed: all three sinks are requested, and the delete
    # opt-in lives inside DeleteBackfillUpload.queue_jobs (the delete_upload flag),
    # never in stage selection — a requested stage in skip_stages aborts cpg-flow's
    # graph build when its expected outputs are missing, and the validator rejects
    # any backfill-mode stage selection for the same reason.
    stages = (  # type: ignore[ReportUnknownVariableType]
        [BackfillGvcfsFromUpload, SomalierExtract, DeleteBackfillUpload] if BACKFILL_MODE else [DeleteDataInIca]
    )

    # Fail fast on the submitter, before any job is queued to the executor.
    validate_configuration()

    run_workflow(name='dragen_align_pa', stages=stages, dry_run=args.dry_run)  # type: ignore[ReportUnknownVariableType]


if __name__ == '__main__':
    cli_main()
