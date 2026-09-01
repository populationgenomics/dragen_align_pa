#!/usr/bin/env python3


from argparse import ArgumentParser

from cpg_flow.stage import StageDecorator
from cpg_flow.workflow import run_workflow  # type: ignore[ReportUnknownVariableType]

from dragen_align_pa.stages import (  # type: ignore[ReportUnknownVariableType]
    BACKFILL_MODE,
    BackfillGvcfsFromUpload,
    DeleteBackfillUpload,
    DeleteDataInIca,
    SomalierExtract,
)
from dragen_align_pa.validator import validate_configuration


def terminal_stages(backfill_mode: bool) -> list[StageDecorator]:
    """The requested sink stages for each mode.

    The backfill graph is fixed: all three sinks are requested, and the delete
    opt-in lives inside DeleteBackfillUpload.queue_jobs (the delete_upload flag),
    never in stage selection — a requested stage in skip_stages aborts cpg-flow's
    graph build when its expected outputs are missing, and the validator rejects
    any backfill-mode stage selection for the same reason.
    """
    if backfill_mode:
        return [BackfillGvcfsFromUpload, SomalierExtract, DeleteBackfillUpload]
    return [DeleteDataInIca]


def cli_main():
    # CLI entrypoint
    parser = ArgumentParser()
    parser.add_argument('--dry_run', action='store_true', help='Dry run')
    args = parser.parse_args()

    stages = terminal_stages(BACKFILL_MODE)  # type: ignore[ReportUnknownVariableType]

    # Fail fast on the submitter, before any job is queued to the executor.
    validate_configuration()

    run_workflow(name='dragen_align_pa', stages=stages, dry_run=args.dry_run)  # type: ignore[ReportUnknownVariableType]


if __name__ == '__main__':
    cli_main()
