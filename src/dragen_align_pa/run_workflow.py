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

    # cpg-flow drops a requested stage that appears in skip_stages WITHOUT expanding
    # its dependencies (`_resolve_implicit_stages` stops at it), so the opt-in delete
    # stage cannot be the sole requested stage: request its ancestors explicitly and
    # let skip_stages toggle only the delete itself.
    stages = (  # type: ignore[ReportUnknownVariableType]
        [BackfillGvcfsFromUpload, SomalierExtract, DeleteBackfillUpload] if BACKFILL_MODE else [DeleteDataInIca]
    )

    # Fail fast on the submitter, before any job is queued to the executor.
    validate_configuration()

    run_workflow(name='dragen_align_pa', stages=stages, dry_run=args.dry_run)  # type: ignore[ReportUnknownVariableType]


if __name__ == '__main__':
    cli_main()
