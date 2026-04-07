#!/usr/bin/env python3


from argparse import ArgumentParser

from cpg_flow.workflow import run_workflow  # type: ignore[ReportUnknownVariableType]

from dragen_align_pa.stages import DeleteDataInIca, DownloadDataFromIca  # type: ignore[ReportUnknownVariableType]


def cli_main():
    # CLI entrypoint
    parser = ArgumentParser()
    parser.add_argument('--dry_run', action='store_true', help='Dry run')
    args = parser.parse_args()

    stages = [DeleteDataInIca, DownloadDataFromIca]  # type: ignore[ReportUnknownVariableType]

    run_workflow(name='dragen_align_pa', stages=stages, dry_run=args.dry_run)  # type: ignore[ReportUnknownVariableType]


if __name__ == '__main__':
    cli_main()
