#!/usr/bin/env python3


from argparse import ArgumentParser

from cpg_flow.workflow import run_workflow  # type: ignore[ReportUnknownVariableType]

from dragen_align_pa.stages import DeleteDataInIca  # type: ignore[ReportUnknownVariableType]


def cli_main():
    # CLI entrypoint
    parser = ArgumentParser()
    parser.add_argument('--dry_run', action='store_true', help='Dry run')
    args = parser.parse_args()

    # Note - in production-pipelines the main.py script sets up layers of default configuration,
    # overlaid with workflow-specific configuration, and then runs the workflow.
    # If you want to re-use that model, this should be carried out before entering the workflow

    # Otherwise all configuration should be done by providing all relevant configs to analysis-runner
    # https://github.com/populationgenomics/team-docs/blob/main/cpg_utils_config.md#config-in-analysis-runner-jobs

    stages = [DeleteDataInIca]  # type: ignore[ReportUnknownVariableType]

    run_workflow(name='dragen_align_pa', stages=stages, dry_run=args.dry_run)  # type: ignore[ReportUnknownVariableType]


if __name__ == '__main__':
    cli_main()
