"""Register a backfilled SG's cram, base gVCF and recal gVCF in metamist, in that order.

`sequencing_group.gvcf` resolves to the *latest* gvcf analysis by timestamp, so the
recal gVCF must be registered after the base gVCF. cpg-flow's decorator mechanism
(`analysis_type=` on `@stage`) cannot guarantee that, nor survive a copy-succeeded /
registration-failed re-run: its registration job depends on the stage's payload jobs
but is never appended to the stage's job list, so downstream dependency edges don't
cover it and no expected output gates it. This module runs all three registrations
sequentially in one process, gated by a marker file, and is invoked as a CLI
(`python3 -m dragen_align_pa.backfill_registration`) from the
BackfillGvcfsFromUpload stage's BashJob. The cram has no ordering constraint of its
own; it lives here so the single marker covers every backfill registration.
"""

import json
from argparse import ArgumentParser
from typing import Any

from cpg_flow.status import complete_analysis_job


def run(
    cram: str,
    base_gvcf: str,
    recal_gvcf: str,
    sg_id: str,
    project_name: str,
    meta: dict[str, Any],
) -> dict[str, Any]:
    """Register the cram, base gVCF, then recal gVCF, and return the marker payload."""
    for output, analysis_type in ((cram, 'cram'), (base_gvcf, 'gvcf'), (recal_gvcf, 'gvcf')):
        complete_analysis_job(
            output,
            analysis_type,
            [],
            [sg_id],
            project_name,
            meta,
        )
    return {'sg_id': sg_id, 'registered': [cram, base_gvcf, recal_gvcf]}


def main() -> None:
    # The marker is written to an explicit file rather than captured from stdout:
    # complete_analysis_job logs `Created Analysis(...)` lines to stdout, which
    # would corrupt a redirected JSON payload.
    parser = ArgumentParser()
    parser.add_argument('--cram', required=True)
    parser.add_argument('--base-gvcf', required=True)
    parser.add_argument('--recal-gvcf', required=True)
    parser.add_argument('--sg-id', required=True)
    parser.add_argument('--project-name', required=True)
    parser.add_argument('--meta-json', required=True)
    parser.add_argument('--marker-file', required=True)
    args = parser.parse_args()

    marker = run(
        cram=args.cram,
        base_gvcf=args.base_gvcf,
        recal_gvcf=args.recal_gvcf,
        sg_id=args.sg_id,
        project_name=args.project_name,
        meta=json.loads(args.meta_json),
    )
    with open(args.marker_file, 'w') as fh:
        json.dump(marker, fh)


if __name__ == '__main__':
    main()
