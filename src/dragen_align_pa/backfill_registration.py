"""Register a backfilled SG's base and recal gVCFs in metamist, in that order.

`sequencing_group.gvcf` resolves to the *latest* gvcf analysis by timestamp, so
the recal gVCF must be registered after the base gVCF. cpg-flow's decorator
mechanism (`analysis_type=` on `@stage`) cannot guarantee that: its registration
job depends on the stage's payload jobs but is never appended to the stage's job
list, so a downstream stage's registration can overtake an upstream one. This
module runs both registrations sequentially in one process instead, and is
invoked as a CLI (`python3 -m dragen_align_pa.backfill_registration`) from the
BackfillGvcfsFromUpload stage's BashJob.
"""

import json
from argparse import ArgumentParser
from typing import Any

from cpg_flow.status import complete_analysis_job


def run(
    base_gvcf: str,
    recal_gvcf: str,
    sg_id: str,
    project_name: str,
    meta: dict[str, Any],
) -> dict[str, Any]:
    """Register the base gVCF, then the recal gVCF, and return the marker payload."""
    for output in (base_gvcf, recal_gvcf):
        complete_analysis_job(
            output,
            'gvcf',
            [],
            [sg_id],
            project_name,
            meta,
        )
    return {'sg_id': sg_id, 'registered': [base_gvcf, recal_gvcf]}


def main() -> None:
    # Prints the marker payload to stdout; the calling BashJob redirects it to a
    # job output file that Hail Batch writes to the stage's registration marker.
    parser = ArgumentParser()
    parser.add_argument('--base-gvcf', required=True)
    parser.add_argument('--recal-gvcf', required=True)
    parser.add_argument('--sg-id', required=True)
    parser.add_argument('--project-name', required=True)
    parser.add_argument('--meta-json', required=True)
    args = parser.parse_args()

    marker = run(
        base_gvcf=args.base_gvcf,
        recal_gvcf=args.recal_gvcf,
        sg_id=args.sg_id,
        project_name=args.project_name,
        meta=json.loads(args.meta_json),
    )
    print(json.dumps(marker))


if __name__ == '__main__':
    main()
