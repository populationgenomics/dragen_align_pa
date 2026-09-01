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

Registration is idempotent across every replay path (Hail Batch job retry, a
mid-trio failure, a stage re-queue with the marker present): the CLI exits early
when the GCS marker already exists, and otherwise skips any output that already has
a completed analysis of the same type in metamist. Replays preserve the ordering
guarantee — a skipped output was registered before the outputs that follow it.
"""

import json
from argparse import ArgumentParser
from typing import Any

import cpg_utils
from cpg_flow.status import complete_analysis_job
from loguru import logger
from metamist.graphql import gql, query


def _existing_completed_outputs(sg_id: str) -> set[tuple[str, str]]:
    """(type, output) pairs of this SG's completed analyses in metamist."""
    analyses_query = gql(
        request_string="""
        query BackfillRegisteredAnalyses($sgId: String!) {
          sequencingGroups(id: {eq: $sgId}) {
            analyses {
              type
              status
              output
            }
          }
        }
    """
    )
    result = query(analyses_query, variables={'sgId': sg_id})
    sequencing_groups = result.get('sequencingGroups', [])
    if not sequencing_groups:
        raise ValueError(f'No sequencing group found in metamist with ID {sg_id}')
    return {
        (analysis['type'], analysis['output'])
        for analysis in sequencing_groups[0].get('analyses', [])
        if str(analysis.get('status', '')).upper() == 'COMPLETED' and analysis.get('output')
    }


def _read_existing_marker(marker_gcs_path: str) -> str | None:
    marker = cpg_utils.to_path(marker_gcs_path)
    if not marker.exists():
        return None
    with marker.open() as fh:
        return fh.read()


def run(
    cram: str,
    base_gvcf: str,
    recal_gvcf: str,
    sg_id: str,
    project_name: str,
    meta: dict[str, Any],
) -> dict[str, Any]:
    """Register the cram, base gVCF, then recal gVCF, and return the marker payload.

    Outputs that already have a completed analysis of the same type are skipped, so
    a replayed run never duplicates metamist rows.
    """
    already_registered = _existing_completed_outputs(sg_id)
    for output, analysis_type in ((cram, 'cram'), (base_gvcf, 'gvcf'), (recal_gvcf, 'gvcf')):
        if (analysis_type, output) in already_registered:
            logger.info(f'{analysis_type} analysis for {output} already registered; skipping')
            continue
        complete_analysis_job(
            output,
            analysis_type,
            [],
            [sg_id],
            project_name,
            # complete_analysis_job mutates the meta it receives (pops keys, adds
            # size), so each call gets its own copy.
            dict(meta),
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
    parser.add_argument('--marker-file', required=True, help='Local path Hail Batch uploads to the marker')
    parser.add_argument('--marker-gcs-path', required=True, help='Final GCS marker path, checked for early exit')
    args = parser.parse_args()

    # A re-queued stage with the marker already in GCS (e.g. a copied file went
    # missing) must not re-register; preserve the existing marker content so the
    # subsequent upload is a no-op rewrite.
    existing_marker = _read_existing_marker(args.marker_gcs_path)
    if existing_marker is not None:
        logger.info(f'Registration marker {args.marker_gcs_path} already exists; skipping registration')
        with open(args.marker_file, 'w') as fh:
            fh.write(existing_marker)
        return

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
