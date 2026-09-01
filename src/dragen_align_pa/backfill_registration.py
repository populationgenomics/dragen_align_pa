"""Register a backfilled SG's cram, base gVCF and recal gVCF in metamist, in that order.

`sequencing_group.gvcf` resolves last-row-wins over the project's completed gvcf
analyses (cpg-flow takes the final row of the metamist response, effectively the
newest), so the recal gVCF must be registered after the base gVCF. cpg-flow's decorator mechanism
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
an active completed analysis of the same type in metamist. The dedup alone cannot
preserve ordering against pre-existing ICA-flow rows (recal registered, base not:
skipping the recal would leave the freshly registered base as the latest gvcf), so
the recal is re-registered whenever the base was newly registered in this
invocation, and after registering, the run fails loudly unless the recal is the
latest completed gvcf analysis for the SG — which also surfaces interleavings from
concurrent runs covering the same sequencing group.
"""

import json
from argparse import ArgumentParser
from typing import Any

import cpg_utils
from cpg_flow.status import complete_analysis_job
from loguru import logger
from metamist.graphql import gql, query


def _completed_analyses(sg_id: str, project_name: str) -> list[dict]:
    """This SG's active completed analyses in the registration project, in response order.

    Filters `active: {eq: true}` like cpg-flow's GET_ANALYSES_QUERY (an archived
    analysis must not suppress re-registration), and `project` because the SG may
    carry analyses in other metamist projects that registration and sg.gvcf
    resolution never see. `project_name` is the same string `complete_analysis_job`
    registers into — the caller already applied the `-test` bump.

    Rows are deliberately NOT sorted: cpg-flow's resolution takes the last row of
    the server response with no sort, and this module must agree with the consumer.
    """
    analyses_query = gql(
        request_string="""
        query BackfillRegisteredAnalyses($sgId: String!, $project: String!) {
          sequencingGroups(id: {eq: $sgId}) {
            analyses(active: {eq: true}, project: {eq: $project}) {
              type
              status
              output
            }
          }
        }
    """
    )
    result = query(analyses_query, variables={'sgId': sg_id, 'project': project_name})
    sequencing_groups = result.get('sequencingGroups', [])
    if not sequencing_groups:
        raise ValueError(f'No sequencing group found in metamist with ID {sg_id}')
    return [
        analysis
        for analysis in sequencing_groups[0].get('analyses', [])
        if str(analysis.get('status', '')).upper() == 'COMPLETED' and analysis.get('output')
    ]


def _existing_completed_outputs(sg_id: str, project_name: str) -> set[tuple[str, str]]:
    """(type, output) pairs of this SG's active completed analyses in the project."""
    return {(analysis['type'], analysis['output']) for analysis in _completed_analyses(sg_id, project_name)}


def _assert_recal_is_latest(sg_id: str, recal_gvcf: str, project_name: str) -> None:
    """Fail loudly unless the recal gVCF is the newest completed gvcf analysis.

    cpg-flow's sequencing-group gvcf resolution is last-row-wins over the
    project-scoped response, so anything else (a concurrent run's interleaving,
    or a pre-existing ordering the dedup skipped over) would silently hand
    downstream consumers the non-MLR base gVCF.
    """
    gvcf_rows = [analysis for analysis in _completed_analyses(sg_id, project_name) if analysis['type'] == 'gvcf']
    if not gvcf_rows or gvcf_rows[-1]['output'] != recal_gvcf:
        latest = gvcf_rows[-1]['output'] if gvcf_rows else None
        raise RuntimeError(
            f'After backfill registration for {sg_id}, the latest completed gvcf analysis '
            f'is {latest!r}, not the recal gVCF {recal_gvcf!r}. sequencing_group.gvcf would '
            f'resolve to the wrong file — investigate before re-running (was a concurrent '
            f'run covering this SG in flight?).',
        )


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

    Outputs that already have an active completed analysis of the same type are
    skipped, so a replayed run never duplicates metamist rows — except the recal
    gVCF, which is re-registered whenever the base gVCF was newly registered in
    this invocation: a skipped recal predating a fresh base row would make the
    base the latest gvcf analysis. Ends by asserting the recal is the latest
    completed gvcf for the SG.
    """
    already_registered = _existing_completed_outputs(sg_id, project_name)
    base_newly_registered = False
    for output, analysis_type in ((cram, 'cram'), (base_gvcf, 'gvcf'), (recal_gvcf, 'gvcf')):
        recal_must_follow_new_base = output == recal_gvcf and base_newly_registered
        if (analysis_type, output) in already_registered and not recal_must_follow_new_base:
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
        if output == base_gvcf:
            base_newly_registered = True
    _assert_recal_is_latest(sg_id, recal_gvcf, project_name)
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
    parser.add_argument(
        '--marker-file',
        required=True,
        help='Local path this script writes; Hail Batch uploads it to --marker-gcs-path',
    )
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
