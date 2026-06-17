"""
Build a DRAGEN CNV Panel of Normals (PON) from already-run target counts.

This is a STANDALONE, one-off operator script.
It does no CNV math. DRAGEN does all panel normalisation itself at run time.
DRAGEN does NOT accept a pre-computed/merged PON file, its
"PON" is simply a list of per-sample gc-corrected target-counts files.

So this script's whole job is data management, in five steps, per panel:

    1. RESOLVE  — for each sequencing group, derive the GCS path to its
                  `<SG>.target.counts.gc-corrected.gz` in the pipeline output.
    2. PRESERVE — copy each counts file to a durable GCS provenance location,
                  so a later DRAGEN re-run of that SG can't overwrite the
                  original we built the panel from.
    3. PERSIST  — upload each counts file to ICA *reference* storage. Per-run
                  ICA outputs get deleted; reference data persists run-to-run.
                  Each upload yields a `fil.…` ID.
    4. LIST     — write `<panel>.normals.txt`, one BASENAME per line (see note
                  below), and upload it to ICA too (it needs its own file ID).
    5. REGISTER — print a ready-to-paste snippet for `ICA_FILE_IDS` in
                  `dragen_align_pa.constants`, keyed by the list + counts
                  basenames, so the submitter can resolve them by name.

WHY BASENAMES IN THE LIST (not ICA paths or file IDs):
    `constants.py` documents that ICA localises all analysis inputs into a
    single working directory at run time, and DRAGEN's CLI args reference files
    by basename. The DRAGEN guide (p.130) also states relative paths in the PON
    list are supported, resolved against the current working directory. So the
    count files are passed to the analysis as data inputs (by file ID), land
    next to the list in cwd, and the list refers to them by bare basename.

WIRING STILL TO DO (downstream of this script, the part the test exercises):
    The submitter (`jobs/submit_dragen_batch.py`) has no `cnv_normals_list` /
    `cnv_normals_files` parameterCode yet. Passing the list + the count files
    into the DRAGEN ICA analysis (and adding `--cnv-normals-list` /
    `--cnv-input` to the DRAGEN args) is a separate pipeline change. This
    script only produces and registers the artifacts that change will consume.

Example (bioheart WGS wiring panel from 4 of the 5 COH2308 SGs; 5th is the case):

    python scripts/build_cnv_panel_of_normals.py \\
        --panel-name bioheart-wgs-wiring-20260611 \\
        --sequencing-groups CPGAAAAAA CPGBBBBBB CPGCCCCCC CPGDDDDDD  \\
        --ica-reference-folder /references/exo_CNV_panels_normals/bioheart-wgs-wiring-20260611 \\
        --provenance-prefix gs://cpg-bioheart-test/ica/dragen_3_7_8/pon_provenance
"""

import argparse
import os
import tempfile
from typing import Literal

from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_cli_utils, utils

# DRAGEN names gc-corrected counts <SG>.target.counts.gc-corrected.gz. The PON
# list points at these (guide p.130 example, and gc-corrected is recommended
# for WGS downstream). Override via --counts-suffix for plain counts on WES
# designs below the ~200k-target GC-correction threshold (guide p.127).
DEFAULT_COUNTS_SUFFIX = '.target.counts.gc-corrected.gz'


def _counts_basename(sg: str, counts_suffix: str) -> str:
    return f'{sg}{counts_suffix}'


def _gcs_counts_path(gcs_metrics_prefix: str, sg: str, counts_suffix: str) -> str:
    """Pipeline layout: <prefix>/<SG>/<SG>.target.counts.gc-corrected.gz."""
    return f'{gcs_metrics_prefix.rstrip("/")}/{sg}/{_counts_basename(sg, counts_suffix)}'


def _preserve_to_provenance(gcs_counts_path: str, provenance_prefix: str, panel_name: str, basename: str) -> str:
    """Copy the counts file to a durable provenance location. Returns the dest path."""
    dest = f'{provenance_prefix.rstrip("/")}/{panel_name}/{basename}'
    utils.run_subprocess_with_log(
        ['gcloud', 'storage', 'cp', gcs_counts_path, dest],
        f'Preserve {basename} to provenance',
    )
    return dest


def _upload_counts_to_ica(
    api_instance: 'project_data_api.ProjectDataApi',
    path_params: dict[str, str],
    gcs_counts_path: str,
    ica_reference_folder: str,
    basename: str,
    ica_local_dir: str,
) -> str:
    """Download counts from GCS, upload to the ICA reference folder, return its file ID."""
    ica_folder_path = ica_reference_folder.rstrip('/') + '/'
    utils.validate_cli_path_input(gcs_counts_path, 'gcs_counts_path')
    utils.validate_cli_path_input(ica_folder_path, 'ica_folder_path')

    # Skip the round-trip if it's already AVAILABLE in ICA (idempotent re-runs).
    existing = ica_api_utils.get_file_details_from_ica(api_instance, path_params, ica_folder_path, basename)
    if existing and existing['details']['status'] == 'AVAILABLE':
        logger.info(f'{basename} already AVAILABLE in ICA reference folder; reusing {existing["id"]}.')
        return existing['id']

    local_path = os.path.join(ica_local_dir, basename)
    utils.run_subprocess_with_log(
        ['gcloud', 'storage', 'cp', gcs_counts_path, local_path],
        f'Download {basename} from GCS',
    )
    ica_cli_utils.upload_local_file(local_path, ica_folder_path)

    return _require_file_id(api_instance, path_params, ica_folder_path, basename)


def _require_file_id(
    api_instance: 'project_data_api.ProjectDataApi',
    path_params: dict[str, str],
    ica_folder_path: str,
    basename: str,
) -> str:
    """Re-fetch the file ID after upload; fail loudly if ICA can't find it."""
    data = ica_api_utils.get_file_details_from_ica(api_instance, path_params, ica_folder_path, basename)
    if not data or not data.get('id'):
        raise ValueError(f'Upload of {basename} to {ica_folder_path} reported no file ID in ICA.')
    return data['id']


def build_panel(
    panel_name: str,
    sequencing_groups: list[str],
    ica_reference_folder: str,
    provenance_prefix: str | None,
    counts_suffix: str,
) -> dict[str, str]:
    """Run steps 1-5 for one panel. Returns {basename: ica_file_id} for every
    artifact registered (the counts files and the normals list)."""
    ica_folder_path = ica_reference_folder.rstrip('/') + '/'
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    path_params = {'projectId': secrets['projectID']}

    metrics_prefix = str(utils.get_output_path('dragen_metrics'))

    file_ids: dict[str, str] = {}

    # The icav2 CLI (used for uploads) needs to be authenticated once up front.
    ica_cli_utils.authenticate_ica_cli()

    with ica_api_utils.get_ica_api_client() as api_client, tempfile.TemporaryDirectory() as ica_local_dir:
        api_instance = project_data_api.ProjectDataApi(api_client)

        for sg in sequencing_groups:
            basename = _counts_basename(sg, counts_suffix)
            gcs_counts_path = _gcs_counts_path(metrics_prefix, sg, counts_suffix)
            logger.info(f'[{panel_name}] {sg}: {gcs_counts_path}')

            if provenance_prefix:
                _preserve_to_provenance(gcs_counts_path, provenance_prefix, panel_name, basename)

            file_ids[basename] = _upload_counts_to_ica(
                api_instance=api_instance,
                path_params=path_params,
                gcs_counts_path=gcs_counts_path,
                ica_reference_folder=ica_reference_folder,
                basename=basename,
                ica_local_dir=ica_local_dir,
            )

        list_basename = f'{panel_name}.normals.txt'
        list_local = os.path.join(ica_local_dir, list_basename)
        with open(list_local, 'w') as fh:
            for sg in sequencing_groups:
                fh.write(_counts_basename(sg, counts_suffix) + '\n')
        logger.info(f'[{panel_name}] wrote {list_basename} with {len(sequencing_groups)} entries.')
        ica_cli_utils.upload_local_file(list_local, ica_folder_path)
        file_ids[list_basename] = _require_file_id(api_instance, path_params, ica_folder_path, list_basename)

    return file_ids


def _print_registration_snippet(panel_name: str, file_ids: dict[str, str]) -> None:
    """Emit a paste-ready ICA_FILE_IDS block for constants.py."""
    logger.info(
        f'Panel "{panel_name}" built. Add these to dragen_align_pa.constants.ICA_FILE_IDS '
        f'so the submitter can resolve them by name:',
    )
    print(f'\n    # --- CNV PON: {panel_name} ---')
    for basename, fid in file_ids.items():
        print(f"    '{basename}': '{fid}',")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description='Build a DRAGEN CNV Panel of Normals from existing target counts.')
    parser.add_argument('--panel-name', required=True, help='Panel label, used for the list filename and folder.')
    parser.add_argument(
        '--sequencing-groups', required=True, nargs='+', help='CPG sequencing-group IDs to include as normals.',
    )
    parser.add_argument(
        '--ica-reference-folder', required=True,
        help='Destination ICA reference folder, e.g. /references/exo_CNV_panels_normals/<panel>',
    )
    parser.add_argument(
        '--provenance-prefix', default=None,
        help='Optional GCS prefix to snapshot the counts into before upload (provenance). Omit to skip.',
    )
    parser.add_argument(
        '--counts-suffix', default=DEFAULT_COUNTS_SUFFIX,
        help=f'Counts file suffix (default {DEFAULT_COUNTS_SUFFIX}). Use .target.counts.gz for plain counts.',
    )
    args = parser.parse_args()

    file_ids = build_panel(
        panel_name=args.panel_name,
        sequencing_groups=args.sequencing_groups,
        ica_reference_folder=args.ica_reference_folder,
        provenance_prefix=args.provenance_prefix,
        counts_suffix=args.counts_suffix,
    )
    _print_registration_snippet(args.panel_name, file_ids)


if __name__ == '__main__':
    main()
