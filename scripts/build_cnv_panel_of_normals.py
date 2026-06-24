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
import json
import os
import tempfile
from typing import Literal

from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_cli_utils, ica_utils, utils

# WES tool (WGS self-normalises and never builds a PON). DRAGEN's gc-corrected
# counts <SG>.target.counts.gc-corrected.gz are the right PON input when GC
# correction is on, which is the default for WES designs with enough targets.
# Designs below the ~200k-target threshold should run with GC correction
# disabled (guide p.127): pass --counts-suffix .target.counts.gz to build the
# panel from the plain counts instead.
DEFAULT_COUNTS_SUFFIX = '.target.counts.gc-corrected.gz'


def _counts_basename(sg: str, counts_suffix: str) -> str:
    """Filename (no directory) of a sequencing group's target-counts file."""
    return f'{sg}{counts_suffix}'


def _gcs_counts_path(gcs_metrics_prefix: str, sg: str, counts_suffix: str) -> str:
    """GCS path to a sequencing group's counts file: <prefix>/<SG>/<SG><counts_suffix>."""
    return f'{gcs_metrics_prefix.rstrip("/")}/{sg}/{_counts_basename(sg, counts_suffix)}'


def _preserve_to_provenance(gcs_counts_path: str, provenance_prefix: str, panel_name: str, basename: str) -> str:
    """Snapshot a counts file to a durable GCS provenance location.

    Args:
        gcs_counts_path: Source GCS path of the counts file.
        provenance_prefix: GCS prefix under which to store the snapshot.
        panel_name: Panel label; used as the destination subfolder.
        basename: Counts filename, reused as the destination filename.

    Returns:
        The GCS path the file was copied to.

    Raises:
        subprocess.CalledProcessError: If the gcloud copy fails.
    """
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
    """Persist one counts file to ICA reference storage and return its file ID.

    Skips the download + upload round-trip if the file is already AVAILABLE in
    the target folder, so re-runs are idempotent.

    Args:
        api_instance: ICA project-data API client.
        path_params: ICA path params (the projectId).
        gcs_counts_path: Source GCS path of the counts file.
        ica_reference_folder: Destination ICA reference folder.
        basename: Counts filename, used both in ICA and to look the file up.
        ica_local_dir: Local scratch dir used to stage the download before upload.

    Returns:
        The ICA file ID (``fil.…``) of the uploaded or already-present file.

    Raises:
        FileNotFoundError: If the file is not AVAILABLE in ICA after upload.
        ValueError: If ICA returns no file ID for it.
    """
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
    ica_utils.wait_for_file_available(api_instance, path_params, file_name=basename, folder_path=ica_folder_path)
    return _require_file_id(api_instance, path_params, ica_folder_path, basename)


def _require_file_id(
    api_instance: 'project_data_api.ProjectDataApi',
    path_params: dict[str, str],
    ica_folder_path: str,
    basename: str,
) -> str:
    """Look up an uploaded file's ICA file ID.

    Args:
        api_instance: ICA project-data API client.
        path_params: ICA path params (the projectId).
        ica_folder_path: ICA folder the file lives in.
        basename: The file's name.

    Returns:
        The ICA file ID (``fil.…``).

    Raises:
        ValueError: If ICA has no file ID for ``basename`` in that folder.
    """
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
    """Build one Panel of Normals and persist it to ICA reference storage.

    Counts are read from the current run's config-derived output directory, so
    invoke this in the config context whose bucket holds them.

    Args:
        panel_name: Panel label; names the ``.normals.txt`` list and the
            provenance subfolder.
        sequencing_groups: Sequencing group IDs whose gc-corrected target
            counts form the panel.
        ica_reference_folder: Destination ICA folder for the uploaded counts
            and list file.
        provenance_prefix: GCS prefix to snapshot counts into before upload;
            ``None`` skips the preservation step.
        counts_suffix: Suffix of the per-SG counts file (e.g.
            ``.target.counts.gc-corrected.gz``).

    Returns:
        Mapping of each uploaded artifact's basename (the per-SG counts files
        and the normals list) to its ICA ``fil.…`` ID, ready to paste into
        ``ICA_FILE_IDS``.

    Raises:
        ValueError: If an upload completes but ICA returns no file ID for it.
    """
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
        ica_utils.wait_for_file_available(api_instance, path_params, file_name=list_basename, folder_path=ica_folder_path)
        file_ids[list_basename] = _require_file_id(api_instance, path_params, ica_folder_path, list_basename)

    return file_ids


def _print_registration_snippet(panel_name: str, file_ids: dict[str, str]) -> None:
    """Print the panel's basename->file-ID map as a JSON block for ICA_FILE_IDS.

    Args:
        panel_name: Panel label, shown in the printed header.
        file_ids: Map of artifact basename to ICA file ID.
    """
    logger.info(
        f'Panel "{panel_name}" built ({len(file_ids)} entries). Merge this block into '
        f'ICA_FILE_IDS in dragen_align_pa.constants:',
    )
    print(f'\n# --- CNV PON: {panel_name} ---')
    print(json.dumps(file_ids, indent=4))


def main() -> None:
    """Parse CLI args, build the panel, and print its ICA_FILE_IDS block."""
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
