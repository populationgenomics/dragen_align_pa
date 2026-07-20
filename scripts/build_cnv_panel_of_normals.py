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
    3. PERSIST  — rewrite each file's sample identity with a suffix (default
                  `_pon`) and upload it to ICA *reference* storage under the
                  renamed basename. Per-run ICA outputs get deleted; reference
                  data persists run-to-run. Each upload yields a `fil.…` ID.
    4. LIST     — write `<panel>.normals.txt`, one renamed BASENAME per line
                  (see note below), and upload it to ICA too (its own file ID).
    5. REGISTER — print a ready-to-paste JSON block, `{<panel-name>: {pon_list_file
                  -> list `fil.…` ID, basename -> count `fil.…` ID, …}}`, to merge
                  into `ICA_PON_FILE_IDS` in `dragen_align_pa.constants`. A run then
                  selects the panel by name (`[presets.exome].cnv_normals_panel`)
                  instead of listing IDs.

WHY THE RENAME (`--rename-suffix`, default `_pon`):
    DRAGEN aborts CNV calling if a case sample is detected as a member of its
    own panel of normals (`caseSampleNotInPoN`). Suffixing every panel sample's
    identity — the filename, the `#Original input file` line, and the counts
    column-header sample name — guarantees no panel entry matches a case SG, so
    a sample can sit in a panel that is also used to call it.

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
        --cohort-or-sequencing-groups CPGAAAAAA CPGBBBBBB CPGCCCCCC CPGDDDDDD  \\
        --ica-reference-folder /references/exo_CNV_panels_normals/bioheart-wgs-wiring-20260611 \\
        --provenance-prefix gs://cpg-bioheart-test/ica/dragen_3_7_8/pon_provenance

    OR

    python scripts/build_cnv_panel_of_normals.py \\
        --panel-name bioheart-wgs-wiring-20260611 \\
        --cohort-or-sequencing-groups COHXXXXX  \\
        --ica-reference-folder /references/exo_CNV_panels_normals/bioheart-wgs-wiring-20260611 \\
        --provenance-prefix gs://cpg-bioheart-test/ica/dragen_3_7_8/pon_provenance
"""

import argparse
import gzip
import json
import os
import re
import tempfile

import cpg_utils
import cpg_utils.config
from graphql import DocumentNode
from icasdk.apis.tags import project_data_api  # noqa: TC002  (used only in annotations; keep as a runtime import)
from loguru import logger
from metamist.graphql import gql, query

from dragen_align_pa import ica_api_utils, ica_cli_utils, ica_utils, utils
from dragen_align_pa.constants_registry import ROLE_DRAGEN_ALIGN

# WES tool (WGS self-normalises and never builds a PON). DRAGEN's gc-corrected
# counts <SG>.target.counts.gc-corrected.gz are the right PON input when GC
# correction is on, which is the default for WES designs with enough targets.
# Designs below the ~200k-target threshold should run with GC correction
# disabled (guide p.127): pass --counts-suffix .target.counts.gz to build the
# panel from the plain counts instead.
DEFAULT_COUNTS_SUFFIX = '.target.counts.gc-corrected.gz'

# 0-based index of the sample-name column in a counts-file header line:
# contig, start, stop, name, <sample>, improper_pairs.
_SAMPLE_NAME_COLUMN = 4

DATASET = cpg_utils.config.config_retrieve(['workflow', 'dataset'])
ACCESS_LEVEL = cpg_utils.config.get_access_level()
PROJECT = f'{DATASET}{"" if ACCESS_LEVEL != "test" else "-" + ACCESS_LEVEL}'

SEQUENCING_GROUP_QUERY: DocumentNode = gql(
    """
query sg_query($cohort: String, $project: String) {
    cohorts(id: {eq: $cohort}, project: {eq: $project}) {
        sequencingGroups {
            id
        }
    }
}
""",
)


def _counts_basename(sg: str, counts_suffix: str) -> str:
    """Filename (no directory) of a sequencing group's target-counts file."""
    return f'{sg}{counts_suffix}'


def _pon_basename(sg: str, counts_suffix: str, rename_suffix: str) -> str:
    """Renamed (suffixed) counts filename for the panel copy uploaded to ICA."""
    return _counts_basename(f'{sg}{rename_suffix}', counts_suffix)


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
    # Never overwrite an existing snapshot. The provenance copy exists so a later DRAGEN
    # re-run of this SG can't replace the counts the panel was built from; re-running this
    # script must not itself clobber that snapshot with (possibly newer) source data.
    if cpg_utils.to_path(dest).exists():
        logger.info(f'Provenance snapshot {dest} already exists; leaving it untouched.')
        return dest
    utils.run_subprocess_with_log(
        ['gcloud', 'storage', 'cp', gcs_counts_path, dest],
        f'Preserve {basename} to provenance',
    )
    return dest


def _rewrite_counts_identity(local_in: str, local_out: str, sg: str, rename_suffix: str) -> None:
    """Suffix the sample identity in a gzipped counts file so it can't match a case SG.

    Prevents DRAGEN's ``caseSampleNotInPoN`` abort by renaming every
    identity-bearing field; data rows pass through unchanged.

    Args:
        local_in: Source gzipped counts file.
        local_out: Destination for the rewritten gzipped counts file.
        sg: Sequencing-group token suffixed in ``#`` comment lines (e.g. the
            ``#Original input file`` line).
        rename_suffix: Suffix appended to the sample identity (e.g. ``_pon``).
            Also appended to the column-header sample name (the 5th field of the
            first non-comment line), whatever its value — on re-ID'd samples it
            differs from ``sg``.
    """
    pon = f'{sg}{rename_suffix}'
    header_seen = False
    with gzip.open(local_in, 'rt') as fin, gzip.open(local_out, 'wt') as fout:
        for line in fin:
            if line.startswith('#'):
                # Whole-token replace only: a bare str.replace would also rewrite an SG
                # id that happens to be a prefix of a longer id sharing the same digits.
                out_line = re.sub(rf'\b{re.escape(sg)}\b', pon, line)
            elif not header_seen:
                fields = line.rstrip('\n').split('\t')
                if len(fields) > _SAMPLE_NAME_COLUMN:
                    fields[_SAMPLE_NAME_COLUMN] = f'{fields[_SAMPLE_NAME_COLUMN]}{rename_suffix}'
                out_line = '\t'.join(fields) + '\n'
                header_seen = True
            else:
                out_line = line
            fout.write(out_line)


def _upload_counts_to_ica(
    api_instance: 'project_data_api.ProjectDataApi',
    path_params: dict[str, str],
    gcs_counts_path: str,
    ica_reference_folder: str,
    sg: str,
    counts_suffix: str,
    rename_suffix: str,
    ica_local_dir: str,
) -> str:
    """Rename a counts file's sample identity and persist it to ICA reference storage.

    Downloads the original counts, rewrites its identity to ``<sg><rename_suffix>``,
    and uploads it under the renamed basename. Skips the round-trip if the
    renamed file is already AVAILABLE, so re-runs are idempotent.

    Args:
        api_instance: ICA project-data API client.
        path_params: ICA path params (the projectId).
        gcs_counts_path: Source GCS path of the original counts file.
        ica_reference_folder: Destination ICA reference folder.
        sg: Sequencing-group ID of the source counts file.
        counts_suffix: Suffix of the counts file (e.g. ``.target.counts.gc-corrected.gz``).
        rename_suffix: Suffix applied to the panel sample identity (e.g. ``_pon``).
        ica_local_dir: Local scratch dir used to stage the download + rewrite.

    Returns:
        The ICA file ID (``fil.…``) of the uploaded or already-present renamed file.

    Raises:
        FileNotFoundError: If the file is not AVAILABLE in ICA after upload.
        ValueError: If ICA returns no file ID for it.
    """
    ica_folder_path = ica_reference_folder.rstrip('/') + '/'
    pon_basename = _pon_basename(sg, counts_suffix, rename_suffix)
    utils.validate_cli_path_input(gcs_counts_path, 'gcs_counts_path')
    utils.validate_cli_path_input(ica_folder_path, 'ica_folder_path')

    # Skip the round-trip if the renamed file is already AVAILABLE in ICA.
    existing = ica_api_utils.get_file_details_from_ica(api_instance, path_params, ica_folder_path, pon_basename)
    if existing and existing['details']['status'] == 'AVAILABLE':
        logger.info(f'{pon_basename} already AVAILABLE in ICA reference folder; reusing {existing["id"]}.')
        return existing['id']

    original_local = os.path.join(ica_local_dir, _counts_basename(sg, counts_suffix))
    utils.run_subprocess_with_log(
        ['gcloud', 'storage', 'cp', gcs_counts_path, original_local],
        f'Download {sg} counts from GCS',
    )
    pon_local = os.path.join(ica_local_dir, pon_basename)
    _rewrite_counts_identity(original_local, pon_local, sg, rename_suffix)
    ica_cli_utils.upload_local_file(pon_local, ica_folder_path)
    ica_utils.wait_for_file_available(api_instance, path_params, file_name=pon_basename, folder_path=ica_folder_path)
    return _require_file_id(api_instance, path_params, ica_folder_path, pon_basename)


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


def _resolve_sequencing_groups(cohort_or_sequencing_groups: list[str]) -> list[str]:
    """Resolve the CLI argument into a concrete list of sequencing-group IDs.

    A single ``COH``-prefixed argument is treated as a cohort and expanded to its
    member SGs via metamist; anything else is taken as a literal list of SG IDs.

    Raises:
        ValueError: If a cohort ID is mixed with other tokens, if the cohort is not
            found in metamist, or if it resolves to no sequencing groups.
    """
    if not any(arg.startswith('COH') for arg in cohort_or_sequencing_groups):
        return cohort_or_sequencing_groups

    if len(cohort_or_sequencing_groups) != 1:
        raise ValueError(
            'A cohort ID must be passed on its own, not mixed with sequencing-group IDs; '
            f'got {cohort_or_sequencing_groups}.',
        )

    cohort_id = cohort_or_sequencing_groups[0]
    cohorts = query(SEQUENCING_GROUP_QUERY, variables={'cohort': cohort_id, 'project': PROJECT})['cohorts']
    if not cohorts:
        raise ValueError(f'Cohort {cohort_id} was not found in metamist.')
    sequencing_groups = [sg['id'] for sg in cohorts[0]['sequencingGroups']]
    if not sequencing_groups:
        raise ValueError(f'Cohort {cohort_id} resolved to no sequencing groups. Is the cohort in the supplied project?')
    return sequencing_groups


def build_panel(
    panel_name: str,
    cohort_or_sequencing_groups: list[str],
    ica_reference_folder: str,
    provenance_prefix: str | None,
    counts_suffix: str,
    rename_suffix: str,
) -> dict[str, str]:
    """Build one Panel of Normals and persist it to ICA reference storage.

    Counts are read from the current run's config-derived output directory, so
    invoke this in the config context whose bucket holds them.

    Args:
        panel_name: Panel label; names the ``.normals.txt`` list and the
            provenance subfolder.
        cohort_or_sequencing_groups: Either a single ``COH`` cohort ID (expanded
            to its member SGs via metamist) or a literal list of SG IDs whose
            gc-corrected target counts form the panel.
        ica_reference_folder: Destination ICA folder for the uploaded counts
            and list file.
        provenance_prefix: GCS prefix to snapshot counts into before upload;
            ``None`` skips the preservation step.
        counts_suffix: Suffix of the per-SG counts file (e.g.
            ``.target.counts.gc-corrected.gz``).
        rename_suffix: Suffix applied to each panel sample's identity (filename,
            ``#Original input file`` line, and column-header sample name) so no
            panel entry matches a case SG (e.g. ``_pon``).

    Returns:
        Mapping of each uploaded artifact's renamed basename (the per-SG counts
        files and the normals list) to its ICA ``fil.…`` ID.

    Raises:
        ValueError: If an upload completes but ICA returns no file ID for it.
    """
    ica_folder_path = ica_reference_folder.rstrip('/') + '/'
    # The PON is built in the DRAGEN-align project: the counts come from its runs and the
    # renamed panel files are uploaded to its reference storage. The role resolves against the
    # configured [ica.projects].project_root family for CLI auth, the API client, and path params.
    metrics_prefix = str(utils.get_output_path('dragen_metrics'))

    file_ids: dict[str, str] = {}

    # The icav2 CLI (used for uploads) needs to be authenticated once up front.
    ica_cli_utils.authenticate_ica_cli(ROLE_DRAGEN_ALIGN)

    sequencing_groups = _resolve_sequencing_groups(cohort_or_sequencing_groups)
    with (
        ica_api_utils.ica_project_data_api(ROLE_DRAGEN_ALIGN) as (api_instance, path_params),
        tempfile.TemporaryDirectory() as ica_local_dir,
    ):
        for sg in sequencing_groups:
            original_basename = _counts_basename(sg, counts_suffix)
            pon_basename = _pon_basename(sg, counts_suffix, rename_suffix)
            gcs_counts_path = _gcs_counts_path(metrics_prefix, sg, counts_suffix)
            logger.info(f'[{panel_name}] {sg} -> {pon_basename}: {gcs_counts_path}')

            if provenance_prefix:
                _preserve_to_provenance(gcs_counts_path, provenance_prefix, panel_name, original_basename)

            file_ids[pon_basename] = _upload_counts_to_ica(
                api_instance=api_instance,
                path_params=path_params,
                gcs_counts_path=gcs_counts_path,
                ica_reference_folder=ica_reference_folder,
                sg=sg,
                counts_suffix=counts_suffix,
                rename_suffix=rename_suffix,
                ica_local_dir=ica_local_dir,
            )

        list_basename = f'{panel_name}.normals.txt'
        existing_list = ica_api_utils.get_file_details_from_ica(
            api_instance, path_params, ica_folder_path, list_basename
        )
        if existing_list and existing_list['details']['status'] == 'AVAILABLE':
            # Mirror the per-counts idempotency skip: re-uploading would leave a second
            # same-named list file in ICA and make the registered file ID ambiguous.
            logger.info(f'{list_basename} already AVAILABLE in ICA reference folder; reusing {existing_list["id"]}.')
            file_ids[list_basename] = existing_list['id']
        else:
            list_local = os.path.join(ica_local_dir, list_basename)
            with open(list_local, 'w') as fh:
                for sg in sequencing_groups:
                    fh.write(_pon_basename(sg, counts_suffix, rename_suffix) + '\n')
            logger.info(f'[{panel_name}] wrote {list_basename} with {len(sequencing_groups)} entries.')
            ica_cli_utils.upload_local_file(list_local, ica_folder_path)
            ica_utils.wait_for_file_available(
                api_instance, path_params, file_name=list_basename, folder_path=ica_folder_path
            )
            file_ids[list_basename] = _require_file_id(api_instance, path_params, ica_folder_path, list_basename)

    return file_ids


def _print_registration_snippet(panel_name: str, file_ids: dict[str, str]) -> None:
    """Print the panel's ICA_PON_FILE_IDS entry as a ready-to-paste JSON block.

    The normals-list file is emitted under the ``pon_list_file`` key; the per-SG
    count files are emitted as a plain list of IDs under ``count_file_ids``. Their
    basenames are intentionally dropped — they embed CPG sample IDs (blocked by
    the CPG-ID pre-commit hook) and are never consumed. See ``ICA_PON_FILE_IDS``
    in ``dragen_align_pa.constants`` for the consumed structure.

    Args:
        panel_name: Panel label, also the JSON block's top-level key.
        file_ids: Map of artifact basename to ICA file ID (as built by build_panel).
    """
    list_basename = f'{panel_name}.normals.txt'
    entry = {
        'pon_list_file': file_ids[list_basename],
        'count_file_ids': [file_id for name, file_id in file_ids.items() if name != list_basename],
    }
    logger.info(
        f'Panel "{panel_name}" built ({len(file_ids)} entries). Merge this block into '
        f'ICA_PON_FILE_IDS in dragen_align_pa.constants:',
    )
    print(f'\n# --- CNV PON: {panel_name} ---')
    print(json.dumps({panel_name: entry}, indent=4))


def main() -> None:
    """Parse CLI args, build the panel, and print its ICA_FILE_IDS block."""
    parser = argparse.ArgumentParser(description='Build a DRAGEN CNV Panel of Normals from existing target counts.')
    parser.add_argument('--panel-name', required=True, help='Panel label, used for the list filename and folder.')
    parser.add_argument(
        '--cohort-or-sequencing-groups',
        required=True,
        nargs='+',
        help='Cohort ID or space separated list of CPG sequencing-group IDs to include as normals.',
    )
    parser.add_argument(
        '--ica-reference-folder',
        required=True,
        help='Destination ICA reference folder, e.g. /references/exo_CNV_panels_normals/<panel>',
    )
    parser.add_argument(
        '--provenance-prefix',
        default=None,
        help='Optional GCS prefix to snapshot the counts into before upload (provenance). Omit to skip.',
    )
    parser.add_argument(
        '--counts-suffix',
        default=DEFAULT_COUNTS_SUFFIX,
        help=f'Counts file suffix (default {DEFAULT_COUNTS_SUFFIX}). Use .target.counts.gz for plain counts.',
    )
    parser.add_argument(
        '--rename-suffix',
        default='_pon',
        help="Suffix applied to each panel sample's identity (filename + in-file "
        'sample names) so DRAGEN never sees a case SG as a panel member. '
        "Default '_pon'.",
    )
    args = parser.parse_args()

    file_ids = build_panel(
        panel_name=args.panel_name,
        cohort_or_sequencing_groups=args.cohort_or_sequencing_groups,
        ica_reference_folder=args.ica_reference_folder,
        provenance_prefix=args.provenance_prefix,
        counts_suffix=args.counts_suffix,
        rename_suffix=args.rename_suffix,
    )
    _print_registration_snippet(args.panel_name, file_ids)


if __name__ == '__main__':
    main()
