"""Mirror per-batch artefacts (passfail.json, summary.json, reports/) from ICA to GCS.

One run per cohort; iterates over every successfully-submitted batch in the
batches file. Files are streamed directly from ICA to GCS via the existing
`ica_utils.stream_ica_file_to_gcs` helper — no local staging, no icav2 CLI.
"""

import json
from collections import Counter
from dataclasses import dataclass, field

import cpg_utils.config
import icasdk
import requests
from google.cloud import exceptions as gcs_exceptions
from google.cloud import storage
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.batches import BatchesFile
from dragen_align_pa.constants import BUCKET_NAME
from dragen_align_pa.constants_registry import ica_project_name, resolve_ica_project_id


@dataclass
class _StreamStats:
    """Per-cohort marker-payload tracking.

    Tracks *identities* (not just counts) for failures so an operator reading
    the marker payload can see which specific files failed and run a targeted
    retry instead of having to scrape job logs.

    `lookup_failure` and `stream_failure` are split so a cohort-wide auth /
    connectivity outage (lookup-heavy) is distinguishable from a transfer /
    GCS outage (stream-heavy). `FileNotFoundError` at lookup is legitimate
    absence and is NOT recorded as a failure. `skipped` counts files already
    present in GCS from a previous (partial) run.
    """

    success: int = 0
    skipped: int = 0
    lookup_failures: list[dict[str, str]] = field(default_factory=list)
    stream_failures: list[dict[str, str]] = field(default_factory=list)

    @property
    def total_failure(self) -> int:
        return len(self.lookup_failures) + len(self.stream_failures)


def _stream_silently(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    file_id: str,
    file_name: str,
    gcs_bucket: storage.Bucket,
    gcs_prefix: str,
    context: str,
    stats: _StreamStats,
) -> None:
    """Stream one file to GCS; warn-and-skip on transient ICA / HTTP / GCS errors.

    Skips outright if the GCS blob already exists, supporting a 'delete the
    marker and re-run' retry workflow that re-attempts only previously-failed
    files. GCS resumable uploads only finalize on completion, so a stream
    failure in a previous run won't leave a partial blob — an existing blob
    means a previous run committed successfully.

    Wraps `stream_ica_file_to_gcs` so one transient blip doesn't abort the
    whole cohort mid-loop. MD5-mismatch (`ValueError`) is intentionally not
    caught — we pass `expected_md5_hash=None` here so it's unreachable, but a
    future caller passing a hash should crash hard on integrity violations.
    If MD5 verification is ever wired through this function, the skip-if-exists
    branch above needs to be gated on a "verified" sidecar or removed —
    trusting an existing blob is only safe in the no-hash regime.
    """
    gcs_blob_path = f'{gcs_prefix}/{file_name}'
    if gcs_bucket.blob(gcs_blob_path).exists():
        stats.skipped += 1
        return
    try:
        ica_utils.stream_ica_file_to_gcs(
            api_instance=api_instance,
            path_parameters=path_parameters,
            file_id=file_id,
            file_name=file_name,
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_prefix,
            expected_md5_hash=None,
        )
    except (icasdk.ApiException, requests.RequestException, gcs_exceptions.GoogleCloudError) as e:
        logger.warning(f'{context}: streaming {file_name} failed ({e}); skipping.')
        stats.stream_failures.append(
            {
                'context': context,
                'file_name': file_name,
                'error': str(e),
            }
        )
        return
    stats.success += 1


def _stream_named_file(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    parent_folder: str,
    file_name: str,
    gcs_bucket: storage.Bucket,
    gcs_prefix: str,
    stats: _StreamStats,
) -> None:
    """Find one named file in `parent_folder` and stream it to `gcs_prefix/file_name`.

    `FileNotFoundError` at lookup is legitimate absence — passfail.json and
    summary.json may be missing on a catastrophically-failed batch — and is
    NOT counted as a failure. ICA `ApiException` is `lookup_failure` (the
    file may exist; we couldn't address it).
    """
    try:
        file_id = ica_api_utils.find_file_id_by_name(
            api_instance=api_instance,
            path_parameters=path_parameters,
            parent_folder_path=parent_folder,
            file_name=file_name,
        )
    except FileNotFoundError:
        logger.warning(f'{file_name} not present in {parent_folder}; skipping.')
        return
    except icasdk.ApiException as e:
        logger.warning(
            f'ICA API error while looking up {file_name} in {parent_folder}: {e}; skipping.',
        )
        stats.lookup_failures.append(
            {
                'parent_folder': parent_folder,
                'file_name': file_name,
                'error': str(e),
            }
        )
        return

    _stream_silently(
        api_instance=api_instance,
        path_parameters=path_parameters,
        file_id=file_id,
        file_name=file_name,
        gcs_bucket=gcs_bucket,
        gcs_prefix=gcs_prefix,
        context=f'lookup={parent_folder}',
        stats=stats,
    )


def run(
    batches_file_path: cpg_utils.Path,
    gcs_output_root: cpg_utils.Path,
    marker_path: cpg_utils.Path,
    cohort_name: str,
) -> None:
    """For each successfully-submitted batch, mirror passfail.json/summary.json/reports/.

    Raises `RuntimeError` if streams were attempted and ZERO succeeded, so
    cpg-flow retries instead of writing a green marker over an empty GCS
    state. Partial failures still write the marker; failure counts in the
    payload let operators decide whether to re-run.
    """
    batches_file = BatchesFile(path=batches_file_path)
    batches_file.read()

    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(BUCKET_NAME)
    # Assert (don't silently removeprefix) so a future `output_path` override
    # returning a different bucket doesn't no-op into a wrong-bucket write.
    expected_prefix = f'gs://{BUCKET_NAME}/'
    gcs_output_str = str(gcs_output_root)
    if not gcs_output_str.startswith(expected_prefix):
        raise ValueError(
            f'gcs_output_root {gcs_output_str!r} does not start with expected '
            f'bucket prefix {expected_prefix!r}; refusing to derive a relative '
            f'GCS prefix that would land objects in the wrong bucket.',
        )
    base_prefix = gcs_output_str.removeprefix(expected_prefix)

    # BatchesFile schema doesn't enforce batch_index uniqueness; duplicates would
    # silently clobber each other's GCS prefix.
    counts = Counter(b['batch_index'] for b in batches_file.batches)
    if duplicates := sorted(idx for idx, n in counts.items() if n > 1):
        raise ValueError(
            f'Duplicate batch_index values in {batches_file_path}: {duplicates}; refusing to overwrite GCS artefacts.',
        )
    dragen_project = ica_project_name('dragen_align')
    path_parameters = {'projectId': resolve_ica_project_id(dragen_project)}

    stats = _StreamStats()
    batches_processed = 0

    with ica_api_utils.get_ica_api_client(dragen_project) as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        for batch_entry in batches_file.batches:
            if not batch_entry.get('pipeline_id'):
                logger.info(
                    f'Batch index {batch_entry["batch_index"]} has no '
                    f'pipeline_id (status={batch_entry.get("status")!r}); '
                    f'skipping artefact download.',
                )
                continue

            batch_index = batch_entry['batch_index']
            batches_processed += 1

            # Batch analysis-run root (the per-SG folders sit one level below this).
            ica_folder = ica_utils.ica_run_path(
                cohort_name,
                batch_entry['user_reference'],
                batch_entry['pipeline_id'],
            ).as_folder()
            batch_name = f'{cohort_name}_batch{batch_index:04d}'
            gcs_prefix = f'{base_prefix}/{batch_name}'

            for name in ('passfail.json', 'summary.json'):
                _stream_named_file(
                    api_instance=api_instance,
                    path_parameters=path_parameters,
                    parent_folder=ica_folder,
                    file_name=name,
                    gcs_bucket=gcs_bucket,
                    gcs_prefix=gcs_prefix,
                    stats=stats,
                )

            # Recursive: DRAGEN's reports/ has nested subdirs; non-recursive would
            # silently drop them. Folder may be absent on a catastrophically-failed
            # batch — 404 is expected there, anything else is a real lookup failure.
            reports_folder = f'{ica_folder}reports/'
            try:
                report_files = ica_utils.list_ica_files(
                    api_instance=api_instance,
                    path_parameters=path_parameters,
                    base_ica_folder_path=reports_folder,
                    recursive=True,
                )
            except icasdk.ApiException as e:
                logger.warning(
                    f'Batch {batch_name}: reports/ folder not enumerable at {reports_folder} ({e}); skipping.',
                )
                stats.lookup_failures.append(
                    {
                        'parent_folder': reports_folder,
                        'file_name': '<reports/ enumeration>',
                        'error': str(e),
                    }
                )
                report_files = []

            for report_relative_path, report_id in report_files:
                _stream_silently(
                    api_instance=api_instance,
                    path_parameters=path_parameters,
                    file_id=report_id,
                    file_name=report_relative_path,
                    gcs_bucket=gcs_bucket,
                    gcs_prefix=f'{gcs_prefix}/reports',
                    context=f'batch {batch_name} reports/',
                    stats=stats,
                )

    if stats.success == 0 and stats.total_failure > 0:
        raise RuntimeError(
            f'Cohort {cohort_name}: every artefact stream failed '
            f'(lookup_failures={stats.lookup_failures}, '
            f'stream_failures={stats.stream_failures}, successes=0). '
            f'No files landed on GCS. Re-run the stage; if the failure '
            f'persists, investigate ICA / GCS connectivity for this project.',
        )

    summary_msg = (
        f'Cohort {cohort_name} batch-artefact download: '
        f'batches_processed={batches_processed}, '
        f'success={stats.success}, '
        f'skipped={stats.skipped}, '
        f'lookup_failures={len(stats.lookup_failures)}, '
        f'stream_failures={len(stats.stream_failures)}.'
    )
    if stats.total_failure > 0:
        logger.warning(summary_msg)
    else:
        logger.info(summary_msg)

    with marker_path.open('w') as fh:
        json.dump(
            {
                'cohort_name': cohort_name,
                'batches_processed': batches_processed,
                'success_count': stats.success,
                'skipped_count': stats.skipped,
                'lookup_failures': stats.lookup_failures,
                'stream_failures': stats.stream_failures,
            },
            fh,
            indent=2,
        )
