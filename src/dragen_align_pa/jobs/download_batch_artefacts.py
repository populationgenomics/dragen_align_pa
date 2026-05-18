"""Mirror per-batch artefacts (passfail.json, summary.json, reports/) from ICA to GCS.

One run per cohort; iterates over every successfully-submitted batch in the
batches file. Files are streamed directly from ICA to GCS via the existing
`ica_utils.stream_ica_file_to_gcs` helper — no local staging, no icav2 CLI.
"""

import json
from typing import Literal

import cpg_utils
import icasdk
import requests
from cpg_utils.config import config_retrieve
from google.cloud import exceptions as gcs_exceptions
from google.cloud import storage
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_utils
from dragen_align_pa.batches import BatchesFile
from dragen_align_pa.constants import BUCKET_NAME


def _stream_silently(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    file_id: str,
    file_name: str,
    gcs_bucket: storage.Bucket,
    gcs_prefix: str,
    context: str,
) -> bool:
    """Stream one file to GCS; warn-and-skip on transient ICA / HTTP / GCS errors.

    Returns `True` on success, `False` on a transient failure that was
    caught + logged. Callers aggregate these for the per-cohort summary.

    Wraps `ica_utils.stream_ica_file_to_gcs` so a single transient blip on
    one file does not abort the whole cohort's batch-artefacts run mid-loop
    and leave partial GCS state with no marker file. Missed files are
    observable both via the marker payload (which records the failure
    count) and via the absence of GCS objects; both are re-fetched by
    re-running the stage. `context` is included in the warning log to make
    the source location traceable.

    Note: `ValueError` from `stream_ica_file_to_gcs`'s MD5-mismatch branch
    (`ica_utils.py:192`) is **intentionally not caught**. This helper passes
    `expected_md5_hash=None`, so that branch is unreachable from here. A
    future caller that supplies an expected hash inherits a hard crash on
    mismatch, which is the correct behaviour for integrity violations.
    """
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
        return False
    return True


def _stream_named_file(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    parent_folder: str,
    file_name: str,
    gcs_bucket: storage.Bucket,
    gcs_prefix: str,
) -> bool | None:
    """Find one named file in `parent_folder` and stream it to `gcs_prefix/file_name`.

    Returns:
    - `True`: file was streamed.
    - `False`: file existed at lookup but the stream step failed (transient).
    - `None`: file is legitimately absent (passfail.json / summary.json may
      be missing on a catastrophically-failed batch — not counted as a
      failure for marker accounting).

    Lookup-side ICA `ApiException` is bucketed as `False` (transient
    failure) rather than `None` (absent) because the file may exist; only
    `FileNotFoundError` from the SDK indicates definitive absence.
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
        return None
    except icasdk.ApiException as e:
        logger.warning(
            f'ICA API error while looking up {file_name} in {parent_folder}: {e}; skipping.',
        )
        return False

    return _stream_silently(
        api_instance=api_instance,
        path_parameters=path_parameters,
        file_id=file_id,
        file_name=file_name,
        gcs_bucket=gcs_bucket,
        gcs_prefix=gcs_prefix,
        context=f'lookup={parent_folder}',
    )


def run(
    batches_file_path: cpg_utils.Path,
    gcs_output_root: cpg_utils.Path,
    marker_path: cpg_utils.Path,
    cohort_name: str,
) -> None:
    """For each successfully-submitted batch, mirror passfail.json/summary.json/reports/.

    Writes `marker_path` on completion so the stage has a deterministic
    expected_output. `cohort_name` is passed explicitly from the stage
    (rather than parsed out of the filename) so the job is decoupled from
    `ManageDragenPipeline.expected_outputs`'s filename convention.

    Schema validation: uses `BatchesFile.read()` to enforce schema_version
    + every required per-batch key — a malformed batches file raises
    `ValueError` here instead of a bare `KeyError` deep in the loop.
    """
    batches_file = BatchesFile(path=batches_file_path)
    batches_file.read()

    output_folder = config_retrieve(['ica', 'data_prep', 'output_folder'])

    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(BUCKET_NAME)
    # `gcs_output_root` is a `cpg_utils.Path` like `gs://{BUCKET}/ica/{ver}/output/dragen_batch_metrics`;
    # `gcs_prefix` for `stream_ica_file_to_gcs` must be relative to the bucket.
    # Assert the expected bucket prefix rather than silently `removeprefix`-ing —
    # if `output_path` ever returns a different bucket (test override, future
    # `category` redirect), `removeprefix` would be a no-op and we'd write
    # objects under a path like `gs://other-bucket/...` inside `BUCKET_NAME`.
    expected_prefix = f'gs://{BUCKET_NAME}/'
    gcs_output_str = str(gcs_output_root)
    if not gcs_output_str.startswith(expected_prefix):
        raise ValueError(
            f'gcs_output_root {gcs_output_str!r} does not start with expected '
            f'bucket prefix {expected_prefix!r}; refusing to derive a relative '
            f'GCS prefix that would land objects in the wrong bucket.',
        )
    base_prefix = gcs_output_str.removeprefix(expected_prefix)

    # Pre-scan for duplicate batch_index. The BatchesFile schema doesn't enforce
    # uniqueness, and two entries with the same index would silently clobber each
    # other's GCS prefix. Detect upfront so the error fires before any I/O.
    indices = [b['batch_index'] for b in batches_file.batches]
    if len(set(indices)) != len(indices):
        from collections import Counter  # noqa: PLC0415
        duplicates = sorted(idx for idx, n in Counter(indices).items() if n > 1)
        raise ValueError(
            f'Duplicate batch_index values in {batches_file_path}: {duplicates}; '
            f'refusing to overwrite GCS artefacts.',
        )

    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    path_parameters = {'projectId': secrets['projectID']}

    success_count = 0
    failure_count = 0
    batches_processed = 0

    with ica_api_utils.get_ica_api_client() as api_client:
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

            # PrepareIcaForDragenAnalysis creates one folder named `cohort_name`
            # under `output_folder/`; ICA writes each batch's analysis run inside
            # it. Path layout mirrors `utils.get_ica_sample_folder` minus the
            # trailing `/{sg_name}/` (this stage operates at the batch root).
            ica_folder = (
                f'/{BUCKET_NAME}/{output_folder}/{cohort_name}/'
                f'{batch_entry["user_reference"]}-{batch_entry["pipeline_id"]}/'
            )
            batch_name = f'{cohort_name}_batch{batch_index:04d}'
            gcs_prefix = f'{base_prefix}/{batch_name}'

            for name in ('passfail.json', 'summary.json'):
                result = _stream_named_file(
                    api_instance=api_instance,
                    path_parameters=path_parameters,
                    parent_folder=ica_folder,
                    file_name=name,
                    gcs_bucket=gcs_bucket,
                    gcs_prefix=gcs_prefix,
                )
                if result is True:
                    success_count += 1
                elif result is False:
                    failure_count += 1
                # result is None: file legitimately absent — not counted.

            # `reports/` — recursively enumerate every file under the subfolder
            # and stream each. DRAGEN's reports/ tree has nested subdirectories
            # (e.g. report_files/samples/), so the non-recursive
            # `list_and_filter_ica_files` would silently drop the nested files;
            # `list_ica_files_recursive` walks the whole tree and returns
            # relative paths that preserve the nested layout when reassembled
            # under the GCS `reports/` prefix.
            #
            # The folder may be absent on a catastrophically-failed batch (e.g.
            # single-sample retry that aborted before producing reports); treat
            # that as a non-fatal warning so the rest of the batches continue.
            # Note: a mid-walk ApiException discards any files collected so far
            # for this folder — re-running the stage re-fetches everything.
            reports_folder = f'{ica_folder}reports/'
            try:
                report_files = ica_utils.list_ica_files_recursive(
                    api_instance=api_instance,
                    path_parameters=path_parameters,
                    base_ica_folder_path=reports_folder,
                )
            except icasdk.ApiException as e:
                logger.warning(
                    f'Batch {batch_name}: reports/ folder not enumerable at '
                    f'{reports_folder} ({e}); skipping.',
                )
                report_files = []

            for report_relative_path, report_id in report_files:
                if _stream_silently(
                    api_instance=api_instance,
                    path_parameters=path_parameters,
                    file_id=report_id,
                    file_name=report_relative_path,
                    gcs_bucket=gcs_bucket,
                    gcs_prefix=f'{gcs_prefix}/reports',
                    context=f'batch {batch_name} reports/',
                ):
                    success_count += 1
                else:
                    failure_count += 1

    # Total-failure guard: if we attempted streams (failure_count > 0) and not a
    # single one succeeded, raising propagates the failure to cpg-flow rather
    # than writing a green marker over a useless GCS state. Partial failures
    # (some successes + some failures) still write the marker — the failure
    # count goes into the JSON payload so operators can decide whether to re-run.
    if failure_count > 0 and success_count == 0:
        raise RuntimeError(
            f'Cohort {cohort_name}: every artefact stream failed '
            f'({failure_count} attempts, 0 successes). No files landed on GCS. '
            f'Re-run the stage; if the failure persists, investigate ICA / GCS '
            f'connectivity for this project.',
        )

    summary_msg = (
        f'Cohort {cohort_name} batch-artefact download: '
        f'batches_processed={batches_processed}, success={success_count}, '
        f'failure={failure_count}.'
    )
    if failure_count > 0:
        logger.warning(summary_msg)
    else:
        logger.info(summary_msg)

    with marker_path.open('w') as fh:
        json.dump(
            {
                'cohort_name': cohort_name,
                'batches_processed': batches_processed,
                'success_count': success_count,
                'failure_count': failure_count,
            },
            fh,
        )
