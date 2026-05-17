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
from dragen_align_pa.constants import BUCKET_NAME


def _stream_silently(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    file_id: str,
    file_name: str,
    gcs_bucket: storage.Bucket,
    gcs_prefix: str,
    context: str,
) -> None:
    """Stream one file to GCS; warn-and-skip on transient ICA / HTTP / GCS errors.

    Wraps `ica_utils.stream_ica_file_to_gcs` so a single transient blip on
    one file does not abort the whole cohort's batch-artefacts run mid-loop
    and leave partial GCS state with no marker file. Missed files are
    observable via the absence of GCS objects and can be re-fetched by
    re-running the stage. `context` is included in the warning log to make
    the source location of the failure traceable (typically the batch name
    plus what the file is).
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


def _stream_named_file(
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
    parent_folder: str,
    file_name: str,
    gcs_bucket: storage.Bucket,
    gcs_prefix: str,
) -> None:
    """Find one named file in `parent_folder` and stream it to `gcs_prefix/file_name`.

    Logs and returns silently if the file is not present (passfail.json /
    summary.json may legitimately be absent on a catastrophically-failed
    batch). Tolerates transient ICA / HTTP / GCS errors at both the lookup
    AND the stream step so a single blip doesn't abort the cohort run.
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
        return

    _stream_silently(
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
) -> None:
    """For each successfully-submitted batch, mirror passfail.json/summary.json/reports/.

    Writes `marker_path` on completion so the stage has a deterministic expected_output.
    """
    with batches_file_path.open('r') as fh:
        data = json.load(fh)

    output_folder = config_retrieve(['ica', 'data_prep', 'output_folder'])
    cohort_name = batches_file_path.name.replace('_batches.json', '')

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

    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    path_parameters = {'projectId': secrets['projectID']}

    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)
        for batch_entry in data['batches']:
            if not batch_entry.get('pipeline_id'):
                continue

            # PrepareIcaForDragenAnalysis creates one folder named `cohort_name`
            # under `output_folder/`; ICA writes each batch's analysis run inside
            # it. Path layout mirrors `utils.get_ica_sample_folder` minus the
            # trailing `/{sg_name}/` (this stage operates at the batch root).
            ica_folder = (
                f'/{BUCKET_NAME}/{output_folder}/{cohort_name}/'
                f'{batch_entry["user_reference"]}-{batch_entry["pipeline_id"]}/'
            )
            batch_name = f'{cohort_name}_batch{batch_entry["batch_index"]:04d}'
            gcs_prefix = f'{base_prefix}/{batch_name}'

            for name in ('passfail.json', 'summary.json'):
                _stream_named_file(
                    api_instance=api_instance,
                    path_parameters=path_parameters,
                    parent_folder=ica_folder,
                    file_name=name,
                    gcs_bucket=gcs_bucket,
                    gcs_prefix=gcs_prefix,
                )

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
                _stream_silently(
                    api_instance=api_instance,
                    path_parameters=path_parameters,
                    file_id=report_id,
                    file_name=report_relative_path,
                    gcs_bucket=gcs_bucket,
                    gcs_prefix=f'{gcs_prefix}/reports',
                    context=f'batch {batch_name} reports/',
                )

    with marker_path.open('w') as fh:
        fh.write(f'Downloaded batch artefacts for {cohort_name}\n')
