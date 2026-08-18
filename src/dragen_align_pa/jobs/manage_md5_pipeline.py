import heapq
import json
import math
import os
import time
from collections import Counter
from collections.abc import Callable
from functools import partial
from typing import NamedTuple, NoReturn

import cpg_utils.config
import pandas as pd
from cpg_flow.targets import Cohort
from cpg_utils.config import try_get_ar_guid
from icasdk.apis.tags import project_data_api
from loguru import logger

from dragen_align_pa import ica_api_utils, ica_cli_utils, ica_utils
from dragen_align_pa.constants.constants_registry import ROLE_DRAGEN_ALIGN
from dragen_align_pa.paths import IcaPath
from dragen_align_pa.jobs import run_intake_qc_pipeline
from dragen_align_pa.jobs.ica_pipeline_manager import manage_ica_pipeline_loop


def compute_md5_chunk_size(n_files: int, total_bytes: int, max_concurrent_pods: int, max_pod_bytes: int) -> int:
    """Return the per-chunk file count that respects the per-pod byte cap.

    Derives the number of waves from the cohort's bytes: enough chunks that no
    pod streams more than `max_pod_bytes`, rounded up to full waves of
    `max_concurrent_pods` so every running pod carries a near-equal share.

    Args:
        n_files: Total number of FASTQ files to checksum.
        total_bytes: Combined size of all FASTQ files in bytes.
        max_concurrent_pods: Maximum pods ICA runs concurrently for this pipeline.
        max_pod_bytes: Upper bound on the bytes a single chunk may hold.

    Returns:
        The number of files per chunk.

    Raises:
        ValueError: If `n_files`, `max_concurrent_pods`, or `max_pod_bytes` is
            not positive, or `total_bytes` is negative.
    """
    if n_files <= 0 or max_concurrent_pods <= 0 or max_pod_bytes <= 0 or total_bytes < 0:
        raise ValueError(
            f'n_files, max_concurrent_pods, and max_pod_bytes must be positive and '
            f'total_bytes must be non-negative, '
            f'got {n_files=}, {total_bytes=}, {max_concurrent_pods=}, {max_pod_bytes=}',
        )
    min_chunks_for_bytes = math.ceil(total_bytes / max_pod_bytes)
    waves = max(1, math.ceil(min_chunks_for_bytes / max_concurrent_pods))
    n_chunks = min(n_files, waves * max_concurrent_pods)
    chunk_size = math.ceil(n_files / n_chunks)
    # With few, large files the ceil can merge blocks below the byte minimum
    # (e.g. 26 files into 25 chunks -> size 2 -> only 13 actual blocks over the
    # cap); shrink until the actual block count meets it. chunk_size 1 yields
    # n_files blocks, the most possible without splitting files.
    while chunk_size > 1 and math.ceil(n_files / chunk_size) < min_chunks_for_bytes:
        chunk_size -= 1
    return chunk_size


def pack_fastq_ids_by_size(id_to_size: dict[str, int], chunk_size: int) -> list[str]:
    """Order FASTQ file IDs so sequential `chunk_size` blocks are byte-balanced.

    The MD5 pipeline splits the uploaded ID list into consecutive blocks of
    `chunk_size` lines, one pod per block, and each pod streams its files
    serially — so a block's byte total sets that pod's runtime.

    Args:
        id_to_size: Mapping of ICA file ID to its size in bytes.
        chunk_size: Number of lines per block in the pipeline's split.

    Returns:
        All file IDs, ordered to balance each consecutive `chunk_size`-line
        block's byte total as far as the fixed block capacities allow. For
        chunk sizes from `compute_md5_chunk_size` the blocks come out
        near-equal; a pathological standalone `chunk_size` (e.g. one forcing a
        tiny remainder block) cannot balance regardless of order.

    Raises:
        ValueError: If `chunk_size` is not positive.
    """
    if chunk_size <= 0:
        raise ValueError(f'chunk_size must be positive, got {chunk_size}')
    n_blocks = math.ceil(len(id_to_size) / chunk_size)
    # Every block must hold exactly chunk_size files except the last (the
    # pipeline splits strictly by line count), so only the final block may
    # take the remainder.
    capacities = [chunk_size] * n_blocks
    if len(id_to_size) % chunk_size:
        capacities[-1] = len(id_to_size) % chunk_size

    blocks: list[list[str]] = [[] for _ in range(n_blocks)]
    # Longest-processing-time packing: place each file, largest first, into
    # the block with the smallest byte total that still has a free slot.
    open_blocks: list[tuple[int, int]] = [(0, index) for index in range(n_blocks)]
    heapq.heapify(open_blocks)
    for file_id in sorted(id_to_size, key=lambda fid: (-id_to_size[fid], fid)):
        byte_total, index = heapq.heappop(open_blocks)
        blocks[index].append(file_id)
        if len(blocks[index]) < capacities[index]:
            heapq.heappush(open_blocks, (byte_total + id_to_size[file_id], index))
    return [file_id for block in blocks for file_id in block]


class FastqFileDetails(NamedTuple):
    """Name and size a FASTQ file resolves to in ICA."""

    name: str
    size_in_bytes: int


# ICA runs at most this many concurrent pods for the MD5 pipeline; a tenant-side
# quota we cannot alter or query — it was measured from run metrics (a hard
# plateau of exactly 25 running pods), not read from any API or config.
_MD5_MAX_CONCURRENT_PODS = 25
# Byte cap per chunk: keeps any single pod's serial streaming work bounded
# (~0.25 TB is under an hour at the observed ~75 MB/s single-stream rate).
# The chunk size is always computed from these two constants; there is
# deliberately no config override.
_MD5_MAX_POD_BYTES = 250_000_000_000


def _plan_md5_chunks(ica_fastq_info: dict[str, FastqFileDetails]) -> tuple[list[str], int]:
    """Choose the chunk size and byte-balanced ID order for a cohort's FASTQs.

    Computes the chunk size that keeps every chunk under the per-pod byte cap
    while filling the ICA pod quota in full waves, then orders the IDs so each
    chunk holds a near-equal share of the cohort's bytes.

    Args:
        ica_fastq_info: Mapping of ICA file ID to the file's name and size.

    Returns:
        The file IDs ordered for byte-balanced chunks, and the chunk size to
        pass to the pipeline.
    """
    id_to_size = {file_id: details.size_in_bytes for file_id, details in ica_fastq_info.items()}
    chunk_size = compute_md5_chunk_size(
        n_files=len(id_to_size),
        total_bytes=sum(id_to_size.values()),
        max_concurrent_pods=_MD5_MAX_CONCURRENT_PODS,
        max_pod_bytes=_MD5_MAX_POD_BYTES,
    )
    ordered_ids = pack_fastq_ids_by_size(id_to_size=id_to_size, chunk_size=chunk_size)
    block_byte_totals = [
        sum(id_to_size[file_id] for file_id in ordered_ids[i : i + chunk_size])
        for i in range(0, len(ordered_ids), chunk_size)
    ]
    logger.info(
        f'MD5 chunk plan: {len(block_byte_totals)} chunks of up to {chunk_size} files '
        f'({len(ordered_ids)} files, {sum(block_byte_totals):,} bytes total); '
        f'per-chunk bytes min {min(block_byte_totals):,}, max {max(block_byte_totals):,}',
    )
    return ordered_ids, chunk_size


def _get_fastq_ica_id_list(
    fastq_filenames: list[str],
    api_instance: project_data_api.ProjectDataApi,
    path_parameters: dict[str, str],
) -> dict[str, FastqFileDetails]:
    """Finds ICA file IDs for a list of FASTQ filenames.

    Queries ICA in batches for the given filenames and reconciles the result
    against the manifest: every expected filename must resolve to exactly one
    ICA file ID.

    Args:
        fastq_filenames: FASTQ filenames expected from the manifest.
        api_instance: ICA project-data API client used to run the queries.
        path_parameters: ICA path parameters (e.g. project ID) for the query.

    Returns:
        A mapping of ICA file ID to the file's name and size in bytes for
        every resolved FASTQ file.

    Raises:
        ValueError: If any manifest filename is missing in ICA, resolves to
            more than one ICA file, or ICA returns a filename the manifest
            does not expect, or a returned file has no usable size. The
            message names the offending files.
    """
    ica_fastq_info: dict[str, FastqFileDetails] = {}

    # Handle potentially large lists by batching API calls
    batch_size = cpg_utils.config.config_retrieve(['ica', 'api', 'batch_size'], default=20)
    for i in range(0, len(fastq_filenames), batch_size):
        batch_filenames = fastq_filenames[i : i + batch_size]
        logger.info(
            f'Querying ICA for {len(batch_filenames)} FASTQ IDs (batch {i // batch_size + 1})...',
        )
        api_response = ica_api_utils.ica_retry(
            api_instance.get_project_data_list,
            path_params=path_parameters,
            query_params={'filename': batch_filenames, 'filenameMatchMode': 'EXACT'},
        )
        for item in api_response.body['items']:
            file_id: str = item['data']['id']
            file_name: str = item['data']['details']['name']
            # Optional in the ICA schema (e.g. a file still uploading); a
            # defaulted size would silently unbalance the chunk packing.
            size_in_bytes = item['data']['details'].get('fileSizeInBytes')
            if size_in_bytes is None:
                raise ValueError(
                    f'ICA returned no fileSizeInBytes for {file_name!r} ({file_id}); '
                    f'the file may not be fully uploaded (status not AVAILABLE).',
                )
            ica_fastq_info[file_id] = FastqFileDetails(name=file_name, size_in_bytes=int(size_in_bytes))

    # Reconcile by name, not count: a filename existing twice in ICA plus a
    # genuinely missing manifest file leaves the counts equal, so a count-only
    # check would vouch for a file that was never checksummed.
    found_name_counts = Counter(details.name for details in ica_fastq_info.values())
    expected_names = set(fastq_filenames)
    missing_filenames: list[str] = [name for name in fastq_filenames if name not in found_name_counts]
    duplicated_filenames: list[str] = sorted(name for name, count in found_name_counts.items() if count > 1)
    unexpected_filenames: list[str] = sorted(name for name in found_name_counts if name not in expected_names)
    if missing_filenames or duplicated_filenames or unexpected_filenames:
        problems: list[str] = []
        if missing_filenames:
            problems.append(f'{len(missing_filenames)} file(s) missing in ICA: {missing_filenames}')
        if duplicated_filenames:
            problems.append(
                f'{len(duplicated_filenames)} filename(s) resolving to more than one ICA file: '
                f'{duplicated_filenames}',
            )
        if unexpected_filenames:
            problems.append(f'{len(unexpected_filenames)} filename(s) not in the manifest: {unexpected_filenames}')
        message: str = 'Manifest FASTQs did not reconcile against ICA. ' + ' '.join(problems)
        logger.error(message)
        raise ValueError(message)

    logger.info(f'Found {len(ica_fastq_info)} total FASTQ file IDs.')
    return ica_fastq_info


def _create_md5_output_folder(
    folder_path: str,
    api_instance: project_data_api.ProjectDataApi,
    cohort_name: str,
    path_parameters: dict[str, str],
) -> str:
    """

    Creates the output folder in ICA for the MD5 pipeline.
    """
    object_id, _ = ica_utils.create_upload_object_id(
        api_instance=api_instance,
        path_params=path_parameters,
        folder_name=cohort_name,
        file_name=cohort_name,
        folder_path=folder_path,
        object_type='FOLDER',
    )
    return object_id


def _submit_md5_run(
    cohort_name: str,
    fastq_list_file_id: str,
    ar_guid: str,
    md5_outputs_folder_id: str,
    chunk_size: int,
) -> str:
    """
    Submits the MD5 intake QC pipeline to ICA.
    (This is the original submit function from this file)
    """
    logger.info(f'Submitting new MD5 ICA pipeline for {cohort_name}')
    with ica_api_utils.ica_project_analysis_api(ROLE_DRAGEN_ALIGN) as (api_instance, path_parameters):
        md5_pipeline_id: str = run_intake_qc_pipeline.run_md5_pipeline(
            cohort_name=cohort_name,
            fastq_list_file_id=fastq_list_file_id,
            api_instance=api_instance,
            path_parameters=path_parameters,
            ar_guid=ar_guid,
            md5_outputs_folder_id=md5_outputs_folder_id,
            chunk_size=chunk_size,
        )
    return md5_pipeline_id


# manage_ica_pipeline_loop's cancel branch never submits, so reaching this
# callable means the loop's control flow regressed; fail loudly rather than
# launch an analysis the user just asked to cancel.
def _fail_submit_during_cancellation() -> NoReturn:
    """Raise on any submission attempt made while cancellation is requested."""
    raise RuntimeError(
        'MD5 pipeline submission attempted while cancel_cohort_run=true; '
        'the management loop must never submit during cancellation.',
    )


def _prepare_md5_submission(
    cohort_name: str,
    outputs: dict[str, cpg_utils.Path],
    manifest_file_path: cpg_utils.Path,
) -> Callable[[], str]:
    """Perform the MD5 pre-submission setup and build the submit callable.

    Reads the manifest, resolves the ICA file ID for every FASTQ, uploads the
    ID-list file to ICA, and creates the pipeline output folder.

    Args:
        cohort_name: The cohort being processed.
        outputs: The stage's declared outputs (receives the FASTQ ID mapping).
        manifest_file_path: Path to the manifest naming the expected FASTQs.

    Returns:
        A no-argument callable that submits the MD5 pipeline run and returns
        the ICA pipeline ID.
    """
    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        try:
            supplied_manifest_data: pd.DataFrame = pd.read_csv(
                manifest_fh,
                usecols=[cpg_utils.config.config_retrieve(['manifest', 'filenames'])],
            )
            fastq_filenames: list[str] = supplied_manifest_data[
                cpg_utils.config.config_retrieve(['manifest', 'filenames'])
            ].to_list()
        except ValueError:
            manifest_fh.seek(0)
            header: list[str] = manifest_fh.readline().split()
            logger.error(
                f'Expected to read the column: {cpg_utils.config.config_retrieve(["manifest", "filenames"])} \n'
                f'from the manifest file. Got instead: {header}'
            )
            raise

    ar_guid: str = try_get_ar_guid()
    fastq_list_file_id: str
    md5_outputs_folder_id: str

    with ica_api_utils.ica_project_data_api(ROLE_DRAGEN_ALIGN) as (api_instance, path_parameters):
        # Get all ica file ids for the fastq files
        ica_fastq_info: dict[str, FastqFileDetails] = _get_fastq_ica_id_list(
            fastq_filenames=fastq_filenames,
            api_instance=api_instance,
            path_parameters=path_parameters,
        )

        if not ica_fastq_info:
            logger.error('No FASTQ file IDs found in ICA. Cannot start MD5 pipeline.')
            raise ValueError('No FASTQ file IDs found in ICA.')

        # Upload the FASTQ ID list to ICA
        fastq_list_folder = (ica_utils.ica_cohort_path(cohort_name) / 'fastq_lists').as_folder()
        fastq_list_filename = f'{cohort_name}_{ar_guid}_fastq_ids.txt'

        # Write the FASTQ ID list to a temporary file
        # If not running with Hail Batch, make the file in the working directory
        if not os.environ.get('BATCH_TMPDIR'):
            fastq_list_filename_path: str = os.path.join('.', fastq_list_filename)
        else:
            fastq_list_filename_path = os.path.join(os.environ['BATCH_TMPDIR'], fastq_list_filename)
        # Order the IDs so the pipeline's sequential count-based split yields
        # byte-balanced chunks that fill the ICA pod quota evenly.
        ordered_fastq_ids, chunk_size = _plan_md5_chunks(ica_fastq_info)
        with open(fastq_list_filename_path, 'w') as fq_outpath:
            fq_outpath.write('\n'.join(ordered_fastq_ids))

        ica_cli_utils.authenticate_ica_cli(ROLE_DRAGEN_ALIGN)
        ica_cli_utils.upload_local_file(
            local_file_path=fastq_list_filename_path,
            ica_folder_path=fastq_list_folder,
        )
        with outputs['fastq_ids_outpath'].open('w') as fq_outpath:
            json.dump({file_id: details.name for file_id, details in ica_fastq_info.items()}, fq_outpath)

        # Find the uploaded file to get its ID, with retries for eventual consistency
        fastq_list_file_details = None
        max_retries = 5
        retry_delay_seconds = 15
        for attempt in range(max_retries):
            fastq_list_file_details = ica_api_utils.get_file_details_from_ica(
                api_instance=api_instance,
                path_params=path_parameters,
                ica_folder_path=fastq_list_folder,
                file_name=fastq_list_filename,
            )
            if fastq_list_file_details:
                status = fastq_list_file_details.get('details', {}).get('status')
                if status == 'AVAILABLE':
                    logger.info('File found and is AVAILABLE.')
                    break
                logger.warning(f"File found, but status is '{status}'. Retrying...")

            if attempt < max_retries - 1:
                time.sleep(retry_delay_seconds)

        if not fastq_list_file_details:
            raise FileNotFoundError(
                f'Could not find uploaded fastq list file in ICA after {max_retries} attempts: '
                f'{fastq_list_folder}{fastq_list_filename}',
            )
        fastq_list_file_id = fastq_list_file_details['id']

        # Create output folder
        folder_path: str = IcaPath.output_root().as_folder()
        md5_outputs_folder_id = _create_md5_output_folder(
            folder_path=folder_path,
            api_instance=api_instance,
            cohort_name=cohort_name,
            path_parameters=path_parameters,
        )

    return partial(
        _submit_md5_run,
        cohort_name=cohort_name,
        fastq_list_file_id=fastq_list_file_id,
        ar_guid=ar_guid,
        md5_outputs_folder_id=md5_outputs_folder_id,
        chunk_size=chunk_size,
    )


def run(
    cohort: Cohort,
    outputs: dict[str, cpg_utils.Path],
    manifest_file_path: cpg_utils.Path,
) -> None:
    """Manage the MD5 pipeline run for a cohort inside the PythonJob.

    Performs the pre-submission setup (resolving FASTQ IDs, uploading the
    ID-list file, creating the output folder) and then calls the generic
    pipeline management loop. When `ica.management.cancel_cohort_run` is set,
    skips the setup and hands the loop a submit callable that raises; the
    loop cancels the stored pipeline run.

    Args:
        cohort: The cohort whose FASTQs are checksummed.
        outputs: The stage's declared outputs.
        manifest_file_path: Path to the manifest naming the expected FASTQs.
    """
    cohort_name: str = cohort.name

    # Cancellation submits nothing (the loop aborts the run from the stored
    # pipeline-id file), so skip the FASTQ-ID collection, ID-list upload, and
    # output-folder creation — matching the skip-setup-on-cancel behaviour of
    # manage_dragen_pipeline.py and manage_dragen_mlr.py.
    if cpg_utils.config.config_retrieve(['ica', 'management', 'cancel_cohort_run'], default=False):
        submit_callable: Callable[[], str] = _fail_submit_during_cancellation
    else:
        submit_callable = _prepare_md5_submission(
            cohort_name=cohort_name,
            outputs=outputs,
            manifest_file_path=manifest_file_path,
        )

    manage_ica_pipeline_loop(
        targets_to_process=[cohort],
        outputs=outputs,
        pipeline_name='MD5 Checksum',
        is_mlr_pipeline=False,
        success_file_key_template='md5sum_pipeline_success',
        pipeline_id_file_key_template='md5sum_pipeline_run',
        error_log_key=f'{cohort_name}_md5_errors',
        # Single-cohort pipeline: the loop's per-target name is unused.
        submit_function_factory=lambda _target_name: submit_callable,
        allow_retry=True,
        sleep_time_seconds=300,
        # Zero-tolerance (loop default): any FAILED_FINAL aborts the cohort (the 5%-rate gate was removed branch-wide).
        raise_on_failed_final=True,
    )
