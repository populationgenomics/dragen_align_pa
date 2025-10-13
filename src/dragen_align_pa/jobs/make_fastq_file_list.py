from typing import Any

import cpg_utils
import pandas as pd
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob


def _initalise_fastq_list_job(cohort: Cohort) -> PythonJob:
    job: PythonJob = get_batch().new_python_job(
        name='MakeFastqFileList',
        attributes=cohort.get_job_attrs() or {} | {'tool': 'ICA'},  # pyright: ignore[reportUnknownArgumentType]
    )
    job.image(image=get_driver_image())
    return job


def make_fastq_list_file(
    outputs: dict[str, cpg_utils.Path],
    analysis_output_fids_path: dict[str, cpg_utils.Path],
    cohort: Cohort,
    api_root: str,
) -> PythonJob:
    job: PythonJob = _initalise_fastq_list_job(cohort=cohort)
    output = job.call(
        _run, outputs=outputs, analysis_outputs_fid_path=analysis_output_fids_path, cohort=cohort, api_root=api_root
    )

    return job


def _run(
    outputs: dict[str, cpg_utils.Path],
    analysis_outputs_fid_path: dict[str, cpg_utils.Path],
    cohort: Cohort,
    api_root: str,
) -> None:
    # Somtimes the contents of sequiencing_group.assays.meta['reads'] is a nested list
    # e.g., [['read1', 'read2']] instead of ['read1', 'read2']
    # This function will recursively flatten them into a single list
    def _flatten_list(nested_list: list[Any]) -> list[Any]:
        """
        Recursively flattens a list that may contain nested lists.
        Handles cases like [], ['read1'], [['read1']], [[]], and [['r1'], ['r2']].
        """
        if not nested_list:
            return []
        flat_list: list[Any] = []
        for item in nested_list:
            if isinstance(item, list):
                # If the item is a list, recursively flatten it and extend the main list
                flat_list.extend(_flatten_list(item))
            else:
                # If the item is not a list, append it directly
                flat_list.append(item)
        return flat_list

    manifest_file_path: cpg_utils.Path = config_retrieve(['workflow', 'manifest_gcp_path'])
    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        supplied_manifest_data: pd.DataFrame = pd.read_csv(manifest_fh)

    for sequencing_group in cohort.get_sequencing_groups():
        all_reads_for_sg: list[Any] = []
        for single_assay in sequencing_group.assays:
            if 'reads' in single_assay.meta:
                reads_value = single_assay.meta.get('reads', [])
                if isinstance(reads_value, list):
                    all_reads_for_sg.extend(_flatten_list(reads_value))
        if all_reads_for_sg:
            # Filter the manifest DataFrame to include only rows where 'Filenames'
            # match the reads found for the sequencing group.
            df: pd.DataFrame = supplied_manifest_data[supplied_manifest_data['Filenames'].isin(all_reads_for_sg)]
            if df.empty:
                raise ValueError(f'No matching reads found in manifest for sequencing group {sequencing_group.id}')
