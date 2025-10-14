import re
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


def _write_fastq_list_file(df: pd.DataFrame, outputs: dict[str, cpg_utils.Path], sg_name: str) -> None:
    fastq_list_file_path: cpg_utils.Path = outputs[sg_name]
    fastq_list_header: list[str] = ['RGID', 'RGSM', 'RGLB', 'LANE', 'Read1File', 'Read2File']
    adaptors: re.Pattern[str] = re.compile('_([ACGT]+-[ACGT]+)_')
    df['adaptors'] = df['Filenames'].str.extract(adaptors, expand=False)
    df['Sample_Key'] = df['Filenames'].str.replace(r'_R[12]\.fastq\.gz', '', regex=True)
    df = df.sort_values('Filenames')
    paired_df: pd.DataFrame = (
        df.groupby('Sample_Key')
        .agg(
            # For columns that are the same for both files (like 'Sample ID'), we just take the first entry.
            **{col: 'first' for col in df.columns if col not in ['Filenames', 'Checksum', 'Sample_Key']},
            # For filenames and checksums, we take the first (R1) and last (R2) values from each group.
            Read1File=('Filenames', 'first'),
            Read2File=('Filenames', 'last'),
            R1_Checksum=('Checksum', 'first'),
            R2_Checksum=('Checksum', 'last'),
        )
        .reset_index()
    )

    # 4. Drop the temporary Sample_Key column as it's no longer needed.
    paired_df = paired_df.drop(columns=['Sample_Key'])

    paired_df['RDSM'] = sg_name
    paired_df['RGID'] = (
        paired_df['RDSM']
        + '_'
        + paired_df['adaptors']
        + '_'
        + paired_df['Lane']
        + '_'
        + paired_df['Machine ID']
        + '_'
        + paired_df['Flow cell']
    )
    paired_df['Library'] = sg_name
    paired_df = paired_df.drop(columns=[paired_df.colnames not in fastq_list_header])
    with cpg_utils.to_path(fastq_list_file_path).open('w') as fastq_list_fh:
        df.to_csv(fastq_list_fh, sep=',', index=False, header=True)


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
        filenames: list[str] = []

        items_to_process: list[Any] = nested_list if isinstance(nested_list, list) else [nested_list]

        for item in items_to_process:
            if isinstance(item, list):
                # If the item is a list, recursively flatten it and extend the main list
                filenames.extend(_flatten_list(item))
            elif isinstance(item, dict) and 'basename' in item:
                filenames.append(item['basename'])
        return filenames

    manifest_file_path: cpg_utils.Path = config_retrieve(['workflow', 'manifest_gcp_path'])
    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        supplied_manifest_data: pd.DataFrame = pd.read_csv(manifest_fh)

    for sequencing_group in cohort.get_sequencing_groups():
        all_reads_for_sg: list[Any] = []
        for single_assay in sequencing_group.assays if sequencing_group.assays else []:
            if 'reads' in single_assay.meta:
                reads_value: list[Any] = single_assay.meta.get('reads', [])
                if isinstance(reads_value, list):
                    all_reads_for_sg.extend(_flatten_list(reads_value))
        if all_reads_for_sg:
            print(all_reads_for_sg)
            # Filter the manifest DataFrame to include only rows where 'Filenames'
            # match the reads found for the sequencing group.
            print(supplied_manifest_data)
            df: pd.DataFrame = supplied_manifest_data[supplied_manifest_data['Filenames'].isin(all_reads_for_sg)]
            if df.empty:
                raise ValueError(f'No matching reads found in manifest for sequencing group {sequencing_group.id}')
            _write_fastq_list_file(df=df, outputs=outputs, sg_name=sequencing_group.name)
