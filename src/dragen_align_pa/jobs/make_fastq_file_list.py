import re
from typing import Any

import cpg_utils
import pandas as pd
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve


def _write_fastq_list_file(fq_df: pd.DataFrame, outputs: dict[str, cpg_utils.Path], sg_name: str) -> None:
    fastq_list_file_path: cpg_utils.Path = outputs[sg_name]
    fastq_list_header: list[str] = ['RGID', 'RGSM', 'RGLB', 'Lane', 'Read1File', 'Read2File']
    adaptors: re.Pattern[str] = re.compile('_([ACGT]+-[ACGT]+)_')

    fq_df['adaptors'] = fq_df['Filenames'].str.extract(adaptors, expand=False)
    fq_df['Sample_Key'] = fq_df['Filenames'].str.replace(r'_R[12]\.fastq\.gz', '', regex=True)
    column_mapping: dict[str, str] = {col: col.replace(' ', '_') for col in fq_df.columns if ' ' in col}
    fq_df = fq_df.rename(columns=column_mapping)

    r1_df = fq_df[fq_df['Filenames'].str.contains(r'_R1\.', regex=True)].copy().set_index('Sample_Key')
    r2_df = fq_df[fq_df['Filenames'].str.contains(r'_R2\.', regex=True)].copy().set_index('Sample_Key')

    paired_df = r1_df.merge(
        r2_df[['Filenames', 'Checksum']], left_index=True, right_index=True, suffixes=('_R1', '_R2')
    )

    paired_df = paired_df.rename(columns={'Filenames_R1': 'Read1File', 'Filenames_R2': 'Read2File'})
    paired_df = paired_df.rename(
        columns={'Checksum_R1': 'R1_Checksum', 'Checksum_R2': 'R2_Checksum'}, errors='ignore'
    ).reset_index()

    # Drop the temporary Sample_Key column as it's no longer needed.
    paired_df = paired_df.drop(columns=['Sample_Key'])
    paired_df['RGSM'] = sg_name
    paired_df['RGID'] = (
        paired_df['RGSM']
        + '_'
        + paired_df['adaptors']
        + '_'
        + paired_df['Lane']
        + '_'
        + paired_df['Machine_ID']
        + '_'
        + paired_df['Flow_cell']
    )
    paired_df['RGLB'] = paired_df['Sample_ID']

    paired_df = paired_df[fastq_list_header]
    with cpg_utils.to_path(fastq_list_file_path).open('w') as fastq_list_fh:
        paired_df.to_csv(fastq_list_fh, sep=',', index=False, header=True)


def run(outputs: dict[str, cpg_utils.Path], cohort: Cohort, manifest_file_path: cpg_utils.Path) -> None:
    # Sometimes the contents of sequencing_group.assays.meta['reads'] is a nested list
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

        items_to_process: list[Any] = nested_list

        for item in items_to_process:
            if isinstance(item, list):
                # If the item is a list, recursively flatten it and extend the main list
                filenames.extend(_flatten_list(item))  # pyright: ignore[reportUnknownArgumentType]
            elif isinstance(item, dict) and 'basename' in item:
                filenames.append(item['basename'])  # pyright: ignore[reportUnknownArgumentType]
        return filenames

    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        supplied_manifest_data: pd.DataFrame = pd.read_csv(manifest_fh)

    for sequencing_group in cohort.get_sequencing_groups():
        all_reads_for_sg: list[Any] = []
        for single_assay in sequencing_group.assays if sequencing_group.assays else []:  # pyright: ignore[reportUnknownVariableType]
            if 'reads' in single_assay.meta:
                reads_value: list[Any] = single_assay.meta.get('reads', [])  # pyright: ignore[reportUnknownVariableType]
                if isinstance(reads_value, list):
                    all_reads_for_sg.extend(_flatten_list(reads_value))  # pyright: ignore[reportUnknownArgumentType]
        if all_reads_for_sg:
            fq_df: pd.DataFrame = supplied_manifest_data[
                supplied_manifest_data[config_retrieve(['manifest', 'filenames'])].isin(all_reads_for_sg)
            ]
            if fq_df.empty:
                raise ValueError(f'No matching reads found in manifest for sequencing group {sequencing_group.id}')
            _write_fastq_list_file(fq_df=fq_df, outputs=outputs, sg_name=sequencing_group.name)
