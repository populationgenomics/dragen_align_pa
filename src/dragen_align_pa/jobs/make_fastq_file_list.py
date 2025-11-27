import re

import cpg_utils
import pandas as pd
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve
from loguru import logger


def _write_fastq_list_file(fq_df: pd.DataFrame, outputs: dict[str, cpg_utils.Path], sg_name: str) -> None:
    fastq_list_file_path: cpg_utils.Path = outputs[sg_name]
    logger.info(f'Writing FASTQ list file for sequencing group {sg_name} to {fastq_list_file_path}')
    fastq_list_header: list[str] = [
        'RGID',
        'RGSM',
        'RGLB',
        config_retrieve(['manifest', 'lane']),
        'Read1File',
        'Read2File',
    ]
    adaptors: re.Pattern[str] = re.compile('_([ACGT]+-[ACGT]+)_')

    fq_df['adaptors'] = fq_df[config_retrieve(['manifest', 'filenames'])].str.extract(adaptors, expand=False)
    fq_df['Sample_Key'] = fq_df[config_retrieve(['manifest', 'filenames'])].str.replace(
        r'_R[12]\.fastq\.gz', '', regex=True
    )

    r1_df = (
        fq_df[fq_df[config_retrieve(['manifest', 'filenames'])].str.contains(r'_R1\.', regex=True)]
        .copy()
        .set_index('Sample_Key')
    )
    r2_df = (
        fq_df[fq_df[config_retrieve(['manifest', 'filenames'])].str.contains(r'_R2\.', regex=True)]
        .copy()
        .set_index('Sample_Key')
    )

    paired_df = r1_df.merge(
        r2_df[[config_retrieve(['manifest', 'filenames']), config_retrieve(['manifest', 'checksum'])]],
        left_index=True,
        right_index=True,
        suffixes=('_R1', '_R2'),
    )

    paired_df = paired_df.rename(
        columns={
            config_retrieve(['manifest', 'filenames']) + '_R1': 'Read1File',
            config_retrieve(['manifest', 'filenames']) + '_R2': 'Read2File',
        }
    )
    paired_df = paired_df.rename(
        columns={
            config_retrieve(['manifest', 'checksum']) + '_R1': 'R1_Checksum',
            config_retrieve(['manifest', 'checksum']) + '_R2': 'R2_Checksum',
        },
        errors='ignore',
    ).reset_index()

    # Drop the temporary Sample_Key column as it's no longer needed.
    paired_df = paired_df.drop(columns=['Sample_Key'])
    paired_df['RGSM'] = sg_name
    paired_df['RGID'] = (
        paired_df['RGSM']
        + '_'
        + paired_df['adaptors']
        + '_'
        + paired_df[config_retrieve(['manifest', 'lane'])]
        + '_'
        + paired_df[config_retrieve(['manifest', 'machine_id'])]
        + '_'
        + paired_df[config_retrieve(['manifest', 'flowcell'])]
    )
    paired_df['RGLB'] = paired_df[config_retrieve(['manifest', 'sample_id'])]

    paired_df = paired_df[fastq_list_header]
    with cpg_utils.to_path(fastq_list_file_path).open('w') as fastq_list_fh:
        paired_df.to_csv(fastq_list_fh, sep=',', index=False, header=True)


def run(outputs: dict[str, cpg_utils.Path], cohort: Cohort, manifest_file_path: cpg_utils.Path) -> None:
    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        supplied_manifest_data: pd.DataFrame = pd.read_csv(manifest_fh)

    for sequencing_group in cohort.get_sequencing_groups():
        fq_df: pd.DataFrame = supplied_manifest_data[
            supplied_manifest_data[config_retrieve(['manifest', 'cpg_id'])].isin([sequencing_group.name])
        ]
        if fq_df.empty:
            raise ValueError(f'No matching reads found in manifest for sequencing group {sequencing_group.id}')
        _write_fastq_list_file(fq_df=fq_df, outputs=outputs, sg_name=sequencing_group.name)
