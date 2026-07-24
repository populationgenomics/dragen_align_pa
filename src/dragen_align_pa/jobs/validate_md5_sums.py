import cpg_utils
import pandas as pd
from cpg_utils.config import config_retrieve
from loguru import logger

from dragen_align_pa import utils


def run(
    ica_md5sum_file_path: cpg_utils.Path,
    cohort_name: str,
    success_output_path: cpg_utils.Path,
    manifest_file_path: cpg_utils.Path,
) -> None:
    """Compare ICA-output MD5 sums against the manifest.

    Writes a per-file error log and raises if any mismatch or missing file is found;
    writes a success file when all checksums match.
    """
    mismatched_files: list[str] = []
    error_log_path: cpg_utils.Path = utils.get_prep_path(filename=f'{cohort_name}_md5_errors.log')

    filenames_col = config_retrieve(['manifest', 'filenames'])
    checksum_col = config_retrieve(['manifest', 'checksum'])

    # Load manifest data, indexed by filename for easier lookup
    with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
        supplied_manifest_data: pd.DataFrame = pd.read_csv(
            manifest_fh,
            usecols=[filenames_col, checksum_col],
            dtype={filenames_col: str, checksum_col: str},
        ).set_index(filenames_col)

    # Load ICA MD5 data
    with ica_md5sum_file_path.open('r') as ica_md5_fh:
        ica_md5_data: pd.DataFrame = pd.read_csv(
            ica_md5_fh,
            sep=r'\s+',  # Matches one or more whitespace chars
            header=None,  # Explicitly state no header
            names=['IcaChecksum', 'IcaFileId', 'IcaRawPath'],  # Use descriptive names
            dtype={'IcaChecksum': str, 'IcaFileId': str, 'IcaRawPath': str},
        )
        # Extract filename from the ICA-provided path (e.g. '/path/to/filename.fastq.gz');
        # IcaFileId was only needed to read the file in correctly, so index by filename only.
        ica_md5_data['Filenames'] = ica_md5_data['IcaRawPath'].str.split('/').str[-1]
        ica_md5_data = ica_md5_data.set_index('Filenames')[['IcaChecksum']]

    # Outer join keeps files present in only one list
    merged_checksum_data: pd.DataFrame = supplied_manifest_data.join(ica_md5_data, how='outer')

    # Iterate and check for mismatches or missing files
    for filename, row in merged_checksum_data.iterrows():
        manifest_checksum = row[checksum_col]
        ica_checksum = row['IcaChecksum']

        if pd.isna(manifest_checksum):
            logger.error(f"File '{filename}' found in ICA output but MISSING from manifest.")
            mismatched_files.append(f'{filename} (MISSING FROM MANIFEST)')
        elif pd.isna(ica_checksum):
            logger.error(f"File '{filename}' found in manifest but MISSING from ICA output.")
            mismatched_files.append(f'{filename} (MISSING FROM ICA OUTPUT)')
        elif manifest_checksum.strip().lower() != ica_checksum.strip().lower():  # Case-insensitive compare
            logger.error(
                f"Checksum MISMATCH for '{filename}': Manifest='{manifest_checksum}', ICA='{ica_checksum}'"
            )
            mismatched_files.append(f'{filename} (CHECKSUM MISMATCH)')

    # Handle results
    if mismatched_files:
        logger.error(f'{len(mismatched_files)} files failed MD5 validation.')
        # Best-effort error log: a write failure here must not mask the validation error below.
        try:
            with error_log_path.open('w') as error_fh:
                error_fh.write('Files with MD5 validation errors:\n')
                error_fh.write('===================================\n')
                for line in mismatched_files:
                    error_fh.write(f'- {line}\n')
            logger.info(f'Detailed error report written to {error_log_path}')
        except Exception as log_e:  # noqa: BLE001
            logger.error(f'Failed to write MD5 error log to {error_log_path}: {log_e}')

        raise Exception(
            f'{len(mismatched_files)} files failed MD5 validation. Check the log file at {error_log_path} for details.'
        )

    # All files validated successfully, write the success file
    logger.info('All MD5 checksums validated successfully.')
    with success_output_path.open('w') as success_fh:
        success_fh.write('SUCCESS')
    logger.info(f'Success file written to {success_output_path}')
