import cpg_utils
import pandas as pd
from loguru import logger

from dragen_align_pa import utils


def run(  # noqa: PLR0915
    ica_md5sum_file_path: cpg_utils.Path,
    cohort_name: str,
    success_output_path: cpg_utils.Path,
    manifest_file_path: cpg_utils.Path,
) -> None:
    """
    Compares MD5 sums from ICA pipeline output with the manifest.
    Writes an error log if mismatches are found and raises an Exception.
    Writes a success file if all checksums match.
    """
    mismatched_files: list[str] = []
    error_log_path: cpg_utils.Path = utils.get_prep_path(filename=f'{cohort_name}_md5_errors.log')

    try:
        # Load manifest data
        with cpg_utils.to_path(manifest_file_path).open() as manifest_fh:
            supplied_manifest_data: pd.DataFrame = pd.read_csv(
                manifest_fh,
                usecols=['Filenames', 'Checksum'],
                dtype={'Filenames': str, 'Checksum': str},  # Use str dtype
            ).set_index('Filenames')  # Use filename as index for easier lookup

        # Load ICA MD5 data
        with ica_md5sum_file_path.open('r') as ica_md5_fh:
            ica_md5_data: pd.DataFrame = pd.read_csv(
                ica_md5_fh,
                sep=r'\s+',  # Matches one or more whitespace chars
                header=None,  # Explicitly state no header
                names=['IcaChecksum', 'IcaRawPath'],  # Use descriptive names
                dtype={'IcaChecksum': str, 'IcaRawPath': str},
            )
            # Extract filename from the path provided by ICA MD5 tool
            # Assuming format like '/path/to/filename.fastq.gz'
            ica_md5_data['Filenames'] = ica_md5_data['IcaRawPath'].str.split('/').str[-1]
            ica_md5_data = ica_md5_data.set_index('Filenames')[['IcaChecksum']]  # Keep only checksum, index by filename

        # Merge (outer join handles files present in only one list)
        merged_checksum_data: pd.DataFrame = supplied_manifest_data.join(
            ica_md5_data,
            how='outer',  # Keep all files from both lists
        )

        # Iterate and check for mismatches or missing files
        for filename, row in merged_checksum_data.iterrows():
            manifest_checksum = row['Checksum']
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
            else:
                # Checksums match
                logger.info(f"Checksum OK for '{filename}'")

    except FileNotFoundError as e:
        logger.error(f'Input file not found during MD5 validation: {e}')
        raise
    except pd.errors.EmptyDataError as e:
        logger.error(f'Input file is empty or invalid during MD5 validation: {e}')
        raise
    except Exception as e:
        logger.error(f'An unexpected error occurred during MD5 validation: {e}')
        raise

    # Handle results
    if mismatched_files:
        logger.error(f'{len(mismatched_files)} files failed MD5 validation.')
        # Write detailed error log
        try:
            with error_log_path.open('w') as error_fh:
                error_fh.write('Files with MD5 validation errors:\n')
                error_fh.write('===================================\n')
                for line in mismatched_files:
                    error_fh.write(f'- {line}\n')
            logger.info(f'Detailed error report written to {error_log_path}')
        except Exception as log_e:  # noqa: BLE001
            logger.error(f'Failed to write MD5 error log to {error_log_path}: {log_e}')

        # Raise a clear exception
        raise Exception(
            f'{len(mismatched_files)} files failed MD5 validation. Check the log file at {error_log_path} for details.'
        )
    # All files validated successfully, write the success file
    logger.info('All MD5 checksums validated successfully.')
    try:
        with success_output_path.open('w') as success_fh:
            success_fh.write('SUCCESS')
        logger.info(f'Success file written to {success_output_path}')
    except Exception as success_e:
        logger.error(f'Failed to write MD5 success file to {success_output_path}: {success_e}')
        # Raise an error here as well, because the stage expects this file
        raise
