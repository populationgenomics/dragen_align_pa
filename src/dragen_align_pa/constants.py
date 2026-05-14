from typing import Final

import cpg_utils
from cpg_utils.config import config_retrieve, output_path

_reads_type = config_retrieve(['workflow', 'reads_type'], default=None)
assert _reads_type in {'fastq', 'cram'}, (
    f'Unsupported reads type: {_reads_type}. Valid options are fastq or cram. Please set this in the configuration using [workflow][reads_type].'  # noqa: E501
)
READS_TYPE: Final = _reads_type.lower()
BUCKET: Final = cpg_utils.to_path(output_path(suffix=''))
BUCKET_NAME: Final = str(BUCKET).removeprefix('gs://').removesuffix('/')
DRAGEN_VERSION: Final = config_retrieve(['ica', 'pipelines', 'dragen_version'])

# Placeholder marker for ICA file IDs that haven't been minted yet (i.e. the
# files exist in GCS but haven't been uploaded to ICA). Replace per-entry with
# the real `fil.…` ID once the corresponding upload lands.
_TODO_FID: Final = 'fil.TODO_REPLACE_AFTER_ICA_UPLOAD'

# Registry of BED files referenced by name in config — populates `additional_files`
# at submission time. DRAGEN's CLI args take the file basename (ICA localises all
# inputs into a single working directory at run time); ICA's `additional_files`
# data input takes the file ID. The submitter resolves names → IDs via this map
# so config (`coverage_region_beds`, preset additional_args, etc.) stays
# human-readable and a typo fails fast at startup rather than as an opaque ICA
# error mid-run.
#
# Scope: BEDs only for now. When other reference assets (FASTAs, GFFs, …) move
# under the same name-based contract, add them here too.
ICA_FILE_IDS: Final[dict[str, str]] = {
    'Twist_VCGS_Exome_Covered_Targets_hg38.bed': _TODO_FID,
    'S06588914_Regions_hg38.bed': _TODO_FID,
    'S30409818_Regions.bed': _TODO_FID,
    'S30409818_Covered.bed': _TODO_FID,
}


def resolve_ica_file_id(name: str) -> str:
    """Look up the ICA file ID for a registered BED basename.

    Raises `KeyError` naming the unknown entry and listing the registered
    names, so a config typo surfaces at submitter startup with an
    immediately actionable message.
    """
    try:
        return ICA_FILE_IDS[name]
    except KeyError:
        registered = sorted(ICA_FILE_IDS.keys())
        raise KeyError(
            f'{name!r} is not a registered ICA file basename. '
            f'Add it to ICA_FILE_IDS in dragen_align_pa.constants, or check for typos. '
            f'Registered names: {registered}',
        ) from None
