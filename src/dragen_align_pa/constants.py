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
# the real `fil.…` ID once the corresponding upload lands. Any value starting
# with this prefix is rejected by `resolve_ica_file_id` to keep stock-config
# submissions from passing the literal sentinel to ICA and failing opaquely
# there.
_TODO_FID_PREFIX: Final = 'fil.TODO_'
_TODO_FID: Final = f'{_TODO_FID_PREFIX}REPLACE_AFTER_ICA_UPLOAD'

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
    'Twist_VCGS_Exome_Covered_Targets_hg38.bed': 'fil.60130ada16264ed28a7008deb1d54636',
    'S30409818_Regions.bed': 'fil.5d4da6b9c2c74abcb00608deb2229b88',
    'S30409818_Covered.bed': 'fil.625777f457c84b508a7108deb1d54636',
}


def resolve_ica_file_id(name: str) -> str:
    """Look up the ICA file ID for a registered BED basename.

    Raises `KeyError` naming the unknown entry and listing the registered
    names, so a config typo surfaces at submitter startup with an
    immediately actionable message. Raises `ValueError` if the registered
    value is still the `fil.TODO_…` placeholder — that means the basename is
    registered but the upload to ICA hasn't happened yet, which would
    otherwise surface as an opaque ICA "no such file" failure mid-run.
    """
    try:
        file_id = ICA_FILE_IDS[name]
    except KeyError:
        registered = sorted(ICA_FILE_IDS.keys())
        raise KeyError(
            f'{name!r} is not a registered ICA file basename. '
            f'Add it to ICA_FILE_IDS in dragen_align_pa.constants, or check for typos. '
            f'Registered names: {registered}',
        ) from None
    if file_id.startswith(_TODO_FID_PREFIX):
        raise ValueError(
            f'ICA file ID for {name!r} has not been registered yet — the entry in '
            f'dragen_align_pa.constants.ICA_FILE_IDS still holds the placeholder '
            f'{file_id!r}. Upload the file to ICA and replace the placeholder with '
            f'the real fil.… ID before running.',
        )
    return file_id
