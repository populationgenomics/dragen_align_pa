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

# Canonical exome design names. Each per-cohort exome run must resolve to exactly one.
CANONICAL_DESIGN_CRE: Final = 'CRE'
CANONICAL_DESIGN_CREV2: Final = 'CREv2'
CANONICAL_DESIGN_TWIST: Final = 'TWIST'
CANONICAL_DESIGNS: Final = frozenset({CANONICAL_DESIGN_CRE, CANONICAL_DESIGN_CREV2, CANONICAL_DESIGN_TWIST})

# Map raw metamist sequencing_library / capture-kit values to canonical designs. Populate
# as new values are encountered in metamist (the assay.meta['sequencing_library'] field is
# the source). Codes encountered at CPG to date appear as substrings of read filenames or
# sequencing_library.
DESIGN_TO_CANONICAL: Final[dict[str, str]] = {
    'SSXTLICRE': CANONICAL_DESIGN_CRE,
    'SSQXTCREV2': CANONICAL_DESIGN_CREV2,
    'SSXTLICREV2': CANONICAL_DESIGN_CREV2,
    'TwistWES1VCGS1': CANONICAL_DESIGN_TWIST,
}

# Canonical design -> ICA file ID for the (unpadded) target bed used for vc_target,
# cnv_target, and sv_call_regions. Populate with real fil.* IDs once beds are uploaded
# to ICA. Placeholders below are intentionally not in fil.XXX format so they cannot be
# mistaken for real ICA artefacts.
CANONICAL_TO_BED_ID: Final[dict[str, str]] = {
    CANONICAL_DESIGN_CRE: 'PLACEHOLDER-cre-bed-not-yet-uploaded-to-ica',
    CANONICAL_DESIGN_CREV2: 'PLACEHOLDER-crev2-bed-not-yet-uploaded-to-ica',
    CANONICAL_DESIGN_TWIST: 'PLACEHOLDER-twist-bed-not-yet-uploaded-to-ica',
}
