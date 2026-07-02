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


# ICA projects
ICA_PROJECT_IDS: Final[dict[str, str]] = {
    'OurDNA-DRAGEN-378': '5c3a60b0-1458-4e37-8877-ec6b25dc4003',
    'ourdna-dragen-mlr-jobs': 'f2f55709-f8d4-4364-bb04-c41975d4c0ed',
    'ourdna-data-upload-agrf': 'e7a1d085-f12e-4cff-acda-2334338585a8',
    'tenk10k': '<>',
    'tenk10k-mlr-jobs': '<>',
}

# MLR setup information
mlr_hash_table = 'ica://ourdna-dragen-mlr-jobs/data/ref/hashtable/hg38_alt_masked_graph_v2/DRAGEN/9'
ANALYSIS_INSTANCE_TIER: Final[str] = 'economy'

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
# Scope: target BEDs, QC coverage-region BEDs, and the cross-contamination VCF
# today. When other reference assets (FASTAs, GFFs, …) move under the same
# name-based contract, add them here too.
ICA_FILE_IDS: Final[dict[str, str]] = {
    'Twist_VCGS_Exome_Covered_Targets_hg38.bed': 'fil.60130ada16264ed28a7008deb1d54636',
    'S30409818_Regions.bed': 'fil.5d4da6b9c2c74abcb00608deb2229b88',
    'S30409818_Covered.bed': 'fil.625777f457c84b508a7108deb1d54636',
    # QC reference assets. The cross-contamination VCF is seqtype-agnostic; the
    # two coverage-region BEDs are WGS QC regions (wired into [ica.qc.genome]).
    'SNP_NCBI_GRCh38.vcf': 'fil.fd99781d0a9044c1441608de15afe1ac',
    'wgs_coverage_regions.hg38_minus_N.interval_list.bed': 'fil.434cd66e92844a1f1f6a08de15159355',
    'acmg59_allofus_19dec2019.GRC38.wGenes.NEW.bed': 'fil.d37b27f6c28a4f6852ae08de17298bbd',
    # Folders in ICA containing the reference genomes for CRAM -> BAM conversion
    # When realigning existing non-Dragen CRAMs
    'hg38_masked.fasta': 'fol.df2129db2c88419cbe0408dd600dce1f',
    'hg38_unmasked.fasta': 'fol.d45ec3a17cf241f5b61b08dd7c524fb7',
}


# Canonical exome design names. Each exome cohort resolves to exactly one.
CANONICAL_DESIGN_CREV2: Final = 'CREv2'
CANONICAL_DESIGN_TWIST: Final = 'TWIST'

# Exact-match map from metamist assay.meta['sequencing_library'] values to
# canonical designs. Populate as new values are encountered in metamist. Exact
# match (not substring) avoids prefix collisions like SSQXTCRE being a prefix of SSQXTCREV2.
DESIGN_TO_CANONICAL: Final[dict[str, str]] = {
    'SSQXTCREV2': CANONICAL_DESIGN_CREV2,
    'SSXTLICREV2': CANONICAL_DESIGN_CREV2,
    'TwistWES1VCGS1': CANONICAL_DESIGN_TWIST,
    'AgilentCREv2WES': CANONICAL_DESIGN_CREV2,
}

# Per canonical design: the BED basenames known to belong to it. The validator
# in utils.assert_cohort_design_matches_configured_bed uses this to check that
# the basenames named in [presets.exome.bed_names] match the cohort's resolved
# design. All entries must also be registered in ICA_FILE_IDS above.
# CRE (v1) intentionally has no entry: Agilent doesn't distribute a Regions
# BED for CRE, and the Covered BED hasn't been uploaded yet. A CRE cohort run
# will fail at the validator with "No DESIGN_TO_BEDS entry for design 'CRE'"
# until the Covered BED is registered.
DESIGN_TO_BEDS: Final[dict[str, frozenset[str]]] = {
    CANONICAL_DESIGN_CREV2: frozenset({'S30409818_Regions.bed', 'S30409818_Covered.bed'}),
    CANONICAL_DESIGN_TWIST: frozenset({'Twist_VCGS_Exome_Covered_Targets_hg38.bed'}),
}


def resolve_ica_project_id(name: str) -> str:
    """Look up the ICA project ID for a registered project name.

    Raises `KeyError` naming the unknown entry and listing the registered
    names, so a config typo surfaces at submitter startup with an
    immediately actionable message.
    """
    try:
        return ICA_PROJECT_IDS[name]
    except KeyError as e:
        raise KeyError(f'Unknown ICA project name: {name}. Registered names: {list(ICA_PROJECT_IDS.keys())}') from e


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
