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
ICA_PROJECT_IDS: Final[dict[str, dict[str, str | None]]] = {
    'ourdna': {
        'OurDNA-DRAGEN-378': '5c3a60b0-1458-4e37-8877-ec6b25dc4003',
        'ourdna-dragen-mlr-jobs': 'f2f55709-f8d4-4364-bb04-c41975d4c0ed',
        'ourdna-data-upload-agrf': 'e7a1d085-f12e-4cff-acda-2334338585a8',
    },
    'tenk10k': {
        'Tenk10k_Dragen_378': 'b9e2edbd-6ff8-4e76-b839-bb029e59fb73',
        'Tenk10K_Dragen_MLR_Jobs': '16bb091c-5866-4e39-929f-2b678457b772',
        'tenk10k_fastq_upload': None,  # Explicit `None`, as we have to ask collaborators to delete this data.
    },
}

# Per dataset family (see `project_family`): the field in the `illumina_cpg_workbench_api`
# secret that holds that dataset's ICA API key. OurDNA uses the base `apiKey`; each other
# dataset has its own. Add an entry when onboarding a dataset with a distinct API key.
ICA_API_KEY_FIELD_BY_FAMILY: Final[dict[str, str]] = {
    'ourdna': 'apiKey',
    'tenk10k': 'tenk10k_apiKey',
}

# ICA projects we are permitted to delete uploaded FASTQ data from. Every other FASTQ-upload
# project is collaborator-owned and must be registered with a `None` ID in ICA_PROJECT_IDS so
# `DeleteDataInIca` skips it (we ask collaborators to delete) rather than attempting a delete
# we're not authorised to make. A registered, non-None FASTQ project that is NOT listed here is
# treated as a misconfiguration and rejected at delete time.
FASTQ_DELETABLE_PROJECTS: Final[frozenset[str]] = frozenset({'ourdna-data-upload-agrf'})

# MLR setup information.
# Project-relative MLR hash table path; the ICA project comes from `[ica.projects].dragen_mlr`
# at use time via `IcaPath.from_relpath(MLR_HASH_TABLE_RELPATH).as_url('dragen_mlr')`.
# The hashtable is provided as a part of the popgen cli package in every ICA project and always exists at this path.
MLR_HASH_TABLE_RELPATH: Final = 'data/ref/hashtable/hg38_alt_masked_graph_v2/DRAGEN/9'
ANALYSIS_INSTANCE_TIER: Final[str] = 'economy'

# Placeholder marker for ICA file IDs that haven't been minted yet (i.e. the
# files exist in GCS but haven't been uploaded to ICA). Replace per-entry with
# the real `fil.…` ID once the corresponding upload lands. Any value starting
# with this prefix is rejected by the file-ID resolvers in constants_registry
# (which reads TODO_FID_PREFIX) to keep stock-config submissions from passing
# the literal sentinel to ICA and failing opaquely there.
TODO_FID_PREFIX: Final = 'fil.TODO_'
_TODO_FID: Final = f'{TODO_FID_PREFIX}REPLACE_AFTER_ICA_UPLOAD'

# Registry of reference files/folders in ICA (BEDs, QC assets, reference genomes). Referenced
# by basename from config or directly in code. Resolve via `resolve_ica_file_id`.
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

# The MLR pipeline's config JSON lives in the ICA project MLR runs in, so this is keyed by the
# `[ica.projects].dragen_mlr` project name (distinct from ICA_FILE_IDS' reference-asset basenames).
# Resolve via `resolve_mlr_config_file_id`.
MLR_CONFIG_FILE_ID_BY_PROJECT: Final[dict[str, str]] = {
    'ourdna-dragen-mlr-jobs': 'fil.91c3e63114fc43dc31ed08dde927d6b4',
    'Tenk10K_Dragen_MLR_Jobs': _TODO_FID,
}


# Canonical exome design names. Each exome cohort resolves to exactly one.
CANONICAL_DESIGN_CREV2: Final = 'CREv2'
CANONICAL_DESIGN_TWIST: Final = 'TWIST'

# Exact-match map from the SG's `sg.meta['sequencing_library']` values to
# canonical designs. Populate as new values are encountered in metamist. Exact
# match (not substring) avoids prefix collisions like SSQXTCRE being a prefix of SSQXTCREV2.
DESIGN_TO_CANONICAL: Final[dict[str, str]] = {
    'SSQXTCREV2': CANONICAL_DESIGN_CREV2,
    'SSXTLICREV2': CANONICAL_DESIGN_CREV2,
    'TwistWES1VCGS1': CANONICAL_DESIGN_TWIST,
    'AgilentCREv2WES': CANONICAL_DESIGN_CREV2,
}

# Per canonical design: the BED basenames known to belong to it. The validator
# in validator.assert_cohort_design_matches_configured_bed uses this to check that
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
