from typing import Final, TypedDict

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
# with this prefix is rejected by the file-ID resolvers in constants_registry
# (which reads TODO_FID_PREFIX) to keep stock-config submissions from passing
# the literal sentinel to ICA and failing opaquely there.
TODO_FID_PREFIX: Final = 'fil.TODO_'
_TODO_FID: Final = f'{TODO_FID_PREFIX}REPLACE_AFTER_ICA_UPLOAD'


# ICA project setup
# Contains the following:
# A project block informing the user of the project names and IDs for running pipelines in ICA
# An api key dict recording the name of the apikey secret in secretsmanager
# An ICA file ID for the MLR config JSON
class IcaProject(TypedDict):
    project_name: str
    project_id: str | None


class IcaFamilySetup(TypedDict):
    projects: dict[str, IcaProject]
    api_key: dict[str, str]
    mlr_config_json: dict[str, str]
    can_delete_fastq: bool


ICA_PROJECT_SETUP: Final[dict[str, IcaFamilySetup]] = {
    'ourdna': {
        'projects': {
            'dragen_align': {
                'project_name': 'OurDNA-DRAGEN-378',
                'project_id': '5c3a60b0-1458-4e37-8877-ec6b25dc4003',
            },
            'dragen_mlr': {
                'project_name': 'ourdna-dragen-mlr-jobs',
                'project_id': 'f2f55709-f8d4-4364-bb04-c41975d4c0ed',
            },
            'fastq_upload': {
                'project_name': 'ourdna-data-upload-agrf',
                'project_id': 'e7a1d085-f12e-4cff-acda-2334338585a8',
            },
        },
        'api_key': {'name': 'apiKey'},
        'mlr_config_json': {'ica_file_id': 'fil.a1007afeae3741bb815108dedba2c6eb'},
        'can_delete_fastq': True,
    },
    'tenk10k': {
        'projects': {
            'dragen_align': {
                'project_name': 'Tenk10k_Dragen_378',
                'project_id': 'b9e2edbd-6ff8-4e76-b839-bb029e59fb73',
            },
            'dragen_mlr': {
                'project_name': 'Tenk10K_Dragen_MLR_Jobs',
                'project_id': '16bb091c-5866-4e39-929f-2b678457b772',
            },
            'fastq_upload': {
                'project_name': 'tenk10k_fastq_upload',
                'project_id': None,  # Explicit `None`, as we have to ask collaborators to delete this data.
            },
        },
        'api_key': {'name': 'tenk10k_apiKey'},
        'mlr_config_json': {'ica_file_id': _TODO_FID},
        'can_delete_fastq': False,  # Controlled by collaborators
    },
}

# MLR setup information.
# Project-relative MLR hash table path; the ICA project is the configured family's `dragen_mlr`
# project, resolved at use time via `IcaPath.from_relpath(MLR_HASH_TABLE_RELPATH).as_url(ROLE_DRAGEN_MLR)`.
# The hashtable is provided as a part of the popgen cli package in every ICA project and always exists at this path.
# We always use an 'economy' instance.
MLR_HASH_TABLE_RELPATH: Final = 'data/ref/hashtable/hg38_alt_masked_graph_v2/DRAGEN/9'
ANALYSIS_INSTANCE_TIER: Final[str] = 'economy'


# Registry of reference files/folders in ICA (BEDs, QC assets, reference genomes). Referenced
# by basename from config or directly in code. Resolve via `constants_registry.resolve_ica_file_id`.
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


# CNV Panels of Normals (WES only), keyed by panel name. Each panel holds:
#   - 'pon_list_file'  -> the `fil.…` ID of the `<panel>.normals.txt` list file
#     (DRAGEN reads that list, by basename, via --cnv-normals-list),
#   - 'count_file_ids' -> the list of `fil.…` IDs for the per-SG count files.
# We store only file IDs — never the count-file basenames. Those basenames embed
# CPG sample IDs (blocked by the CPG-ID pre-commit hook) and serve no purpose
# here: ICA localises each file by its own registered name, and DRAGEN reads the
# actual filenames from the `.normals.txt` list at runtime. The submitter sends
# every ID (list + counts) as `additional_files` data inputs and derives the
# --cnv-normals-list basename as `<panel>.normals.txt` from the panel name.
# Built and printed by scripts/build_cnv_panel_of_normals.py; a run selects a
# panel by name via [presets.exome].cnv_normals_panel, so the operator never
# lists file IDs by hand. Resolve via constants_registry.resolve_cnv_normals_panel.
ICA_PON_FILE_IDS: Final[dict[str, dict[str, str | list[str]]]] = {}


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
