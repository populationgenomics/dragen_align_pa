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

# Placeholder marker for ICA file IDs not yet minted (the files exist in GCS but aren't uploaded
# to ICA). Replace per-entry with the real `fil.…` ID once the upload lands. The file-ID
# resolvers in constants_registry reject any value with this prefix, so a stock-config
# submission fails loud here rather than opaquely inside ICA.
TODO_FID_PREFIX: Final = 'fil.TODO_'
_TODO_FID: Final = f'{TODO_FID_PREFIX}REPLACE_AFTER_ICA_UPLOAD'


# The config-free DRAGEN batch / pipeline-management constants live in `batch_constants.py` so
# they can be imported without a loaded config.


# ICA project setup: per-family project names+IDs, the apikey secret name, and the MLR config
# JSON file ID.
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

# MLR setup. Project-relative MLR hash table path, resolved at use time via
# `IcaPath.from_relpath(MLR_HASH_TABLE_RELPATH).as_url(ROLE_DRAGEN_MLR)`. The hashtable ships
# with the popgen cli package in every ICA project and always exists at this path. We always use
# an 'economy' instance.
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
ICA_PON_FILE_IDS: Final[dict[str, dict[str, str | list[str]]]] = {
    'COH13032_NSWHP_CREv2_Early': {
        'pon_list_file': 'fil.3e0dab60d8e4493a2af108dee90cdd0a',
        'count_file_ids': [
            'fil.02c5162d68d94c4d112a08dee8d37bcb',
            'fil.2d1f0f0c96af40d8435b08dee5978940',
            'fil.1a3c2bd5cebb4ce2a93908dee2d9bf8f',
            'fil.5c98659b365d437c83c308dee8e8fb33',
            'fil.2b67386174174f6360cd08dee8fe3764',
            'fil.2eb829bed942441fa93a08dee2d9bf8f',
            'fil.b426204757574ebf2f6908dee8fbb222',
            'fil.9a458c60f4f9472f2f6a08dee8fbb222',
            'fil.ca7b5405650a4cf260ce08dee8fe3764',
            'fil.b1f4a7618d12455183c608dee8e8fb33',
            'fil.32c60d20347d44bc83c708dee8e8fb33',
            'fil.c7dfb1500fca46fd2f6c08dee8fbb222',
            'fil.b8e6156ed0364487a93d08dee2d9bf8f',
            'fil.12b74fee797c4d90112f08dee8d37bcb',
            'fil.cdd61202065e466160d108dee8fe3764',
            'fil.70a88a76dadd4cb6435f08dee5978940',
            'fil.e66ddd1ed0cc4576113008dee8d37bcb',
            'fil.d7ae2ef94cfe493aa93f08dee2d9bf8f',
            'fil.06387792159e424d113108dee8d37bcb',
            'fil.469b1742f2d84d7960d208dee8fe3764',
            'fil.5383b6067dc9426283cb08dee8e8fb33',
            'fil.363d0f64d2cb4a52a94108dee2d9bf8f',
            'fil.c41bb885fe2f444c60d308dee8fe3764',
            'fil.29f22f1a28894d75436208dee5978940',
            'fil.9635578ff18543162f7108dee8fbb222',
            'fil.e6e394502d294b46a94208dee2d9bf8f',
            'fil.0a56bb228f80483fa94308dee2d9bf8f',
            'fil.7fe3589b5e584feea94408dee2d9bf8f',
            'fil.81a41fe9fc994c7f60d408dee8fe3764',
            'fil.813d977703e945cf60d508dee8fe3764',
            'fil.ef3ddbc2f686497f113808dee8d37bcb',
            'fil.a8cecb04ca23446f2f7408dee8fbb222',
            'fil.ce32120cf4184fc6436708dee5978940',
            'fil.f4ef6ee64998441e60d708dee8fe3764',
            'fil.bda8244aea5c4105113908dee8d37bcb',
            'fil.f85260301ce7410260d908dee8fe3764',
            'fil.6fd87340f15d4ad5436908dee5978940',
            'fil.62303f9a7f4047e9113a08dee8d37bcb',
            'fil.20ad16aafa114bad113b08dee8d37bcb',
            'fil.45aa4891e4f0473ba94808dee2d9bf8f',
            'fil.6828a8d22973471083d108dee8e8fb33',
            'fil.bf1bb914425844532f7608dee8fbb222',
            'fil.aade3f36ce0b423260da08dee8fe3764',
            'fil.4f0667f7e67449542f7708dee8fbb222',
            'fil.21238584efe94295436e08dee5978940',
            'fil.378ccc55f14242dda94b08dee2d9bf8f',
            'fil.a57905e3be724bef437008dee5978940',
            'fil.2c352219784042c1a94c08dee2d9bf8f',
            'fil.5d8eb3888664424883d308dee8e8fb33',
            'fil.f064381ee46f43cf437208dee5978940',
            'fil.322a6a5ae48a464460dd08dee8fe3764',
            'fil.f9ccd5ed3b314bb783d408dee8e8fb33',
            'fil.4ff0499f830d406283d608dee8e8fb33',
            'fil.c6459b62e3294a5b113f08dee8d37bcb',
            'fil.f71390d9ab994223a94f08dee2d9bf8f',
            'fil.5ed3b85e6bf94f77a95008dee2d9bf8f',
            'fil.08bbb3e7bb7141ca83d708dee8e8fb33',
            'fil.827c5765dbfb4cab83d808dee8e8fb33',
            'fil.4ac008ad86f548c0437708dee5978940',
            'fil.9a9e76dfd0bc4b37437808dee5978940',
            'fil.4d9a2a32edce4466437908dee5978940',
            'fil.04395b61ca474b23114308dee8d37bcb',
            'fil.2dc80648d56046cf114408dee8d37bcb',
            'fil.78c6c1cc4b814ff360e108dee8fe3764',
            'fil.a75bb36c4d22493a83da08dee8e8fb33',
            'fil.ef696f23ff0440a7437d08dee5978940',
            'fil.beef96c0718345f42f7d08dee8fbb222',
            'fil.98dab3c29b13402b437f08dee5978940',
            'fil.2ad2dd9f8bd24520114508dee8d37bcb',
            'fil.223aec39d5ba46e92f7f08dee8fbb222',
            'fil.a793c3e6823248b7a95908dee2d9bf8f',
            'fil.0ff627b8ecb8496a438208dee5978940',
            'fil.69b8fe116a5143212f8508dee8fbb222',
            'fil.00b4232140c847d2114908dee8d37bcb',
            'fil.b7de2ae02b03442a114b08dee8d37bcb',
            'fil.341ee83f25e448f82f8908dee8fbb222',
            'fil.2c53974960834bd92f8b08dee8fbb222',
            'fil.4c5ed276c12c4e8060e808dee8fe3764',
            'fil.e4d44394f00b49b5438e08dee5978940',
            'fil.a2be65c77ff340d083e908dee8e8fb33',
            'fil.b30aa31072004787439008dee5978940',
            'fil.90485b003e7743a260ed08dee8fe3764',
            'fil.94bd2ef865da447a60ee08dee8fe3764',
            'fil.e1987fb418d242b32f9808dee8fbb222',
            'fil.bdc854f08bd149512f9908dee8fbb222',
            'fil.690a1dfb4b3a446f83f008dee8e8fb33',
            'fil.eda3d2e84d48464da96608dee2d9bf8f',
            'fil.6b22c47aedc44471a96708dee2d9bf8f',
            'fil.2f2ea31c948d4d37115d08dee8d37bcb',
            'fil.1753f006fe834aba2f9f08dee8fbb222',
            'fil.0dd130b2783c412483f208dee8e8fb33',
            'fil.c086d82008d34b7c83f308dee8e8fb33',
            'fil.a1ed3a497e7a44682fa108dee8fbb222',
            'fil.7ec8396bef69491983f408dee8e8fb33',
            'fil.d1eb05f998c046de2fa308dee8fbb222',
            'fil.01eafd4fcb9c40b7439808dee5978940',
            'fil.ea9cd46b4b894945a96c08dee2d9bf8f',
            'fil.e804e0171ef644b42aee08dee90cdd0a',
            'fil.c5dc62bfa0ad449960f608dee8fe3764',
            'fil.fed0ccdc9a29428d83f508dee8e8fb33',
        ],
    },
    'COH13040_NSWHP_CREv2_Late': {
        'pon_list_file': 'fil.8a914791d8d64c472aef08dee90cdd0a',
        'count_file_ids': [
            'fil.05961de4d1ec4569a93808dee2d9bf8f',
            'fil.12f0527b90094855435c08dee5978940',
            'fil.913701ccd8d4416860cc08dee8fe3764',
            'fil.e3c1929783664845112b08dee8d37bcb',
            'fil.06c9f5e84fd24af4112c08dee8d37bcb',
            'fil.24292a4b8a9743f0112d08dee8d37bcb',
            'fil.87d7f5e5d8494fab83c408dee8e8fb33',
            'fil.2a9b49d003c8460f112e08dee8d37bcb',
            'fil.6590bdfd52fa419083c508dee8e8fb33',
            'fil.ac155a4cc4f54071a93c08dee2d9bf8f',
            'fil.ea5e2814020e41bb2f6b08dee8fbb222',
            'fil.e99eebc30a51406d435d08dee5978940',
            'fil.a531cd8078144a9260d008dee8fe3764',
            'fil.c616eb3ac7464fb1a93e08dee2d9bf8f',
            'fil.c852092a9cf74667435e08dee5978940',
            'fil.5e10dda70e404ad5436008dee5978940',
            'fil.582fcfefdb824a3883c908dee8e8fb33',
            'fil.e54442758d6449af83ca08dee8e8fb33',
            'fil.1e66f84742e74ac12f6e08dee8fbb222',
            'fil.b0d6e8c97ce84e4a113208dee8d37bcb',
            'fil.1ebd7b4ea6764924a94008dee2d9bf8f',
            'fil.6dba3c2f32174bc9436108dee5978940',
            'fil.f5fea31550c749c92f7008dee8fbb222',
            'fil.f1afca53d04e4a19436308dee5978940',
            'fil.726e27eb4f974b5e113408dee8d37bcb',
            'fil.ca7d54c1f923483d436408dee5978940',
            'fil.44186e4ccd20428d113508dee8d37bcb',
            'fil.8893810ce6474cfa436508dee5978940',
            'fil.ff5aa9d1cc0242d4436608dee5978940',
            'fil.5dd84ece554b40f0113708dee8d37bcb',
            'fil.2455e755e86b42642f7308dee8fbb222',
            'fil.c8b76d90cd5546f183cd08dee8e8fb33',
            'fil.17ef1572b1754a1760d608dee8fe3764',
            'fil.7fbe5ae7593946b760d808dee8fe3764',
            'fil.b3c88f903611498183ce08dee8e8fb33',
            'fil.7763245d5bbd4bcda94608dee2d9bf8f',
            'fil.5964515cd12a4ad383d008dee8e8fb33',
            'fil.5612abd4fd2f4e07a94708dee2d9bf8f',
            'fil.2562768b452e4abe436a08dee5978940',
            'fil.dede7b54905b4a52436b08dee5978940',
            'fil.351789936f3e4786a94908dee2d9bf8f',
            'fil.8c28a16ca6df4e1ba94a08dee2d9bf8f',
            'fil.879ec6d125844650436c08dee5978940',
            'fil.ff392d13c719436f436d08dee5978940',
            'fil.0c05e36c04434d09436f08dee5978940',
            'fil.509f1075e0d54a5d60db08dee8fe3764',
            'fil.e13d78b24d58459b60dc08dee8fe3764',
            'fil.9fa020815afa4a972f7908dee8fbb222',
            'fil.0f8cc707ebc7455b2f7a08dee8fbb222',
            'fil.642617338fd34b7a437308dee5978940',
            'fil.6068405ca11d490da94e08dee2d9bf8f',
            'fil.45771836a05a4e87437408dee5978940',
            'fil.007fb38edb1a49992f7b08dee8fbb222',
            'fil.d4bf3d9cf3f546f9437508dee5978940',
            'fil.c2f4c0e5f1a14699437608dee5978940',
            'fil.b637a628ee7c4eb5114008dee8d37bcb',
            'fil.519cdf8a655d479f60de08dee8fe3764',
            'fil.be8080b5e30142bba95208dee2d9bf8f',
            'fil.9d5dc99fab8748b7a95308dee2d9bf8f',
            'fil.19100d935eed46b683d908dee8e8fb33',
            'fil.ce4ebc4963724f28114208dee8d37bcb',
            'fil.13afd70592b047e060df08dee8fe3764',
            'fil.2213beb9067241b760e008dee8fe3764',
            'fil.7bb9618d7e7c41f460e208dee8fe3764',
            'fil.fe4c09727f8048c983db08dee8e8fb33',
            'fil.e588de99951e420c437e08dee5978940',
            'fil.c9e78a25f1824049a95708dee2d9bf8f',
            'fil.59846730bf8f40a2a95808dee2d9bf8f',
            'fil.6fbedb62dec7410983dc08dee8e8fb33',
            'fil.a6689a7e98a349852f7e08dee8fbb222',
            'fil.66adaf9c8e8a4e4e114608dee8d37bcb',
            'fil.7fb7e3b028644de8114708dee8d37bcb',
            'fil.49254f742ff646ff83df08dee8e8fb33',
            'fil.0765cd988dfb4824a95c08dee2d9bf8f',
            'fil.109774a9623b4ca9a95f08dee2d9bf8f',
            'fil.d920f14bf86f4ed283e008dee8e8fb33',
            'fil.a093ff5cf45d45cf115008dee8d37bcb',
            'fil.82b9a5cbc72448fc438908dee5978940',
            'fil.5b7f8818fea1492b115308dee8d37bcb',
            'fil.d7a6106fc47744bc83e808dee8e8fb33',
            'fil.04a0f1f27c874bcb438f08dee5978940',
            'fil.afff7cd874ef4bb42f9508dee8fbb222',
            'fil.19aa7a8971c14bf9a96508dee2d9bf8f',
            'fil.8aa9311637c246a283ef08dee8e8fb33',
            'fil.b8b2b2fc2b5d4607439408dee5978940',
            'fil.4d79175d05da4b622f9c08dee8fbb222',
            'fil.11227bebc46f446d83f108dee8e8fb33',
            'fil.f1211da3bb6849cd115c08dee8d37bcb',
            'fil.0d87c81138744ec260f008dee8fe3764',
            'fil.3f6026af7fa54abca96908dee2d9bf8f',
            'fil.ba49334de0754e5560f108dee8fe3764',
            'fil.2bddd7e75d3b4789439608dee5978940',
            'fil.b2accf525e504f4c60f308dee8fe3764',
            'fil.a17bde64d2814cfc439708dee5978940',
            'fil.268589ef346f4da960f408dee8fe3764',
            'fil.cf908313995e45832fa408dee8fbb222',
            'fil.9e03d41958364b4ca96b08dee2d9bf8f',
            'fil.48f8d229401f4074a96d08dee2d9bf8f',
            'fil.6ea19e5cceab4acf2fa508dee8fbb222',
            'fil.5761ddce8e794f072fa608dee8fbb222',
        ],
    },
    'COH13024_VCGS_CREv2': {
        'pon_list_file': 'fil.6106e08c6a06481a349c08dee667e4ef',
        'count_file_ids': [
            'fil.ec14c49845f74ca6a78408dee5e49068',
            'fil.29b0d2b1d75648d7209008dee601f3cd',
            'fil.3f657cc5eec5487a209508dee601f3cd',
            'fil.01914f60db5443e7ed7a08dee60f7aab',
            'fil.7e2b844114514a97ed7b08dee60f7aab',
            'fil.0071eda7c9fd414fa26608dee2d9bf8f',
            'fil.22d5b6e96941467ca78508dee5e49068',
            'fil.2efb5194521f4378209808dee601f3cd',
            'fil.dc79b6dbf35c4611a26908dee2d9bf8f',
            'fil.a5beb3381f2546283c5c08dee5978940',
            'fil.f84212620d914732ed7e08dee60f7aab',
            'fil.7a20ed4950f645dced7f08dee60f7aab',
            'fil.d01ceb40389d42c9edec08dee604496a',
            'fil.c76365d156e84d5ea26b08dee2d9bf8f',
            'fil.6b95006379d14f5deded08dee604496a',
            'fil.198034d0dd034702ed8008dee60f7aab',
            'fil.6b058d610a60465bedee08dee604496a',
            'fil.79c5bbbe3a0c4af8edef08dee604496a',
            'fil.87414e32e7e2489f3c5f08dee5978940',
            'fil.688abe7a19c048c1a26c08dee2d9bf8f',
            'fil.180c5ced1ccf4deced8308dee60f7aab',
            'fil.f12abbb3d2e340413c6308dee5978940',
            'fil.a031b69d0b5b49d5ed8408dee60f7aab',
            'fil.2180cd8191a542d3a26d08dee2d9bf8f',
            'fil.5aa7af2df7974cfc3c6408dee5978940',
            'fil.646fd380e8554c4a20a008dee601f3cd',
            'fil.0f23bc1ca2144e42a26e08dee2d9bf8f',
            'fil.8462f674f5424c2920a108dee601f3cd',
            'fil.9ce37910501949bbed8608dee60f7aab',
            'fil.cf9e1445592245103c6608dee5978940',
            'fil.6295d9df93c44cd9a78708dee5e49068',
            'fil.0397bf36bfee419320a308dee601f3cd',
            'fil.79efa823c025469fed8708dee60f7aab',
            'fil.9b41cdbde3e54f62edf208dee604496a',
            'fil.1564664746354fa8ed8808dee60f7aab',
            'fil.6fbfa5bbe19d4740edf408dee604496a',
            'fil.edfa2ac15e69404fed8908dee60f7aab',
            'fil.b0eb603ac6f4499420a508dee601f3cd',
            'fil.fa07c6aa3ca746ecedf508dee604496a',
            'fil.8deba75d72a24fb420a608dee601f3cd',
            'fil.9b5c75b0ef5d43a420a708dee601f3cd',
            'fil.1c7332ceddc043dfedf608dee604496a',
            'fil.fb01863f7ab84e6d3c6908dee5978940',
            'fil.a37bb82aaafe4b233c6a08dee5978940',
            'fil.9f3f5821d3ee4dfea78808dee5e49068',
            'fil.a84dd4ec2df1486a3c6b08dee5978940',
            'fil.b873434c4c9d46b920a908dee601f3cd',
            'fil.00f0261cc68047eaa27508dee2d9bf8f',
            'fil.700ea1d6b0f04beea27608dee2d9bf8f',
            'fil.8bff3ca986bf42e43c6c08dee5978940',
            'fil.86046172f6f44714a78908dee5e49068',
            'fil.5ee709d720b14484ed8c08dee60f7aab',
            'fil.303a7d8d8b47499cedfb08dee604496a',
            'fil.d2d48fa24c6b4146ed8d08dee60f7aab',
            'fil.233667e66501425820ac08dee601f3cd',
            'fil.3e8bf3e380bf4f3a20ad08dee601f3cd',
            'fil.b4d04237d9994905edfd08dee604496a',
            'fil.7a7e4f6e2e844b7720ae08dee601f3cd',
            'fil.e7461a44e15a457c3c7108dee5978940',
            'fil.97298575252940b7edff08dee604496a',
            'fil.ac04cf949e064f7e20af08dee601f3cd',
            'fil.43096f17b8784a6da78a08dee5e49068',
            'fil.ad6be31e874941ee20b008dee601f3cd',
            'fil.c7ec3ea786ed4766ee0008dee604496a',
            'fil.740fb456e2904ddb3c7208dee5978940',
            'fil.553b62f75a4541ba3c7408dee5978940',
            'fil.99142313fa4f4c16a27a08dee2d9bf8f',
            'fil.7be2a25282d549f53c7508dee5978940',
            'fil.77f5536d473c44d73c7608dee5978940',
            'fil.6d104d3763e046853c7708dee5978940',
            'fil.0ec5bd2f27424114a27b08dee2d9bf8f',
            'fil.348b490d5d1c46663c7808dee5978940',
            'fil.33c2a7c9c9144c95ee0308dee604496a',
            'fil.412c0f16ad864211ed9008dee60f7aab',
            'fil.95f9039cb4c84d4ca78c08dee5e49068',
            'fil.d29d82d39d844fe13c7a08dee5978940',
            'fil.d6b37259fc724263ed9108dee60f7aab',
            'fil.c1257e9a1dc24a79ed9208dee60f7aab',
            'fil.a93be563c8ad4f313c7b08dee5978940',
            'fil.4f26cb0e48a8487120b308dee601f3cd',
            'fil.16c0791ad54f4c4bee0508dee604496a',
            'fil.9a1399cab2234362a78d08dee5e49068',
            'fil.5231d9a88ed34ef520b408dee601f3cd',
            'fil.6731d7d5867c485aa27c08dee2d9bf8f',
            'fil.c3076fc1cae64745ed9608dee60f7aab',
            'fil.833a847829374cb93c7c08dee5978940',
            'fil.c4dcda117ee8415ba27d08dee2d9bf8f',
            'fil.d46112ad7cff4b80ed9808dee60f7aab',
            'fil.94def4a8008045563c7d08dee5978940',
            'fil.80e530b9c99e40633c7e08dee5978940',
            'fil.da25571f611249693c8008dee5978940',
            'fil.fb94bb0509964996a79108dee5e49068',
            'fil.af01d8beca1543ca20b608dee601f3cd',
            'fil.0b8b383b0e384e59ed9a08dee60f7aab',
            'fil.30d3a8ccf9e84706a79308dee5e49068',
            'fil.3bdc3fe5085f4138a79408dee5e49068',
            'fil.478bc777188c41dda28108dee2d9bf8f',
            'fil.2d93eccbe483477b3c8308dee5978940',
            'fil.935d23d856c7427ff94408dee677fc72',
            'fil.92e8e53f353f4c6d349b08dee667e4ef',
        ],
    },
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

# Per canonical design: the BED basenames known to belong to it, used by
# validator.assert_cohort_design_matches_configured_bed to check [presets.exome.bed_names]
# against the cohort's resolved design. All entries must also be in ICA_FILE_IDS above.
# CRE (v1) intentionally has no entry (Agilent ships no Regions BED for CRE and the Covered BED
# isn't uploaded yet), so a CRE cohort fails at the validator until the Covered BED is registered.
DESIGN_TO_BEDS: Final[dict[str, frozenset[str]]] = {
    CANONICAL_DESIGN_CREV2: frozenset({'S30409818_Regions.bed', 'S30409818_Covered.bed'}),
    CANONICAL_DESIGN_TWIST: frozenset({'Twist_VCGS_Exome_Covered_Targets_hg38.bed'}),
}
