from typing import Final

import cpg_utils
from cpg_utils.config import config_retrieve, output_path

READS_TYPE: Final = config_retrieve(['workflow', 'reads_type'], default=None).lower()
assert READS_TYPE in {'fastq', 'cram'}, (
    f'Unsupported reads type: {READS_TYPE}. Valid options are fastq or cram. Please set this in the configuration using [workflow][reads_type].'  # noqa: E501
)
BUCKET: Final = cpg_utils.to_path(output_path(suffix=''))
BUCKET_NAME: Final = str(BUCKET).removeprefix('gs://').removesuffix('/')
DRAGEN_VERSION: Final = config_retrieve(['ica', 'pipelines', 'dragen_version'])
