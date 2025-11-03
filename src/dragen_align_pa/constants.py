from typing import Final

import cpg_utils
from cpg_utils.config import config_retrieve, output_path

READS_TYPE: Final = config_retrieve(['workflow', 'reads_type']).lower()
BUCKET: Final = cpg_utils.to_path(output_path(suffix=''))
BUCKET_NAME: Final = str(BUCKET).removeprefix('gs://').removesuffix('/')
DRAGEN_VERSION: Final = config_retrieve(['ica', 'pipelines', 'dragen_version'])
