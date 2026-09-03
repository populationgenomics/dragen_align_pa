"""
This module defines shared data structures and types used across the pipeline.
"""

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class FileTypeSpec:
    """Groups the ICA-side file suffixes and the GCS prefix for a per-file downloaded file type."""

    gcs_prefix: str  # e.g., 'cram'
    data_suffix: str  # e.g., 'cram'
    index_suffix: str  # e.g., 'cram.crai'
    md5_suffix: str  # e.g., 'md5sum' or 'md5'

    def ica_names(self, sg_name: str) -> frozenset[str]:
        """The ICA object names this file type owns: data, index and both md5 companions.

        The bulk metrics download leaves all four to the per-file stage (which writes the
        data md5 beside the data file under the `.md5sum` name); the index md5 is not fetched
        at all, since the reheader job recomputes it for the recal gVCF.
        """
        data = f'{sg_name}.{self.data_suffix}'
        index = f'{sg_name}.{self.index_suffix}'
        return frozenset({data, index, f'{data}.{self.md5_suffix}', f'{index}.{self.md5_suffix}'})


CRAM: Final = FileTypeSpec(gcs_prefix='cram', data_suffix='cram', index_suffix='cram.crai', md5_suffix='md5sum')
BASE_GVCF: Final = FileTypeSpec(
    gcs_prefix='base_gvcf',
    data_suffix='hard-filtered.gvcf.gz',
    index_suffix='hard-filtered.gvcf.gz.tbi',
    md5_suffix='md5sum',
)
# MLR writes its md5 companions as `.md5`, unlike DRAGEN's `.md5sum`.
RECAL_GVCF: Final = FileTypeSpec(
    gcs_prefix='recal_gvcf',
    data_suffix='hard-filtered.recal.gvcf.gz',
    index_suffix='hard-filtered.recal.gvcf.gz.tbi',
    md5_suffix='md5',
)
PER_FILE_SPECS: Final[tuple[FileTypeSpec, ...]] = (CRAM, BASE_GVCF, RECAL_GVCF)
