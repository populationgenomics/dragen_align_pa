"""
This module defines shared data structures and types used across the pipeline.
"""

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class FileTypeSpec:
    """The ICA-side names of a per-file downloaded file type: data, index and their md5 companions."""

    data_suffix: str  # e.g., 'cram'
    index_suffix: str  # e.g., 'cram.crai'
    md5_suffix: str  # suffix of the data file's md5 companion, e.g., 'md5sum' or 'md5'
    index_md5_suffix: str | None = None  # None when the producer writes no md5 for the index

    # The bulk metrics download excludes every name here so the per-file stage alone
    # decides where the data md5 lands (beside the data file, always as `.md5sum`). An
    # index md5, where the producer writes one, is not fetched by anything: the reheader
    # job recomputes it for the recal gVCF, and DRAGEN writes none for the cram or base gVCF.
    def data_name(self, sg_name: str) -> str:
        """The ICA object name of the data file, e.g. `SG1.cram`."""
        return f'{sg_name}.{self.data_suffix}'

    def index_name(self, sg_name: str) -> str:
        """The ICA object name of the index, e.g. `SG1.cram.crai`."""
        return f'{sg_name}.{self.index_suffix}'

    def data_md5_name(self, sg_name: str) -> str:
        """The ICA object name of the data file's md5 companion, e.g. `SG1.cram.md5sum`."""
        return f'{self.data_name(sg_name)}.{self.md5_suffix}'

    def index_md5_name(self, sg_name: str) -> str | None:
        """The ICA object name of the index's md5 companion, or None when the producer writes none."""
        if self.index_md5_suffix is None:
            return None
        return f'{self.index_name(sg_name)}.{self.index_md5_suffix}'

    def ica_names(self, sg_name: str) -> frozenset[str]:
        """Every ICA object name this file type owns: data, index, the data md5 and any index md5."""
        names = {self.data_name(sg_name), self.index_name(sg_name), self.data_md5_name(sg_name)}
        index_md5 = self.index_md5_name(sg_name)
        if index_md5 is not None:
            names.add(index_md5)
        return frozenset(names)


CRAM: Final = FileTypeSpec(data_suffix='cram', index_suffix='cram.crai', md5_suffix='md5sum')
BASE_GVCF: Final = FileTypeSpec(
    data_suffix='hard-filtered.gvcf.gz',
    index_suffix='hard-filtered.gvcf.gz.tbi',
    md5_suffix='md5sum',
)
# MLR writes `.md5` companions (unlike DRAGEN's `.md5sum`), for the index as well as the data.
RECAL_GVCF: Final = FileTypeSpec(
    data_suffix='hard-filtered.recal.gvcf.gz',
    index_suffix='hard-filtered.recal.gvcf.gz.tbi',
    md5_suffix='md5',
    index_md5_suffix='md5',
)
PER_FILE_SPECS: Final[tuple[FileTypeSpec, ...]] = (CRAM, BASE_GVCF, RECAL_GVCF)
