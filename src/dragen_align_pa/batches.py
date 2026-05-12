from dataclasses import dataclass


@dataclass
class Batch:
    """Internal target representing a batch of SGs for the unified DRAGEN pipeline.

    Not a cpg-flow target type — only `.name` is consumed by `manage_ica_pipeline_loop`.
    """

    cohort_name: str
    batch_index: int
    sg_names: list[str]

    @property
    def name(self) -> str:
        return f'{self.cohort_name}-batch{self.batch_index:04d}'
