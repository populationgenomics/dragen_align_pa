from __future__ import annotations

from typing import TYPE_CHECKING, cast

from cpg_utils.config import config_retrieve, image_path
from cpg_utils.hail_batch import Batch, get_batch

if TYPE_CHECKING:
    import cpg_utils
    from hailtop.batch.job import BashJob
    from hailtop.batch.resource import ResourceGroup


def reheader_mlr_gvcf(
    base_gvcf_path: cpg_utils.Path,
    recal_gvcf_path: cpg_utils.Path,
    reheadered_gvcf_path: cpg_utils.Path,
) -> BashJob:
    """
    Reheader the MLR gVCF to insert the gvcf ref block information from the original gVCF header.
    """

    b: Batch = get_batch()
    job: BashJob = b.new_bash_job(name='reheader_mlr_gvcf')
    job.image(image_path('bcftools', '1.23-2'))
    job.storage(storage=config_retrieve(['workflow', 'reheader_mlr_gvcf', 'storage'], default='16GB'))

    # Hail Batch's b.read_input_group / job.declare_resource_group return a
    # ResourceGroup at runtime, but the static return types resolve to the
    # Resource base which lacks __getitem__. Cast so mypy permits the
    # bracket-style key access used in the f-string below.
    gvcf_input_group = cast(
        'ResourceGroup',
        b.read_input_group(
            base_gvcf=str(base_gvcf_path),
            recal_gvcf=str(recal_gvcf_path),
        ),
    )

    job.declare_resource_group(
        reheadered_gvcf={
            'gvcf.gz': '{root}.gvcf.gz',
            'gvcf.gz.tbi': '{root}.gvcf.gz.tbi',
        }
    )

    reheadered_gvcf_outputs = cast('ResourceGroup', job.reheadered_gvcf)

    job.command(
        f"""
        set -euo pipefail

        bcftools annotate --no-version --write-index=tbi \\
            -h <(bcftools view -h {gvcf_input_group['base_gvcf']} | grep '^##GVCFBlock') \\
            {gvcf_input_group['recal_gvcf']} -o {reheadered_gvcf_outputs['gvcf.gz']} -Oz

        """
    )

    b.write_output(reheadered_gvcf_outputs, str(reheadered_gvcf_path).removesuffix('.gvcf.gz'))

    return job
