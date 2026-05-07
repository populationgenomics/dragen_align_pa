from __future__ import annotations

import cpg_utils
from cpg_utils.config import image_path
from cpg_utils.hail_batch import Batch, get_batch
from hailtop.batch.job import BashJob


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
    job.storage(storage='16GB')

    gvcf_input_group = b.read_input_group(
        base_gvcf=str(base_gvcf_path),
        recal_gvcf=str(recal_gvcf_path),
    )

    job.declare_resource_group(
        reheadered_gvcf={
            'gvcf.gz': '{root}.gvcf.gz',
            'gvcf.gz.tbi': '{root}.gvcf.gz.tbi',
        }
    )

    reheadered_gvcf_outputs = job.reheadered_gvcf

    job.command(
        f"""
        set -euo pipefail

        bcftools view -h {gvcf_input_group['base_gvcf']} > base_header.txt
        bcftools view -h {gvcf_input_group['recal_gvcf']} > recal_header.txt


        awk 'FNR==NR {{ if (/^##GVCFBlock/) blocks = blocks $0 ORS; next }} \\
        !inserted && /^##INFO=/ {{ printf "%s", blocks; inserted = 1 }} \\
        {{ print }}' base_header.txt recal_header.txt > new_header.txt \\
        && bcftools reheader -h new_header.txt {gvcf_input_group['recal_gvcf']} -o {reheadered_gvcf_outputs['gvcf.gz']}


        # Index the reheadered gVCF
        bcftools index -t {reheadered_gvcf_outputs['gvcf.gz']}
        """
    )

    b.write_output(reheadered_gvcf_outputs, str(reheadered_gvcf_path).removesuffix('.gvcf.gz'))

    return job
