"""
Create Hail Batch jobs for Somalier extract using a BashJob, following
the standard workflow job structure.
"""

from cpg_flow.filetypes import CramPath
from cpg_flow.targets import SequencingGroup
from cpg_flow.utils import can_reuse
from cpg_utils import Path
from cpg_utils.config import get_driver_image, reference_path
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import BashJob
from loguru import logger

from dragen_align_pa import utils


def somalier_extract(
    sequencing_group: SequencingGroup,
    cram_path: CramPath,
    out_somalier_path: Path,
    overwrite: bool = True,
) -> BashJob | None:
    """
    Public function to create and configure the Somalier extract Job.
    """
    if can_reuse(out_somalier_path, overwrite):
        logger.info(f'Reusing existing Somalier output: {out_somalier_path}')
        return None

    if not cram_path.index_path:
        raise ValueError(f'CRAM for somalier is required to have CRAI index ({cram_path})')

    # Initialize the job
    b = get_batch()
    somalier_job: BashJob = b.new_job(
        name=f'Somalier extract {sequencing_group.id}',
        attributes=(sequencing_group.get_job_attrs() or {}) | {'tool': 'somalier'},
    )
    somalier_job.image(image=get_driver_image())
    somalier_job.storage(storage=utils.calculate_needed_storage(cram_path=cram_path.path))
    somalier_job.memory('8Gi')

    # Read GCS inputs. Hail Batch will localize them.
    # read_input_group localizes CRAM and CRAI together.
    b_cram = b.read_input_group(
        cram=str(cram_path.path),
        crai=str(cram_path.index_path),
    )
    b_ref_fasta = b.read_input(str(reference_path('broad/ref_fasta')))
    b.read_input(str(reference_path('broad/ref_fasta')) + '.fai')  # Localize FAI
    b_somalier_sites = b.read_input(str(reference_path('somalier_sites')))

    # Define the output file within the job's temporary output directory
    somalier_job.out_somalier = somalier_job.outdir[f'{sequencing_group.id}.somalier']
    final_output_path_in_job = f'{somalier_job.outdir}/{sequencing_group.id}.somalier'

    # Set the command. Use the localized file paths.
    somalier_job.command(
        f"""
        somalier extract \\
        -d $BATCH_TMPDIR/{somalier_job.outdir} \\
        --sites {b_somalier_sites} \\
        -f {b_ref_fasta} \\
        {b_cram['cram']}

        CRAM_BASENAME=$(basename {b_cram['cram']})
        SOMALIER_OUTPUT_NAME=${{CRAM_BASENAME%.cram}}.somalier
        CREATED_FILE_PATH=$BATCH_TMPDIR/{somalier_job.outdir}/$SOMALIER_OUTPUT_NAME

        mv $CREATED_FILE_PATH {final_output_path_in_job}
        """
    )

    # Write the declared output file to its final GCS location
    b.write_output(somalier_job.out_somalier, str(out_somalier_path))

    return somalier_job
