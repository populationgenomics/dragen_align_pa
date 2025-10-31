from cpg_flow.stage import StageInput, StageInputNotFoundError
from cpg_flow.targets import Cohort
from cpg_utils import Path, to_path
from cpg_utils.config import get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import BashJob
from loguru import logger


def run_multiqc(
    cohort: Cohort,
    inputs: StageInput,
    outputs: dict[str, str],
) -> BashJob | None:
    """
    Creates and calls the Job to run MultiQC.
    Gathers all required QC input paths.
    """
    from dragen_align_pa.stages import DownloadDataFromIca, SomalierExtract  # noqa: PLC0415

    # 1. Get Dragen metric directory prefixes for each SG
    dragen_metric_prefixes: list[Path] = []
    for sg in cohort.get_sequencing_groups():
        try:
            # The output of DownloadDataFromIca is the directory prefix
            prefix = inputs.as_path(target=sg, stage=DownloadDataFromIca)
            dragen_metric_prefixes.append(prefix)
        except StageInputNotFoundError:
            logger.warning(f'Dragen metrics directory not found for {sg.id}, skipping for MultiQC')

    # 2. Get Somalier paths for each SG
    somalier_paths_dict: dict[str, Path] = inputs.as_path_by_target(stage=SomalierExtract)
    somalier_paths: list[Path] = list(somalier_paths_dict.values())

    # 3. Collect all individual Dragen CSV file paths
    all_dragen_csv_paths: list[Path] = []
    for prefix in dragen_metric_prefixes:
        try:
            # Use rglob to find all CSV files recursively within the SG's metric directory
            found_paths = [to_path(p) for p in prefix.rglob('*.csv')]
            all_dragen_csv_paths.extend(found_paths)
        except FileNotFoundError:
            logger.warning(f'Directory {prefix} not found when searching for Dragen CSVs.')
        except Exception as e:
            logger.error(f'Error searching for CSVs in {prefix}: {e}')

    # 4. Combine Dragen CSV paths and Somalier paths
    all_qc_paths: list[Path] = all_dragen_csv_paths + somalier_paths

    if not all_qc_paths:
        logger.warning('No QC files (Dragen CSVs or Somalier) found to aggregate with MultiQC')
        return None  # Return None to signal the stage to skip

    logger.info(f'Found {len(all_qc_paths)} QC files for MultiQC aggregation.')
    if all_qc_paths:
        logger.info(f'Example QC paths: {all_qc_paths[:5]}')

    # 5. Create the Job
    b = get_batch()
    multiqc_job: BashJob = b.new_job(
        name='MultiQC',
        attributes=(cohort.get_job_attrs() or {}) | {'tool': 'MultiQC'},
    )
    multiqc_job.image(image=get_driver_image())
    multiqc_job.storage('10Gi')

    # Read all QC files into the job's input directory
    # Hail Batch will place them all in b.input_dir
    input_files = [b.read_input(str(p)) for p in all_qc_paths]  # noqa: F841

    report_name = f'{cohort.name}_multiqc_report'
    multiqc_job.declare_resource_group(
        out={
            'html': f'{report_name}.html',
            'json': f'{report_name}_data/multiqc_data.json',
        }
    )

    # Define the command
    multiqc_job.command(
        f"""
        multiqc \\
        {multiqc_job.input_dir} \\
        -o {multiqc_job.outdir} \\
        --title 'MultiQC Report for {cohort.name}' \\
        --filename '{report_name}.html' \\
        --cl-config 'max_table_rows: 10000'

        mv {multiqc_job.outdir}/{report_name}.html {multiqc_job.html}
        mv {multiqc_job.outdir}/{report_name}_data/multiqc_data.json {multiqc_job.json}
        """
    )

    # Write outputs to their final GCS locations
    b.write_output(multiqc_job.html, outputs['multiqc_report'])
    b.write_output(multiqc_job.json, outputs['multiqc_data'])

    return multiqc_job
