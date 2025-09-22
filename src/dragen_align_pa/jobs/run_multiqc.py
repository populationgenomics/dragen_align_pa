import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import get_driver_image, output_path
from cpg_utils.hail_batch import command, get_batch
from hailtop.batch.job import BashJob


def _initalise_multiqc_job(cohort: Cohort) -> BashJob:
    multiqc_job: BashJob = get_batch().new_bash_job(
        name='RunMultiQc',
        attributes=(cohort.get_job_attrs() or {} | {'tool': 'MultiQC'}),  # type: ignore[ReportUnknownVariableType]
    )
    multiqc_job.image(image=get_driver_image())
    return multiqc_job


def run_multiqc(cohort: Cohort, dragen_metric_prefixes: cpg_utils.Path, outputs: dict[str, str]) -> BashJob:
    multiqc_job: BashJob = _initalise_multiqc_job(cohort=cohort)

    sequencing_groups: str = ('|').join(cohort.get_sequencing_group_ids())
    multiqc_job.command(
        command=command(
            f"""
        mkdir $BATCH_TMPDIR/input_data
        gcloud storage ls {dragen_metric_prefixes}/*/*.csv | grep -Ev '{sequencing_groups}' > files
        while read line
        do
            gcloud storage cp $line $BATCH_TMPDIR/input_data
        done < files

        multiqc -f $BATCH_TMPDIR/input_data -o output \\
        --title MultiQC Report for <b>{cohort.name}</b> \\
        --filename report.html \\
        --cl-config "max_table_rows: 10000"

        cp output/report.html {multiqc_job.html}
        cp output/report_data/multiqc_data.json {multiqc_job.json}
        """
        )
    )
    get_batch().write_output(resource=multiqc_job.html, dest=output_path(outputs['multiqc_report'], category='web'))
    get_batch().write_output(resource=multiqc_job.json, dest=output_path(outputs['multiqc_report']))

    return multiqc_job
