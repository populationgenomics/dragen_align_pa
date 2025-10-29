import sys
from typing import TYPE_CHECKING

import cpg_utils
from cpg_flow.filetypes import CramPath
from cpg_flow.inputs import get_multicohort
from cpg_flow.stage import (
    CohortStage,
    SequencingGroupStage,
    StageInput,
    StageOutput,
    stage,  # type: ignore[ReportUnknownVariableType]
)
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_utils.config import output_path
from loguru import logger

from dragen_align_pa.constants import (
    BUCKET,
    DRAGEN_VERSION,
    GCP_FOLDER_FOR_ICA_DOWNLOAD,
    GCP_FOLDER_FOR_ICA_PREP,
    GCP_FOLDER_FOR_RUNNING_PIPELINE,
    READS_TYPE,
)
from dragen_align_pa.jobs import (
    delete_data_in_ica,
    download_ica_pipeline_outputs,
    download_md5_results,
    download_specific_files_from_ica,
    make_fastq_file_list,
    manage_dragen_mlr,
    manage_dragen_pipeline,
    manage_md5_pipeline,
    prepare_ica_for_analysis,
    run_multiqc,
    somalier_extract,
    upload_data_to_ica,
    upload_fastq_file_list,
    validate_md5_sums,
)

if TYPE_CHECKING:
    from hailtop.batch.job import PythonJob


logger.remove(0)
logger.add(sink=sys.stdout, format='{time} - {level} - {message}')


# No need to register this stage in Metamist I think, just ICA prep
@stage()
class PrepareIcaForDragenAnalysis(CohortStage):
    """Set up ICA for a single realignment run.

    Creates a folder ID for the Dragen output to be written into.
    """

    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        results: dict[str, cpg_utils.Path] = {
            **{
                sg_name: BUCKET / GCP_FOLDER_FOR_ICA_PREP / f'{sg_name}_output_fid.json'
                for sg_name in cohort.get_sequencing_group_ids()
            }
        }
        return results

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput:  # noqa: ARG002
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

        ica_prep_job: PythonJob = prepare_ica_for_analysis.run_ica_prep_job(
            cohort=cohort,
            output=outputs,
        )

        return self.make_outputs(
            target=cohort,
            data=outputs,  # pyright: ignore[reportArgumentType]
            jobs=ica_prep_job,
        )


@stage(required_stages=[PrepareIcaForDragenAnalysis])
class FastqIntakeQc(CohortStage):
    """Generate md5 sums for each uploaded fastq file.

    Check these sums against the supplied md5sums to check for any corruption in transit.
    """

    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        intake_qc_results: dict[str, cpg_utils.Path] = {
            'fastq_ids_outpath': BUCKET / GCP_FOLDER_FOR_ICA_PREP / f'{cohort.name}_fastq_ids.txt',
            'md5sum_pipeline_run': BUCKET / GCP_FOLDER_FOR_ICA_PREP / f'{cohort.name}_ica_md5sum_pipeline.json',
            'md5sum_pipeline_success': BUCKET / GCP_FOLDER_FOR_ICA_PREP / f'{cohort.name}_md5_pipeline_success',
            f'{cohort.name}_md5_errors': BUCKET / GCP_FOLDER_FOR_ICA_PREP / f'{cohort.name}_md5_errors.log',
        }
        return intake_qc_results

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:  # noqa: ARG002
        if READS_TYPE == 'fastq':
            outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)
            md5job: PythonJob = manage_md5_pipeline.run_md5_management_job(cohort=cohort, outputs=outputs)

            return self.make_outputs(target=cohort, data=outputs, jobs=md5job)  # pyright: ignore[reportArgumentType]

        return None


@stage(required_stages=[FastqIntakeQc])
class DownloadMd5Results(CohortStage):
    """
    Downloads the 'all_md5.txt' result file from a successful
    MD5 Checksum pipeline run.
    """

    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        return {'ica_md5sum_file': BUCKET / GCP_FOLDER_FOR_ICA_PREP / f'{cohort.name}_ica_md5sum.md5sum'}

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

        if READS_TYPE == 'fastq':
            # Get the pipeline run file from the previous stage
            md5_pipeline_file: cpg_utils.Path = inputs.as_path(
                target=cohort, stage=FastqIntakeQc, key='md5sum_pipeline_run'
            )

            download_job: PythonJob = download_md5_results.download_md5_results_job(
                cohort=cohort,
                md5_pipeline_file=md5_pipeline_file,
                md5_outpath=outputs['ica_md5sum_file'],
            )

            return self.make_outputs(target=cohort, data=outputs, jobs=download_job)  # pyright: ignore[reportArgumentType]

        return None


@stage(required_stages=[DownloadMd5Results])
class ValidateMd5Sums(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        return BUCKET / GCP_FOLDER_FOR_ICA_PREP / f'{cohort.name}_md5_validation_success.txt'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        outputs: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        if READS_TYPE == 'fastq':
            ica_md5sum_file_path: cpg_utils.Path = inputs.as_path(
                target=cohort, stage=DownloadMd5Results, key='ica_md5sum_file'
            )
            md5_validation_job: PythonJob = validate_md5_sums.validate_md5_sums(
                ica_md5sum_file_path=ica_md5sum_file_path,
                cohort=cohort,
                outputs=outputs,
            )

            return self.make_outputs(target=cohort, data=outputs, jobs=md5_validation_job)

        return None


@stage(required_stages=[PrepareIcaForDragenAnalysis, ValidateMd5Sums])
class MakeFastqFileList(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        results: dict[str, cpg_utils.Path] = {
            **{
                sg_name: BUCKET / GCP_FOLDER_FOR_ICA_PREP / f'{sg_name}_fastq_list.csv'
                for sg_name in cohort.get_sequencing_group_ids()
            }
        }
        return results

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:  # noqa: ARG002
        if READS_TYPE == 'fastq':
            outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

            make_fastq_list_file_job: PythonJob = make_fastq_file_list.make_fastq_list_file(
                outputs=outputs,
                cohort=cohort,
            )
            return self.make_outputs(target=cohort, data=outputs, jobs=make_fastq_list_file_job)  # pyright: ignore[reportArgumentType]
        return None


@stage
class UploadDataToIca(SequencingGroupStage):
    def expected_outputs(self, sequencing_group: SequencingGroup) -> cpg_utils.Path:
        return BUCKET / GCP_FOLDER_FOR_ICA_PREP / f'{sequencing_group.name}_fids.json'

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:  # noqa: ARG002
        output: cpg_utils.Path = self.expected_outputs(sequencing_group=sequencing_group)
        if READS_TYPE == 'cram':
            upload_job: PythonJob = upload_data_to_ica.upload_data_to_ica(
                sequencing_group=sequencing_group,
                output=str(output),
            )

            return self.make_outputs(
                target=sequencing_group,
                data=output,
                jobs=upload_job,
            )
        return None


@stage(required_stages=[MakeFastqFileList, PrepareIcaForDragenAnalysis])
class UploadFastqFileList(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:  # pyright: ignore[reportIncompatibleMethodOverride]
        results: cpg_utils.Path = BUCKET / GCP_FOLDER_FOR_ICA_PREP / f'{cohort.name}_fastq_csv_file_list_fid.json'
        return results

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        outputs: cpg_utils.Path = self.expected_outputs(cohort=cohort)
        if READS_TYPE == 'fastq':
            fastq_list_file_path_dict: dict[str, cpg_utils.Path] = inputs.as_dict(
                target=cohort,
                stage=MakeFastqFileList,
            )

            upload_fastq_list_job: PythonJob = upload_fastq_file_list.upload_fastq_file_list(
                cohort=cohort,
                outputs=outputs,
                fastq_list_file_path_dict=fastq_list_file_path_dict,
            )

            return self.make_outputs(target=cohort, data=outputs, jobs=upload_fastq_list_job)  # pyright: ignore[reportArgumentType]

        return None


@stage(
    required_stages=[
        PrepareIcaForDragenAnalysis,
        UploadDataToIca,
        UploadFastqFileList,
        MakeFastqFileList,
        FastqIntakeQc,
    ],
)
class ManageDragenPipeline(CohortStage):
    """
    Due to the nature of the Dragen pipeline and stage dependencies, we need to run, monitor and cancel the pipeline in the same stage.

    This stage handles the following tasks:
    1. Cancels a previous pipeline running on ICA if requested.
        - Set the `cancel_cohort_run` flag to `true` in the config and the stage will read the pipeline ID from the JSON file and cancel it.
    2. Resumes monitoring a previous pipeline run if it was interrupted.
        - Set the `monitor_previous` flag to `true` in the config. This will read the pipeline ID from the JSON file and monitor it.
    3. Initiates a new Dragen pipeline run if no previous run is found or if resuming is not requested.
    4. Monitors the progress of the Dragen pipeline run.
    """  # noqa: E501

    def expected_outputs(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        cohort: Cohort,
    ) -> dict[str, cpg_utils.Path]:
        sg_bucket: cpg_utils.Path = cohort.dataset.prefix()
        prefix: cpg_utils.Path = sg_bucket / GCP_FOLDER_FOR_RUNNING_PIPELINE
        results: dict[str, cpg_utils.Path] = {f'{cohort.name}_errors': prefix / f'{cohort.name}_errors.log'}
        for sequencing_group in cohort.get_sequencing_groups():
            sg_name: str = sequencing_group.name
            results |= {f'{sg_name}_success': prefix / f'{sg_name}_pipeline_success.json'}
            results |= {f'{sg_name}_pipeline_id_and_arguid': prefix / f'{sg_name}_pipeline_id_and_arguid.json'}

        return results

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

        cram_ica_fids_path: dict[str, cpg_utils.Path] | None = None
        fastq_csv_list_file_path: cpg_utils.Path | None = None
        fastq_ids_path: cpg_utils.Path | None = None
        individual_fastq_file_list_paths: dict[str, cpg_utils.Path] | None = None

        # Inputs from previous stages
        if READS_TYPE == 'cram':
            cram_ica_fids_path = inputs.as_path_by_target(stage=UploadDataToIca)
        elif READS_TYPE == 'fastq':
            fastq_csv_list_file_path = inputs.as_path(target=cohort, stage=UploadFastqFileList)
            fastq_ids_path = inputs.as_path(target=cohort, stage=FastqIntakeQc, key='fastq_ids_outpath')
            individual_fastq_file_list_paths = inputs.as_dict(target=cohort, stage=MakeFastqFileList)

        analysis_output_fids_path: dict[str, cpg_utils.Path] = inputs.as_dict(
            target=cohort, stage=PrepareIcaForDragenAnalysis
        )

        management_job: PythonJob = manage_dragen_pipeline.manage_ica_pipeline(
            cohort=cohort,
            outputs=outputs,
            cram_ica_fids_path=cram_ica_fids_path,
            fastq_csv_list_file_path=fastq_csv_list_file_path,
            fastq_ids_path=fastq_ids_path,
            individual_fastq_file_list_paths=individual_fastq_file_list_paths,
            analysis_output_fids_path=analysis_output_fids_path,
        )

        return self.make_outputs(
            target=cohort,
            data=outputs,  # pyright: ignore[reportArgumentType]
            jobs=management_job,
        )


@stage(required_stages=[ManageDragenPipeline])
class ManageDragenMlr(CohortStage):
    def expected_outputs(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        cohort: Cohort,
    ) -> dict[str, cpg_utils.Path]:
        sg_bucket: cpg_utils.Path = cohort.dataset.prefix()
        prefix: cpg_utils.Path = sg_bucket / GCP_FOLDER_FOR_RUNNING_PIPELINE
        results: dict[str, cpg_utils.Path] = {f'{cohort.name}_mlr_errors': prefix / f'{cohort.name}_mlr_errors.log'}
        for sequencing_group in cohort.get_sequencing_groups():
            sg_name: str = sequencing_group.name
            results |= {
                f'{sg_name}_mlr_success': prefix / f'{sg_name}_mlr_pipeline_success.json',
                f'{sg_name}_mlr_pipeline_id': prefix / f'{sg_name}_mlr_pipeline_id.json',
            }
        return results

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

        # Inputs from previous stage
        pipeline_id_arguid_path_dict: dict[str, cpg_utils.Path] = inputs.as_dict(
            target=cohort,
            stage=ManageDragenPipeline,
        )

        mlr_job: PythonJob = manage_dragen_mlr.run_mlr(
            cohort=cohort,
            pipeline_id_arguid_path_dict=pipeline_id_arguid_path_dict,
            outputs=outputs,
        )

        return self.make_outputs(target=cohort, data=outputs, jobs=mlr_job)  # pyright: ignore[reportArgumentType]


@stage(
    analysis_type='cram',
    analysis_keys=['cram'],
    required_stages=[ManageDragenPipeline],
)
class DownloadCramFromIca(SequencingGroupStage):
    """
    Download cram and crai files from ICA separately. This is to allow registrations of the cram files
    in metamist to be done via stage decorators. The pipeline ID needs to be read within the Hail BashJob to get the current
    pipeline ID. If read outside the job, it will get the pipeline ID from the previous pipeline run.
    """  # noqa: E501

    def expected_outputs(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        sequencing_group: SequencingGroup,
    ) -> dict[str, cpg_utils.Path]:
        bucket_name: cpg_utils.Path = sequencing_group.dataset.prefix()
        return {
            'cram': bucket_name / GCP_FOLDER_FOR_ICA_DOWNLOAD / 'cram' / f'{sequencing_group.name}.cram',
            'crai': bucket_name / GCP_FOLDER_FOR_ICA_DOWNLOAD / 'cram' / f'{sequencing_group.name}.cram.crai',
        }

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(sequencing_group=sequencing_group)

        # Inputs from previous stage
        pipeline_id_arguid_path: cpg_utils.Path = inputs.as_dict(
            target=get_multicohort().get_cohorts()[0],
            stage=ManageDragenPipeline,
        )[f'{sequencing_group.name}_pipeline_id_and_arguid']

        ica_download_job: PythonJob = download_specific_files_from_ica.download_data_from_ica(
            job_name='DownloadCramFromIca',
            sequencing_group=sequencing_group,
            filetype='cram',
            pipeline_id_arguid_path=pipeline_id_arguid_path,
        )

        return self.make_outputs(
            target=sequencing_group,
            data=outputs,  # pyright: ignore[reportArgumentType]
            jobs=ica_download_job,
        )


@stage(
    analysis_type='gvcf',
    analysis_keys=['gvcf'],
    required_stages=[ManageDragenPipeline],
)
class DownloadGvcfFromIca(SequencingGroupStage):
    def expected_outputs(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        sequencing_group: SequencingGroup,
    ) -> dict[str, cpg_utils.Path]:
        bucket_name: cpg_utils.Path = sequencing_group.dataset.prefix()
        return {
            'gvcf': bucket_name
            / GCP_FOLDER_FOR_ICA_DOWNLOAD
            / 'base_gvcf'
            / f'{sequencing_group.name}.hard-filtered.gvcf.gz',
            'gvcf_tbi': bucket_name
            / GCP_FOLDER_FOR_ICA_DOWNLOAD
            / 'base_gvcf'
            / f'{sequencing_group.name}.hard-filtered.gvcf.gz.tbi',
        }

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput:
        """
        Download gVCF and gVCF TBI files from ICA separately. This is to allow registrations of the gVCF files
        in metamist to be done via stage decorators. The pipeline ID needs to be read within the Hail BashJob to get the current
        pipeline ID. If read outside the job, it will get the pipeline ID from the previous pipeline run.
        """  # noqa: E501
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(sequencing_group=sequencing_group)

        # Inputs from previous stage
        pipeline_id_arguid_path: cpg_utils.Path = inputs.as_dict(
            target=get_multicohort().get_cohorts()[0],
            stage=ManageDragenPipeline,
        )[f'{sequencing_group.name}_pipeline_id_and_arguid']

        ica_download_job: PythonJob = download_specific_files_from_ica.download_data_from_ica(
            job_name='DownloadGvcfFromIca',
            sequencing_group=sequencing_group,
            filetype='base_gvcf',
            pipeline_id_arguid_path=pipeline_id_arguid_path,
        )

        return self.make_outputs(
            target=sequencing_group,
            data=outputs,  # pyright: ignore[reportArgumentType]
            jobs=ica_download_job,
        )


@stage(
    analysis_type='gvcf',
    analysis_keys=['gvcf'],
    required_stages=[DownloadGvcfFromIca, ManageDragenMlr, ManageDragenPipeline],
)
class DownloadMlrGvcfFromIca(SequencingGroupStage):
    def expected_outputs(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        sequencing_group: SequencingGroup,
    ) -> dict[str, cpg_utils.Path]:
        bucket_name: cpg_utils.Path = sequencing_group.dataset.prefix()
        return {
            'gvcf': bucket_name
            / GCP_FOLDER_FOR_ICA_DOWNLOAD
            / 'recal_gvcf'
            / f'{sequencing_group.name}.hard-filtered.recal.gvcf.gz',
            'gvcf_tbi': bucket_name
            / GCP_FOLDER_FOR_ICA_DOWNLOAD
            / 'recal_gvcf'
            / f'{sequencing_group.name}.hard-filtered.recal.gvcf.gz.tbi',
        }

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput:
        """
        Download gVCF and gVCF TBI files from ICA separately. This is to allow registrations of the gVCF files
        in metamist to be done via stage decorators. The pipeline ID needs to be read within the Hail BashJob to get the current
        pipeline ID. If read outside the job, it will get the pipeline ID from the previous pipeline run.
        """  # noqa: E501
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(sequencing_group=sequencing_group)

        # Inputs from previous stage
        pipeline_id_arguid_path: cpg_utils.Path = inputs.as_dict(
            target=get_multicohort().get_cohorts()[0],
            stage=ManageDragenPipeline,
        )[f'{sequencing_group.name}_pipeline_id_and_arguid']

        ica_download_job: PythonJob = download_specific_files_from_ica.download_data_from_ica(
            job_name='DownloadMlrGvcfFromIca',
            sequencing_group=sequencing_group,
            filetype='recal_gvcf',
            pipeline_id_arguid_path=pipeline_id_arguid_path,
        )

        return self.make_outputs(
            target=sequencing_group,
            data=outputs,  # pyright: ignore[reportArgumentType]
            jobs=ica_download_job,
        )


@stage(
    analysis_type='ica_data_download',
    required_stages=[
        ManageDragenPipeline,
        DownloadCramFromIca,
        DownloadGvcfFromIca,
        DownloadMlrGvcfFromIca,
    ],
)
class DownloadDataFromIca(SequencingGroupStage):
    """
    Download all files from ICA for a single realignment run except the CRAM and GVCF files.
    Register this batch download in Metamist.
    Does not register individual files in Metamist.
    """

    def expected_outputs(
        self,
        sequencing_group: SequencingGroup,
    ) -> cpg_utils.Path:
        bucket_name: cpg_utils.Path = sequencing_group.dataset.prefix()
        return bucket_name / GCP_FOLDER_FOR_ICA_DOWNLOAD / 'dragen_metrics' / f'{sequencing_group.name}'

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput:
        outputs: cpg_utils.Path = self.expected_outputs(sequencing_group=sequencing_group)

        # Inputs from previous stage
        pipeline_id_arguid_path: cpg_utils.Path = inputs.as_dict(
            target=get_multicohort().get_cohorts()[0],
            stage=ManageDragenPipeline,
        )[f'{sequencing_group.name}_pipeline_id_and_arguid']

        ica_download_job: PythonJob = download_ica_pipeline_outputs.download_bulk_data_from_ica(
            sequencing_group=sequencing_group,
            pipeline_id_arguid_path=pipeline_id_arguid_path,
        )

        return self.make_outputs(
            target=sequencing_group,
            data=outputs,
            jobs=ica_download_job,
        )


@stage(required_stages=[DownloadCramFromIca])  # Depends on CRAM being downloaded
class SomalierExtract(SequencingGroupStage):
    """
    Run Somalier extract on CRAM files to generate fingerprints.
    """

    def expected_outputs(self, sequencing_group: SequencingGroup) -> cpg_utils.Path:
        """
        Expected Somalier fingerprint output file.
        Uses SG ID for filename.
        """
        # Define the output path within the main QC output area
        somalier_dir = sequencing_group.dataset.prefix() / GCP_FOLDER_FOR_ICA_DOWNLOAD / 'somalier'
        return somalier_dir / f'{sequencing_group.id}.somalier'

    def queue_jobs(
        self,
        sequencing_group: SequencingGroup,
        inputs: StageInput,
    ) -> StageOutput | None:
        """
        Queue a job to run somalier extract.
        """
        cram_path = inputs.as_path(
            target=sequencing_group,
            stage=DownloadCramFromIca,
            key='cram',
        )
        crai_path = inputs.as_path(
            target=sequencing_group,
            stage=DownloadCramFromIca,
            key='crai',
        )

        out_somalier_path = self.expected_outputs(sequencing_group)

        job: PythonJob | None = somalier_extract.somalier_extract(
            sequencing_group=sequencing_group,
            cram_path=CramPath(cram_path, crai_path),
            out_somalier_path=out_somalier_path,
            overwrite=sequencing_group.forced or self.forced,
        )

        if job:
            return self.make_outputs(
                sequencing_group,
                data=out_somalier_path,
                jobs=job,
            )
        # If can_reuse returns None, job is skipped
        return self.make_outputs(sequencing_group, data=out_somalier_path, skipped=True)


@stage(required_stages=[DownloadDataFromIca, SomalierExtract])
class RunMultiQc(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> dict[str, str]:  # pyright: ignore[reportIncompatibleMethodOverride]
        multiqc_data: str = output_path(f'ica/{DRAGEN_VERSION}/qc/{cohort.name}_multiqc_data.json')
        multiqc_report: str = output_path(f'ica/{DRAGEN_VERSION}/qc/{cohort.name}_multiqc_report.html', category='web')
        return {
            'multiqc_data': multiqc_data,
            'multiqc_report': multiqc_report,
        }

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        outputs: dict[str, str] = self.expected_outputs(cohort=cohort)

        multiqc_job: PythonJob | None = run_multiqc.run_multiqc(
            cohort=cohort,
            inputs=inputs,
            outputs=outputs,
        )

        if not multiqc_job:
            logger.warning('MultiQC job was not created (no input files found). Skipping stage.')
            return self.make_outputs(cohort, skipped=True)

        return self.make_outputs(target=cohort, data=outputs, jobs=multiqc_job)  # pyright: ignore[reportArgumentType]


# Change this to a sequencing group stage to be safer.
@stage(
    required_stages=[
        PrepareIcaForDragenAnalysis,
        UploadDataToIca,
        DownloadCramFromIca,
        DownloadGvcfFromIca,
        DownloadMlrGvcfFromIca,
        DownloadDataFromIca,
        RunMultiQc,
        FastqIntakeQc,
    ]
)
class DeleteDataInIca(CohortStage):
    """
    Delete all the data in ICA for a dataset, so we don't pay storage costs
    once processing is finished. This includes generated analysis folders
    and the original source data (uploaded CRAMs or FASTQs).
    """

    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        bucket_name: cpg_utils.Path = cohort.dataset.prefix()
        # Changed to cohort-level placeholder
        return bucket_name / GCP_FOLDER_FOR_ICA_PREP / f'{cohort.name}_delete_placeholder.txt'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        # Get all analysis output folder FIDs (generated data)
        analysis_output_fids_paths: dict[str, cpg_utils.Path] = inputs.as_dict(
            target=cohort, stage=PrepareIcaForDragenAnalysis
        )

        # Initialize paths for source data
        cram_fid_paths_dict: dict[str, cpg_utils.Path] | None = None
        fastq_ids_list_path: cpg_utils.Path | None = None

        # Conditionally get source data FIDs based on READS_TYPE
        if READS_TYPE == 'cram':
            # Get all uploaded CRAM FIDs
            cram_fid_paths_dict = inputs.as_path_by_target(stage=UploadDataToIca)
        elif READS_TYPE == 'fastq':
            # Get the path to the list of FASTQ FIDs
            fastq_ids_list_path = inputs.as_path(target=cohort, stage=FastqIntakeQc, key='fastq_ids_outpath')

        outputs: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        ica_delete_job: PythonJob = delete_data_in_ica.delete_data_in_ica(
            cohort=cohort,
            analysis_output_fids_paths=analysis_output_fids_paths,
            cram_fid_paths_dict=cram_fid_paths_dict,
            fastq_ids_list_path=fastq_ids_list_path,
        )

        return self.make_outputs(target=cohort, data=outputs, jobs=ica_delete_job)
