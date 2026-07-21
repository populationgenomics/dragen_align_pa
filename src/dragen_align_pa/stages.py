import sys
from typing import TYPE_CHECKING

import cpg_utils
from cpg_flow.filetypes import CramPath
from cpg_flow.stage import (
    CohortStage,
    SequencingGroupStage,
    StageInput,
    StageOutput,
    stage,  # type: ignore[ReportUnknownVariableType]
)
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_utils.config import config_retrieve, get_driver_image
from loguru import logger

from dragen_align_pa.constants import (
    READS_TYPE,
)
from dragen_align_pa.file_types import FileTypeSpec
from dragen_align_pa.jobs import (
    delete_data_in_ica,
    download_batch_artefacts,
    download_ica_pipeline_outputs,
    download_md5_results,
    download_specific_files_from_ica,
    make_fastq_file_list,
    manage_dragen_mlr,
    manage_dragen_pipeline,
    manage_md5_pipeline,
    prepare_ica_for_analysis,
    reheader_mlr_gvcf,
    somalier_extract,
    upload_data_to_ica,
    validate_md5_sums,
)
from dragen_align_pa.utils import (
    calculate_needed_storage,
    get_batch_artefacts_root,
    get_manifest_path_for_cohort,
    get_output_path,
    get_per_sg_state_path,
    get_pipeline_path,
    get_prep_path,
    initialise_python_job,
)

if TYPE_CHECKING:
    from hailtop.batch.job import BashJob, PythonJob


logger.remove(0)
logger.add(sink=sys.stdout, format='{time} - {level} - {message}')


# No need to register this stage in Metamist I think, just ICA prep
@stage()
class PrepareIcaForDragenAnalysis(CohortStage):
    """Create a single cohort-level analysis output folder on ICA."""

    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:  # pyright: ignore[reportIncompatibleMethodOverride]
        return get_prep_path(filename=f'{cohort.name}_analysis_output_fid.json')

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput:  # noqa: ARG002
        output: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        job: PythonJob = initialise_python_job(
            job_name='PrepareIcaForDragenAnalysis',
            target=cohort,
            tool_name='ICA',
        )
        job.image(image=get_driver_image())
        job.call(prepare_ica_for_analysis.run, cohort=cohort, output=output)

        return self.make_outputs(target=cohort, data=output, jobs=job)


@stage(required_stages=[PrepareIcaForDragenAnalysis])
class FastqIntakeQc(CohortStage):
    """Generate md5 sums for each uploaded fastq file.

    Check these sums against the supplied md5sums to check for any corruption in transit.
    """

    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        intake_qc_results: dict[str, cpg_utils.Path] = {
            'fastq_ids_outpath': get_prep_path(filename=f'{cohort.name}_fastq_ids.json'),
            'md5sum_pipeline_run': get_prep_path(filename=f'{cohort.name}_ica_md5sum_pipeline.json'),
            'md5sum_pipeline_success': get_prep_path(filename=f'{cohort.name}_md5_pipeline_success'),
            f'{cohort.name}_md5_errors': get_prep_path(filename=f'{cohort.name}_md5_errors.log'),
        }
        return intake_qc_results

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:  # noqa: ARG002
        if READS_TYPE == 'fastq':
            outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)
            manifest_file_path: cpg_utils.Path = get_manifest_path_for_cohort(cohort=cohort)
            job: PythonJob = initialise_python_job(
                job_name='ManageMd5Pipeline',
                target=cohort,
                tool_name='ICA-MD5-Manager',
            )
            job.image(image=get_driver_image())
            job.call(
                manage_md5_pipeline.run,
                cohort=cohort,
                outputs=outputs,
                manifest_file_path=manifest_file_path,
            )

            return self.make_outputs(target=cohort, data=outputs, jobs=job)  # pyright: ignore[reportArgumentType]

        return None


@stage(required_stages=[FastqIntakeQc])
class DownloadMd5Results(CohortStage):
    """
    Downloads the 'all_md5.txt' result file from a successful
    MD5 Checksum pipeline run.
    """

    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:  # pyright: ignore[reportIncompatibleMethodOverride]
        return get_prep_path(filename=f'{cohort.name}_ica_md5sum.md5sum')

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        outputs: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        if READS_TYPE == 'fastq':
            # Get the pipeline run file from the previous stage
            md5_pipeline_file: cpg_utils.Path = inputs.as_path(
                target=cohort, stage=FastqIntakeQc, key='md5sum_pipeline_run'
            )

            job: PythonJob = initialise_python_job(
                job_name='DownloadMd5Results',
                target=cohort,
                tool_name='ICA-Python',
            )
            job.image(image=get_driver_image())
            job.call(
                download_md5_results.run,
                cohort_name=cohort.name,
                md5_pipeline_file=md5_pipeline_file,
                md5_outpath=outputs,
            )

            return self.make_outputs(target=cohort, data=outputs, jobs=job)  # pyright: ignore[reportArgumentType]

        return None


@stage(required_stages=[DownloadMd5Results])
class ValidateMd5Sums(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        return get_prep_path(filename=f'{cohort.name}_md5_validation_success.txt')

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        outputs: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        if READS_TYPE == 'fastq':
            ica_md5sum_file_path: cpg_utils.Path = inputs.as_path(target=cohort, stage=DownloadMd5Results)
            manifest_file_path: cpg_utils.Path = get_manifest_path_for_cohort(cohort=cohort)
            job: PythonJob = initialise_python_job(
                job_name='ValidateMd5Sums',
                target=cohort,
                tool_name='validate-md5',
            )
            job.image(image=get_driver_image())

            job.call(
                validate_md5_sums.run,
                ica_md5sum_file_path=ica_md5sum_file_path,
                cohort_name=cohort.name,
                success_output_path=outputs,
                manifest_file_path=manifest_file_path,
            )

            return self.make_outputs(target=cohort, data=outputs, jobs=job)

        return None


@stage(required_stages=[PrepareIcaForDragenAnalysis, ValidateMd5Sums])
class MakeFastqFileList(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        results: dict[str, cpg_utils.Path] = {
            **{
                sg_name: get_prep_path(filename=f'{sg_name}_fastq_list.csv')
                for sg_name in cohort.get_sequencing_group_ids()
            }
        }
        return results

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:  # noqa: ARG002
        if READS_TYPE == 'fastq':
            outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)
            manifest_file_path: cpg_utils.Path = get_manifest_path_for_cohort(cohort=cohort)

            job: PythonJob = initialise_python_job(
                job_name='MakeFastqFileList',
                target=cohort,
                tool_name='ICA',
            )

            job.image(image=get_driver_image())
            job.call(make_fastq_file_list.run, outputs=outputs, cohort=cohort, manifest_file_path=manifest_file_path)

            return self.make_outputs(target=cohort, data=outputs, jobs=job)  # pyright: ignore[reportArgumentType]
        return None


@stage
class UploadDataToIca(SequencingGroupStage):
    def expected_outputs(self, sequencing_group: SequencingGroup) -> cpg_utils.Path:
        return get_prep_path(filename=f'{sequencing_group.name}_fids.json')

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:  # noqa: ARG002
        output: cpg_utils.Path = self.expected_outputs(sequencing_group=sequencing_group)
        if READS_TYPE == 'cram':
            upload_folder = config_retrieve(['ica', 'data_prep', 'upload_folder'])

            job: PythonJob = initialise_python_job(
                job_name='UploadDataToIca',
                target=sequencing_group,
                tool_name='ICA-Python',
            )
            job.image(image=get_driver_image())
            job.storage(calculate_needed_storage(cram_path=sequencing_group.cram.path))
            job.memory('8Gi')
            job.spot(is_spot=False)

            job.call(
                upload_data_to_ica.run,
                sequencing_group=sequencing_group,
                output_path_str=output,
                upload_folder=upload_folder,
            )

            return self.make_outputs(
                target=sequencing_group,
                data=output,
                jobs=job,
            )
        return None


@stage(
    required_stages=[
        PrepareIcaForDragenAnalysis,
        UploadDataToIca,
        MakeFastqFileList,
        FastqIntakeQc,
    ],
)
class ManageDragenPipeline(CohortStage):
    """Submit cohort batches to the unified DRAGEN pipeline and monitor them.

    Inputs (selected by READS_TYPE):
        cram: per-SG ICA state from UploadDataToIca
        fastq: fastq_ids map from FastqIntakeQc + per-SG CSVs from MakeFastqFileList
                (combined into per-batch CSVs and uploaded to ICA inline by the submitter)
        + analysis output folder FID from PrepareIcaForDragenAnalysis (both modes)

    Outputs (2 cohort-level entries + one per sequencing group):

        {
            '<cohort>_batches':            <cohort>_batches.json            # batch plan
            '<cohort>_pipeline_complete':  <cohort>_pipeline_complete.json  # success marker
            '<sg>_pipeline_id_and_arguid': <sg>_pipeline_id_and_arguid.json # per-SG ICA state
            ... one of these per SG ...
        }

    All paths live under `get_pipeline_path()`. See `expected_outputs` for why
    per-batch scratch files (errors.log, per-batch success/pipeline_id) are
    deliberately excluded from this dict.
    """

    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        # Only DETERMINISTIC outputs go in expected_outputs — anything cpg-flow
        # can't find when re-evaluating this stage triggers a re-run, so the
        # set must be exactly the files a successful `run()` always writes.
        # Variable-existence files (per-batch success/pipeline_id, errors.log
        # written by the monitor loop) are internal orchestrator scratch
        # and the orchestrator computes their paths inline via
        # `get_pipeline_path()` rather than going through expected_outputs.
        # `_pipeline_complete` is the canonical "stage completed without raising"
        # signal — written ONLY as the final action of a successful run(). Any
        # earlier raise (residual SG failure, cancel, ICA error) skips it and the
        # stage is correctly seen as failed.
        return {
            f'{cohort.name}_{config_retrieve(["workflow", "sequencing_type"])}_'
            f'{config_retrieve(["workflow", "reads_type"])}_batches': get_pipeline_path(
                filename=f'{cohort.name}_batches.json'
            ),
            f'{cohort.name}_pipeline_complete': get_pipeline_path(
                filename=f'{cohort.name}_pipeline_complete.json',
            ),
        } | {
            f'{sg.name}_pipeline_id_and_arguid': get_pipeline_path(
                filename=f'{sg.name}_pipeline_id_and_arguid.json',
            )
            for sg in cohort.get_sequencing_groups()
        }

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput:
        outputs = self.expected_outputs(cohort=cohort)

        cram_state_paths: dict[str, cpg_utils.Path] | None = None
        fastq_ids_path: cpg_utils.Path | None = None
        per_sg_fastq_list_paths: dict[str, cpg_utils.Path] | None = None

        if READS_TYPE == 'cram':
            cram_state_paths = inputs.as_path_by_target(stage=UploadDataToIca)
        elif READS_TYPE == 'fastq':
            fastq_ids_path = inputs.as_path(target=cohort, stage=FastqIntakeQc, key='fastq_ids_outpath')
            per_sg_fastq_list_paths = inputs.as_dict(target=cohort, stage=MakeFastqFileList)

        analysis_output_fid_path: cpg_utils.Path = inputs.as_path(
            target=cohort,
            stage=PrepareIcaForDragenAnalysis,
        )

        job: PythonJob = initialise_python_job(
            job_name=f'Manage Dragen pipeline runs for cohort: {cohort.name}',
            target=cohort,
            tool_name='Dragen',
        )
        job.image(image=get_driver_image())
        job.spot(is_spot=False)

        job.call(
            manage_dragen_pipeline.run,
            cohort=cohort,
            outputs=outputs,
            cram_state_paths=cram_state_paths,
            fastq_ids_path=fastq_ids_path,
            per_sg_fastq_list_paths=per_sg_fastq_list_paths,
            analysis_output_fid_path=analysis_output_fid_path,
        )

        return self.make_outputs(target=cohort, data=outputs, jobs=job)  # pyright: ignore[reportArgumentType]


@stage(required_stages=[ManageDragenPipeline])
class ManageDragenMlr(CohortStage):
    """Submit per-SG DRAGEN MLR runs and wait for them to complete."""

    def expected_outputs(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        cohort: Cohort,
    ) -> dict[str, cpg_utils.Path]:
        results: dict[str, cpg_utils.Path] = {
            f'{cohort.name}_mlr_errors': get_pipeline_path(filename=f'{cohort.name}_mlr_errors.log')
        }
        for sequencing_group in cohort.get_sequencing_groups():
            sg_name: str = sequencing_group.name
            results |= {
                f'{sg_name}_mlr_success': get_pipeline_path(filename=f'{sg_name}_mlr_pipeline_success.json'),
                f'{sg_name}_mlr_pipeline_id': get_pipeline_path(filename=f'{sg_name}_mlr_pipeline_id.json'),
            }
        return results

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

        # Inputs from previous stage
        pipeline_id_arguid_path_dict: dict[str, cpg_utils.Path] = inputs.as_dict(
            target=cohort,
            stage=ManageDragenPipeline,
        )

        job: PythonJob = initialise_python_job(
            job_name='MlrWithDragen',
            target=cohort,
            tool_name='ICA',
        )
        job.image(image=get_driver_image())
        job.spot(is_spot=False)

        job.call(
            manage_dragen_mlr.run,
            cohort=cohort,
            pipeline_id_arguid_path_dict=pipeline_id_arguid_path_dict,
            outputs=outputs,
        )

        return self.make_outputs(target=cohort, data=outputs, jobs=job)  # pyright: ignore[reportArgumentType]


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
        return {
            'cram': get_output_path(filename=f'cram/{sequencing_group.name}.cram'),
            'crai': get_output_path(filename=f'cram/{sequencing_group.name}.cram.crai'),
        }

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(sequencing_group=sequencing_group)
        cohort, pipeline_id_arguid_path = get_per_sg_state_path(inputs, sequencing_group, ManageDragenPipeline)

        job = download_specific_files_from_ica.make_download_job(
            job_name='DownloadCramFromIca',
            sequencing_group=sequencing_group,
            file_spec=FileTypeSpec(
                gcs_prefix='cram',
                data_suffix='cram',
                index_suffix='cram.crai',
                md5_suffix='md5sum',
            ),
            pipeline_id_arguid_path=pipeline_id_arguid_path,
            cohort_name=cohort.name,
            gcs_output_dir=outputs['cram'].parent,
        )
        return self.make_outputs(target=sequencing_group, data=outputs, jobs=job)  # pyright: ignore[reportArgumentType]


@stage(
    analysis_type='gvcf',
    analysis_keys=['gvcf'],
    required_stages=[ManageDragenPipeline],
)
class DownloadGvcfFromIca(SequencingGroupStage):
    """Download base gVCF + index from ICA into the cohort's per-SG GCS folder."""

    def expected_outputs(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        sequencing_group: SequencingGroup,
    ) -> dict[str, cpg_utils.Path]:
        return {
            'gvcf': get_output_path(filename=f'base_gvcf/{sequencing_group.name}.hard-filtered.gvcf.gz'),
            'gvcf_tbi': get_output_path(filename=f'base_gvcf/{sequencing_group.name}.hard-filtered.gvcf.gz.tbi'),
        }

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(sequencing_group=sequencing_group)
        cohort, pipeline_id_arguid_path = get_per_sg_state_path(inputs, sequencing_group, ManageDragenPipeline)

        job = download_specific_files_from_ica.make_download_job(
            job_name='DownloadGvcfFromIca',
            sequencing_group=sequencing_group,
            file_spec=FileTypeSpec(
                gcs_prefix='base_gvcf',
                data_suffix='hard-filtered.gvcf.gz',
                index_suffix='hard-filtered.gvcf.gz.tbi',
                md5_suffix='md5sum',
            ),
            pipeline_id_arguid_path=pipeline_id_arguid_path,
            cohort_name=cohort.name,
            gcs_output_dir=outputs['gvcf'].parent,
        )
        return self.make_outputs(target=sequencing_group, data=outputs, jobs=job)  # pyright: ignore[reportArgumentType]


@stage(
    required_stages=[DownloadGvcfFromIca, ManageDragenMlr, ManageDragenPipeline],
)
class DownloadMlrGvcfFromIca(SequencingGroupStage):
    """Download the MLR-refined gVCF + index.

    MLR writes into the parent DRAGEN batch's per-SG subfolder, so the path
    resolves through `ManageDragenPipeline`'s state file. The `ManageDragenMlr`
    dependency exists for ordering only.
    """

    def expected_outputs(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        sequencing_group: SequencingGroup,
    ) -> dict[str, cpg_utils.Path]:
        return {
            'gvcf': get_output_path(
                filename=f'recal_gvcf/{sequencing_group.name}.hard-filtered.recal.gvcf.gz', category='tmp'
            ),
            'gvcf_tbi': get_output_path(
                filename=f'recal_gvcf/{sequencing_group.name}.hard-filtered.recal.gvcf.gz.tbi', category='tmp'
            ),
        }

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(sequencing_group=sequencing_group)
        # MLR writes into the DRAGEN batch's per-SG folder, so we resolve via
        # ManageDragenPipeline's state (NOT ManageDragenMlr's).
        cohort, pipeline_id_arguid_path = get_per_sg_state_path(inputs, sequencing_group, ManageDragenPipeline)

        job = download_specific_files_from_ica.make_download_job(
            job_name='DownloadMlrGvcfFromIca',
            sequencing_group=sequencing_group,
            file_spec=FileTypeSpec(
                gcs_prefix='recal_gvcf',
                data_suffix='hard-filtered.recal.gvcf.gz',
                index_suffix='hard-filtered.recal.gvcf.gz.tbi',
                md5_suffix='md5',
            ),
            pipeline_id_arguid_path=pipeline_id_arguid_path,
            cohort_name=cohort.name,
            gcs_output_dir=outputs['gvcf'].parent,
        )
        return self.make_outputs(target=sequencing_group, data=outputs, jobs=job)  # pyright: ignore[reportArgumentType]


@stage(
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
        return get_output_path(filename=f'dragen_metrics/{sequencing_group.name}')

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput:
        outputs: cpg_utils.Path = self.expected_outputs(sequencing_group=sequencing_group)
        cohort, pipeline_id_arguid_path = get_per_sg_state_path(inputs, sequencing_group, ManageDragenPipeline)

        ica_download_job: PythonJob = initialise_python_job(
            job_name='Download ICA bulk data',
            target=sequencing_group,
            tool_name='ICA-Python',
        )
        ica_download_job.image(image=get_driver_image())
        ica_download_job.spot(is_spot=False)
        ica_download_job.memory(memory='8Gi')

        ica_download_job.call(
            download_ica_pipeline_outputs.run,
            sequencing_group=sequencing_group,
            pipeline_id_arguid_path=pipeline_id_arguid_path,
            cohort_name=cohort.name,
        )

        return self.make_outputs(
            target=sequencing_group,
            data=outputs,
            jobs=ica_download_job,
        )


@stage(required_stages=[ManageDragenPipeline])
class DownloadBatchArtefactsFromIca(CohortStage):
    """Per-batch download of passfail.json / summary.json / reports/ from ICA.

    Only depends on `ManageDragenPipeline` (not `DownloadDataFromIca`) so a
    per-SG download failure doesn't block the cohort-level diagnostics.
    """

    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:  # pyright: ignore[reportIncompatibleMethodOverride]
        return get_batch_artefacts_root() / f'{cohort.name}_artefacts_done.json'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput:
        batches_file_path: cpg_utils.Path = inputs.as_dict(target=cohort, stage=ManageDragenPipeline)[
            f'{cohort.name}_{config_retrieve(["workflow", "sequencing_type"])}_'
            f'{config_retrieve(["workflow", "reads_type"])}_batches'
        ]
        gcs_output_root = get_batch_artefacts_root()
        marker_path = self.expected_outputs(cohort=cohort)

        job: PythonJob = initialise_python_job(
            job_name='DownloadBatchArtefactsFromIca',
            target=cohort,
            tool_name='ICA-Python',
        )
        job.image(image=get_driver_image())
        job.memory('4Gi')
        job.spot(is_spot=False)
        job.call(
            download_batch_artefacts.run,
            batches_file_path=batches_file_path,
            gcs_output_root=gcs_output_root,
            marker_path=marker_path,
            cohort_name=cohort.name,
        )

        return self.make_outputs(target=cohort, data=marker_path, jobs=job)


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
        return get_output_path(filename=f'somalier/{sequencing_group.id}.somalier')

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

        job: BashJob | None = somalier_extract.somalier_extract(
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


# Pseudo end-stage: depends on every download stage (plus SomalierExtract) so the
# pipeline collapses to a single terminal node once DeleteDataInIca is set aside.
# Without this, DownloadDataFromIca, DownloadBatchArtefactsFromIca, SomalierExtract
# and ReheaderMlrGvcf are all parallel sinks (their only dependent is the optional
# DeleteDataInIca cleanup stage). The body still only consumes the two gVCF inputs;
# the extra edges are ordering-only.
@stage(
    required_stages=[
        DownloadCramFromIca,
        DownloadGvcfFromIca,
        DownloadMlrGvcfFromIca,
        DownloadDataFromIca,
        DownloadBatchArtefactsFromIca,
        SomalierExtract,
    ],
    analysis_type='gvcf',
    analysis_keys=['gvcf'],
)
class ReheaderMlrGvcf(SequencingGroupStage):
    """
    Reheader the MLR gVCF to insert correct reference block information that the MLR process removes.
    """

    def expected_outputs(self, sequencing_group: SequencingGroup) -> dict[str, cpg_utils.Path]:
        return {
            'gvcf': get_output_path(filename=f'recal_gvcf/{sequencing_group.name}.hard-filtered.recal.gvcf.gz'),
            'gvcf_tbi': get_output_path(filename=f'recal_gvcf/{sequencing_group.name}.hard-filtered.recal.gvcf.gz.tbi'),
            'gvcf_md5': get_output_path(
                filename=f'recal_gvcf/{sequencing_group.name}.hard-filtered.recal.gvcf.gz.md5sum'
            ),
            'gvcf_tbi_md5': get_output_path(
                filename=f'recal_gvcf/{sequencing_group.name}.hard-filtered.recal.gvcf.gz.tbi.md5sum'
            ),
        }

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:

        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(sequencing_group=sequencing_group)

        base_gvcf_path: cpg_utils.Path = inputs.as_path(
            target=sequencing_group,
            stage=DownloadGvcfFromIca,
            key='gvcf',
        )
        recal_gvcf_path: cpg_utils.Path = inputs.as_path(
            target=sequencing_group,
            stage=DownloadMlrGvcfFromIca,
            key='gvcf',
        )
        reheader_mlr_gvcf_job: BashJob = reheader_mlr_gvcf.reheader_mlr_gvcf(
            base_gvcf_path=base_gvcf_path,
            recal_gvcf_path=recal_gvcf_path,
            reheadered_gvcf_path=outputs['gvcf'],
        )

        return self.make_outputs(target=sequencing_group, data=outputs, jobs=reheader_mlr_gvcf_job)  # pyright: ignore[reportArgumentType]


# ReheaderMlrGvcf is the single terminal stage and transitively depends on every
# download stage (+ SomalierExtract), so listing those here would just double up.
# Prepare/UploadDataToIca/FastqIntakeQc stay because queue_jobs consumes their
# outputs via inputs.as_path — cpg-flow only resolves inputs from declared deps.
@stage(
    required_stages=[
        PrepareIcaForDragenAnalysis,
        UploadDataToIca,
        FastqIntakeQc,
        ReheaderMlrGvcf,
    ],
)
class DeleteDataInIca(CohortStage):
    """Delete cohort outputs + source CRAMs/FASTQs from ICA to release storage.

    Two project-scoped passes: DRAGEN runs project for the cohort folder
    (cascades to per-batch analyses + per-SG outputs + per-batch FASTQ list
    CSVs) and the uploaded source CRAMs; supplier project for linked FASTQs
    (FASTQ mode only). Each pass verifies delete via `get_project_data`
    after a 60s settle for ICA's async state machine.
    """

    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        return get_pipeline_path(filename=f'{cohort.name}_delete_complete.json')

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput:
        cohort_analysis_output_fid_path: cpg_utils.Path = inputs.as_path(
            target=cohort,
            stage=PrepareIcaForDragenAnalysis,
        )

        cram_fid_paths_dict: dict[str, cpg_utils.Path] | None = None
        fastq_ids_list_path: cpg_utils.Path | None = None
        if READS_TYPE == 'cram':
            cram_fid_paths_dict = inputs.as_path_by_target(stage=UploadDataToIca)
        elif READS_TYPE == 'fastq':
            fastq_ids_list_path = inputs.as_path(
                target=cohort,
                stage=FastqIntakeQc,
                key='fastq_ids_outpath',
            )

        output_path: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        ica_delete_job: PythonJob = initialise_python_job(
            job_name='DeleteDataInIca',
            target=cohort,
            tool_name='ICA',
        )
        ica_delete_job.image(image=get_driver_image())
        ica_delete_job.call(
            delete_data_in_ica.run,
            cohort_name=cohort.name,
            output_path=output_path,
            cohort_analysis_output_fid_path=cohort_analysis_output_fid_path,
            cram_fid_paths_dict=cram_fid_paths_dict,
            fastq_ids_list_path=fastq_ids_list_path,
        )

        return self.make_outputs(target=cohort, data=output_path, jobs=ica_delete_job)
