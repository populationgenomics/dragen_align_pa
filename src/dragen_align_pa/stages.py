import sys
from typing import TYPE_CHECKING, Final

import cpg_utils
from cpg_flow.inputs import get_multicohort
from cpg_flow.stage import (
    CohortStage,
    SequencingGroupStage,
    StageInput,
    StageOutput,
    stage,  # type: ignore[ReportUnknownVariableType]
)
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_utils.cloud import get_path_components_from_gcp_path
from cpg_utils.config import config_retrieve
from loguru import logger

from dragen_align_pa.jobs import (
    delete_data_in_ica,
    download_ica_pipeline_outputs,
    download_specific_files_from_ica,
    manage_dragen_mlr,
    manage_dragen_pipeline,
    prepare_ica_for_analysis,
    upload_data_to_ica,
)

if TYPE_CHECKING:
    from hailtop.batch.job import BashJob, PythonJob

DRAGEN_VERSION: Final = config_retrieve(['ica', 'pipelines', 'dragen_version'])
GCP_FOLDER_FOR_ICA_PREP: Final = f'ica/{DRAGEN_VERSION}/prepare'
GCP_FOLDER_FOR_RUNNING_PIPELINE: Final = f'ica/{DRAGEN_VERSION}/pipelines'
GCP_FOLDER_FOR_ICA_DOWNLOAD: Final = f'ica/{DRAGEN_VERSION}/output'
ICA_REST_ENDPOINT: Final = 'https://ica.illumina.com/ica/rest'
ICA_CLI_SETUP: Final = """
mkdir -p $HOME/.icav2
echo "server-url: ica.illumina.com" > /root/.icav2/config.yaml

set +x
gcloud secrets versions access latest --secret=illumina_cpg_workbench_api --project=cpg-common | jq -r .apiKey > key
gcloud secrets versions access latest --secret=illumina_cpg_workbench_api --project=cpg-common | jq -r .projectID > projectID
echo "x-api-key: $(cat key)" >> $HOME/.icav2/config.yaml
icav2 projects enter $(cat projectID)
set -x
"""  # noqa: E501

logger.remove(0)
logger.add(sink=sys.stdout, format='{time} - {level} - {message}')


# No need to register this stage in Metamist I think, just ICA prep
@stage
class PrepareIcaForDragenAnalysis(SequencingGroupStage):
    """Set up ICA for a single realignment run.

    Creates a folder ID for the Dragen output to be written into.
    """

    def expected_outputs(self, sequencing_group: SequencingGroup) -> cpg_utils.Path:
        sg_bucket: cpg_utils.Path = sequencing_group.dataset.prefix()
        return sg_bucket / GCP_FOLDER_FOR_ICA_PREP / f'{sequencing_group.name}_output_fid.json'

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput:  # noqa: ARG002
        bucket_name: str = get_path_components_from_gcp_path(path=str(object=sequencing_group.cram))['bucket']
        outputs: cpg_utils.Path = self.expected_outputs(sequencing_group=sequencing_group)

        ica_prep_job: PythonJob = prepare_ica_for_analysis.run_ica_prep_job(
            sequencing_group=sequencing_group,
            output=str(outputs),
            api_root=ICA_REST_ENDPOINT,
            sg_name=sequencing_group.name,
            bucket_name=bucket_name,
        )

        return self.make_outputs(
            target=sequencing_group,
            data=outputs,
            jobs=ica_prep_job,
        )


@stage
class UploadDataToIca(SequencingGroupStage):
    def expected_outputs(self, sequencing_group: SequencingGroup) -> cpg_utils.Path:
        sg_bucket: cpg_utils.Path = sequencing_group.dataset.prefix()
        return sg_bucket / GCP_FOLDER_FOR_ICA_PREP / f'{sequencing_group.name}_fids.json'

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput:  # noqa: ARG002
        output: cpg_utils.Path = self.expected_outputs(sequencing_group=sequencing_group)

        upload_job: BashJob = upload_data_to_ica.upload_data_to_ica(
            sequencing_group=sequencing_group,
            ica_cli_setup=ICA_CLI_SETUP,
            output=str(output),
        )

        return self.make_outputs(
            target=sequencing_group,
            data=output,
            jobs=upload_job,
        )


@stage(
    required_stages=[PrepareIcaForDragenAnalysis, UploadDataToIca],
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

    def expected_outputs(
        self,
        cohort: Cohort,
    ) -> dict[str, cpg_utils.Path]:
        sg_bucket: cpg_utils.Path = cohort.dataset.prefix()
        prefix: cpg_utils.Path = sg_bucket / GCP_FOLDER_FOR_RUNNING_PIPELINE
        results: dict[str, cpg_utils.Path] = {f'{cohort.name}_errors': prefix / f'{cohort.name}_errors.log'}
        for sequencing_group in cohort.get_sequencing_groups():
            sg_name: str = sequencing_group.name
            results |= {
                f'{sg_name}_success': prefix / f'{sg_name}_pipeline_success.json',
                f'{sg_name}_pipeline_id_and_arguid': prefix / f'{sg_name}_pipeline_id_and_arguid.json',
            }
        return results

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

        # Inputs from previous stages
        ica_fids_path: dict[str, cpg_utils.Path] = inputs.as_path_by_target(stage=UploadDataToIca)
        analysis_output_fids_path: dict[str, cpg_utils.Path] = inputs.as_path_by_target(
            stage=PrepareIcaForDragenAnalysis
        )

        management_job: PythonJob = manage_dragen_pipeline.manage_ica_pipeline(
            cohort=cohort,
            outputs=outputs,
            ica_fids_path=ica_fids_path,
            analysis_output_fids_path=analysis_output_fids_path,
            api_root=ICA_REST_ENDPOINT,
        )

        return self.make_outputs(
            target=cohort,
            data=outputs,
            jobs=management_job,
        )


@stage(required_stages=[ManageDragenPipeline])
class ManageDragenMlr(CohortStage):
    def expected_outputs(
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
            bucket=get_path_components_from_gcp_path(str(cohort.dataset.prefix()))['bucket'],
            ica_cli_setup=ICA_CLI_SETUP,
            pipeline_id_arguid_path_dict=pipeline_id_arguid_path_dict,
            api_root=ICA_REST_ENDPOINT,
            outputs=outputs,
        )

        return self.make_outputs(target=cohort, data=outputs, jobs=mlr_job)


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

    def expected_outputs(
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

        ica_download_job: BashJob = download_specific_files_from_ica.download_data_from_ica(
            job_name='DownloadCramFromIca',
            sequencing_group=sequencing_group,
            filetype='cram',
            bucket=get_path_components_from_gcp_path(path=str(object=sequencing_group.cram))['bucket'],
            ica_cli_setup=ICA_CLI_SETUP,
            gcp_folder_for_ica_download=GCP_FOLDER_FOR_ICA_DOWNLOAD,
            pipeline_id_arguid_path=pipeline_id_arguid_path,
        )

        return self.make_outputs(
            target=sequencing_group,
            data=outputs,
            jobs=ica_download_job,
        )


@stage(
    analysis_type='gvcf',
    analysis_keys=['gvcf'],
    required_stages=[ManageDragenPipeline],
)
class DownloadGvcfFromIca(SequencingGroupStage):
    def expected_outputs(
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

        ica_download_job: BashJob = download_specific_files_from_ica.download_data_from_ica(
            job_name='DownloadGvcfFromIca',
            sequencing_group=sequencing_group,
            filetype='base_gvcf',
            bucket=get_path_components_from_gcp_path(path=str(object=sequencing_group.cram))['bucket'],
            ica_cli_setup=ICA_CLI_SETUP,
            gcp_folder_for_ica_download=GCP_FOLDER_FOR_ICA_DOWNLOAD,
            pipeline_id_arguid_path=pipeline_id_arguid_path,
        )

        return self.make_outputs(
            target=sequencing_group,
            data=outputs,
            jobs=ica_download_job,
        )


@stage(
    analysis_type='gvcf',
    analysis_keys=['gvcf'],
    required_stages=[DownloadGvcfFromIca, ManageDragenMlr],
)
class DownloadMlrGvcfFromIca(SequencingGroupStage):
    def expected_outputs(
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
        mlr_pipeline_id: cpg_utils.Path = inputs.as_dict(
            target=get_multicohort().get_cohorts()[0],
            stage=ManageDragenMlr,
        )[f'{sequencing_group.name}_mlr_pipeline_id']

        ica_download_job: BashJob = download_specific_files_from_ica.download_data_from_ica(
            job_name='DownloadMlrGvcfFromIca',
            sequencing_group=sequencing_group,
            filetype='recal_gvcf',
            bucket=get_path_components_from_gcp_path(path=str(object=sequencing_group.cram))['bucket'],
            ica_cli_setup=ICA_CLI_SETUP,
            gcp_folder_for_ica_download=GCP_FOLDER_FOR_ICA_DOWNLOAD,
            pipeline_id_arguid_path=mlr_pipeline_id,
        )

        return self.make_outputs(
            target=sequencing_group,
            data=outputs,
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

        ica_download_job: BashJob = download_ica_pipeline_outputs.download_bulk_data_from_ica(
            sequencing_group=sequencing_group,
            gcp_folder_for_ica_download=GCP_FOLDER_FOR_ICA_DOWNLOAD,
            pipeline_id_arguid_path=pipeline_id_arguid_path,
            ica_cli_setup=ICA_CLI_SETUP,
        )

        return self.make_outputs(
            target=sequencing_group,
            data=outputs,
            jobs=ica_download_job,
        )


@stage(required_stages=[DownloadDataFromIca])
class DeleteDataInIca(CohortStage):
    """
    Delete all the data in ICA for a dataset, so we don't pay storage costs once processing is finished
    """

    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        bucket_name: cpg_utils.Path = cohort.dataset.prefix()
        return bucket_name / GCP_FOLDER_FOR_ICA_PREP / 'placeholder_for_delete.txt'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:  # noqa: ARG002
        outputs: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        bucket_name: str = str(cohort.dataset.prefix())

        ica_delete_job: PythonJob = delete_data_in_ica.delete_data_in_ica(
            cohort=cohort,
            bucket=bucket_name,
            api_root=ICA_REST_ENDPOINT,
        )

        return self.make_outputs(target=cohort, data=outputs, jobs=ica_delete_job)
