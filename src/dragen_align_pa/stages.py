import logging
from math import ceil
from typing import TYPE_CHECKING, Final

import coloredlogs
import cpg_utils
from cpg_flow.stage import SequencingGroupStage, StageInput, StageOutput, stage  # type: ignore  # noqa: PGH003
from cpg_flow.targets import SequencingGroup
from cpg_utils.cloud import get_path_components_from_gcp_path
from cpg_utils.config import config_retrieve

from dragen_align_pa.jobs import (
    download_ica_pipeline_outputs,
    download_specific_files_from_ica,
    manage_dragen_pipeline,
    prepare_ica_for_analysis,
    upload_data_to_ica,
)

if TYPE_CHECKING:
    from pathlib import Path

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


coloredlogs.install(level=logging.INFO)


def calculate_needed_storage(
    cram: str,
) -> str:
    logging.info(f'Checking blob size for {cram}')
    storage_size: int = cpg_utils.to_path(cram).stat().st_size
    return f'{ceil(ceil((storage_size / (1024**3)) + 3) * 1.2)}Gi'


# No need to register this stage in Metamist I think, just ICA prep
@stage
class PrepareIcaForDragenAnalysis(SequencingGroupStage):
    """Set up ICA for a single realignment run.

    Creates a folder ID for the Dragen output to be written into.
    """

    def expected_outputs(self, sequencing_group: SequencingGroup) -> cpg_utils.Path:
        sg_bucket: cpg_utils.Path = sequencing_group.dataset.prefix()
        return sg_bucket / GCP_FOLDER_FOR_ICA_PREP / f'{sequencing_group.name}_output_fid.json'

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:  # noqa: ARG002
        bucket_name: str = get_path_components_from_gcp_path(path=str(object=sequencing_group.cram))['bucket']

        outputs = self.expected_outputs(sequencing_group=sequencing_group)
        prepare_ica_job: PythonJob = prepare_ica_for_analysis.initalise_ica_prep_job(sequencing_group=sequencing_group)
        prepare_ica_for_analysis.run_ica_prep_job(
            ica_prep_job=prepare_ica_job,
            output=str(outputs),
            ica_analysis_output_folder=config_retrieve(['ica', 'data_prep', 'output_folder']),
            api_root=ICA_REST_ENDPOINT,
            sg_name=sequencing_group.name,
            bucket_name=bucket_name,
        )

        return self.make_outputs(
            target=sequencing_group,
            data=outputs,
            jobs=prepare_ica_job,
        )


@stage
class UploadDataToIca(SequencingGroupStage):
    def expected_outputs(self, sequencing_group: SequencingGroup) -> cpg_utils.Path:
        sg_bucket: cpg_utils.Path = sequencing_group.dataset.prefix()
        return sg_bucket / GCP_FOLDER_FOR_ICA_PREP / f'{sequencing_group.name}_fids.json'

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:  # noqa: ARG002
        output = self.expected_outputs(sequencing_group=sequencing_group)

        upload_job: BashJob = upload_data_to_ica.upload_data_to_ica(
            job=upload_data_to_ica.initalise_upload_job(sequencing_group=sequencing_group),
            sequencing_group=sequencing_group,
            ica_cli_setup=ICA_CLI_SETUP,
            output=str(output),
        )

        return self.make_outputs(
            target=sequencing_group,
            data=output,
            jobs=upload_job,
        )


# As a SequencingGroupStage, this is expensive for large cohorts. The choices are either
# - Refactor into CohortStage -> no downloads can start until the entire cohort has finished
# - Wait for throttling to become a feature (if possible) and throttle this stage
@stage(
    required_stages=[PrepareIcaForDragenAnalysis, UploadDataToIca],  # type: ignore  # noqa: PGH003
    analysis_type='dragen_align_genotype',
    analysis_keys=['pipeline_id'],
)
class ManageDragenPipeline(SequencingGroupStage):
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
        sequencing_group: SequencingGroup,
    ) -> dict[str, cpg_utils.Path]:
        sg_bucket: cpg_utils.Path = sequencing_group.dataset.prefix()
        return {
            'success': sg_bucket / GCP_FOLDER_FOR_RUNNING_PIPELINE / f'{sequencing_group.name}_pipeline_success.json',
            'pipeline_id': sg_bucket / GCP_FOLDER_FOR_RUNNING_PIPELINE / f'{sequencing_group.name}_pipeline_id.txt',
        }

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:
        outputs: dict[str, cpg_utils.Path | Path] = self.expected_outputs(sequencing_group=sequencing_group)

        management_job: PythonJob = manage_dragen_pipeline.manage_ica_pipeline(
            job=manage_dragen_pipeline.initalise_management_job(
                sequencing_group=sequencing_group,
                pipeline_id_file=str(outputs['pipeline_id']),
            ),
            sequencing_group=sequencing_group,
            pipeline_id_file=str(outputs['pipeline_id']),
            ica_fids_path=str(inputs.as_path(target=sequencing_group, stage=UploadDataToIca)),  # type: ignore  # noqa: PGH003
            analysis_output_fid_path=str(inputs.as_path(target=sequencing_group, stage=PrepareIcaForDragenAnalysis)),  # type: ignore  # noqa: PGH003
            api_root=ICA_REST_ENDPOINT,
            output=str(outputs['success']),
        )

        return self.make_outputs(
            target=sequencing_group,
            data=outputs,
            jobs=management_job,
        )


@stage(analysis_type='dragen_mlr', required_stages=[ManageDragenPipeline])  # type: ignore  # noqa: PGH003
class GvcfMlrWithDragen(SequencingGroupStage):
    def expected_outputs(
        self,
        sequencing_group: SequencingGroup,
    ) -> None:
        pass

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:
        pass


@stage(required_stages=[GvcfMlrWithDragen])  # type: ignore  # noqa: PGH003
class MonitorGvcfMlrWithDragen(SequencingGroupStage):
    def expected_outputs(
        self,
        sequencing_group: SequencingGroup,
    ) -> None:
        pass

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:
        pass


@stage(
    analysis_type='cram',
    analysis_keys=['cram'],
    required_stages=[ManageDragenPipeline],  # type: ignore  # noqa: PGH003
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

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:
        ica_download_job: BashJob = download_specific_files_from_ica.download_data_from_ica(
            job=download_specific_files_from_ica.initalise_download_job(
                sequencing_group=sequencing_group,
                job_name='DownloadCramFromIca',
            ),
            sequencing_group=sequencing_group,
            filetype='cram',
            bucket=get_path_components_from_gcp_path(path=str(object=sequencing_group.cram))['bucket'],
            ica_cli_setup=ICA_CLI_SETUP,
            gcp_folder_for_ica_download=GCP_FOLDER_FOR_ICA_DOWNLOAD,
            pipeline_id_path=str(
                inputs.as_path(target=sequencing_group, stage=ManageDragenPipeline, key='pipeline_id')  # type: ignore  # noqa: PGH003
            ),
        )

        return self.make_outputs(
            target=sequencing_group,
            data=self.expected_outputs(sequencing_group=sequencing_group),
            jobs=ica_download_job,
        )


@stage(
    analysis_type='gvcf',
    analysis_keys=['gvcf'],
    required_stages=[ManageDragenPipeline],  # type: ignore  # noqa: PGH003
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

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:
        """
        Download gVCF and gVCF TBI files from ICA separately. This is to allow registrations of the gVCF files
        in metamist to be done via stage decorators. The pipeline ID needs to be read within the Hail BashJob to get the current
        pipeline ID. If read outside the job, it will get the pipeline ID from the previous pipeline run.
        """  # noqa: E501
        ica_download_job: BashJob = download_specific_files_from_ica.download_data_from_ica(
            job=download_specific_files_from_ica.initalise_download_job(
                sequencing_group=sequencing_group,
                job_name='DownloadGvcfFromIca',
            ),
            sequencing_group=sequencing_group,
            filetype='gvcf',
            bucket=get_path_components_from_gcp_path(path=str(object=sequencing_group.cram))['bucket'],
            ica_cli_setup=ICA_CLI_SETUP,
            gcp_folder_for_ica_download=GCP_FOLDER_FOR_ICA_DOWNLOAD,
            pipeline_id_path=str(
                inputs.as_path(target=sequencing_group, stage=ManageDragenPipeline, key='pipeline_id')  # type: ignore  # noqa: PGH003
            ),
        )

        return self.make_outputs(
            target=sequencing_group,
            data=self.expected_outputs(sequencing_group=sequencing_group),
            jobs=ica_download_job,
        )


@stage(
    analysis_type='ica_data_download',
    required_stages=[ManageDragenPipeline, DownloadCramFromIca, DownloadGvcfFromIca],  # type: ignore  # noqa: PGH003
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

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:
        ica_download_job: BashJob = download_ica_pipeline_outputs.download_bulk_data_from_ica(
            job=download_ica_pipeline_outputs.initalise_bulk_download_job(sequencing_group=sequencing_group),
            sequencing_group=sequencing_group,
            gcp_folder_for_ica_download=GCP_FOLDER_FOR_ICA_DOWNLOAD,
            pipeline_id_path=str(
                inputs.as_path(
                    target=sequencing_group,
                    stage=ManageDragenPipeline,  # type: ignore  # noqa: PGH003
                    key='pipeline_id',
                )
            ),
            ica_cli_setup=ICA_CLI_SETUP,
        )

        return self.make_outputs(
            target=sequencing_group,
            data=self.expected_outputs(sequencing_group=sequencing_group),
            jobs=ica_download_job,
        )
