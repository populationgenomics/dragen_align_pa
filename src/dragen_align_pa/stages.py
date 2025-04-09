import logging
from math import ceil
from pathlib import Path
from typing import TYPE_CHECKING, Final

import coloredlogs
import cpg_utils
from cpg_flow.stage import SequencingGroupStage, StageInput, StageOutput, stage
from cpg_flow.targets import SequencingGroup
from cpg_utils.cloud import get_path_components_from_gcp_path
from cpg_utils.config import config_retrieve
from cpg_utils.hail_batch import Batch, authenticate_cloud_credentials_in_job, command, get_batch
from hailtop.batch.job import BashJob

from dragen_align_pa.jobs import manage_dragen_pipeline
from src.dragen_align_pa.jobs import (
    prepare_ica_for_analysis,
    upload_data_to_ica,
)

if TYPE_CHECKING:
    from hailtop.batch.job import BashJob

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
"""


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

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:
        bucket_name: str = get_path_components_from_gcp_path(path=str(object=sequencing_group.cram))['bucket']

        outputs = self.expected_outputs(sequencing_group=sequencing_group)
        prepare_ica_job = prepare_ica_for_analysis.run_ica_prep_job(
            ica_prep_job=prepare_ica_for_analysis.initalise_ica_prep_job(sequencing_group=sequencing_group),
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

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:
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
    required_stages=[PrepareIcaForDragenAnalysis, UploadDataToIca],
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
    """

    def expected_outputs(
        self,
        sequencing_group: SequencingGroup,
    ) -> dict[str, cpg_utils.Path]:
        sg_bucket: cpg_utils.Path = sequencing_group.dataset.prefix()
        return {
            'success': sg_bucket / GCP_FOLDER_FOR_RUNNING_PIPELINE / f'{sequencing_group.name}_pipeline_success.json',
            'pipeline_id': sg_bucket / GCP_FOLDER_FOR_RUNNING_PIPELINE / f'{sequencing_group.name}_pipeline_id.json',
        }

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:
        outputs: dict[str, cpg_utils.Path | Path] = self.expected_outputs(sequencing_group=sequencing_group)

        management_job = manage_dragen_pipeline.manage_ica_pipeline(
            job=manage_dragen_pipeline.initalise_management_job(
                sequencing_group=sequencing_group,
                pipeline_id_file=str(outputs['pipeline_id']),
            ),
            sequencing_group=sequencing_group,
            pipeline_id_file=str(outputs['pipeline_id']),
            ica_fids_path=str(inputs.as_path(target=sequencing_group, stage=UploadDataToIca)),
            analysis_output_fid_path=str(inputs.as_path(target=sequencing_group, stage=PrepareIcaForDragenAnalysis)),
            api_root=ICA_REST_ENDPOINT,
            output=str(outputs['success']),
        )

        return self.make_outputs(
            target=sequencing_group,
            data=outputs,
            jobs=management_job,
        )


@stage(analysis_type='dragen_mlr', required_stages=[ManageDragenPipeline])
class GvcfMlrWithDragen(SequencingGroupStage):
    def expected_outputs(
        self,
        sequencing_group: SequencingGroup,
    ) -> None:
        pass

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:
        pass


@stage(required_stages=[GvcfMlrWithDragen])
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
    required_stages=[PrepareIcaForDragenAnalysis, ManageDragenPipeline],
)
class DownloadCramFromIca(SequencingGroupStage):
    """
    Download cram and crai files from ICA separately. This is to allow registrations of the cram files
    in metamist to be done via stage decorators. The pipeline ID needs to be read within the Hail BashJob to get the current
    pipeline ID. If read outside the job, it will get the pipeline ID from the previous pipeline run.
    """

    def expected_outputs(
        self,
        sequencing_group: SequencingGroup,
    ) -> cpg_utils.Path:
        bucket_name: cpg_utils.Path = sequencing_group.dataset.prefix()
        return {
            'cram': bucket_name / GCP_FOLDER_FOR_ICA_DOWNLOAD / 'cram' / f'{sequencing_group.name}.cram',
            'crai': bucket_name / GCP_FOLDER_FOR_ICA_DOWNLOAD / 'cram' / f'{sequencing_group.name}.cram.crai',
        }

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:
        bucket_name: str = get_path_components_from_gcp_path(path=str(object=sequencing_group.cram))['bucket']
        ica_analysis_output_folder = config_retrieve(['ica', 'data_prep', 'output_folder'])

        batch_instance: Batch = get_batch()
        ica_download_job: BashJob = batch_instance.new_bash_job(
            name='DownloadCramFromIca',
            attributes=(self.get_job_attrs(sequencing_group) or {}) | {'tool': 'ICA'},
        )

        pipeline_id_path: cpg_utils.Path = inputs.as_path(
            target=sequencing_group,
            stage=ManageDragenPipeline,
            key='pipeline_id',
        )

        ica_download_job.storage(storage=calculate_needed_storage(cram=str(sequencing_group.cram)))
        ica_download_job.memory('8Gi')
        ica_download_job.image(image=config_retrieve(['workflow', 'driver_image']))

        # Download just the CRAM and CRAI files  with ICA. Don't log projectId or API key
        authenticate_cloud_credentials_in_job(ica_download_job)
        ica_download_job.command(
            command(
                f"""
                function download_cram {{
                cram_id=$(icav2 projectdata list --parent-folder /{bucket_name}/{ica_analysis_output_folder}/{sequencing_group.name}/{sequencing_group.name}-$pipeline_id/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.cram --match-mode EXACT -o json | jq -r '.items[].id')
                crai_id=$(icav2 projectdata list --parent-folder /{bucket_name}/{ica_analysis_output_folder}/{sequencing_group.name}/{sequencing_group.name}-$pipeline_id/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.cram.crai --match-mode EXACT -o json | jq -r '.items[].id')
                cram_md5=$(icav2 projectdata list --parent-folder /{bucket_name}/{ica_analysis_output_folder}/{sequencing_group.name}/{sequencing_group.name}-$pipeline_id/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.cram.md5sum --match-mode EXACT -o json | jq -r '.items[].id')
                icav2 projectdata download $cram_id $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.cram --exclude-source-path
                icav2 projectdata download $crai_id $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.cram.crai --exclude-source-path
                icav2 projectdata download $cram_md5 $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.cram.md5sum --exclude-source-path

                # Get md5sum of the downloaded CRAM file and compare it with the ICA md5sum
                # Checking here because using icav2 package to download which doesn't automatically perform checksum matching
                ica_md5_hash=$(cat $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.cram.md5sum)
                cram_md5=$(cat $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.cram | md5sum | cut -d " " -f1)
                if [ "$cram_md5" != "$ica_md5_hash" ]; then
                    echo "Error: MD5 checksums do not match!"
                    echo "ICA MD5: $ica_md5_hash"
                    echo "Cram MD5: $cram_md5"
                    exit 1
                else
                    echo "MD5 checksums match."
                fi

                # Copy the CRAM and CRAI files to the bucket
                # Checksums are already checked by `gcloud storage cp`
                gcloud storage cp $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.cram gs://{bucket_name}/{GCP_FOLDER_FOR_ICA_DOWNLOAD}/cram/
                gcloud storage cp $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.cram.crai gs://{bucket_name}/{GCP_FOLDER_FOR_ICA_DOWNLOAD}/cram/
                gcloud storage cp $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.cram.md5sum gs://{bucket_name}/{GCP_FOLDER_FOR_ICA_DOWNLOAD}/cram/
                }}

                {ICA_CLI_SETUP}
                mkdir -p $BATCH_TMPDIR/{sequencing_group.name}
                pipeline_id_filename=$(basename {pipeline_id_path})
                gcloud storage cp {pipeline_id_path} .
                pipeline_id=$(cat $pipeline_id_filename)
                echo "Pipeline ID: $pipeline_id"

                retry download_cram
                """,
                define_retry_function=True,
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
    required_stages=[ManageDragenPipeline],
)
class DownloadGvcfFromIca(SequencingGroupStage):
    def expected_outputs(
        self,
        sequencing_group: SequencingGroup,
    ) -> cpg_utils.Path:
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
        """
        bucket_name: str = get_path_components_from_gcp_path(path=str(object=sequencing_group.cram))['bucket']
        ica_analysis_output_folder = config_retrieve(['ica', 'data_prep', 'output_folder'])

        batch_instance: Batch = get_batch()
        ica_download_job: BashJob = batch_instance.new_bash_job(
            name='DownloadGvcfFromIca',
            attributes=(self.get_job_attrs(sequencing_group) or {}) | {'tool': 'ICA'},
        )

        pipeline_id_path: cpg_utils.Path = inputs.as_path(
            target=sequencing_group,
            stage=ManageDragenPipeline,
            key='pipeline_id',
        )

        ica_download_job.storage(storage=calculate_needed_storage(cram=str(sequencing_group.cram)))
        ica_download_job.memory('8Gi')
        ica_download_job.image(image=config_retrieve(['workflow', 'driver_image']))

        # Download just the CRAM and CRAI files  with ICA. Don't log projectId or API key
        authenticate_cloud_credentials_in_job(ica_download_job)
        ica_download_job.command(
            command(
                f"""
                function download_gvcf {{
                gvcf_id=$(icav2 projectdata list --parent-folder /{bucket_name}/{ica_analysis_output_folder}/{sequencing_group.name}/{sequencing_group.name}-$pipeline_id/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.hard-filtered.gvcf.gz --match-mode EXACT -o json | jq -r '.items[].id')
                gvcf_tbi_id=$(icav2 projectdata list --parent-folder /{bucket_name}/{ica_analysis_output_folder}/{sequencing_group.name}/{sequencing_group.name}-$pipeline_id/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.hard-filtered.gvcf.gz.tbi --match-mode EXACT -o json | jq -r '.items[].id')
                gvcf_md5_id=$(icav2 projectdata list --parent-folder /{bucket_name}/{ica_analysis_output_folder}/{sequencing_group.name}/{sequencing_group.name}-$pipeline_id/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.hard-filtered.gvcf.gz.md5sum --match-mode EXACT -o json | jq -r '.items[].id')
                icav2 projectdata download $gvcf_id $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.hard-filtered.gvcf.gz --exclude-source-path
                icav2 projectdata download $gvcf_tbi_id $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.hard-filtered.gvcf.gz.tbi --exclude-source-path
                icav2 projectdata download $gvcf_md5_id $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.hard-filtered.gvcf.gz.md5sum --exclude-source-path

                # Get md5sum of the downloaded gVCF file and compare it with the ICA md5sum
                # Checking here because using icav2 package to download which doesn't automatically perform checksum matching
                ica_md5_hash=$(cat $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.hard-filtered.gvcf.gz.md5sum)
                gvcf_md5=$(cat $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.hard-filtered.gvcf.gz | md5sum | cut -d " " -f1)
                if [ "$gvcf_md5" != "$ica_md5_hash" ]; then
                    echo "Error: MD5 checksums do not match!"
                    echo "ICA MD5: $ica_md5_hash"
                    echo "Gvcf MD5: $gvcf_md5"
                    exit 1
                else
                    echo "MD5 checksums match."
                fi

                # Copy the gVCF and gVCF TBI files to the bucket
                # Checksums are already checked by `gcloud storage cp`
                gcloud storage cp $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.hard-filtered.gvcf.gz gs://{bucket_name}/{GCP_FOLDER_FOR_ICA_DOWNLOAD}/base_gvcf/
                gcloud storage cp $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.hard-filtered.gvcf.gz.tbi gs://{bucket_name}/{GCP_FOLDER_FOR_ICA_DOWNLOAD}/base_gvcf/
                gcloud storage cp $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.hard-filtered.gvcf.gz.md5sum gs://{bucket_name}/{GCP_FOLDER_FOR_ICA_DOWNLOAD}/base_gvcf/
                }}

                {ICA_CLI_SETUP}
                mkdir -p $BATCH_TMPDIR/{sequencing_group.name}
                pipeline_id_filename=$(basename {pipeline_id_path})
                gcloud storage cp {pipeline_id_path} .
                pipeline_id=$(cat $pipeline_id_filename)
                echo "Pipeline ID: $pipeline_id"

                retry download_gvcf
                """,
                define_retry_function=True,
            ),
        )

        return self.make_outputs(
            target=sequencing_group,
            data=self.expected_outputs(sequencing_group=sequencing_group),
            jobs=ica_download_job,
        )


@stage(
    analysis_type='ica_data_download',
    required_stages=[ManageDragenPipeline, DownloadCramFromIca, DownloadGvcfFromIca],
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
        bucket_name: str = get_path_components_from_gcp_path(path=str(object=sequencing_group.cram))['bucket']
        ica_analysis_output_folder = config_retrieve(['ica', 'data_prep', 'output_folder'])
        pipeline_id_path: cpg_utils.Path = inputs.as_path(
            target=sequencing_group,
            stage=ManageDragenPipeline,
            key='pipeline_id',
        )

        batch_instance: Batch = get_batch()
        ica_download_job: BashJob = batch_instance.new_bash_job(
            name='DownloadDataFromIca',
            attributes=(self.get_job_attrs(sequencing_group) or {}) | {'tool': 'ICA'},
        )

        ica_download_job.storage(storage=calculate_needed_storage(cram=str(sequencing_group.cram)))
        ica_download_job.memory('8Gi')
        ica_download_job.image(image=config_retrieve(['workflow', 'driver_image']))

        # Download an entire folder (except crams and gvcfs) with ICA. Don't log projectId or API key
        authenticate_cloud_credentials_in_job(ica_download_job)
        ica_download_job.command(
            command(
                rf"""
                function download_extra_data {{
                files_and_ids=$(icav2 projectdata list --parent-folder /{bucket_name}/{ica_analysis_output_folder}/{sequencing_group.name}/{sequencing_group.name}-$pipeline_id/{sequencing_group.name}/ -o json | jq -r '.items[] | select(.details.name | test(".cram|.gvcf") | not) | "\(.details.name) \(.id)"')
                while IFS= read -r line; do
                    name=$(echo "$line" | awk '{{print $1}}')
                    id=$(echo "$line" | awk '{{print $2}}')
                    echo "Downloading $name with ID $id"
                    icav2 projectdata download $id $BATCH_TMPDIR/{sequencing_group.name}/$name --exclude-source-path
                done <<< "$files_and_ids"
                gcloud storage cp --recursive $BATCH_TMPDIR/{sequencing_group.name}/* gs://{bucket_name}/{GCP_FOLDER_FOR_ICA_DOWNLOAD}/dragen_metrics/{sequencing_group.name}/
                }}

                {ICA_CLI_SETUP}
                # List all files in the folder except crams and gvcf and download them
                mkdir -p $BATCH_TMPDIR/{sequencing_group.name}
                pipeline_id_filename=$(basename {pipeline_id_path})
                gcloud storage cp {pipeline_id_path} .
                pipeline_id=$(cat $pipeline_id_filename)
                echo "Pipeline ID: $pipeline_id"

                retry download_extra_data
                """,
                define_retry_function=True,
            ),
        )

        return self.make_outputs(
            target=sequencing_group,
            data=self.expected_outputs(sequencing_group=sequencing_group),
            jobs=ica_download_job,
        )
