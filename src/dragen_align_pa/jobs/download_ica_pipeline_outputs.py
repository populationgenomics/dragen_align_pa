"""Download all the non CRAM / GVCF outputs from ICA"""

from cpg_flow.targets import SequencingGroup
from cpg_utils.cloud import get_path_components_from_gcp_path
from cpg_utils.config import config_retrieve
from cpg_utils.hail_batch import authenticate_cloud_credentials_in_job, command, get_batch
from hailtop.batch.job import BashJob


def initalise_bulk_download_job(sequencing_group: SequencingGroup) -> BashJob:
    bulk_download_job = get_batch().new_bash_job(
        name='DownloadDataFromIca',
        attributes=(sequencing_group.get_job_attrs(sequencing_group) or {}) | {'tool': 'ICA'},  # type: ignore  # noqa: PGH003
    )
    bulk_download_job.image(image=config_retrieve(['workflow', 'driver_image']))
    return bulk_download_job


def download_bulk_data_from_ica(
    job: BashJob,
    sequencing_group: SequencingGroup,
    gcp_folder_for_ica_download: str,
    pipeline_id_path: str,
    ica_cli_setup: str,
) -> BashJob:
    authenticate_cloud_credentials_in_job(job=job)
    ica_analysis_output_folder = config_retrieve(['ica', 'data_prep', 'output_folder'])
    bucket: str = get_path_components_from_gcp_path(path=str(object=sequencing_group.cram))['bucket']
    job.command(
        command(
            rf"""
            function download_extra_data {{
            files_and_ids=$(icav2 projectdata list --parent-folder /{bucket}/{ica_analysis_output_folder}/{sequencing_group.name}/{sequencing_group.name}-$pipeline_id/{sequencing_group.name}/ -o json | jq -r '.items[] | select(.details.name | test(".cram|.gvcf") | not) | "\(.details.name) \(.id)"')
            while IFS= read -r line; do
                name=$(echo "$line" | awk '{{print $1}}')
                id=$(echo "$line" | awk '{{print $2}}')
                echo "Downloading $name with ID $id"
                icav2 projectdata download $id $BATCH_TMPDIR/{sequencing_group.name}/$name --exclude-source-path
            done <<< "$files_and_ids"
            gcloud storage cp --recursive $BATCH_TMPDIR/{sequencing_group.name}/* gs://{bucket}/{gcp_folder_for_ica_download}/dragen_metrics/{sequencing_group.name}/
            }}

            {ica_cli_setup}
            # List all files in the folder except crams and gvcf and download them
            mkdir -p $BATCH_TMPDIR/{sequencing_group.name}
            pipeline_id_filename=$(basename {pipeline_id_path})
            gcloud storage cp {pipeline_id_path} .
            pipeline_id=$(cat $pipeline_id_filename)
            echo "Pipeline ID: $pipeline_id"

            retry download_extra_data
            """,  # noqa: E501
            define_retry_function=True,
        ),
    )
    return job
