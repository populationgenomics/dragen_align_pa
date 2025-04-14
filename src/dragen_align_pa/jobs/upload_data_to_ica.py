import logging

import coloredlogs
from cpg_flow.targets import SequencingGroup
from cpg_utils.cloud import get_path_components_from_gcp_path
from cpg_utils.config import config_retrieve
from cpg_utils.hail_batch import authenticate_cloud_credentials_in_job, command, get_batch
from hailtop.batch.job import BashJob

from dragen_align_pa.utils import calculate_needed_storage


def initalise_upload_job(sequencing_group: SequencingGroup) -> BashJob:
    upload_job: BashJob = get_batch().new_bash_job(
        name='UploadDataToIca',
        attributes=(sequencing_group.get_job_attrs(sequencing_group) or {}) | {'tool': 'ICA'},  # type: ignore  # noqa: PGH003
    )

    upload_job.image(image=config_retrieve(['workflow', 'driver_image']))
    upload_job.storage(calculate_needed_storage(cram=str(sequencing_group.cram)))

    return upload_job


def upload_data_to_ica(job: BashJob, sequencing_group: SequencingGroup, ica_cli_setup: str, output: str) -> BashJob:
    upload_folder = config_retrieve(['ica', 'data_prep', 'upload_folder'])
    bucket: str = get_path_components_from_gcp_path(str(sequencing_group.cram))['bucket']
    authenticate_cloud_credentials_in_job(job)
    coloredlogs.install(level=logging.INFO)
    logging.info(f'Uploading CRAM and CRAI for {sequencing_group.name}')

    # Check if the CRAM and CRAI already exists in ICA before uploading. If they exist, just return the ID for the CRAM and CRAI  # noqa: E501
    # The internal `command` method is a wrapper from cpg_utils.hail_batch that extends the normal hail batch command
    job.command(
        command(
            f"""
            function copy_from_gcp {{
                mkdir -p $BATCH_TMPDIR/{sequencing_group.name}
                gcloud storage cp {sequencing_group.cram} $BATCH_TMPDIR/{sequencing_group.name}/
                gcloud storage cp {sequencing_group.cram}.crai $BATCH_TMPDIR/{sequencing_group.name}/
            }}
            function upload_cram {{
                icav2 projectdata upload $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.cram /{bucket}/{upload_folder}/{sequencing_group.name}/
            }}
            function upload_crai {{
                icav2 projectdata upload $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.cram.crai /{bucket}/{upload_folder}/{sequencing_group.name}/
            }}

            function get_fids {{
                # Add a random delay before calling the ICA API to hopefully stop empty JSON files from being written to GCP
                sleep $(shuf -i 1-30 -n 1)
                icav2 projectdata list --parent-folder /{bucket}/{upload_folder}/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.cram --match-mode EXACT -o json | jq -r '.items[].id' > cram_id
                icav2 projectdata list --parent-folder /{bucket}/{upload_folder}/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.cram.crai --match-mode EXACT -o json | jq -r '.items[].id' > crai_id

                jq -n --arg cram_id $(cat cram_id) --arg crai_id $(cat crai_id) '{{cram_fid: $cram_id, crai_fid: $crai_id}}' > {job.ofile}
            }}

            {ica_cli_setup}
            copy_from_gcp
            cram_status=$(icav2 projectdata list --parent-folder /{bucket}/{upload_folder}/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.cram --match-mode EXACT -o json | jq -r '.items[].details.status')
            crai_status=$(icav2 projectdata list --parent-folder /{bucket}/{upload_folder}/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.cram.crai --match-mode EXACT -o json | jq -r '.items[].details.status')

            if [[ $cram_status != "AVAILABLE" ]]
            then
                retry upload_cram
            fi

            if [[ $crai_status != "AVAILABLE" ]]
            then
                retry upload_crai
            fi

            get_fids

            # Try 10 times to call the ICA API to get the required file ID data
            counter=0
            while [ $counter -le 10 ]
                do
                get_fids
                if [ -s {job.ofile} ]
                then
                    break
                elif [ $counter -eq 10 ]
                then
                    exit 1
                fi
                counter=$((counter+1))
            done
            """,  # noqa: E501
            define_retry_function=True,
        ),
    )
    get_batch().write_output(job.ofile, output)

    return job
