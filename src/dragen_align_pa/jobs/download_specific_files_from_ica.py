from typing import Literal

import cpg_utils
from cpg_flow.targets import SequencingGroup
from cpg_utils.config import config_retrieve
from cpg_utils.hail_batch import authenticate_cloud_credentials_in_job, command, get_batch
from hailtop.batch.job import BashJob
from loguru import logger

from dragen_align_pa.utils import calculate_needed_storage


def _initalise_download_job(sequencing_group: SequencingGroup, job_name: str) -> BashJob:
    download_job: BashJob = get_batch().new_bash_job(
        name=job_name,
        attributes=(sequencing_group.get_job_attrs() or {}) | {'tool': 'ICA'},  # type: ignore  # noqa: PGH003
    )

    download_job.image(image=config_retrieve(['images', 'ica']))
    download_job.storage(calculate_needed_storage(cram=str(sequencing_group.cram)))
    download_job.memory('8Gi')

    return download_job


def download_data_from_ica(
    job_name: str,
    sequencing_group: SequencingGroup,
    filetype: str,
    bucket: str,
    ica_cli_setup: str,
    gcp_folder_for_ica_download: str,
    pipeline_id_path: cpg_utils.Path,
) -> BashJob:
    logger.info(f'Downloading {filetype} and {filetype} index for {sequencing_group.name}')

    job: BashJob = _initalise_download_job(sequencing_group=sequencing_group, job_name=job_name)
    authenticate_cloud_credentials_in_job(job=job)

    ica_analysis_output_folder = config_retrieve(['ica', 'data_prep', 'output_folder'])
    data: Literal['cram', 'hard-filtered.gvcf.gz'] = 'cram' if filetype == 'cram' else 'hard-filtered.gvcf.gz'
    index: Literal['crai', 'tbi'] = 'crai' if filetype == 'cram' else 'tbi'
    if filetype == 'cram':
        gcp_prefix = 'cram'
    elif filetype == 'gvcf':
        gcp_prefix = 'base_gvcf'
    else:
        gcp_prefix = 'mlr_gvcf'

    job.command(
        command(
            f"""
                function download_individual_files {{
                main_data=$(icav2 projectdata list --parent-folder /{bucket}/{ica_analysis_output_folder}/{sequencing_group.name}/{sequencing_group.name}-$pipeline_id/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.{data} --match-mode EXACT -o json | jq -r '.items[].id')
                index=$(icav2 projectdata list --parent-folder /{bucket}/{ica_analysis_output_folder}/{sequencing_group.name}/{sequencing_group.name}-$pipeline_id/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.{data}.{index} --match-mode EXACT -o json | jq -r '.items[].id')
                md5=$(icav2 projectdata list --parent-folder /{bucket}/{ica_analysis_output_folder}/{sequencing_group.name}/{sequencing_group.name}-$pipeline_id/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.{data}.md5sum --match-mode EXACT -o json | jq -r '.items[].id')
                icav2 projectdata download $main_data $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.{data} --exclude-source-path
                icav2 projectdata download $index $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.{data}.{index} --exclude-source-path
                icav2 projectdata download $md5 $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.{data}.md5sum --exclude-source-path

                # Get md5sum of the downloaded data file file and compare it with the ICA md5sum
                # Checking here because using icav2 package to download which doesn't automatically perform checksum matching
                ica_md5_hash=$(cat $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.{data}.md5sum)
                self_md5=$(cat $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.{data} | md5sum | cut -d " " -f1)
                if [ "$self_md5" != "$ica_md5_hash" ]; then
                    echo "Error: MD5 checksums do not match!"
                    echo "ICA MD5: $ica_md5_hash"
                    echo "Self MD5: $cram_md5"
                    exit 1
                else
                    echo "MD5 checksums match."
                fi

                # Copy the data and index files to the bucket
                # Checksums are already checked by `gcloud storage cp`
                gcloud storage cp $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.{data} gs://{bucket}/{gcp_folder_for_ica_download}/{gcp_prefix}/
                gcloud storage cp $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.{data}.{index} gs://{bucket}/{gcp_folder_for_ica_download}/{gcp_prefix}/
                gcloud storage cp $BATCH_TMPDIR/{sequencing_group.name}/{sequencing_group.name}.{data}.md5sum gs://{bucket}/{gcp_folder_for_ica_download}/{gcp_prefix}/
                }}

                {ica_cli_setup}
                mkdir -p $BATCH_TMPDIR/{sequencing_group.name}
                pipeline_id_filename=$(basename {pipeline_id_path})
                gcloud storage cp {pipeline_id_path} .
                pipeline_id=$(cat $pipeline_id_filename)
                echo "Pipeline ID: $pipeline_id"

                retry download_individual_files
                """,  # noqa: E501
            define_retry_function=True,
        )
    )
    return job
