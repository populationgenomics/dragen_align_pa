import subprocess

import cpg_utils
from cpg_flow.targets import SequencingGroup
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger


def _initalise_mlr_job(sequencing_group: SequencingGroup) -> PythonJob:
    mlr_job: PythonJob = get_batch().new_python_job(
        name='MlrWithDragen',
        attributes=(sequencing_group.get_job_attrs() or {}) | {'tool': 'ICA'},  # type: ignore[ReportUnknownVariableType]
    )
    mlr_job.image(image=get_driver_image())
    return mlr_job


def run_mlr(
    sequencing_group: SequencingGroup, bucket: str, ica_cli_setup: str, pipeline_id_arguid_path: cpg_utils.Path
) -> PythonJob:
    ica_analysis_output_folder = config_retrieve(['ica', 'data_prep', 'output_folder'])

    mlr_project: str = config_retrieve(['ica', 'projects', 'dragen_mlr'])
    dragen_align_project: str = config_retrieve(['ica', 'projects', 'dragen_align'])
    mlr_config_json: str = config_retrieve(['ica', 'mlr', 'config_json'])
    mlr_hash_table: str = config_retrieve(['ica', 'mlr', 'mlr_hash_table'])

    sg_name: str = sequencing_group.name

    output_prefix: str = (
        f'ica://{dragen_align_project}/{bucket}/{config_retrieve(["ica", "data_prep", "output_folder"])}/{sg_name}'
    )

    job: PythonJob = _initalise_mlr_job(sequencing_group=sequencing_group)

    mlr_analysis_command: str = f"""
        # Get pipeline ID and ar_guid from previous step
        pipeline_id_arguid_filename=$(basename {pipeline_id_arguid_path})
        gcloud storage cp {pipeline_id_arguid_path} .
        pipeline_id=$(cat $pipeline_id_arguid_filename | jq -r .pipeline_id)
        ar_guid=$(cat $pipeline_id_arguid_filename | jq -r .ar_guid)

        # General authentication
        {ica_cli_setup}
        cram_path=$(icav2 projectdata list --parent-folder /{bucket}/{ica_analysis_output_folder}/{sequencing_group.name}/{sequencing_group.name}_${{ar_guid}}_-${{pipeline_id}}/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.cram --match-mode EXACT -o json | jq -r '.items[].details.path')
        gvcf_path=$(icav2 projectdata list --parent-folder /{bucket}/{ica_analysis_output_folder}/{sequencing_group.name}/{sequencing_group.name}_${{ar_guid}}_-${{pipeline_id}}/{sequencing_group.name}/ --data-type FILE --file-name {sequencing_group.name}.hard-filtered.gvcf.gz --match-mode EXACT -o json | jq -r '.items[].details.path')
        cram="ica://OurDNA-DRAGEN-378${{cram_path}}"
        gvcf="ica://OurDNA-DRAGEN-378${{gvcf_path}}"

        icav2 projects enter {mlr_project}

        # Download the mlr config JSON
        icav2 projectdata download {mlr_config_json} $BATCH_TMPDIR/mlr_config.json --exclude-source-path

        popgen-cli dragen-mlr submit \
        --input-project-config-file-path $BATCH_TMPDIR/mlr_config.json \
        --output-analysis-json-folder-path {sg_name} \
        --run-id {sg_name}-mlr \
        --sample-id {sg_name} \
        --input-ht-folder-url {mlr_hash_table} \
        --output-folder-url {output_prefix}/{sg_name}_${{ar_guid}}_-${{pipeline_id}}/{sg_name} \
        --input-align-file-url ${{cram}} \
        --input-gvcf-file-url ${{gvcf}} \
        --analysis-instance-tier {config_retrieve(['ica', 'mlr', 'analysis_instance_tier'])}

        cat {sg_name}/sample-{sg_name}-run-{sg_name}-mlr.json | jq -r ".id"
    """  # noqa: E501

    job.call(
        _run,
        mlr_analysis_command,
    )

    # output file name: CPG280131.hard-filtered.recal.gvcf.gz

    return job


def _run(mlr_analysis_command: str) -> None:
    logger.info('Starting MLR processing and monitoring')
    mlr_analysis_id: str = subprocess.run(  # noqa: S602
        mlr_analysis_command, shell=True, stdout=subprocess.STDOUT, check=False
    ).stdout.decode()
    logger.info(f'MLR analysis ID: {mlr_analysis_id}')
