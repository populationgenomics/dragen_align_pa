import json
import subprocess
import time

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger

from dragen_align_pa.jobs import cancel_ica_pipeline_run, monitor_dragen_pipeline
from dragen_align_pa.utils import delete_pipeline_id_file


def _initalise_mlr_job(cohort: Cohort) -> PythonJob:
    mlr_job: PythonJob = get_batch().new_python_job(
        name='MlrWithDragen',
        attributes=(cohort.get_job_attrs() or {}) | {'tool': 'ICA'},  # type: ignore[ReportUnknownVariableType]
    )
    mlr_job.image(image=get_driver_image())
    return mlr_job


def _submit_mlr_run(
    pipeline_id_arguid_path: cpg_utils.Path,
    bucket: str,
    ica_analysis_output_folder: str,
    sg_name: str,
    ica_cli_setup: str,
    mlr_project: str,
    mlr_config_json: str,
    mlr_hash_table: str,
    output_prefix: str,
) -> str:
    with pipeline_id_arguid_path.open() as pid_arguid_fhandle:
        data: dict[str, str] = json.load(pid_arguid_fhandle)
        pipeline_id: str = data['pipeline_id']
        ar_guid: str = data['ar_guid']

    mlr_analysis_command: str = f"""
        # General authentication
        {ica_cli_setup}
        cram_path=$(icav2 projectdata list --parent-folder /{bucket}/{ica_analysis_output_folder}/{sg_name}/{sg_name}_{ar_guid}_-{pipeline_id}/{sg_name}/ --data-type FILE --file-name {sg_name}.cram --match-mode EXACT -o json | jq -r '.items[].details.path')
        gvcf_path=$(icav2 projectdata list --parent-folder /{bucket}/{ica_analysis_output_folder}/{sg_name}/{sg_name}_{ar_guid}_-{pipeline_id}/{sg_name}/ --data-type FILE --file-name {sg_name}.hard-filtered.gvcf.gz --match-mode EXACT -o json | jq -r '.items[].details.path')
        cram="ica://OurDNA-DRAGEN-378${{cram_path}}"
        gvcf="ica://OurDNA-DRAGEN-378${{gvcf_path}}"

        icav2 projects enter {mlr_project}

        # Download the mlr config JSON
        icav2 projectdata download {mlr_config_json} $BATCH_TMPDIR/mlr_config.json --exclude-source-path > /dev/null 2>&1

        popgen-cli dragen-mlr submit \
        --input-project-config-file-path $BATCH_TMPDIR/mlr_config.json \
        --output-analysis-json-folder-path {sg_name} \
        --run-id {sg_name}-mlr \
        --sample-id {sg_name} \
        --input-ht-folder-url {mlr_hash_table} \
        --output-folder-url {output_prefix}/{sg_name}_{ar_guid}_-{pipeline_id}/{sg_name} \
        --input-align-file-url ${{cram}} \
        --input-gvcf-file-url ${{gvcf}} \
        --analysis-instance-tier {config_retrieve(['ica', 'mlr', 'analysis_instance_tier'])} > /dev/null 2>&1

        cat {sg_name}/sample-{sg_name}-run-{sg_name}-mlr.json | jq -r ".id"
    """  # noqa: E501
    mlr_analysis_id: str = (
        subprocess.run(mlr_analysis_command, shell=True, capture_output=True, check=False).stdout.decode().strip()
    )

    return mlr_analysis_id


def run_mlr(
    cohort: Cohort,
    bucket: str,
    ica_cli_setup: str,
    pipeline_id_arguid_path_dict: dict[str, cpg_utils.Path],
    api_root: str,
    outputs: dict[str, cpg_utils.Path],
) -> PythonJob:
    job: PythonJob = _initalise_mlr_job(cohort=cohort)

    job.call(
        _run,
        cohort,
        bucket,
        ica_cli_setup,
        pipeline_id_arguid_path_dict,
        api_root=api_root,
        outputs=outputs,
    )

    return job


def _run(  # noqa: PLR0915
    cohort: Cohort,
    bucket: str,
    ica_cli_setup: str,
    pipeline_id_arguid_path_dict: dict[str, cpg_utils.Path],
    api_root: str,
    outputs: dict[str, cpg_utils.Path],
) -> None:
    logger.info('Starting MLR processing and monitoring')
    # Error sink needs to be configured here
    logger.add(sink='tmp_errors.log', format='{time} - {level} - {message}', level='ERROR')

    ica_analysis_output_folder: str = config_retrieve(['ica', 'data_prep', 'output_folder'])
    mlr_project: str = config_retrieve(['ica', 'projects', 'dragen_mlr'])
    dragen_align_project: str = config_retrieve(['ica', 'projects', 'dragen_align'])
    mlr_config_json: str = config_retrieve(['ica', 'mlr', 'config_json'])
    mlr_hash_table: str = config_retrieve(['ica', 'mlr', 'mlr_hash_table'])

    running_pipelines: list[str] = []
    cancelled_pipelines: list[str] = []
    failed_pipelines: list[str] = []
    # If a sequencing group is in this list, we can skip checking its status
    completed_pipelines: list[str] = []

    while (len(completed_pipelines) + len(cancelled_pipelines) + len(failed_pipelines)) < len(
        cohort.get_sequencing_groups()
    ):
        for sequencing_group in cohort.get_sequencing_groups():
            sg_name: str = sequencing_group.name
            mlr_pipeline_success_file: cpg_utils.Path = outputs[f'{sg_name}_mlr_success']
            mlr_pipeline_id_file: cpg_utils.Path = outputs[f'{sg_name}_mlr_pipeline_id']

            output_prefix: str = f'ica://{dragen_align_project}/{bucket}/{config_retrieve(["ica", "data_prep", "output_folder"])}/{sg_name}'  # noqa: E501

            # In case of Hail Batch crashes, find previous completed runs so we can skip trying to monitor them
            if mlr_pipeline_success_file.exists() and sg_name not in completed_pipelines:
                completed_pipelines.append(sg_name)
                continue

            if any(
                [
                    sg_name in completed_pipelines,
                    sg_name in cancelled_pipelines,
                    sg_name in failed_pipelines,
                ]
            ):
                continue

            # If a pipeline ID file doesn't exist we have to submit a new run, regardless of other settings
            if not mlr_pipeline_id_file.exists():
                mlr_analysis_id: str = _submit_mlr_run(
                    pipeline_id_arguid_path=pipeline_id_arguid_path_dict[f'{sg_name}_pipeline_id_and_arguid'],
                    bucket=bucket,
                    ica_analysis_output_folder=ica_analysis_output_folder,
                    sg_name=sg_name,
                    ica_cli_setup=ica_cli_setup,
                    mlr_project=mlr_project,
                    mlr_config_json=mlr_config_json,
                    mlr_hash_table=mlr_hash_table,
                    output_prefix=output_prefix,
                )
                with mlr_pipeline_id_file.open('w') as mlr_fhandle:
                    mlr_fhandle.write(json.dumps({'pipeline_id': mlr_analysis_id}))
            else:
                # Get an existing pipeline ID
                with mlr_pipeline_id_file.open('r') as mlr_pid_fhandle:
                    mlr_analysis_id = json.load(mlr_pid_fhandle)['pipeline_id']
                # Cancel a running job in ICA
                if config_retrieve(key=['ica', 'management', 'cancel_cohort_run'], default=False):
                    logger.info(f'Cancelling pipeline run: {mlr_analysis_id} for sequencing group {sg_name}')
                    cancel_ica_pipeline_run.run(ica_pipeline_id=mlr_analysis_id, api_root=api_root, is_mlr=True)
                    delete_pipeline_id_file(pipeline_id_file=str(mlr_pipeline_id_file))

            mlr_pipeline_status: str = monitor_dragen_pipeline.run(
                ica_pipeline_id=mlr_analysis_id, api_root=api_root, is_mlr=True
            )

            if mlr_pipeline_status == 'INPROGRESS':
                running_pipelines.append(sg_name)

            elif mlr_pipeline_status == 'SUCCEEDED':
                logger.info(f'Pipeline run {mlr_analysis_id} has succeeded for {sg_name}')
                completed_pipelines.append(sg_name)
                # Testing fix
                if sg_name in running_pipelines:
                    running_pipelines.remove(sg_name)
                # Write the success to GCP
                with mlr_pipeline_success_file.open('w') as success_file:
                    success_file.write(f'ICA pipeline {mlr_analysis_id} has succeeded for sequencing group {sg_name}.')

            elif mlr_pipeline_status in ['ABORTING', 'ABORTED']:
                logger.info(f'The pipeline run {mlr_analysis_id} has been cancelled for sample {sg_name}.')
                cancelled_pipelines.append(sg_name)
                if sg_name in running_pipelines:
                    running_pipelines.remove(sg_name)
                delete_pipeline_id_file(pipeline_id_file=str(mlr_pipeline_id_file))

            elif mlr_pipeline_status in ['FAILED', 'FAILEDFINAL']:
                # Log failed ICA pipeline to a file somewhere
                if sg_name in running_pipelines:
                    running_pipelines.remove(sg_name)
                failed_pipelines.append(sg_name)
                delete_pipeline_id_file(pipeline_id_file=str(mlr_pipeline_id_file))
                logger.error(
                    f'The pipeline {mlr_analysis_id} has failed, deleting pipeline ID file {sg_name}_pipeline_id'
                )

            # ICA has a max concurrent running pipeline limit of 20, but I have observed it as low as 16 before.
            # Once we reach this number of running pipelines, we don't need to check on the status of others.
            if len(running_pipelines) >= 16:  # noqa: PLR2004
                continue
        # If some pipelines have been cancelled, abort this pipeline
        # This code will only trigger if a 'cancel pipeline' run is submitted before this master pipeline run is
        # cancelled in Hail Batch
        if cancelled_pipelines:
            # If there are both cancelled and failed pipelines,
            # write the failed pipelines to the error log before exiting.
            if failed_pipelines:
                with open('tmp_errors.log') as tmp_log_handle:
                    lines: list[str] = tmp_log_handle.readlines()
                    with outputs[f'{cohort.name}_errors'].open('a') as gcp_error_log_file:
                        gcp_error_log_file.write('\n'.join(lines))
            raise Exception(f'The following MLR pipelines have been cancelled: {" ".join(cancelled_pipelines)}')

        # If more than 5% of pipelines are failing, exit now so we can investigate
        if failed_pipelines and float(len(failed_pipelines)) / float(len(cohort.get_sequencing_groups())) > 0.05:  # noqa: PLR2004
            raise Exception(
                f'More than 5% of MLR pipelines have failed. Failing pipelines: {" ".join(failed_pipelines)}'
            )
        # Catch just in case everything is finished on the first pass through the loop
        if (len(completed_pipelines) + len(cancelled_pipelines) + len(failed_pipelines)) == len(
            cohort.get_sequencing_groups()
        ):
            break
        # Wait 10 minutes before checking again
        time.sleep(600)
    with open('tmp_errors.log') as tmp_log_handle:
        lines = tmp_log_handle.readlines()
        with outputs[f'{cohort.name}_mlr_errors'].open('w') as gcp_error_log_file:
            gcp_error_log_file.write('\n'.join(lines))
