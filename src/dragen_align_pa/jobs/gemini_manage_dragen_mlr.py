import json
import subprocess
from collections.abc import Callable
from functools import partial

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger

from dragen_align_pa.constants import BUCKET, ICA_CLI_SETUP
from dragen_align_pa.jobs.gemini_ica_pipeline_manager import manage_ica_pipeline_loop


def _initalise_mlr_job(cohort: Cohort) -> PythonJob:
    mlr_job: PythonJob = get_batch().new_python_job(
        name='MlrWithDragen',
        attributes=(cohort.get_job_attrs() or {}) | {'tool': 'ICA'},  # type: ignore[ReportUnknownVariableType]
    )
    mlr_job.image(image=get_driver_image())
    return mlr_job


def _submit_mlr_run(
    pipeline_id_arguid_path: cpg_utils.Path,
    ica_analysis_output_folder: str,
    sg_name: str,
    mlr_project: str,
    mlr_config_json: str,
    mlr_hash_table: str,
    output_prefix: str,
) -> str:
    with pipeline_id_arguid_path.open() as pid_arguid_fhandle:
        data: dict[str, str] = json.load(pid_arguid_fhandle)
        pipeline_id = data['pipeline_id']
        ar_guid = f'_{data["ar_guid"]}_'

    mlr_analysis_command: str = f"""
        # General authentication
        {ICA_CLI_SETUP}
        cram_path=$(icav2 projectdata list --parent-folder /{BUCKET}/{ica_analysis_output_folder}/{sg_name}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name}/ --data-type FILE --file-name {sg_name}.cram --match-mode EXACT -o json | jq -r '.items[].details.path')
        gvcf_path=$(icav2 projectdata list --parent-folder /{BUCKET}/{ica_analysis_output_folder}/{sg_name}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name}/ --data-type FILE --file-name {sg_name}.hard-filtered.gvcf.gz --match-mode EXACT -o json | jq -r '.items[].details.path')
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
        --output-folder-url {output_prefix}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name} \
        --input-align-file-url ${{cram}} \
        --input-gvcf-file-url ${{gvcf}} \
        --analysis-instance-tier {config_retrieve(['ica', 'mlr', 'analysis_instance_tier'])} > /dev/null 2>&1

        cat {sg_name}/sample-{sg_name}-run-{sg_name}-mlr.json | jq -r ".id"
    """  # noqa: E501
    mlr_analysis_id: str = (
        subprocess.run(mlr_analysis_command, shell=True, capture_output=True, check=False).stdout.decode().strip()
    )
    logger.info(f'MLR pipeline ID for {sg_name} is {mlr_analysis_id}')
    if not mlr_analysis_id:
        raise ValueError(f'Failed to submit MLR pipeline for {sg_name}')

    return mlr_analysis_id


def run_mlr(
    cohort: Cohort,
    pipeline_id_arguid_path_dict: dict[str, cpg_utils.Path],
    outputs: dict[str, cpg_utils.Path],
) -> PythonJob:
    job: PythonJob = _initalise_mlr_job(cohort=cohort)

    job.call(
        _run,
        cohort=cohort,
        pipeline_id_arguid_path_dict=pipeline_id_arguid_path_dict,
        outputs=outputs,
    )

    return job


def _run(
    cohort: Cohort,
    pipeline_id_arguid_path_dict: dict[str, cpg_utils.Path],
    outputs: dict[str, cpg_utils.Path],
) -> None:
    """
    Calls the generic pipeline manager with settings for the MLR pipeline.
    """
    ica_analysis_output_folder: str = config_retrieve(['ica', 'data_prep', 'output_folder'])
    mlr_project: str = config_retrieve(['ica', 'projects', 'dragen_mlr'])
    dragen_align_project: str = config_retrieve(['ica', 'projects', 'dragen_align'])
    mlr_config_json: str = config_retrieve(['ica', 'mlr', 'config_json'])
    mlr_hash_table: str = config_retrieve(['ica', 'mlr', 'mlr_hash_table'])

    logger.info(f'Dataset name is: {cohort.dataset.name}')

    def _create_submit_callable(sg_name: str) -> Callable[[], str]:
        """Creates a zero-argument callable for pipeline submission."""
        output_prefix: str = (
            f'ica://{dragen_align_project}/{BUCKET}/{config_retrieve(["ica", "data_prep", "output_folder"])}/{sg_name}'
        )

        return partial(
            _submit_mlr_run,
            pipeline_id_arguid_path=pipeline_id_arguid_path_dict[f'{sg_name}_pipeline_id_and_arguid'],
            ica_analysis_output_folder=ica_analysis_output_folder,
            sg_name=sg_name,
            mlr_project=mlr_project,
            mlr_config_json=mlr_config_json,
            mlr_hash_table=mlr_hash_table,
            output_prefix=output_prefix,
        )

    manage_ica_pipeline_loop(
        cohort=cohort,
        outputs=outputs,
        pipeline_name='MLR',
        is_mlr_pipeline=True,
        success_file_key_template='{sg_name}_mlr_success',
        pipeline_id_file_key_template='{sg_name}_mlr_pipeline_id',
        error_log_key=f'{cohort.name}_mlr_errors',
        submit_function_factory=_create_submit_callable,
        allow_retry=False,
        sleep_time_seconds=330,
    )
