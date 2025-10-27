import json
import os
import subprocess
from collections.abc import Callable
from functools import partial
from typing import Any

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve, get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger

from dragen_align_pa.constants import BUCKET_NAME, ICA_CLI_SETUP
from dragen_align_pa.jobs.ica_pipeline_manager import manage_ica_pipeline_loop


def _initalise_mlr_job(cohort: Cohort) -> PythonJob:
    mlr_job: PythonJob = get_batch().new_python_job(
        name='MlrWithDragen',
        attributes=(cohort.get_job_attrs() or {}) | {'tool': 'ICA'},  # type: ignore[ReportUnknownVariableType]
    )
    mlr_job.image(image=get_driver_image())
    return mlr_job


def _run_command(
    command: str | list[str],
    capture_output: bool = False,
    shell: bool = False,
) -> subprocess.CompletedProcess:
    """
    Runs a subprocess command with robust error logging.
    """
    executable = '/bin/bash' if shell else None
    cmd_str = command if isinstance(command, str) else ' '.join(command)

    try:
        logger.info(f'Running command: {cmd_str}')
        return subprocess.run(
            command,
            check=True,
            text=True,
            capture_output=capture_output,
            shell=shell,
            executable=executable,
        )
    except subprocess.CalledProcessError as e:
        logger.error(f'Command failed with return code {e.returncode}: {cmd_str}')
        if e.stdout:
            logger.error(f'STDOUT: {e.stdout.strip()}')
        if e.stderr:
            logger.error(f'STDERR: {e.stderr.strip()}')
        # Re-raise as a generic exception to fail the job
        raise ValueError('A subprocess command failed. See logs.') from e


def _find_ica_file_path(parent_folder: str, file_name: str) -> str:
    """
    Finds a file in ICA and returns its full `details.path`.
    """
    command = [
        'icav2',
        'projectdata',
        'list',
        '--parent-folder',
        parent_folder,
        '--data-type',
        'FILE',
        '--file-name',
        file_name,
        '--match-mode',
        'EXACT',
        '-o',
        'json',
    ]
    result = _run_command(command, capture_output=True)
    try:
        data = json.loads(result.stdout)
        if not data.get('items'):
            raise ValueError(f'No file found with name "{file_name}" in folder "{parent_folder}"')

        file_path = data['items'][0].get('details', {}).get('path')
        if not file_path:
            raise ValueError(f'File "{file_name}" found, but it has no "details.path" in API response.')

        logger.info(f'Found {file_name} at path: {file_path}')
        return file_path

    except json.JSONDecodeError:
        logger.error(f'Failed to decode JSON from icav2 list command: {result.stdout}')
        raise
    except (ValueError, IndexError) as e:
        logger.error(f'Error parsing icav2 list output for {file_name}: {e}')
        raise


def _submit_mlr_run(
    pipeline_id_arguid_path: cpg_utils.Path,
    ica_analysis_output_folder: str,
    sg_name: str,
    mlr_project: str,
    mlr_config_json: str,
    mlr_hash_table: str,
    output_prefix: str,
) -> str:
    """
    Submits the DRAGEN MLR pipeline by running individual CLI commands
    and parsing the JSON output file.
    """
    with pipeline_id_arguid_path.open() as pid_arguid_fhandle:
        data: dict[str, str] = json.load(pid_arguid_fhandle)
        pipeline_id = data['pipeline_id']
        ar_guid = f'_{data["ar_guid"]}_'

    batch_tmpdir = os.environ.get('BATCH_TMPDIR', '/batch')
    local_config_path = os.path.join(batch_tmpdir, 'mlr_config.json')
    ica_base_folder = (
        f'/{BUCKET_NAME}/{ica_analysis_output_folder}/{sg_name}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name}/'
    )

    try:
        # --- 1. Authenticate ---
        # shell=True is required for the multi-line ICA_CLI_SETUP script
        _run_command(ICA_CLI_SETUP, shell=True)

        # --- 2. Find input file paths ---
        cram_path: str = _find_ica_file_path(ica_base_folder, f'{sg_name}.cram')
        gvcf_path: str = _find_ica_file_path(ica_base_folder, f'{sg_name}.hard-filtered.gvcf.gz')

        cram_url: str = f'ica://OurDNA-DRAGEN-378/{cram_path.lstrip("/")}'
        gvcf_url: str = f'ica://OurDNA-DRAGEN-378/{gvcf_path.lstrip("/")}'

        # --- 3. Set ICA project context ---
        _run_command(['icav2', 'projects', 'enter', mlr_project])

        # --- 4. Download MLR config JSON ---
        _run_command(
            [
                'icav2',
                'projectdata',
                'download',
                mlr_config_json,
                local_config_path,
                '--exclude-source-path',
            ]
        )

        # --- 5. Build and run the popgen-cli command ---
        # This is built as a list to avoid shell=True
        submit_command: list[str] = [
            'popgen-cli',
            'dragen-mlr',
            'submit',
            '--input-project-config-file-path',
            local_config_path,
            '--output-analysis-json-folder-path',
            sg_name,
            '--run-id',
            f'{sg_name}-mlr',
            '--sample-id',
            sg_name,
            '--input-ht-folder-url',
            mlr_hash_table,
            '--output-folder-url',
            f'{output_prefix}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name}',
            '--input-align-file-url',
            cram_url,
            '--input-gvcf-file-url',
            gvcf_url,
            '--analysis-instance-tier',
            config_retrieve(['ica', 'mlr', 'analysis_instance_tier']),
        ]
        _run_command(submit_command)

        # --- 6. Read the pipeline ID from the output JSON ---
        output_json_path = os.path.join(sg_name, f'sample-{sg_name}-run-{sg_name}-mlr.json')
        if not os.path.exists(output_json_path):
            raise FileNotFoundError(f'popgen-cli did not produce expected output file: {output_json_path}')

        with open(output_json_path) as f:
            submission_data: dict[str, Any] = json.load(f)

        mlr_analysis_id = submission_data.get('id')
        if not mlr_analysis_id:
            raise ValueError(f'Submission output file "{output_json_path}" is missing the "id" key.')

        logger.info(f'MLR pipeline ID for {sg_name} is {mlr_analysis_id}')
        return mlr_analysis_id

    except (subprocess.CalledProcessError, FileNotFoundError, ValueError, json.JSONDecodeError) as e:
        logger.error(f'Failed to submit MLR pipeline for {sg_name}: {e}')
        raise


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
        output_prefix: str = f'ica://{dragen_align_project}/{BUCKET_NAME}/{config_retrieve(["ica", "data_prep", "output_folder"])}/{sg_name}'

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
