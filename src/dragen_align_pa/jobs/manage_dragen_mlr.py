import json
import os
import subprocess
from collections.abc import Callable
from functools import partial
from typing import Any

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils.config import config_retrieve
from loguru import logger

from dragen_align_pa import ica_cli_utils, utils
from dragen_align_pa.constants import BUCKET_NAME
from dragen_align_pa.jobs.ica_pipeline_manager import manage_ica_pipeline_loop


def _mlr_authenticate_and_enter_project(mlr_project: str) -> None:
    """Authenticates the ICA CLI and enters the specified MLR project."""
    # shell=True is required for the multi-line ICA_CLI_SETUP script
    utils.run_subprocess_with_log(  # noqa: S604
        ica_cli_utils.ICA_CLI_SETUP,
        'Authenticate ICA CLI',
        shell=True,
    )
    # Set ICA project context
    utils.run_subprocess_with_log(
        ['icav2', 'projects', 'enter', mlr_project],
        f'Set ICA project to {mlr_project}',
    )


def _mlr_find_input_urls(ica_base_folder: str, sg_name: str) -> tuple[str, str]:
    """Finds the CRAM and gVCF file paths in ICA and returns them as URLs."""
    cram_path: str = ica_cli_utils.find_ica_file_path_by_name(
        ica_base_folder,
        f'{sg_name}.cram',
    )
    gvcf_path: str = ica_cli_utils.find_ica_file_path_by_name(
        ica_base_folder,
        f'{sg_name}.hard-filtered.gvcf.gz',
    )

    # Assumes the CRAMs are in the 'OurDNA-DRAGEN-378' project.
    # This could be parameterized if needed.
    cram_url: str = f'ica://OurDNA-DRAGEN-378/{cram_path.lstrip("/")}'
    gvcf_url: str = f'ica://OurDNA-DRAGEN-378/{gvcf_path.lstrip("/")}'

    return cram_url, gvcf_url


def _mlr_download_config(mlr_config_json_fid: str, local_tmp_dir: str) -> str:
    """Downloads the MLR config JSON to a local temp path."""
    local_config_path = os.path.join(local_tmp_dir, 'mlr_config.json')
    utils.run_subprocess_with_log(
        [
            'icav2',
            'projectdata',
            'download',
            mlr_config_json_fid,
            local_config_path,
            '--exclude-source-path',
        ],
        'Download MLR config',
    )
    return local_config_path


def _mlr_build_popgen_cli_command(
    local_config_path: str,
    output_analysis_json_folder: str,
    run_id: str,
    sample_id: str,
    mlr_hash_table: str,
    output_folder_url: str,
    cram_url: str,
    gvcf_url: str,
) -> list[str]:
    """Builds the popgen-cli command as a list of strings."""
    return [
        'popgen-cli',
        'dragen-mlr',
        'submit',
        '--input-project-config-file-path',
        local_config_path,
        '--output-analysis-json-folder-path',
        output_analysis_json_folder,
        '--run-id',
        run_id,
        '--sample-id',
        sample_id,
        '--input-ht-folder-url',
        mlr_hash_table,
        '--output-folder-url',
        output_folder_url,
        '--input-align-file-url',
        cram_url,
        '--input-gvcf-file-url',
        gvcf_url,
        '--analysis-instance-tier',
        config_retrieve(['ica', 'mlr', 'analysis_instance_tier']),
    ]


def _mlr_parse_submission_output(output_json_folder: str, run_id: str) -> str:
    """Parses the JSON output from popgen-cli to find the analysis ID."""
    output_json_path = os.path.join(
        output_json_folder,
        f'sample-{output_json_folder}-run-{run_id}.json',
    )
    if not os.path.exists(output_json_path):
        raise FileNotFoundError(
            f'popgen-cli did not produce expected output file: {output_json_path}',
        )

    with open(output_json_path) as f:
        submission_data: dict[str, Any] = json.load(f)

    mlr_analysis_id = submission_data.get('id')
    if not mlr_analysis_id:
        raise ValueError(
            f'Submission output file "{output_json_path}" is missing the "id" key.',
        )
    return mlr_analysis_id


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

    batch_tmpdir = os.environ.get('BATCH_TMPDIR', '/io')
    ica_base_folder = (
        f'/{BUCKET_NAME}/{ica_analysis_output_folder}/{sg_name}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name}/'
    )

    try:
        # --- 1. Find input file paths ---
        cram_url, gvcf_url = _mlr_find_input_urls(ica_base_folder, sg_name)

        # --- 2. Authenticate ---
        _mlr_authenticate_and_enter_project(mlr_project)

        # --- 3. Download MLR config JSON ---
        local_config_path = _mlr_download_config(mlr_config_json, batch_tmpdir)

        # --- 4. Build and run the popgen-cli command ---
        output_folder_url = f'{output_prefix}/{sg_name}{ar_guid}-{pipeline_id}/{sg_name}'
        mlr_run_id = f'{sg_name}-mlr'
        submit_command = _mlr_build_popgen_cli_command(
            local_config_path=local_config_path,
            output_analysis_json_folder=sg_name,
            run_id=mlr_run_id,
            sample_id=sg_name,
            mlr_hash_table=mlr_hash_table,
            output_folder_url=output_folder_url,
            cram_url=cram_url,
            gvcf_url=gvcf_url,
        )
        utils.run_subprocess_with_log(submit_command, 'Submit popgen-cli MLR')

        # --- 5. Read the pipeline ID from the output JSON ---
        mlr_analysis_id = _mlr_parse_submission_output(sg_name, mlr_run_id)

        logger.info(f'MLR pipeline ID for {sg_name} is {mlr_analysis_id}')
        return mlr_analysis_id

    except (subprocess.CalledProcessError, FileNotFoundError, ValueError, json.JSONDecodeError) as e:
        logger.error(f'Failed to submit MLR pipeline for {sg_name}: {e}')
        raise


def run(
    cohort: Cohort,
    pipeline_id_arguid_path_dict: dict[str, cpg_utils.Path],
    outputs: dict[str, cpg_utils.Path],
) -> None:
    """
    Calls the generic pipeline manager with settings for the MLR pipeline.
    """
    ica_analysis_output_folder: str = config_retrieve(
        ['ica', 'data_prep', 'output_folder'],
    )
    mlr_project: str = config_retrieve(['ica', 'projects', 'dragen_mlr'])
    dragen_align_project: str = config_retrieve(['ica', 'projects', 'dragen_align'])
    mlr_config_json: str = config_retrieve(['ica', 'mlr', 'config_json'])
    mlr_hash_table: str = config_retrieve(['ica', 'mlr', 'mlr_hash_table'])

    logger.info(f'Dataset name is: {cohort.dataset.name}')

    def _create_submit_callable(sg_name: str) -> Callable[[], str]:
        """Creates a zero-argument callable for pipeline submission."""
        output_prefix: str = (
            f'ica://{dragen_align_project}/{BUCKET_NAME}/'
            f'{config_retrieve(["ica", "data_prep", "output_folder"])}/{sg_name}'
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
        targets_to_process=cohort.get_sequencing_groups(),
        outputs=outputs,
        pipeline_name='MLR',
        is_mlr_pipeline=True,
        success_file_key_template='{target_name}_mlr_success',
        pipeline_id_file_key_template='{target_name}_mlr_pipeline_id',
        error_log_key=f'{cohort.name}_mlr_errors',
        submit_function_factory=_create_submit_callable,
        allow_retry=False,
        sleep_time_seconds=330,
    )
