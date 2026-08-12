"""In-process submission of DRAGEN MLR analyses through the vendored `popgen_cli` package.

Running `popgen-cli dragen-mlr submit` as a subprocess hides failures: its
submission POST sits in a retry loop that also retries permanent errors (400/401)
practically forever, the real reason only ever appears on its stdlib logging
stream, and `capture_output=True` buffers that stream until the process exits —
which it never does. Submitting in-process instead lets us inject a
transient-only retry policy into popgen_cli's `ICAClient` and route its logging
into loguru, so a permanent error (e.g. a stale MLR config JSON) fails the job
loudly and immediately.
"""

import logging
import os
from collections.abc import Callable
from typing import Any

import popgen_cli.utils.utils as popgen_utils
from loguru import logger
from popgen_cli.dragen_mlr import submit as popgen_submit

from dragen_align_pa import ica_api_utils


def _is_transient_popgen_error(exc: BaseException) -> bool:
    """Tenacity predicate: True when the exception message shows a transient ICA error.

    popgen_cli's `ICAClient._request_base` raises bare `Exception`s shaped
    `API request failed: {status} - {body}`. The status prefix is matched as well
    as the shared body markers, so a 429/503 whose body lacks an ICA error code
    still retries.
    """
    message = str(exc)
    if any(marker in message for marker in ica_api_utils.TRANSIENT_ICA_ERROR_MARKERS):
        return True
    return message.startswith(('API request failed: 429', 'API request failed: 503'))


class TransientRetryRunner:
    """Substitute for popgen_cli's `RetryRunner`, backed by the shared ICA backoff.

    popgen_cli's own runner retries every exception; this one retries only
    transient ICA errors (429/503) and propagates anything else on the first
    occurrence, so a permanent error surfaces with its full response text instead
    of spinning silently.
    """

    def run(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return ica_api_utils.ica_retrying(_is_transient_popgen_error)(func, *args, **kwargs)


class _LoguruHandler(logging.Handler):
    """Forward stdlib logging records into loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        logger.opt(depth=6, exception=record.exc_info).log(level, record.getMessage())


def _intercept_popgen_logging() -> None:
    """Route stdlib logging into loguru.

    popgen_cli reports request failures and submission retries only via the root
    stdlib logger; without this bridge those lines bypass the loguru sinks the
    pipeline's error log monitoring reads.
    """
    logging.basicConfig(handlers=[_LoguruHandler()], level=logging.INFO, force=True)


def submit_analysis(argv: list[str]) -> str:
    """Submit one MLR analysis in-process and return its ICA analysis id.

    Mirrors `popgen_cli.dragen_mlr.submit.main`/`submit_job` (see the vendored
    wheel `third_party/popgen_cli-2.1.0-py3-none-any.whl`,
    `popgen_cli/dragen_mlr/submit.py`) with two deliberate departures:

    - `ICAClient` gets `TransientRetryRunner`, so only transient ICA errors are
      retried and a permanent one (e.g. a 401 from a stale MLR config JSON that
      needs regenerating) propagates immediately with its response text.
    - `submit_cwl_analysis(max_retry=1, retry_sleep=0)` disables the outer
      retry loop; all retrying happens in the runner.

    Args:
        argv: The flag list `popgen-cli dragen-mlr submit` would receive (the
            same argv the subprocess call used to pass).

    Returns:
        The `id` of the created ICA analysis.

    Raises:
        ValueError: If the submission response carries no `id`.
    """
    _intercept_popgen_logging()

    args = popgen_submit.parse_args(argv)
    popgen_submit.check_args(args)
    job_config_dict = popgen_submit.make_job(args)
    # submit_job sets this after make_job; it is serialized into job_def_str below.
    job_config_dict['project_data_list'] = []

    project_config_dict = popgen_utils.read_json(args.input_project_config_file_path)
    ica_client = popgen_utils.ICAClient(
        api_key=project_config_dict['ica_api_key'],
        retry_runner=TransientRetryRunner(),
    )
    # Pre-populates the client cache so submit_cwl_analysis makes no lookup calls,
    # only the analysis-creation POST.
    ica_client._set_cache_using_project_config(project_config_dict)  # noqa: SLF001

    project_config_file = project_config_dict['ica_job_project_config_file']
    project_config_file_name = os.path.basename(project_config_file['data']['details']['path'])
    ica_rdf_url = 'https://platform.illumina.com/rdf/ica/resources'
    input_json = {
        'job_secret_file': {'class': 'File', 'location': project_config_file_name},
        'job_def_str': popgen_utils.json_to_b64str(job_config_dict),
        'cwltool:overrides': {
            'workflow.cwl': {
                'requirements': {
                    'ResourceRequirement': {
                        f'{ica_rdf_url}:type': job_config_dict['analysis_instance_type'],
                        f'{ica_rdf_url}:size': job_config_dict['analysis_instance_size'],
                        f'{ica_rdf_url}:tier': job_config_dict['analysis_instance_tier'],
                        'tmpdirMin': job_config_dict['analysis_instance_scratch_space'],
                    },
                },
            },
        },
    }
    mount_list = [
        {'dataId': project_config_file['data']['id'], 'mountPath': project_config_file_name},
    ]

    analysis_details = ica_client.submit_cwl_analysis(
        project_name=project_config_dict['ica_job_project']['name'],
        project_jobs_folder_path=project_config_dict['ica_job_project_jobs_folder']['data']['details']['path'],
        pipeline_name=project_config_dict['ica_analysis_pipeline']['pipeline']['code'],
        analysis_name=job_config_dict['analysis_name'],
        activation_code_name=project_config_dict['ica_analysis_activation_code']['pipelineBundle']['name'],
        analysis_storage_name=project_config_dict['ica_analysis_storage']['name'],
        input_json=input_json,
        mount_list=mount_list,
        max_retry=1,
        retry_sleep=0,
    )

    analysis_id = analysis_details.get('id')
    if not analysis_id:
        raise ValueError(
            f'MLR submission response for "{job_config_dict["analysis_name"]}" is missing "id".',
        )
    logger.info(f'Submitted MLR analysis {job_config_dict["analysis_name"]}: {analysis_id}')
    return analysis_id
