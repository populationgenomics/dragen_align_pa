"""In-process submission of DRAGEN MLR analyses through the vendored `popgen_cli` package.

Running `popgen-cli dragen-mlr submit` as a subprocess hides failures: its
submission POST sits in a retry loop that also retries permanent errors (400/401)
practically forever, the real reason only ever appears on its stdlib logging
stream, and `capture_output=True` buffers that stream until the process exits —
which it never does. Submitting in-process instead lets us inject a
transient-only retry policy into popgen_cli's `ICAClient`, route its logging
into loguru, POST the analysis directly so a permanent error (e.g. a stale MLR
config JSON) propagates as the real `HTTPError`, and populate the analysis tags
the wheel hardcodes empty.
"""

import inspect
import json
import logging
import os
from collections.abc import Callable
from typing import Any, Final

import popgen_cli.utils.utils as popgen_utils
import requests
from loguru import logger
from popgen_cli.dragen_mlr import submit as popgen_submit

from dragen_align_pa import ica_api_utils

# Mirrors ica_api_utils._RETRYABLE_ICA_STATUSES (429 rate-limit, 503 backend
# unavailable) plus the gateway statuses (502/504) in front of ICA, which the
# retired subprocess path retried indefinitely.
_TRANSIENT_HTTP_STATUSES: Final = frozenset({429, 502, 503, 504})


def _is_transient_popgen_error(exc: BaseException) -> bool:
    """Tenacity predicate: True when the exception shows a transient ICA error.

    popgen_cli's `ICAClient._request_base` logs `API request failed: {status} - {body}`
    and then raises via `response.raise_for_status()`, so the propagating exception is a
    `requests.exceptions.HTTPError` whose status code is matched structurally.
    Connection errors and timeouts are transient by nature. The message-marker
    fallback covers exceptions from other layers that only carry the ICA error text.
    """
    if isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
        return True
    if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
        # `is not None`, not truthiness: Response.__bool__ is False for exactly
        # the error statuses being matched here.
        return exc.response.status_code in _TRANSIENT_HTTP_STATUSES
    message = str(exc)
    return any(marker in message for marker in ica_api_utils.TRANSIENT_ICA_ERROR_MARKERS)


class TransientRetryRunner:
    """Substitute for popgen_cli's `RetryRunner`, backed by the shared ICA backoff.

    popgen_cli's own runner retries every exception; this one retries only
    transient ICA errors (429/502/503/504, connection errors, timeouts) and
    propagates anything else on the first occurrence, so a permanent error
    surfaces with its full response text instead of spinning silently.
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
        # Attribute the loguru record to the frame that called into logging: starting
        # from this emit() frame, walk up past the logging-module frames, counting the
        # loguru `depth` as we go. A fixed depth only fits one exact call chain, and
        # frames are matched by module name, not co_filename — standalone-build
        # pythons compile the stdlib with a build-root path that never equals
        # `logging.__file__`.
        frame = inspect.currentframe()
        depth = 0
        if frame is not None:
            frame = frame.f_back  # step out of emit() itself
            depth = 1
        while frame is not None and frame.f_globals.get('__name__') == 'logging':
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def _intercept_popgen_logging() -> None:
    """Route stdlib logging into loguru.

    popgen_cli reports request failures and submission retries only via the root
    stdlib logger (module-level `logging.warning`, so the records are named `root`
    and cannot be filtered by name — owning the root logger is unavoidable);
    without this bridge those lines bypass the loguru sinks the pipeline's error
    log monitoring reads. Idempotent: `basicConfig(force=True)` removes every
    existing root handler and raises the root level to INFO, so it must run once,
    not once per submission.
    """
    if any(isinstance(handler, _LoguruHandler) for handler in logging.getLogger().handlers):
        return
    logging.basicConfig(handlers=[_LoguruHandler()], level=logging.INFO, force=True)


def submit_analysis(argv: list[str], tags: dict[str, list[str]]) -> str:
    """Submit one MLR analysis in-process and return its ICA analysis id.

    Mirrors `popgen_cli.dragen_mlr.submit.main`/`submit_job`/`submit_cwl_analysis`
    (see the vendored wheel `third_party/popgen_cli-2.1.0-py3-none-any.whl`,
    `popgen_cli/dragen_mlr/submit.py` and `popgen_cli/utils/utils.py`) with these
    deliberate departures:

    - `ICAClient` gets `TransientRetryRunner`, so only transient ICA errors are
      retried, and the analysis-creation POST is made directly instead of via
      `submit_cwl_analysis`, whose do-or-die loop replaces the real error with a
      generic terminal exception — here a permanent error (e.g. a 401 from a
      stale MLR config JSON that needs regenerating) propagates as the actual
      `HTTPError` carrying the response text.
    - `tags` is populated from the caller; the wheel hardcodes all three tag
      lists empty.
    - The request ids come straight from the config JSON rather than the wheel's
      cache-or-fetch getters: a stale config fails loudly on the POST instead of
      being silently patched over by a by-name lookup.
    - The analysis-details JSON file write is dropped; the analysis id is
      returned directly.

    Args:
        argv: The flag list `popgen-cli dragen-mlr submit` would receive (the
            same argv the subprocess call used to pass). `--dry-run true` is
            rejected (this mirror has no dry-run path and would submit for
            real); `--verbose` is accepted but ignored — the loguru bridge
            replaces the wheel's `config_logging`.
        tags: ICA analysis tags (keys `technicalTags`, `userTags`,
            `referenceTags`). ICA exposes no API to read tags back, so they are
            purely for GUI filtering.

    Returns:
        The `id` of the created ICA analysis.

    Raises:
        ValueError: If `--dry-run true` is passed, or the submission response is
            not JSON or carries no `id`.
    """
    _intercept_popgen_logging()

    args = popgen_submit.parse_args(argv)
    popgen_submit.check_args(args)
    if args.dry_run:
        raise ValueError(
            'dry-run is not supported by the in-process MLR submission (it would submit '
            'for real); remove --dry-run.',
        )
    job_config_dict = popgen_submit.make_job(args)
    # submit_job sets this after make_job; it is serialized into job_def_str below.
    job_config_dict['project_data_list'] = []

    project_config_dict = popgen_utils.read_json(args.input_project_config_file_path)
    ica_client = popgen_utils.ICAClient(
        api_key=project_config_dict['ica_api_key'],
        retry_runner=TransientRetryRunner(),
    )

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

    # Request body mirrored from submit_cwl_analysis (utils.py:1742-1763), tags aside.
    headers = {'Content-Type': 'application/vnd.illumina.v4+json'}
    request_body = json.dumps(
        {
            'userReference': job_config_dict['analysis_name'],
            'pipelineId': project_config_dict['ica_analysis_pipeline']['pipeline']['id'],
            'tags': tags,
            'activationCodeDetailId': project_config_dict['ica_analysis_activation_code']['id'],
            'analysisStorageId': project_config_dict['ica_analysis_storage']['id'],
            'outputParentFolderId': project_config_dict['ica_job_project_jobs_folder']['data']['id'],
            'analysisInput': {
                'objectType': 'JSON',
                'inputJson': json.dumps(input_json),
                'dataIds': [mount['dataId'] for mount in mount_list],
                'mounts': mount_list,
            },
        },
    )
    endpoint = f'/projects/{project_config_dict["ica_job_project"]["id"]}/analysis:cwl'
    analysis_details = ica_client._post(endpoint=endpoint, headers=headers, data=request_body)  # noqa: SLF001

    if not isinstance(analysis_details, dict):
        # popgen's _request_base returns response.text (a str) for a non-JSON body.
        raise ValueError(
            f'MLR submission for "{job_config_dict["analysis_name"]}" returned a '
            f'non-JSON response: {analysis_details!r}',
        )
    analysis_id = analysis_details.get('id')
    if not analysis_id:
        raise ValueError(
            f'MLR submission response for "{job_config_dict["analysis_name"]}" is missing "id".',
        )
    logger.info(f'Submitted MLR analysis {job_config_dict["analysis_name"]}: {analysis_id}')
    return analysis_id
