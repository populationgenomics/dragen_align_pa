"""Tests for AR GUID handling in manage_ica_pipeline_loop and submit_dragen_run.

These pin two contracts:

1. When ``force_resubmit`` is true, the loop must NOT preserve a stale AR GUID
   from the existing pipeline-id file — it must use the fresh AR GUID from the
   environment. (Otherwise downstream stages build the wrong ICA folder path.)
2. The manager must pass the chosen AR GUID into the submit-callable factory,
   and ``submit_dragen_run`` must use that AR GUID for the ICA ``userReference``
   instead of independently calling ``try_get_ar_guid()``. (Otherwise the
   AR GUID stored in the pipeline-id file can diverge from the one ICA uses
   to name the run's output folder.)
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import TYPE_CHECKING

import cpg_utils

from dragen_align_pa.jobs import ica_pipeline_manager, run_align_genotype_with_dragen

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import pytest


_SG_NAME = 'sg_a'
_PIPELINE_ID_KEY = '{target_name}_pipeline_id_and_arguid'
_SUCCESS_KEY = '{target_name}_success'
_ERROR_KEY = 'cohort_errors'


def _setup_outputs(tmp_path: Path) -> dict[str, cpg_utils.Path]:
    return {
        _PIPELINE_ID_KEY.format(target_name=_SG_NAME): cpg_utils.to_path(
            str(tmp_path / f'{_SG_NAME}_pipeline_id_and_arguid.json'),
        ),
        _SUCCESS_KEY.format(target_name=_SG_NAME): cpg_utils.to_path(
            str(tmp_path / f'{_SG_NAME}_pipeline_success.json'),
        ),
        _ERROR_KEY: cpg_utils.to_path(str(tmp_path / 'cohort_errors.log')),
    }


def _patch_manager(
    monkeypatch: pytest.MonkeyPatch,
    *,
    force_resubmit: bool,
    fresh_ar_guid: str,
    monitor_status: str = 'SUCCEEDED',
) -> None:
    """Stub the manager's external dependencies for a single-iteration run."""

    def fake_config_retrieve(key, default=None):  # noqa: ANN001, ANN202
        key_tuple = tuple(key)
        if key_tuple == ('ica', 'management', 'force_resubmit'):
            return force_resubmit
        if key_tuple == ('ica', 'management', 'cancel_cohort_run'):
            return False
        return default

    monkeypatch.setattr(ica_pipeline_manager, 'config_retrieve', fake_config_retrieve)
    monkeypatch.setattr(ica_pipeline_manager, 'try_get_ar_guid', lambda: fresh_ar_guid)
    monkeypatch.setattr(
        ica_pipeline_manager.monitor_dragen_pipeline,
        'run',
        lambda **_kwargs: monitor_status,
    )
    monkeypatch.setattr(ica_pipeline_manager.time, 'sleep', lambda *_a, **_kw: None)


def test_force_resubmit_does_not_preserve_stale_ar_guid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pipeline-id file written after force_resubmit cleanup must contain
    the fresh env AR GUID, not the stale one read from the previous file."""
    monkeypatch.chdir(tmp_path)
    outputs = _setup_outputs(tmp_path)

    stale_pid_path = outputs[_PIPELINE_ID_KEY.format(target_name=_SG_NAME)]
    with stale_pid_path.open('w') as fh:
        json.dump({'pipeline_id': 'STALE-PID', 'ar_guid': 'STALE-GUID'}, fh)

    _patch_manager(monkeypatch, force_resubmit=True, fresh_ar_guid='FRESH-GUID')

    def factory(*_args: object, **_kwargs: object) -> Callable[[], str]:
        # signature-agnostic so this test fails only on its own assertion
        return lambda: f'NEW-PID-FOR-{_SG_NAME}'

    ica_pipeline_manager.manage_ica_pipeline_loop(
        targets_to_process=[SimpleNamespace(name=_SG_NAME)],
        outputs=outputs,
        pipeline_name='Dragen',
        is_mlr_pipeline=False,
        success_file_key_template=_SUCCESS_KEY,
        pipeline_id_file_key_template=_PIPELINE_ID_KEY,
        error_log_key=_ERROR_KEY,
        submit_function_factory=factory,
        allow_retry=False,
        sleep_time_seconds=1,
    )

    written = json.loads(stale_pid_path.open().read())
    assert written['ar_guid'] == 'FRESH-GUID', (
        f"force_resubmit preserved the stale AR GUID; got {written['ar_guid']!r}"
    )
    assert written['pipeline_id'] == f'NEW-PID-FOR-{_SG_NAME}'


def test_manager_passes_ar_guid_into_submit_factory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The manager invokes submit_function_factory with (sg_name, ar_guid)
    so that the submitter can stamp the same AR GUID into ICA userReference."""
    monkeypatch.chdir(tmp_path)
    outputs = _setup_outputs(tmp_path)

    _patch_manager(monkeypatch, force_resubmit=False, fresh_ar_guid='FRESH-GUID')

    factory_calls: list[tuple[object, ...]] = []

    def factory(*args: object) -> Callable[[], str]:
        factory_calls.append(args)
        return lambda: 'NEW-PID'

    ica_pipeline_manager.manage_ica_pipeline_loop(
        targets_to_process=[SimpleNamespace(name=_SG_NAME)],
        outputs=outputs,
        pipeline_name='Dragen',
        is_mlr_pipeline=False,
        success_file_key_template=_SUCCESS_KEY,
        pipeline_id_file_key_template=_PIPELINE_ID_KEY,
        error_log_key=_ERROR_KEY,
        submit_function_factory=factory,
        allow_retry=False,
        sleep_time_seconds=1,
    )

    assert factory_calls == [(_SG_NAME, 'FRESH-GUID')]


def test_submit_dragen_run_uses_ar_guid_argument_for_user_reference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """submit_dragen_run must build userReference from its ar_guid argument
    rather than calling try_get_ar_guid() — otherwise the GUID in the
    pipeline-id file and the GUID baked into the ICA folder name can diverge.
    """
    cram_ica_fids_path = cpg_utils.to_path(str(tmp_path / 'cram_fids.json'))
    with cram_ica_fids_path.open('w') as fh:
        json.dump({'cram_fid': 'cram-id-123'}, fh)

    fake_config: dict[tuple, object] = {
        ('ica', 'pipelines', 'dragen_ht_id'): 'ht_id',
        ('ica', 'qc', 'cross_cont_vcf'): 'cc_id',
        ('ica', 'qc', 'coverage_region_1'): 'cr1',
        ('ica', 'qc', 'coverage_region_2'): 'cr2',
        ('workflow', 'reads_type'): 'cram',
        ('ica', 'pipelines', 'cram'): 'dragen_pid',
        ('ica', 'tags', 'user_tags'): [],
        ('ica', 'tags', 'technical_tags'): [],
        ('ica', 'tags', 'reference_tags'): [],
        ('ica', 'cram_references', 'old_cram_reference'): 'old_cram_key',
        ('ica', 'cram_references', 'old_cram_key'): 'cram_ref_id',
    }

    def fake_config_retrieve(key, default=None):  # noqa: ANN001, ANN202
        return fake_config.get(tuple(key), default)

    monkeypatch.setattr(run_align_genotype_with_dragen, 'config_retrieve', fake_config_retrieve)
    # If a future regression reintroduces try_get_ar_guid in this module, force it
    # to a sentinel so the user_reference assertion would catch the divergence.
    monkeypatch.setattr(
        run_align_genotype_with_dragen,
        'try_get_ar_guid',
        lambda: 'ENV-GUID-MUST-NOT-BE-USED',
        raising=False,
    )

    captured: dict[str, object] = {}

    def fake_submit_nextflow_analysis(*, api_instance, path_params, body, header_params):  # noqa: ANN001, ANN202, ARG001
        captured['user_reference'] = body['userReference']
        return 'fake-run-id'

    monkeypatch.setattr(
        run_align_genotype_with_dragen.ica_api_utils,
        'submit_nextflow_analysis',
        fake_submit_nextflow_analysis,
    )

    run_align_genotype_with_dragen.submit_dragen_run(
        cram_ica_fids_path=cram_ica_fids_path,
        fastq_ids_path=None,
        fastq_list_fid_and_filenames_path=None,
        project_id={'projectId': 'proj-id'},
        ica_output_folder_id='folder-id',
        api_instance=None,  # type: ignore[arg-type]
        sg_name=_SG_NAME,
        ar_guid='PASSED-IN-GUID',
    )

    assert captured['user_reference'] == f'{_SG_NAME}_PASSED-IN-GUID_'
