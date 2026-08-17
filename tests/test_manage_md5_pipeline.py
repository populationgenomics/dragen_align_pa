"""Unit tests for the manage_md5_pipeline pre-submission helpers.

Covers `_get_fastq_ica_id_list`'s reconciliation between the manifest's
expected FASTQ filenames and the file IDs actually returned by ICA. The
mismatch path must name the specific files missing in ICA (not just a
count) so an operator can chase them down.
"""

from collections.abc import Callable
from types import SimpleNamespace

import pytest

from dragen_align_pa.jobs import manage_md5_pipeline


def _fake_api_response(names_to_ids: dict[str, str]):
    """Build a stand-in for the ICA `get_project_data_list` response body.

    Args:
        names_to_ids: Mapping of FASTQ filename to the ICA file ID that ICA
            would report for it.

    Returns:
        An object whose `.body['items']` mirrors the shape the production code
        iterates over.
    """
    items = [{'data': {'id': file_id, 'details': {'name': name}}} for name, file_id in names_to_ids.items()]
    return SimpleNamespace(body={'items': items})


def _patch_ica_retry(monkeypatch, names_to_ids: dict[str, str]) -> None:
    """Stub `ica_retry` so it returns a fixed set of ICA files regardless of query.

    Args:
        monkeypatch: The pytest `monkeypatch` fixture.
        names_to_ids: The FASTQ filename-to-ID mapping the stubbed ICA query
            should return.
    """
    monkeypatch.setattr(
        manage_md5_pipeline.ica_api_utils,
        'ica_retry',
        lambda *args, **kwargs: _fake_api_response(names_to_ids),  # noqa: ARG005
    )


# `api_instance.get_project_data_list` is evaluated before `ica_retry` is
# called, so the stub api instance must expose that attribute.
_STUB_API = SimpleNamespace(get_project_data_list=None)


def test_get_fastq_ica_id_list_all_found(monkeypatch):
    """Every manifest filename resolves to an ICA id → name->id map inverted to id->name."""
    _patch_ica_retry(monkeypatch, {'a.fastq.gz': 'fid-a', 'b.fastq.gz': 'fid-b'})
    result = manage_md5_pipeline._get_fastq_ica_id_list(
        fastq_filenames=['a.fastq.gz', 'b.fastq.gz'],
        api_instance=_STUB_API,
        path_parameters={},
    )
    assert result == {'fid-a': 'a.fastq.gz', 'fid-b': 'b.fastq.gz'}


def test_run_skips_presubmission_setup_on_cancel(monkeypatch, tmp_path):
    """`cancel_cohort_run=true` must go straight to the management loop without
    collecting FASTQ IDs, uploading the ID-list file, or creating the output
    folder — the loop aborts the run from the stored pipeline-id file."""
    # This patches the shared cpg_utils.config module, so EVERY config_retrieve call
    # sees the stub for the test's duration — safe only while manage_ica_pipeline_loop
    # is also stubbed below (other keys would silently fall back to their defaults).
    monkeypatch.setattr(
        manage_md5_pipeline.cpg_utils.config,
        'config_retrieve',
        lambda key, default=None: True if key == ['ica', 'management', 'cancel_cohort_run'] else default,
    )

    def _fail_setup(*args: object, **kwargs: object) -> None:  # noqa: ARG001
        raise AssertionError('pre-submission setup must not run during cancellation')

    monkeypatch.setattr(manage_md5_pipeline.ica_api_utils, 'ica_project_data_api', _fail_setup)
    monkeypatch.setattr(manage_md5_pipeline.ica_cli_utils, 'upload_local_file', _fail_setup)
    monkeypatch.setattr(manage_md5_pipeline, '_get_fastq_ica_id_list', _fail_setup)

    captured: dict = {}
    monkeypatch.setattr(
        manage_md5_pipeline,
        'manage_ica_pipeline_loop',
        lambda **kwargs: captured.update(kwargs),
    )

    manage_md5_pipeline.run(
        cohort=SimpleNamespace(name='COH1'),
        outputs={},
        # Never opened on the cancel path; the file deliberately does not exist.
        manifest_file_path=tmp_path / 'manifest.csv',
    )

    submit_callable = captured['submit_function_factory']('COH1')
    with pytest.raises(RuntimeError, match='cancel_cohort_run'):
        submit_callable()


def test_run_prepares_submission_when_not_cancelling(monkeypatch, tmp_path):
    """With `cancel_cohort_run` at its false default, `run()` must build the
    submit callable via `_prepare_md5_submission` and hand exactly that callable
    to the management loop's factory."""

    def _sentinel_submit() -> str:
        return 'pip-1'

    prepared: dict = {}

    def _fake_prepare(*, cohort_name: str, outputs: dict, manifest_file_path: object) -> Callable[[], str]:
        prepared.update(cohort_name=cohort_name, outputs=outputs, manifest_file_path=manifest_file_path)
        return _sentinel_submit

    monkeypatch.setattr(manage_md5_pipeline, '_prepare_md5_submission', _fake_prepare)

    captured: dict = {}
    monkeypatch.setattr(
        manage_md5_pipeline,
        'manage_ica_pipeline_loop',
        lambda **kwargs: captured.update(kwargs),
    )

    outputs: dict = {}
    manifest = tmp_path / 'manifest.csv'
    manage_md5_pipeline.run(
        cohort=SimpleNamespace(name='COH1'),
        outputs=outputs,
        manifest_file_path=manifest,
    )

    assert prepared == {'cohort_name': 'COH1', 'outputs': outputs, 'manifest_file_path': manifest}
    assert captured['submit_function_factory']('COH1') is _sentinel_submit


def test_get_fastq_ica_id_list_mismatch_names_missing_files(monkeypatch):
    """A mismatch must raise and name exactly the files ICA didn't return."""
    # ICA only knows about a.fastq.gz; b and c from the manifest are absent.
    _patch_ica_retry(monkeypatch, {'a.fastq.gz': 'fid-a'})
    with pytest.raises(ValueError) as excinfo:
        manage_md5_pipeline._get_fastq_ica_id_list(
            fastq_filenames=['a.fastq.gz', 'b.fastq.gz', 'c.fastq.gz'],
            api_instance=_STUB_API,
            path_parameters={},
        )
    message = str(excinfo.value)
    assert '2 file(s) missing in ICA' in message
    assert 'b.fastq.gz' in message
    assert 'c.fastq.gz' in message
    # The file that WAS found must not be reported as missing.
    assert "'a.fastq.gz'" not in message.split('missing in ICA:')[1]
