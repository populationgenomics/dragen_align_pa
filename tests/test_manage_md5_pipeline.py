"""Unit tests for the manage_md5_pipeline pre-submission helpers.

Covers `_get_fastq_ica_id_list`'s reconciliation between the manifest's
expected FASTQ filenames and the file IDs actually returned by ICA. The
mismatch path must name the specific files missing in ICA (not just a
count) so an operator can chase them down.
"""

import json
from collections.abc import Callable
from contextlib import contextmanager
from functools import partial
from types import SimpleNamespace

import pytest

from dragen_align_pa.jobs import manage_md5_pipeline


def _fake_api_response(names_to_files: dict[str, tuple[str, int]]):
    """Build a stand-in for the ICA `get_project_data_list` response body.

    Args:
        names_to_files: Mapping of FASTQ filename to the (file ID, size in
            bytes) that ICA would report for it.

    Returns:
        An object whose `.body['items']` mirrors the shape the production code
        iterates over.
    """
    items = [
        {'data': {'id': file_id, 'details': {'name': name, 'fileSizeInBytes': size}}}
        for name, (file_id, size) in names_to_files.items()
    ]
    return SimpleNamespace(body={'items': items})


def _patch_ica_retry(monkeypatch, names_to_files: dict[str, tuple[str, int]]) -> None:
    """Stub `ica_retry` so it returns a fixed set of ICA files regardless of query.

    Args:
        monkeypatch: The pytest `monkeypatch` fixture.
        names_to_files: The FASTQ filename to (file ID, size in bytes) mapping
            the stubbed ICA query should return.
    """
    monkeypatch.setattr(
        manage_md5_pipeline.ica_api_utils,
        'ica_retry',
        lambda *args, **kwargs: _fake_api_response(names_to_files),  # noqa: ARG005
    )


# `api_instance.get_project_data_list` is evaluated before `ica_retry` is
# called, so the stub api instance must expose that attribute.
_STUB_API = SimpleNamespace(get_project_data_list=None)


def test_get_fastq_ica_id_list_all_found(monkeypatch):
    """Every manifest filename resolves to its ICA id with the name and size ICA reports."""
    _patch_ica_retry(monkeypatch, {'a.fastq.gz': ('fid-a', 111), 'b.fastq.gz': ('fid-b', 222)})
    result = manage_md5_pipeline._get_fastq_ica_id_list(
        fastq_filenames=['a.fastq.gz', 'b.fastq.gz'],
        api_instance=_STUB_API,
        path_parameters={},
    )
    assert result == {'fid-a': ('a.fastq.gz', 111), 'fid-b': ('b.fastq.gz', 222)}


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


def test_prepare_md5_submission_packs_ids_and_wires_chunk_size(monkeypatch, tmp_path):
    """The uploaded ID list is byte-balance ordered and the submit callable carries
    the computed chunk size."""
    config = {('manifest', 'filenames'): 'filenames'}
    monkeypatch.setattr(
        manage_md5_pipeline.cpg_utils.config,
        'config_retrieve',
        lambda key, default=None: config.get(tuple(key), default),
    )
    monkeypatch.setattr(manage_md5_pipeline, 'try_get_ar_guid', lambda: 'guid')
    monkeypatch.setenv('BATCH_TMPDIR', str(tmp_path))

    manifest = tmp_path / 'manifest.csv'
    manifest.write_text('filenames\na.fastq.gz\nb.fastq.gz\nc.fastq.gz\nd.fastq.gz\n')

    # Sizes chosen so the packed (largest-first) order differs from insertion
    # order; 4 files -> computed chunk_size 1 -> one file per block, descending.
    info = {
        'fid-a': manage_md5_pipeline.FastqFileDetails(name='a.fastq.gz', size_in_bytes=1),
        'fid-b': manage_md5_pipeline.FastqFileDetails(name='b.fastq.gz', size_in_bytes=100),
        'fid-c': manage_md5_pipeline.FastqFileDetails(name='c.fastq.gz', size_in_bytes=2),
        'fid-d': manage_md5_pipeline.FastqFileDetails(name='d.fastq.gz', size_in_bytes=99),
    }
    monkeypatch.setattr(manage_md5_pipeline, '_get_fastq_ica_id_list', lambda **kwargs: info)  # noqa: ARG005

    @contextmanager
    def _fake_data_api(role):  # noqa: ARG001
        yield _STUB_API, {'projectId': 'proj-1'}

    monkeypatch.setattr(manage_md5_pipeline.ica_api_utils, 'ica_project_data_api', _fake_data_api)

    class _FakeCohortPath:
        def __truediv__(self, other):  # noqa: ARG002
            return SimpleNamespace(as_folder=lambda: '/cohort/fastq_lists/')

    monkeypatch.setattr(manage_md5_pipeline.ica_utils, 'ica_cohort_path', lambda name: _FakeCohortPath())  # noqa: ARG005
    monkeypatch.setattr(manage_md5_pipeline.ica_cli_utils, 'authenticate_ica_cli', lambda role: None)  # noqa: ARG005

    uploaded: dict = {}

    def _capture_upload(*, local_file_path: str, ica_folder_path: str) -> None:  # noqa: ARG001
        with open(local_file_path) as fh:
            uploaded['content'] = fh.read()

    monkeypatch.setattr(manage_md5_pipeline.ica_cli_utils, 'upload_local_file', _capture_upload)
    monkeypatch.setattr(
        manage_md5_pipeline.ica_api_utils,
        'get_file_details_from_ica',
        lambda **kwargs: {'id': 'fil.list', 'details': {'status': 'AVAILABLE'}},  # noqa: ARG005
    )
    monkeypatch.setattr(
        manage_md5_pipeline, 'IcaPath', SimpleNamespace(output_root=lambda: SimpleNamespace(as_folder=lambda: '/out/'))
    )
    monkeypatch.setattr(manage_md5_pipeline, '_create_md5_output_folder', lambda **kwargs: 'fol.out')  # noqa: ARG005

    outputs = {'fastq_ids_outpath': tmp_path / 'fastq_ids.json'}
    submit = manage_md5_pipeline._prepare_md5_submission(
        cohort_name='COH1',
        outputs=outputs,
        manifest_file_path=manifest,
    )

    assert uploaded['content'].splitlines() == ['fid-b', 'fid-d', 'fid-c', 'fid-a']
    assert json.loads(outputs['fastq_ids_outpath'].read_text()) == {
        'fid-a': 'a.fastq.gz',
        'fid-b': 'b.fastq.gz',
        'fid-c': 'c.fastq.gz',
        'fid-d': 'd.fastq.gz',
    }
    assert isinstance(submit, partial)
    assert submit.keywords['chunk_size'] == 1
    assert submit.keywords['fastq_list_file_id'] == 'fil.list'


@pytest.mark.parametrize('details', [{'name': 'a.fastq.gz'}, {'name': 'a.fastq.gz', 'fileSizeInBytes': None}])
def test_get_fastq_ica_id_list_names_file_when_size_is_absent(monkeypatch, details):
    """A file without a usable size (e.g. still uploading) must raise an error naming it."""
    response = SimpleNamespace(body={'items': [{'data': {'id': 'fid-a', 'details': details}}]})
    monkeypatch.setattr(
        manage_md5_pipeline.ica_api_utils,
        'ica_retry',
        lambda *args, **kwargs: response,  # noqa: ARG005
    )
    with pytest.raises(ValueError, match=r'a\.fastq\.gz.*fid-a|fid-a.*a\.fastq\.gz'):
        manage_md5_pipeline._get_fastq_ica_id_list(
            fastq_filenames=['a.fastq.gz'],
            api_instance=_STUB_API,
            path_parameters={},
        )


def test_get_fastq_ica_id_list_duplicate_name_cannot_mask_a_missing_file(monkeypatch):
    """Two ICA files sharing one manifest name plus one truly missing name keeps the
    counts equal, so a count-only check would pass; the reconciliation must raise,
    naming both the missing and the duplicated file."""
    items = [
        {'data': {'id': 'fid-a1', 'details': {'name': 'a.fastq.gz', 'fileSizeInBytes': 1}}},
        {'data': {'id': 'fid-a2', 'details': {'name': 'a.fastq.gz', 'fileSizeInBytes': 1}}},
    ]
    monkeypatch.setattr(
        manage_md5_pipeline.ica_api_utils,
        'ica_retry',
        lambda *args, **kwargs: SimpleNamespace(body={'items': items}),  # noqa: ARG005
    )
    with pytest.raises(ValueError) as excinfo:
        manage_md5_pipeline._get_fastq_ica_id_list(
            fastq_filenames=['a.fastq.gz', 'b.fastq.gz'],
            api_instance=_STUB_API,
            path_parameters={},
        )
    message = str(excinfo.value)
    assert 'b.fastq.gz' in message
    assert 'a.fastq.gz' in message


def test_get_fastq_ica_id_list_mismatch_names_missing_files(monkeypatch):
    """A mismatch must raise and name exactly the files ICA didn't return."""
    # ICA only knows about a.fastq.gz; b and c from the manifest are absent.
    _patch_ica_retry(monkeypatch, {'a.fastq.gz': ('fid-a', 111)})
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
