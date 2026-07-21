"""Unit tests for the manage_md5_pipeline pre-submission helpers.

Covers `_get_fastq_ica_id_list`'s reconciliation between the manifest's
expected FASTQ filenames and the file IDs actually returned by ICA. The
mismatch path must name the specific files missing in ICA (not just a
count) so an operator can chase them down.
"""

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
