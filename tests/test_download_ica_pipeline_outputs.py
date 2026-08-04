"""Tests for the bulk per-SG output download.

The stage used to declare the parent folder as its expected output, which exists as soon as
the first of 100+ files lands — so cpg-flow could not tell a part-way download from a complete
one, and recovering from a mid-run connection failure meant forcing the stage for every
sequencing group. These tests pin the two things that replaced that: a `_SUCCESS` sentinel
written only after the last file, and a per-SG state file recording each transfer as it
happens, so a re-run neither re-mints (rate-limited) URLs nor re-downloads bytes it has.
"""

from unittest.mock import MagicMock

import pytest
import requests

from dragen_align_pa.jobs import download_ica_pipeline_outputs

_ICA_FILES = [
    ('SYN00001.qc.csv', 'fil.a'),
    ('SYN00001.metrics.csv', 'fil.b'),
    ('SYN00001.cram', 'fil.cram'),
]


@pytest.fixture
def patched_job(monkeypatch):
    """Patch the job's collaborators and return the mocks the tests assert on."""
    mocks = MagicMock()
    mocks.bucket = MagicMock()
    # Without this the real `get_ica_api_client` runs, which fetches the ICA API key from
    # Secret Manager: a network call needing credentials the test has no business holding.
    ica_client = MagicMock()
    ica_client.__enter__ = MagicMock(return_value=ica_client)
    ica_client.__exit__ = MagicMock(return_value=False)
    monkeypatch.setattr('dragen_align_pa.ica_api_utils.get_ica_api_client', lambda: ica_client)
    monkeypatch.setattr(
        'dragen_align_pa.ica_utils.get_ica_sample_folder',
        MagicMock(return_value='/ica/folder/'),
    )
    monkeypatch.setattr(
        'dragen_align_pa.ica_utils.list_ica_files',
        MagicMock(return_value=_ICA_FILES),
    )
    monkeypatch.setattr(
        'dragen_align_pa.jobs.download_ica_pipeline_outputs.storage.Client',
        MagicMock(return_value=MagicMock(bucket=MagicMock(return_value=mocks.bucket))),
    )
    monkeypatch.setattr('dragen_align_pa.ica_utils.batch_create_download_urls', mocks.mint)
    monkeypatch.setattr('dragen_align_pa.ica_utils.stream_ica_file_to_gcs', mocks.stream)
    monkeypatch.setattr('dragen_align_pa.ica_utils.DownloadState.load', mocks.load_state)
    monkeypatch.setattr('dragen_align_pa.ica_utils.write_success_sentinel', mocks.sentinel)
    mocks.load_state.return_value = mocks.state
    mocks.mint.return_value = {'fil.a': 'https://u/a', 'fil.b': 'https://u/b'}
    return mocks


def _patch_resumable(monkeypatch, mocks, names):
    """Say which files the state file reports as already downloaded and still present."""
    listing = MagicMock(return_value=set(names))
    monkeypatch.setattr('dragen_align_pa.ica_utils.list_gcs_names', listing)
    mocks.state.resumable.return_value = set(names)
    return listing


def _run(sg_name='SYN00001'):
    sequencing_group = MagicMock()
    sequencing_group.name = sg_name
    download_ica_pipeline_outputs.run(
        sequencing_group=sequencing_group,
        pipeline_id_arguid_path=MagicMock(),
        cohort_name='COH0001',
    )


def test_downloads_everything_when_gcs_is_empty(patched_job, monkeypatch):
    """Nothing present: both non-CRAM files are minted and streamed (CRAM/gVCF are
    sibling stages' work)."""
    _patch_resumable(monkeypatch, patched_job, set())

    _run()

    assert patched_job.mint.call_args.kwargs['file_ids'] == ['fil.a', 'fil.b']
    assert patched_job.stream.call_count == 2


def test_already_present_files_are_not_reminted_or_restreamed(patched_job, monkeypatch):
    """The point of the pre-filter: a re-run after a part-way failure mints URLs for
    the missing file only, instead of re-minting all of them."""
    _patch_resumable(monkeypatch, patched_job, {'SYN00001.qc.csv'})

    _run()

    assert patched_job.mint.call_args.kwargs['file_ids'] == ['fil.b']
    assert patched_job.stream.call_count == 1


def test_fully_downloaded_sg_makes_no_ica_url_calls(patched_job, monkeypatch):
    """A complete SG must cost zero rate-limited mint calls on a re-run."""
    _patch_resumable(monkeypatch, patched_job, {'SYN00001.qc.csv', 'SYN00001.metrics.csv'})

    _run()

    patched_job.mint.assert_not_called()
    patched_job.stream.assert_not_called()


def test_force_redownload_ignores_what_is_already_in_gcs(patched_job, monkeypatch):
    """With force_redownload set, GCS isn't consulted and everything is re-fetched."""
    listing = _patch_resumable(monkeypatch, patched_job, {'SYN00001.qc.csv', 'SYN00001.metrics.csv'})
    monkeypatch.setattr(
        'dragen_align_pa.jobs.download_ica_pipeline_outputs.config_retrieve',
        lambda key, default=None: True if key == ['ica', 'download', 'force_redownload'] else default,
    )

    _run()

    listing.assert_not_called()
    assert patched_job.stream.call_count == 2


def test_neither_the_outputs_nor_the_provenance_marker_are_namespaced_by_cohort(patched_job, monkeypatch):
    """Outputs are keyed by sequencing group alone, so an SG belonging to two cohorts (a
    panel-of-normals cohort drawn from a production one) has ONE copy, never a per-cohort
    duplicate. Only state paths are cohort-scoped — see `utils.get_pipeline_path`.

    The marker has to be SG-keyed for the same reason it exists: a per-cohort marker would let
    each cohort believe it owns the shared prefix, and each would then treat the other's files
    as its own already-downloaded output.
    """
    _patch_resumable(monkeypatch, patched_job, set())

    _run()

    marker_key = patched_job.load_state.call_args.args[1]
    gcs_prefix = patched_job.stream.call_args.kwargs['gcs_prefix']

    assert 'SYN00001' in marker_key
    for path in (marker_key, gcs_prefix):
        assert 'COH0001' not in path, f'{path} is namespaced by cohort; outputs must be keyed by SG alone'


def test_the_state_file_is_loaded_for_this_ica_run(patched_job, monkeypatch):
    """Provenance gates the skip: the state is loaded for the ICA folder being downloaded, so
    a previous analysis's outputs are never mistaken for ours."""
    _patch_resumable(monkeypatch, patched_job, set())

    _run()

    assert patched_job.load_state.call_args.args[2] == '/ica/folder/'


def test_success_sentinel_is_written_only_after_every_file_lands(patched_job, monkeypatch):
    """The sentinel is the stage's declared output, so writing it early would mark a
    part-way download complete and strand the remaining files."""
    _patch_resumable(monkeypatch, patched_job, set())

    _run()

    patched_job.sentinel.assert_called_once()
    assert patched_job.sentinel.call_args.args[1].endswith('dragen_metrics/SYN00001')


def test_no_sentinel_when_a_transfer_fails(patched_job, monkeypatch):
    """A failed download must leave the stage incomplete so cpg-flow re-runs it."""
    _patch_resumable(monkeypatch, patched_job, set())
    patched_job.stream.side_effect = requests.ConnectionError('reset')

    with pytest.raises(requests.ConnectionError):
        _run()

    patched_job.sentinel.assert_not_called()


def test_each_transferred_file_is_recorded_as_it_lands(patched_job, monkeypatch):
    """Recorded per file, not per batch: a job killed mid-chunk resumes from the last file
    that actually landed."""
    _patch_resumable(monkeypatch, patched_job, set())

    _run()

    recorded = [call.args[0] for call in patched_job.state.mark_transferred.call_args_list]
    assert recorded == ['SYN00001.qc.csv', 'SYN00001.metrics.csv']


def test_an_already_complete_sg_still_gets_its_sentinel(patched_job, monkeypatch):
    """A group downloaded before the sentinel existed must be able to converge without
    re-fetching: nothing to do, but the stage still has to declare itself done."""
    _patch_resumable(monkeypatch, patched_job, {'SYN00001.qc.csv', 'SYN00001.metrics.csv'})

    _run()

    patched_job.stream.assert_not_called()
    patched_job.sentinel.assert_called_once()


def test_an_empty_ica_folder_is_an_error_not_a_completed_download(patched_job, monkeypatch):
    """Listing nothing means the analysis produced nothing or the folder is wrong; writing
    _SUCCESS there would permanently mark an empty sequencing group as downloaded."""
    _patch_resumable(monkeypatch, patched_job, set())
    monkeypatch.setattr('dragen_align_pa.ica_utils.list_ica_files', MagicMock(return_value=[]))

    with pytest.raises(ValueError, match='no downloadable outputs'):
        _run()

    patched_job.sentinel.assert_not_called()
