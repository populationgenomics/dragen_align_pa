"""Tests for the bulk per-SG output download.

The stage declares only the parent folder as its expected output, so cpg-flow cannot
tell a part-way download from a complete one: recovering from a mid-run connection
failure means re-running the stage for every sequencing group. These tests pin the
behaviour that makes that re-run cheap — files already in GCS are filtered out
*before* any pre-signed URL is minted, so a re-run neither re-mints (rate-limited)
URLs nor re-downloads bytes it already has.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from dragen_align_pa.jobs import download_ica_pipeline_outputs

_OWNED_SINCE = datetime(2026, 8, 1, tzinfo=UTC)

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
    monkeypatch.setattr('dragen_align_pa.ica_utils.claim_download_for_run', mocks.claim)
    mocks.claim.return_value = _OWNED_SINCE
    mocks.mint.return_value = {'fil.a': 'https://u/a', 'fil.b': 'https://u/b'}
    return mocks


def _patch_listing(monkeypatch, names):
    """Stub the provenance-aware listing and return its mock."""
    listing = MagicMock(return_value=names)
    monkeypatch.setattr('dragen_align_pa.ica_utils.list_gcs_names_written_since', listing)
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
    _patch_listing(monkeypatch, set())

    _run()

    assert patched_job.mint.call_args.kwargs['file_ids'] == ['fil.a', 'fil.b']
    assert patched_job.stream.call_count == 2


def test_already_present_files_are_not_reminted_or_restreamed(patched_job, monkeypatch):
    """The point of the pre-filter: a re-run after a part-way failure mints URLs for
    the missing file only, instead of re-minting all of them."""
    _patch_listing(monkeypatch, {'SYN00001.qc.csv'})

    _run()

    assert patched_job.mint.call_args.kwargs['file_ids'] == ['fil.b']
    assert patched_job.stream.call_count == 1


def test_fully_downloaded_sg_makes_no_ica_url_calls(patched_job, monkeypatch):
    """A complete SG must cost zero rate-limited mint calls on a re-run."""
    _patch_listing(monkeypatch, {'SYN00001.qc.csv', 'SYN00001.metrics.csv'})

    _run()

    patched_job.mint.assert_not_called()
    patched_job.stream.assert_not_called()


def test_force_redownload_ignores_what_is_already_in_gcs(patched_job, monkeypatch):
    """With force_redownload set, GCS isn't consulted and everything is re-fetched."""
    listing = _patch_listing(monkeypatch, {'SYN00001.qc.csv', 'SYN00001.metrics.csv'})
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
    _patch_listing(monkeypatch, set())

    _run()

    marker_key = patched_job.claim.call_args.args[1]
    gcs_prefix = patched_job.stream.call_args.kwargs['gcs_prefix']

    assert 'SYN00001' in marker_key
    for path in (marker_key, gcs_prefix):
        assert 'COH0001' not in path, f'{path} is namespaced by cohort; outputs must be keyed by SG alone'


def test_the_prefix_is_claimed_for_this_ica_run_before_it_is_listed(patched_job, monkeypatch):
    """Provenance gates the skip: the listing must be asked only for objects written since
    this run took ownership, so a previous analysis's outputs are never mistaken for ours."""
    listing = _patch_listing(monkeypatch, set())

    _run()

    assert patched_job.claim.call_args.args[2] == '/ica/folder/'
    assert listing.call_args.args[2] is _OWNED_SINCE
