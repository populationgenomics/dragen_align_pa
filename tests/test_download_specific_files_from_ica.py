"""Tests for the CRAM / gVCF download job.

The file set is a unit: a CRAM is meaningless with another run's index. Only the main file
carries an ICA MD5, so the index would otherwise be kept whenever it merely exists — which is
reachable through the documented recovery path of forcing this stage after a re-analysis.
"""

from unittest.mock import MagicMock

from dragen_align_pa.jobs import download_specific_files_from_ica
from dragen_align_pa.utils import download_job_timeout_seconds


def _orchestrate(monkeypatch, *, main_streamed: bool, force_redownload: bool = False) -> MagicMock:
    """Run the CRAM/index/MD5 orchestration; return the stream mock."""
    monkeypatch.setattr(
        'dragen_align_pa.ica_api_utils.find_file_id_by_name',
        MagicMock(side_effect=['fil.main', 'fil.index', 'fil.md5']),
    )
    monkeypatch.setattr(
        'dragen_align_pa.ica_utils.get_md5_from_ica',
        MagicMock(return_value=('abc123', 'abc123  SYN00001.cram')),
    )
    # True == the main file was (re-)streamed; False == it was skipped as already complete.
    stream = MagicMock(side_effect=[main_streamed, True])
    monkeypatch.setattr('dragen_align_pa.ica_utils.stream_ica_file_to_gcs', stream)

    download_specific_files_from_ica._orchestrate_download(
        api_instance=MagicMock(),
        path_parameters={'projectId': 'p'},
        base_ica_folder_path='/ica/folder/',
        gcs_bucket=MagicMock(),
        gcs_output_path_prefix='ica/output/cram',
        main_file_name='SYN00001.cram',
        index_file_name='SYN00001.cram.crai',
        md5_file_name='SYN00001.cram.md5',
        md5_gcp_name='SYN00001.cram.md5sum',
        force_redownload=force_redownload,
    )
    return stream


def test_a_redownloaded_main_file_forces_its_index_too(monkeypatch):
    """The split-brain case: on a re-analysis the CRAM's MD5 disagrees so it is re-fetched,
    and without this the index from the previous run would be silently kept."""
    stream = _orchestrate(monkeypatch, main_streamed=True)

    index_call = stream.call_args_list[1]
    assert index_call.kwargs['file_name'] == 'SYN00001.cram.crai'
    assert index_call.kwargs['skip_existing'] is False


def test_a_skipped_main_file_leaves_its_index_alone(monkeypatch):
    """When the CRAM was already complete, its index is whatever wrote that CRAM: no re-fetch."""
    stream = _orchestrate(monkeypatch, main_streamed=False)

    assert stream.call_args_list[1].kwargs['skip_existing'] is True


def test_force_redownload_refetches_both(monkeypatch):
    """force_redownload must reach the index as well as the main file."""
    stream = _orchestrate(monkeypatch, main_streamed=True, force_redownload=True)

    assert all(call.kwargs['skip_existing'] is False for call in stream.call_args_list)


def test_the_job_timeout_is_a_generous_backstop_not_a_transfer_budget():
    """It has to outlast a legitimately slow multi-GB CRAM, or it turns a working download
    into a killed job. Pinned because nothing else exercises this value."""
    assert download_job_timeout_seconds() >= 3600
