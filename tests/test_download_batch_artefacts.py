"""Unit tests for `download_batch_artefacts`.

Covers the failure-accounting helpers (`_StreamStats`, `_stream_silently`,
`_stream_named_file`) and the top-level `run` function's contracts:
duplicate-index pre-scan, gcs_output_root prefix assertion, total-failure
RAISE, marker payload shape.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import cpg_utils
import icasdk
import pytest
import requests
from google.cloud import exceptions as gcs_exceptions

from dragen_align_pa.jobs import download_batch_artefacts
from dragen_align_pa.jobs.download_batch_artefacts import (
    _stream_named_file,
    _stream_silently,
    _StreamStats,
)

# ---------------------------------------------------------------------------
# _StreamStats
# ---------------------------------------------------------------------------


def test_stream_stats_total_failure_sums_both_buckets():
    s = _StreamStats(
        success=10,
        lookup_failures=[{}, {}, {}],
        stream_failures=[{}, {}],
    )
    assert s.total_failure == 5


def test_stream_stats_default_is_zero():
    s = _StreamStats()
    assert s.success == 0
    assert s.lookup_failures == []
    assert s.stream_failures == []
    assert s.total_failure == 0


# ---------------------------------------------------------------------------
# _stream_silently
# ---------------------------------------------------------------------------


def _silent_call(stats, raise_exc=None):
    """Invoke _stream_silently with a stream that may raise."""
    stream = MagicMock() if raise_exc is None else MagicMock(side_effect=raise_exc)
    # `_stream_silently` skips when the target blob already exists in GCS
    # (the 'delete the marker and re-run' retry contract). Configure the
    # default bucket mock to report the blob is absent so we exercise the
    # streaming path, not the skip path.
    gcs_bucket = MagicMock()
    gcs_bucket.blob.return_value.exists.return_value = False

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr('dragen_align_pa.ica_utils.stream_ica_file_to_gcs', stream)
        _stream_silently(
            api_instance=MagicMock(),
            path_parameters={'projectId': 'p'},
            file_id='fid',
            file_name='f.txt',
            gcs_bucket=gcs_bucket,
            gcs_prefix='prefix',
            context='ctx',
            stats=stats,
        )
    return stream


def test_stream_silently_success_increments_success():
    stats = _StreamStats()
    _silent_call(stats)
    assert stats.success == 1
    assert stats.stream_failures == []
    assert stats.lookup_failures == []


def test_stream_silently_swallows_api_exception():
    stats = _StreamStats()
    _silent_call(stats, raise_exc=icasdk.ApiException(status=503, reason='boom'))
    assert stats.success == 0
    assert len(stats.stream_failures) == 1
    assert stats.lookup_failures == []


def test_stream_silently_swallows_request_exception():
    stats = _StreamStats()
    _silent_call(stats, raise_exc=requests.RequestException('connection reset'))
    assert len(stats.stream_failures) == 1


def test_stream_silently_swallows_gcs_error():
    stats = _StreamStats()
    _silent_call(stats, raise_exc=gcs_exceptions.GoogleCloudError('5xx'))
    assert len(stats.stream_failures) == 1


def test_stream_silently_propagates_value_error():
    """The MD5-mismatch ValueError branch is intentionally NOT caught; this
    helper passes expected_md5_hash=None so the branch is unreachable in
    practice, but the contract should still propagate to a future caller
    that supplies a hash."""
    stats = _StreamStats()
    with pytest.raises(ValueError, match='md5'):
        _silent_call(stats, raise_exc=ValueError('md5 mismatch'))
    assert stats.success == 0
    assert stats.total_failure == 0


# ---------------------------------------------------------------------------
# _stream_named_file
# ---------------------------------------------------------------------------


def _named_call(stats, *, lookup_exc=None, stream_exc=None, file_id='fid'):
    """Invoke _stream_named_file with optional lookup / stream failures."""
    lookup = MagicMock(return_value=file_id) if lookup_exc is None else MagicMock(side_effect=lookup_exc)
    stream = MagicMock(side_effect=stream_exc) if stream_exc else MagicMock()
    gcs_bucket = MagicMock()
    gcs_bucket.blob.return_value.exists.return_value = False

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr('dragen_align_pa.ica_api_utils.find_file_id_by_name', lookup)
        mp.setattr('dragen_align_pa.ica_utils.stream_ica_file_to_gcs', stream)
        _stream_named_file(
            api_instance=MagicMock(),
            path_parameters={'projectId': 'p'},
            parent_folder='/parent/',
            file_name='target.json',
            gcs_bucket=gcs_bucket,
            gcs_prefix='prefix',
            stats=stats,
        )


def test_stream_named_file_success_increments_success():
    stats = _StreamStats()
    _named_call(stats)
    assert stats.success == 1


def test_stream_named_file_legitimate_absence_does_not_count():
    """FileNotFoundError at lookup is treated as legitimate absence — neither
    success nor failure counter increments. passfail.json may legitimately
    be missing on a catastrophically-failed batch."""
    stats = _StreamStats()
    _named_call(stats, lookup_exc=FileNotFoundError('not there'))
    assert stats.success == 0
    assert stats.lookup_failures == []
    assert stats.stream_failures == []


def test_stream_named_file_lookup_api_exception_increments_lookup_failure():
    stats = _StreamStats()
    _named_call(stats, lookup_exc=icasdk.ApiException(status=503, reason='lookup boom'))
    assert len(stats.lookup_failures) == 1
    assert stats.success == 0
    assert stats.stream_failures == []


def test_stream_named_file_stream_failure_after_successful_lookup_increments_stream_failure():
    """When lookup succeeds but stream fails transiently, the failure is
    counted as stream_failure (not lookup_failure)."""
    stats = _StreamStats()
    _named_call(stats, stream_exc=icasdk.ApiException(status=502, reason='stream boom'))
    assert len(stats.stream_failures) == 1
    assert stats.lookup_failures == []
    assert stats.success == 0


# ---------------------------------------------------------------------------
# run() — high-level contracts
# ---------------------------------------------------------------------------


def _write_batches_file(path: Path, *batches: dict) -> None:
    """Materialise a minimally-valid {cohort}_batches.json at path."""
    payload = {
        'schema_version': 1,
        'batch_size': 5,
        'n_batches': len(batches),
        'batches': list(batches),
    }
    path.write_text(json.dumps(payload))


def _batch_entry(
    *,
    batch_index: int,
    pipeline_id: str | None = 'pid-{0:04d}',
    status: str = 'SUCCEEDED',
) -> dict:
    """Build a complete BatchesFile entry (every key in _REQUIRED_BATCH_KEYS)."""
    pid = pipeline_id.format(batch_index) if isinstance(pipeline_id, str) else pipeline_id
    return {
        'batch_index': batch_index,
        'retry_generation': 0,
        'sg_names': ['SYN00001'],
        'retried_sgs': [],
        'user_reference': f'COH0001-batch{batch_index:04d}_test-guid_',
        'pipeline_id': pid,
        'ar_guid': 'test-guid',
        'analysis_output_folder_fid': None,
        'fastq_list_fid': None,
        'cram_fids': None,
        'status': status,
        'passfail': None,
        'passfail_seen': False,
        'has_been_retried': False,
        'error_strategy': 'auto',
    }


@pytest.fixture
def patched_environment(tmp_path: Path, monkeypatch):
    """Stub the secret fetch, ICA client, GCS client, and config calls."""
    monkeypatch.setattr(
        'dragen_align_pa.jobs.download_batch_artefacts.ica_api_utils.get_ica_secrets',
        lambda project_name: {'projectID': 'proj', 'apiKey': 'key'},  # noqa: ARG005
    )

    # `get_ica_api_client()` is used as a context manager.
    fake_client = MagicMock()
    fake_client.__enter__ = MagicMock(return_value=fake_client)
    fake_client.__exit__ = MagicMock(return_value=False)
    monkeypatch.setattr(
        'dragen_align_pa.jobs.download_batch_artefacts.ica_api_utils.get_ica_api_client',
        lambda project_name: fake_client,  # noqa: ARG005
    )

    fake_storage = MagicMock()
    fake_bucket = MagicMock()
    fake_bucket.blob.return_value.exists.return_value = False
    fake_storage.bucket.return_value = fake_bucket
    monkeypatch.setattr(
        'dragen_align_pa.jobs.download_batch_artefacts.storage.Client',
        lambda: fake_storage,
    )

    monkeypatch.setattr(
        'dragen_align_pa.jobs.download_batch_artefacts.cpg_utils.config.config_retrieve',
        lambda key, default=None: {
            ('ica', 'data_prep', 'output_folder'): 'test-dragen-378',
            ('ica', 'projects', 'dragen_align'): 'OurDNA-DRAGEN-378',
        }.get(tuple(key), default),
    )

    return tmp_path


def test_run_rejects_bad_gcs_output_root_prefix(patched_environment, tmp_path):  # noqa: ARG001
    """If gcs_output_root doesn't start with the expected bucket prefix, the
    job refuses to derive a relative path (would land objects in the wrong
    bucket)."""
    batches_path = tmp_path / 'COH0001_batches.json'
    _write_batches_file(batches_path, _batch_entry(batch_index=0))

    with pytest.raises(ValueError, match='does not start with expected'):
        download_batch_artefacts.run(
            batches_file_path=cpg_utils.to_path(batches_path),
            gcs_output_root=cpg_utils.to_path('gs://other-bucket/artefacts'),
            marker_path=cpg_utils.to_path(tmp_path / 'marker.json'),
            cohort_name='COH0001',
        )


def test_run_rejects_duplicate_batch_index(patched_environment, tmp_path):  # noqa: ARG001
    """The pre-scan fires before any ICA / GCS I/O when batches.json contains
    two entries with the same batch_index."""
    batches_path = tmp_path / 'COH0001_batches.json'
    _write_batches_file(
        batches_path,
        _batch_entry(batch_index=0),
        _batch_entry(batch_index=0, pipeline_id='pid-duplicate'),
    )

    with pytest.raises(ValueError, match='Duplicate batch_index'):
        download_batch_artefacts.run(
            batches_file_path=cpg_utils.to_path(batches_path),
            gcs_output_root=cpg_utils.to_path('gs://cpg-test-dataset-test/artefacts'),
            marker_path=cpg_utils.to_path(tmp_path / 'marker.json'),
            cohort_name='COH0001',
        )


def test_run_raises_runtime_error_on_total_failure(patched_environment, tmp_path, monkeypatch):  # noqa: ARG001
    """If every stream call fails transiently and zero succeed, the run
    raises RuntimeError instead of writing a green marker over empty GCS."""
    batches_path = tmp_path / 'COH0001_batches.json'
    _write_batches_file(batches_path, _batch_entry(batch_index=0))

    # Every lookup returns a file_id, but every stream raises ApiException.
    monkeypatch.setattr(
        'dragen_align_pa.ica_api_utils.find_file_id_by_name',
        MagicMock(return_value='fid'),
    )
    monkeypatch.setattr(
        'dragen_align_pa.ica_utils.stream_ica_file_to_gcs',
        MagicMock(side_effect=icasdk.ApiException(status=503, reason='stream boom')),
    )
    monkeypatch.setattr(
        'dragen_align_pa.ica_utils.list_ica_files',
        MagicMock(return_value=[]),
    )

    marker_path = tmp_path / 'marker.json'
    with pytest.raises(RuntimeError, match='every artefact stream failed'):
        download_batch_artefacts.run(
            batches_file_path=cpg_utils.to_path(batches_path),
            gcs_output_root=cpg_utils.to_path('gs://cpg-test-dataset-test/artefacts'),
            marker_path=cpg_utils.to_path(marker_path),
            cohort_name='COH0001',
        )
    assert not marker_path.exists()


def test_run_writes_marker_payload_with_partial_success(patched_environment, tmp_path, monkeypatch):  # noqa: ARG001
    """Partial success — one file streams OK, one fails — writes the marker
    with the per-bucket counts populated correctly."""
    batches_path = tmp_path / 'COH0001_batches.json'
    _write_batches_file(batches_path, _batch_entry(batch_index=0))

    # First lookup succeeds + stream succeeds; second lookup succeeds + stream fails.
    monkeypatch.setattr(
        'dragen_align_pa.ica_api_utils.find_file_id_by_name',
        MagicMock(return_value='fid'),
    )
    stream_results = iter([None, icasdk.ApiException(status=503, reason='stream boom')])

    def fake_stream(**kwargs):  # noqa: ARG001
        outcome = next(stream_results)
        if isinstance(outcome, Exception):
            raise outcome

    monkeypatch.setattr(
        'dragen_align_pa.ica_utils.stream_ica_file_to_gcs',
        fake_stream,
    )
    monkeypatch.setattr(
        'dragen_align_pa.ica_utils.list_ica_files',
        MagicMock(return_value=[]),
    )

    marker_path = tmp_path / 'marker.json'
    download_batch_artefacts.run(
        batches_file_path=cpg_utils.to_path(batches_path),
        gcs_output_root=cpg_utils.to_path('gs://cpg-test-dataset-test/artefacts'),
        marker_path=cpg_utils.to_path(marker_path),
        cohort_name='COH0001',
    )

    payload = json.loads(marker_path.read_text())
    assert payload['cohort_name'] == 'COH0001'
    assert payload['batches_processed'] == 1
    assert payload['success_count'] == 1
    assert payload['skipped_count'] == 0
    assert payload['lookup_failures'] == []
    assert len(payload['stream_failures']) == 1


def test_run_writes_marker_for_no_op_cohort(patched_environment, tmp_path, monkeypatch):  # noqa: ARG001
    """A batches file where every batch has no pipeline_id (e.g. all PENDING)
    is a legitimate no-op: marker writes with zero counts, no RAISE."""
    batches_path = tmp_path / 'COH0001_batches.json'
    _write_batches_file(
        batches_path,
        _batch_entry(batch_index=0, pipeline_id=None, status='PENDING'),
    )

    # Confirm nothing tries to stream — these mocks would error if invoked.
    monkeypatch.setattr(
        'dragen_align_pa.ica_api_utils.find_file_id_by_name',
        MagicMock(side_effect=AssertionError('should not be called')),
    )

    marker_path = tmp_path / 'marker.json'
    download_batch_artefacts.run(
        batches_file_path=cpg_utils.to_path(batches_path),
        gcs_output_root=cpg_utils.to_path('gs://cpg-test-dataset-test/artefacts'),
        marker_path=cpg_utils.to_path(marker_path),
        cohort_name='COH0001',
    )

    payload = json.loads(marker_path.read_text())
    assert payload == {
        'cohort_name': 'COH0001',
        'batches_processed': 0,
        'success_count': 0,
        'skipped_count': 0,
        'lookup_failures': [],
        'stream_failures': [],
    }
