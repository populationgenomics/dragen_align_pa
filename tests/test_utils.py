"""Unit tests for `utils.get_bed_names_for_seqtype`.

The exome design guards that used to live here moved to `validator.py`; their tests are in
`tests/test_validator.py`. `get_bed_names_for_seqtype` stays in `utils` because
`submit_dragen_batch` also consumes it at run time.
"""

import subprocess
import sys

import pytest
from loguru import logger as loguru_logger

from dragen_align_pa import utils
from dragen_align_pa.utils import get_bed_names_for_seqtype
from tests._config_helpers import _config_factory


def test_get_bed_names_returns_empty_for_genome(monkeypatch):
    """Genome runs have no bed_names block by design; return {} cleanly."""
    monkeypatch.setattr(utils, 'config_retrieve', _config_factory(sequencing_type='genome'))
    assert get_bed_names_for_seqtype() == {}


def test_get_bed_names_raises_for_exome_with_no_block(monkeypatch):
    """Exome runs require a populated bed_names block; missing or empty
    block raises before any ICA submission."""
    monkeypatch.setattr(utils, 'config_retrieve', _config_factory(sequencing_type='exome'))
    with pytest.raises(ValueError, match='is missing or empty'):
        get_bed_names_for_seqtype()


def test_get_bed_names_rejects_partially_empty_values(monkeypatch):
    """Some entries set, some empty -> raise naming only the unset ones.
    This is what the function move is designed to catch."""
    monkeypatch.setattr(
        utils,
        'config_retrieve',
        _config_factory(
            sequencing_type='exome',
            bed_names={'vc_target': 'covered.bed', 'cnv_target': '', 'sv_call_regions': '  '},
        ),
    )
    with pytest.raises(ValueError, match=r"\['cnv_target', 'sv_call_regions'\]"):
        get_bed_names_for_seqtype()


def test_get_bed_names_returns_populated_dict(monkeypatch):
    monkeypatch.setattr(
        utils,
        'config_retrieve',
        _config_factory(
            sequencing_type='exome',
            bed_names={
                'vc_target': 'covered.bed',
                'cnv_target': 'regions.bed',
                'sv_call_regions': 'regions.bed',
            },
        ),
    )
    assert get_bed_names_for_seqtype() == {
        'vc_target': 'covered.bed',
        'cnv_target': 'regions.bed',
        'sv_call_regions': 'regions.bed',
    }


# --- run_subprocess_with_log output logging ---


def _capture_logs(level: str) -> tuple[list[str], int]:
    records: list[str] = []
    sink_id = loguru_logger.add(lambda message: records.append(str(message)), level=level)
    return records, sink_id


def test_run_subprocess_logs_stdout_on_success_by_default():
    records, sink_id = _capture_logs('INFO')
    try:
        utils.run_subprocess_with_log(['echo', 'hello-stdout'], 'Echo test')
    finally:
        loguru_logger.remove(sink_id)

    assert any('hello-stdout' in r for r in records)


def test_run_subprocess_log_output_false_suppresses_success_output():
    """A caller whose stdout is bulk data it parses itself (e.g. the ICA list
    JSON) opts out; the command and completion lines still log."""
    records, sink_id = _capture_logs('INFO')
    # stdout ('blobblobblob') must differ from every argv element, since the
    # command line itself is always logged.
    blob_cmd = [sys.executable, '-c', 'print("blob" * 3)']
    try:
        utils.run_subprocess_with_log(blob_cmd, 'Blob test', log_output=False)
    finally:
        loguru_logger.remove(sink_id)

    assert not any('blobblobblob' in r for r in records)
    assert any('completed successfully' in r for r in records)


def test_run_subprocess_log_output_false_still_logs_failure_output():
    """Suppression applies to the success path only: a failure must keep its
    full captured output in the ERROR record."""
    records, sink_id = _capture_logs('ERROR')
    fail_cmd = [sys.executable, '-c', 'import sys; print("boom-detail", file=sys.stderr); sys.exit(1)']
    try:
        with pytest.raises(subprocess.CalledProcessError):
            utils.run_subprocess_with_log(fail_cmd, 'Failing step', log_output=False)
    finally:
        loguru_logger.remove(sink_id)

    assert any('boom-detail' in r for r in records)
