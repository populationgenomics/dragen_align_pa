"""Unit tests for `utils.get_bed_names_for_seqtype`.

The exome design guards that used to live here moved to `validator.py`; their tests are in
`tests/test_validator.py`. `get_bed_names_for_seqtype` stays in `utils` because
`submit_dragen_batch` also consumes it at run time.
"""

import pytest

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
