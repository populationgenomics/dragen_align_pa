"""Unit tests for the exome design validator helpers in `utils.py`."""

from dataclasses import dataclass, field

import pytest

from dragen_align_pa import utils
from dragen_align_pa.utils import (
    _resolve_sg_canonical_design,
    assert_cohort_design_matches_configured_bed,
)


@dataclass
class _FakeAssay:
    meta: dict = field(default_factory=dict)


@dataclass
class _FakeSG:
    id: str
    assays: list = field(default_factory=list)


@dataclass
class _FakeCohort:
    id: str
    sgs: list = field(default_factory=list)

    def get_sequencing_groups(self) -> list:
        return self.sgs


def _make_sg(sg_id: str, *sequencing_libraries: str) -> _FakeSG:
    return _FakeSG(
        id=sg_id,
        assays=[_FakeAssay(meta={'sequencing_library': lib}) for lib in sequencing_libraries],
    )


# ----- _resolve_sg_canonical_design -----

def test_resolve_design_happy_path():
    sg = _make_sg('CPG_A', 'SSQXTCREV2')
    assert _resolve_sg_canonical_design(sg) == 'CREv2'


def test_resolve_design_multiple_assays_same_canonical():
    """An SG with two assays whose raw strings map to the same canonical
    design (e.g. different prep protocols, same capture) is fine."""
    sg = _make_sg('CPG_A', 'SSQXTCREV2', 'AgilentCREv2WES')
    assert _resolve_sg_canonical_design(sg) == 'CREv2'


def test_resolve_design_no_assays_raises():
    sg = _FakeSG(id='CPG_A', assays=[])
    with pytest.raises(RuntimeError, match=r'no assay\.meta'):
        _resolve_sg_canonical_design(sg)


def test_resolve_design_unmapped_value_raises():
    sg = _make_sg('CPG_A', 'NeverHeardOfThis')
    with pytest.raises(RuntimeError, match='unmapped sequencing_library'):
        _resolve_sg_canonical_design(sg)


def test_resolve_design_multiple_canonicals_raises():
    """An SG whose assays map to different canonical designs (genuinely
    mixed prep within one SG) must fail — the validator can't pick a BED."""
    sg = _make_sg('CPG_A', 'SSQXTCREV2', 'TwistWES1VCGS1')
    with pytest.raises(RuntimeError, match='multiple canonical designs'):
        _resolve_sg_canonical_design(sg)


# ----- assert_cohort_design_matches_configured_bed -----

def _config_factory(sequencing_type='exome', bed_names=None):
    cfg = {('workflow', 'sequencing_type'): sequencing_type}
    if bed_names is not None:
        cfg[
            ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'exome', 'bed_names')
        ] = bed_names

    def fake_retrieve(key, default=None):
        return cfg.get(tuple(key), default)

    return fake_retrieve


def test_validator_no_op_for_non_exome(monkeypatch):
    """Genome runs return immediately — even with no cohort SGs."""
    monkeypatch.setattr(utils, 'config_retrieve', _config_factory(sequencing_type='genome'))
    cohort = _FakeCohort(id='COH0001', sgs=[])
    # No raise expected.
    assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]


def test_validator_rejects_empty_cohort(monkeypatch):
    monkeypatch.setattr(utils, 'config_retrieve', _config_factory())
    cohort = _FakeCohort(id='COH0001', sgs=[])
    with pytest.raises(RuntimeError, match='no sequencing groups'):
        assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]


def test_validator_rejects_mixed_designs(monkeypatch):
    monkeypatch.setattr(utils, 'config_retrieve', _config_factory(
        bed_names={'vc_target': 'S30409818_Covered.bed'},
    ))
    cohort = _FakeCohort(id='COH0001', sgs=[
        _make_sg('CPG_A', 'SSQXTCREV2'),
        _make_sg('CPG_B', 'TwistWES1VCGS1'),
    ])
    with pytest.raises(RuntimeError, match='mixed exome designs'):
        assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]


def test_validator_rejects_missing_bed_names(monkeypatch):
    """All SGs resolve to the same design, but bed_names has no non-blank
    entries → fail before any submission."""
    monkeypatch.setattr(utils, 'config_retrieve', _config_factory(
        bed_names={'vc_target': '', 'cnv_target': '', 'sv_call_regions': ''},
    ))
    cohort = _FakeCohort(id='COH0001', sgs=[_make_sg('CPG_A', 'SSQXTCREV2')])
    with pytest.raises(RuntimeError, match='no non-blank entries'):
        assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]


def test_validator_rejects_bed_outside_design(monkeypatch):
    """Cohort is CREv2 but config uses a Twist BED → fail with both names
    in the message so the operator sees the mismatch."""
    monkeypatch.setattr(utils, 'config_retrieve', _config_factory(
        bed_names={'vc_target': 'Twist_VCGS_Exome_Covered_Targets_hg38.bed'},
    ))
    cohort = _FakeCohort(id='COH0001', sgs=[_make_sg('CPG_A', 'SSQXTCREV2')])
    with pytest.raises(RuntimeError, match=r"CREv2.*Twist_VCGS_Exome_Covered"):
        assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]


def test_validator_happy_path_crev2(monkeypatch):
    monkeypatch.setattr(utils, 'config_retrieve', _config_factory(bed_names={
        'vc_target': 'S30409818_Covered.bed',
        'cnv_target': 'S30409818_Regions.bed',
        'sv_call_regions': 'S30409818_Regions.bed',
    }))
    cohort = _FakeCohort(id='COH0001', sgs=[
        _make_sg('CPG_A', 'SSQXTCREV2'),
        _make_sg('CPG_B', 'AgilentCREv2WES'),  # different lab string, same canonical
    ])
    # No raise expected; validator is silent on success.
    assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]


def test_validator_happy_path_twist(monkeypatch):
    monkeypatch.setattr(utils, 'config_retrieve', _config_factory(bed_names={
        'vc_target': 'Twist_VCGS_Exome_Covered_Targets_hg38.bed',
        'cnv_target': 'Twist_VCGS_Exome_Covered_Targets_hg38.bed',
        'sv_call_regions': 'Twist_VCGS_Exome_Covered_Targets_hg38.bed',
    }))
    cohort = _FakeCohort(id='COH0001', sgs=[_make_sg('CPG_A', 'TwistWES1VCGS1')])
    assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]
