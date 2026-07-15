"""Unit tests for the submit-time guards in `validator.py`.

The exome design check reads config both directly (in `validator`) and indirectly via
`utils.get_bed_names_for_seqtype`, so tests patch `config_retrieve` in BOTH modules — a
single patch on one module would silently miss the other binding.
"""

from dataclasses import dataclass, field

import pytest

from dragen_align_pa import utils, validator
from dragen_align_pa.validator import (
    _resolve_sg_canonical_design,
    assert_cohort_design_matches_configured_bed,
    assert_ica_project_root_resolves,
)


@dataclass
class _FakeSG:
    id: str
    meta: dict[str, str | None]


@dataclass
class _FakeCohort:
    id: str
    sgs: list[_FakeSG] = field(default_factory=list)

    def get_sequencing_groups(self) -> list[_FakeSG]:
        return self.sgs


def _make_sg(sg_id: str, sequencing_library: str) -> _FakeSG:
    return _FakeSG(id=sg_id, meta={'sequencing_library': sequencing_library})


# ----- assert_ica_project_root_resolves -----


def test_project_root_resolves_for_registered_family(monkeypatch):
    """A registered family (all roles present) passes."""
    monkeypatch.setattr(validator, 'config_retrieve', lambda key, default=None: 'ourdna')  # noqa: ARG005
    assert_ica_project_root_resolves()  # no raise


def test_project_root_unknown_family_raises(monkeypatch):
    monkeypatch.setattr(validator, 'config_retrieve', lambda key, default=None: 'not-a-family')  # noqa: ARG005
    with pytest.raises(KeyError, match=r'not-a-family'):
        assert_ica_project_root_resolves()


# ----- _resolve_sg_canonical_design -----


def test_resolve_design_happy_path():
    sg = _make_sg(sg_id='CPG_A', sequencing_library='SSQXTCREV2')
    assert _resolve_sg_canonical_design(sg) == 'CREv2'


def test_resolve_design_nosequencing_library_raises():
    sg = _FakeSG(id='CPG_A', meta={'sequencing_library': None})
    with pytest.raises(RuntimeError, match=r'no meta\[\'sequencing_library\'\]'):
        _resolve_sg_canonical_design(sg)


def test_resolve_design_unmapped_value_raises():
    sg = _make_sg(sg_id='CPG_A', sequencing_library='NeverHeardOfThis')
    with pytest.raises(RuntimeError, match="doesn't map to a canonical design"):
        _resolve_sg_canonical_design(sg)


# ----- assert_cohort_design_matches_configured_bed -----


def _config_factory(sequencing_type='exome', bed_names=None):
    cfg: dict[tuple[str, ...], object] = {('workflow', 'sequencing_type'): sequencing_type}
    if bed_names is not None:
        cfg[('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'exome', 'bed_names')] = bed_names

    def fake_retrieve(key, default=None):
        return cfg.get(tuple(key), default)

    return fake_retrieve


def _patch_config(monkeypatch, factory) -> None:
    """Patch `config_retrieve` in both modules the design check reads it from."""
    monkeypatch.setattr(validator, 'config_retrieve', factory)
    monkeypatch.setattr(utils, 'config_retrieve', factory)


def test_validator_no_op_for_non_exome(monkeypatch):
    """Genome runs return immediately — even with no cohort SGs."""
    _patch_config(monkeypatch, _config_factory(sequencing_type='genome'))
    cohort = _FakeCohort(id='COH0001', sgs=[])
    # No raise expected.
    assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]


def test_validator_rejects_empty_cohort(monkeypatch):
    _patch_config(monkeypatch, _config_factory())
    cohort = _FakeCohort(id='COH0001', sgs=[])
    with pytest.raises(RuntimeError, match='no sequencing groups'):
        assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]


def test_validator_rejects_mixed_designs(monkeypatch):
    _patch_config(monkeypatch, _config_factory(bed_names={'vc_target': 'S30409818_Covered.bed'}))
    cohort = _FakeCohort(
        id='COH0001',
        sgs=[
            _make_sg('CPG_A', 'SSQXTCREV2'),
            _make_sg('CPG_B', 'TwistWES1VCGS1'),
        ],
    )
    with pytest.raises(RuntimeError, match='mixed exome designs'):
        assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]


def test_validator_rejects_missing_bed_names(monkeypatch):
    """All SGs resolve to the same design, but bed_names has no non-empty
    entries. get_bed_names_for_seqtype raises ValueError before the validator
    gets a chance to do its own checks; that surfaces to the operator with the
    actionable "is missing values for [...]" message."""
    _patch_config(
        monkeypatch,
        _config_factory(bed_names={'vc_target': '', 'cnv_target': '', 'sv_call_regions': ''}),
    )
    cohort = _FakeCohort(id='COH0001', sgs=[_make_sg('CPG_A', 'SSQXTCREV2')])
    with pytest.raises(ValueError, match='is missing values for'):
        assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]


def test_validator_rejects_bed_outside_design(monkeypatch):
    """Cohort is CREv2 but config uses a Twist BED → fail with both names
    in the message so the operator sees the mismatch."""
    _patch_config(
        monkeypatch,
        _config_factory(bed_names={'vc_target': 'Twist_VCGS_Exome_Covered_Targets_hg38.bed'}),
    )
    cohort = _FakeCohort(id='COH0001', sgs=[_make_sg('CPG_A', 'SSQXTCREV2')])
    with pytest.raises(RuntimeError, match=r'CREv2.*Twist_VCGS_Exome_Covered'):
        assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]


def test_validator_happy_path_crev2(monkeypatch):
    _patch_config(
        monkeypatch,
        _config_factory(
            bed_names={
                'vc_target': 'S30409818_Covered.bed',
                'cnv_target': 'S30409818_Regions.bed',
                'sv_call_regions': 'S30409818_Regions.bed',
            }
        ),
    )
    cohort = _FakeCohort(
        id='COH0001',
        sgs=[
            _make_sg('CPG_A', 'SSQXTCREV2'),
            _make_sg('CPG_B', 'AgilentCREv2WES'),  # different lab string, same canonical
        ],
    )
    # No raise expected; validator is silent on success.
    assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]


def test_validator_happy_path_twist(monkeypatch):
    _patch_config(
        monkeypatch,
        _config_factory(
            bed_names={
                'vc_target': 'Twist_VCGS_Exome_Covered_Targets_hg38.bed',
                'cnv_target': 'Twist_VCGS_Exome_Covered_Targets_hg38.bed',
                'sv_call_regions': 'Twist_VCGS_Exome_Covered_Targets_hg38.bed',
            }
        ),
    )
    cohort = _FakeCohort(id='COH0001', sgs=[_make_sg('CPG_A', 'TwistWES1VCGS1')])
    assert_cohort_design_matches_configured_bed(cohort)  # type: ignore[arg-type]
