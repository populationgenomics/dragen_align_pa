"""Verify enable_cyp2d6 is wired through config rather than hardcoded.

The CYP2D6 caller adds non-trivial DRAGEN runtime — cohorts that don't
need pharmacogenomics output (or that have hit unrelated CYP2D6-specific
ICA bugs) need to be able to opt out without forking the source.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dragen_align_pa.jobs import run_align_genotype_with_dragen

if TYPE_CHECKING:
    import pytest


def _install_config(monkeypatch: pytest.MonkeyPatch, overrides: dict[tuple[str, ...], Any]) -> None:
    def fake_config_retrieve(key, default=None):  # noqa: ANN001, ANN202
        return overrides.get(tuple(key), default)

    monkeypatch.setattr(run_align_genotype_with_dragen, 'config_retrieve', fake_config_retrieve)


def _cyp2d6_value(params):  # noqa: ANN001, ANN202
    [param] = [p for p in params if p['code'] == 'enable_cyp2d6']
    return param['value']


def test_enable_cyp2d6_defaults_to_true(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_config(monkeypatch, {})  # no override -> default kicks in

    params = run_align_genotype_with_dragen._build_common_parameters()  # noqa: SLF001

    assert _cyp2d6_value(params) == 'true'


def test_enable_cyp2d6_can_be_disabled_via_config(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_config(monkeypatch, {('workflow', 'enable_cyp2d6'): False})

    params = run_align_genotype_with_dragen._build_common_parameters()  # noqa: SLF001

    assert _cyp2d6_value(params) == 'false'


def test_enable_cyp2d6_respects_explicit_true(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_config(monkeypatch, {('workflow', 'enable_cyp2d6'): True})

    params = run_align_genotype_with_dragen._build_common_parameters()  # noqa: SLF001

    assert _cyp2d6_value(params) == 'true'
