"""Shared test builders for fake `config_retrieve` callables.

Import-safe: defines closures only, so importing this module triggers no
`config_retrieve` read and does not fight conftest's import-time config
monkeypatch.
"""


def _config_factory(sequencing_type='exome', bed_names=None):
    cfg: dict[tuple[str, ...], object] = {('workflow', 'sequencing_type'): sequencing_type}
    if bed_names is not None:
        cfg[('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'exome', 'bed_names')] = bed_names

    def fake_retrieve(key, default=None):
        return cfg.get(tuple(key), default)

    return fake_retrieve
