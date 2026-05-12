import pytest

from dragen_align_pa.jobs import submit_dragen_batch


def _config_factory(sequencing_type='genome', preset_args='', user_args=''):
    """Returns a fake `config_retrieve` that exposes the bits `_build_additional_args` needs."""
    cfg = {
        ('workflow', 'sequencing_type'): sequencing_type,
        ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', sequencing_type): {
            'cnv_segmentation_mode': 'SLM' if sequencing_type == 'genome' else 'HSLM',
            'additional_args': preset_args,
            'additional_files': [],
        },
        ('dragen_align_pa', 'manage_dragen_pipeline', 'user'): {'additional_args': user_args, 'additional_files': []},
    }

    def fake_retrieve(key, default=None):
        return cfg.get(tuple(key), default)

    return fake_retrieve


def test_build_additional_args_genome(monkeypatch):
    monkeypatch.setattr(submit_dragen_batch, 'config_retrieve', _config_factory())
    result = submit_dragen_batch._build_additional_args()
    assert '--cnv-segmentation-mode SLM' in result


def test_build_additional_args_includes_hardcoded_common(monkeypatch):
    """Regression guard: every common DRAGEN flag from spec §2 must appear
    in the assembled output. A future refactor that drops one of these would
    silently produce wrong CRAMs/gVCFs."""
    monkeypatch.setattr(submit_dragen_batch, 'config_retrieve', _config_factory())
    result = submit_dragen_batch._build_additional_args()
    for required_flag in (
        '--read-trimmers polyg',
        '--soft-read-trimmers none',
        '--vc-frd-max-effective-depth 40',
        '--vc-enable-joint-detection true',
        '--qc-coverage-ignore-overlaps true',
        '--qc-coverage-count-soft-clipped-bases true',
        '--qc-coverage-reports-1 cov_report,cov_report',
        '--vc-gvcf-gq-bands 10 20 30 40',
        '--vc-emit-ref-confidence GVCF',
        '--vc-enable-vcf-output false',
        '--enable-map-align-output true',
        '--enable-duplicate-marking true',
        '--enable-cyp2d6 true',
        '--repeat-genotype-enable true',
    ):
        assert required_flag in result, f'Missing required hardcoded flag: {required_flag!r}'


def test_build_additional_args_user_appended_last(monkeypatch):
    monkeypatch.setattr(
        submit_dragen_batch, 'config_retrieve',
        _config_factory(preset_args='--cnv-enable-self-normalization true', user_args='--foo bar'),
    )
    result = submit_dragen_batch._build_additional_args()
    # User args appended *after* preset args.
    assert result.index('--cnv-enable-self-normalization') < result.index('--foo bar')


def test_build_additional_args_rejects_placeholder(monkeypatch):
    monkeypatch.setattr(
        submit_dragen_batch, 'config_retrieve',
        _config_factory(sequencing_type='exome', preset_args='--sv-call-regions-bed <bed-name>'),
    )
    with pytest.raises(ValueError, match='placeholder'):
        submit_dragen_batch._build_additional_args()


def test_build_additional_args_rejects_missing_preset(monkeypatch):
    monkeypatch.setattr(
        submit_dragen_batch, 'config_retrieve',
        lambda key, default=None: 'genome' if key == ['workflow', 'sequencing_type'] else default,
    )
    with pytest.raises(ValueError, match=r'Missing config section'):
        submit_dragen_batch._build_additional_args()


def test_build_additional_args_rejects_invalid_sequencing_type(monkeypatch):
    monkeypatch.setattr(submit_dragen_batch, 'config_retrieve', lambda key, default=None: 'transcriptome')
    with pytest.raises(ValueError, match='must be'):
        submit_dragen_batch._build_additional_args()
