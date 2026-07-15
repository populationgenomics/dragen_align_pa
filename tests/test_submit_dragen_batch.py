import json
from unittest.mock import MagicMock

import pandas as pd
import pytest

from dragen_align_pa import utils
from dragen_align_pa.batches import IcaBatch
from dragen_align_pa.jobs import submit_dragen_batch
from dragen_align_pa.jobs.submit_dragen_batch import _MAX_COVERAGE_REGION_BEDS


def _config_factory(
    sequencing_type='genome',
    preset_args='',
    user_args='',
    bed_names=None,
    vc_target_bed_padding=0,
    enable_cyp2d6=None,
):
    """Returns a fake `config_retrieve` that exposes the bits `_build_additional_args` needs."""
    preset = {
        'cnv_segmentation_mode': 'SLM' if sequencing_type == 'genome' else 'HSLM',
        'additional_args': preset_args,
        'additional_files': [],
        'vc_target_bed_padding': vc_target_bed_padding,
    }
    cfg = {
        ('workflow', 'sequencing_type'): sequencing_type,
        ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', sequencing_type): preset,
        ('dragen_align_pa', 'manage_dragen_pipeline', 'user'): {'additional_args': user_args, 'additional_files': []},
    }
    if bed_names is not None:
        cfg[('dragen_align_pa', 'manage_dragen_pipeline', 'presets', sequencing_type, 'bed_names')] = bed_names
    if enable_cyp2d6 is not None:
        cfg[('dragen_align_pa', 'manage_dragen_pipeline', 'enable_cyp2d6')] = enable_cyp2d6

    def fake_retrieve(key, default=None):
        return cfg.get(tuple(key), default)

    return fake_retrieve


def _patch_config(monkeypatch, fake) -> None:
    """Patch config_retrieve on both modules that look it up.

    _build_additional_args / _build_common_data_inputs read config_retrieve
    from submit_dragen_batch's namespace; the call to get_bed_names_for_seqtype
    they make then reads config_retrieve from utils's namespace. Tests have
    to patch both for the fake config to take effect end-to-end.
    """
    monkeypatch.setattr(submit_dragen_batch, 'config_retrieve', fake)
    monkeypatch.setattr(utils, 'config_retrieve', fake)


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
        '--qc-coverage-reports-1 cov_report',
        '--qc-coverage-reports-2 cov_report',
        '--vc-gvcf-gq-bands 10 20 30 40',
        '--vc-emit-ref-confidence GVCF',
        '--vc-enable-vcf-output false',
        '--repeat-genotype-enable true',
    ):
        assert required_flag in result, f'Missing required hardcoded flag: {required_flag!r}'


def test_build_additional_args_cyp2d6_default_on(monkeypatch):
    """No config entry => caller stays on (DRAGEN's own default is off, so omission would silently disable it)."""
    monkeypatch.setattr(submit_dragen_batch, 'config_retrieve', _config_factory())
    result = submit_dragen_batch._build_additional_args()
    assert '--enable-cyp2d6 true' in result


def test_build_additional_args_cyp2d6_explicit_false(monkeypatch):
    monkeypatch.setattr(
        submit_dragen_batch,
        'config_retrieve',
        _config_factory(enable_cyp2d6=False),
    )
    result = submit_dragen_batch._build_additional_args()
    assert '--enable-cyp2d6 false' in result
    assert '--enable-cyp2d6 true' not in result


def test_build_additional_args_user_appended_last(monkeypatch):
    monkeypatch.setattr(
        submit_dragen_batch,
        'config_retrieve',
        _config_factory(preset_args='--cnv-enable-self-normalization true', user_args='--foo bar'),
    )
    result = submit_dragen_batch._build_additional_args()
    # User args appended *after* preset args.
    assert result.index('--cnv-enable-self-normalization') < result.index('--foo bar')


def test_build_additional_args_omits_vc_padding_when_zero(monkeypatch):
    """vc_target_bed_padding = 0 must NOT emit the flag. Stock-config runs
    pass nothing to DRAGEN's --vc-target-bed-padding (which it would
    treat as 0 anyway, but Alex asked for the flag to be absent)."""
    monkeypatch.setattr(submit_dragen_batch, 'config_retrieve', _config_factory(vc_target_bed_padding=0))
    result = submit_dragen_batch._build_additional_args()
    assert '--vc-target-bed-padding' not in result


def test_build_additional_args_emits_vc_padding_when_nonzero(monkeypatch):
    """A per-cohort override that sets vc_target_bed_padding to N>0 must
    add `--vc-target-bed-padding N` to the args (e.g. Twist runs with 50)."""
    monkeypatch.setattr(submit_dragen_batch, 'config_retrieve', _config_factory(vc_target_bed_padding=50))
    result = submit_dragen_batch._build_additional_args()
    assert '--vc-target-bed-padding 50' in result


def test_build_additional_args_rejects_placeholder(monkeypatch):
    _patch_config(
        monkeypatch,
        _config_factory(
            sequencing_type='exome',
            preset_args='--sv-call-regions-bed <bed-name>',
            # Populate bed_names so get_bed_names_for_seqtype doesn't raise first;
            # we want to reach the legacy `<…>` sentinel check.
            bed_names={
                'vc_target': 'covered.bed',
                'cnv_target': 'regions.bed',
                'sv_call_regions': 'regions.bed',
            },
        ),
    )
    with pytest.raises(ValueError, match='placeholder'):
        submit_dragen_batch._build_additional_args()


def test_build_additional_args_substitutes_bed_name_tokens(monkeypatch):
    _patch_config(
        monkeypatch,
        _config_factory(
            sequencing_type='exome',
            preset_args=(
                '--vc-target-bed {vc_target} --cnv-target-bed {cnv_target} --sv-call-regions-bed {sv_call_regions}'
            ),
            bed_names={
                'vc_target': 'covered.bed',
                'cnv_target': 'regions.bed',
                'sv_call_regions': 'regions.bed',
            },
        ),
    )
    result = submit_dragen_batch._build_additional_args()
    assert '--vc-target-bed covered.bed' in result
    assert '--cnv-target-bed regions.bed' in result
    assert '--sv-call-regions-bed regions.bed' in result
    # No leftover `{...}` tokens.
    assert '{' not in result


def test_build_additional_args_rejects_unknown_token(monkeypatch):
    """An args string with a {...} token that bed_names doesn't define must
    fail fast naming the token and listing the configured entries."""
    _patch_config(
        monkeypatch,
        _config_factory(
            sequencing_type='exome',
            preset_args='--vc-target-bed {unknown_entry}',
            bed_names={
                'vc_target': 'covered.bed',
                'cnv_target': 'regions.bed',
                'sv_call_regions': 'regions.bed',
            },
        ),
    )
    with pytest.raises(ValueError, match=r'\{unknown_entry\}'):
        submit_dragen_batch._build_additional_args()


def test_build_common_data_inputs_adds_bed_names_to_additional_files(monkeypatch):
    """bed_names basenames are added to additional_files (resolved via
    ICA_FILE_IDS) and deduped against preset entries. Twist case: a
    basename used by multiple bed_names entries appears only once."""
    _stub_registry(
        monkeypatch,
        {
            'covered.bed': 'fil.covered',
            'regions.bed': 'fil.regions',
            'pon.bed': 'fil.pon',
        },
    )
    cfg = {
        ('ica', 'pipelines', 'dragen_ht_id'): 'fil.refref',
        ('ica', 'qc', 'exome', 'coverage_region_beds'): [],
        ('ica', 'qc', 'cross_cont_vcf'): None,
        ('workflow', 'sequencing_type'): 'exome',
        # preset.additional_files lists an extra (e.g. PoN) not present in bed_names.
        ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'exome', 'additional_files'): ['pon.bed'],
        ('dragen_align_pa', 'manage_dragen_pipeline', 'user', 'additional_files'): [],
        # Three entries, two distinct basenames -- dedupe should collapse.
        ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'exome', 'bed_names'): {
            'vc_target': 'covered.bed',
            'cnv_target': 'regions.bed',
            'sv_call_regions': 'regions.bed',
        },
    }
    _patch_config(monkeypatch, lambda key, default=None: cfg.get(tuple(key), default))
    inputs = submit_dragen_batch._build_common_data_inputs()
    additional = [i for i in inputs if i['parameterCode'] == 'additional_files']
    assert len(additional) == 1
    # Order: preset first, then bed_names in iteration order; deduped.
    assert list(additional[0]['dataIds']) == ['fil.pon', 'fil.covered', 'fil.regions']


def test_build_common_data_inputs_resolves_user_additional_files(monkeypatch):
    """user.additional_files entries are also basenames that must resolve."""
    _stub_registry(monkeypatch, {'user_extra.bed': 'fil.user_extra'})
    cfg = {
        ('ica', 'pipelines', 'dragen_ht_id'): 'fil.refref',
        ('ica', 'qc', 'genome', 'coverage_region_beds'): [],
        ('ica', 'qc', 'cross_cont_vcf'): None,
        ('workflow', 'sequencing_type'): 'genome',
        ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'genome', 'additional_files'): [],
        ('dragen_align_pa', 'manage_dragen_pipeline', 'user', 'additional_files'): ['user_extra.bed'],
    }
    _patch_config(monkeypatch, lambda key, default=None: cfg.get(tuple(key), default))
    inputs = submit_dragen_batch._build_common_data_inputs()
    additional = [i for i in inputs if i['parameterCode'] == 'additional_files']
    assert len(additional) == 1
    assert list(additional[0]['dataIds']) == ['fil.user_extra']


def test_build_additional_args_does_not_flag_common_args_with_future_lt_tokens(monkeypatch):
    """If _COMMON_ADDITIONAL_ARGS ever grows a `<token>` substring (e.g. a
    DRAGEN tag name after a `<`), it must NOT be flagged as an unfilled
    preset placeholder — placeholders are a property of preset/user args,
    not the hardcoded common block. Simulate by monkey-patching
    _COMMON_ADDITIONAL_ARGS with a future-compatible value."""
    monkeypatch.setattr(
        submit_dragen_batch,
        '_COMMON_ADDITIONAL_ARGS',
        submit_dragen_batch._COMMON_ADDITIONAL_ARGS + ' --some-future-flag <somerule>',
    )
    monkeypatch.setattr(submit_dragen_batch, 'config_retrieve', _config_factory())
    # Should succeed: the `<somerule>` came from the common block, not a preset.
    submit_dragen_batch._build_additional_args()


def test_build_additional_args_rejects_missing_preset(monkeypatch):
    monkeypatch.setattr(
        submit_dragen_batch,
        'config_retrieve',
        lambda key, default=None: 'genome' if key == ['workflow', 'sequencing_type'] else default,
    )
    with pytest.raises(ValueError, match=r'Missing config section'):
        submit_dragen_batch._build_additional_args()


def test_build_additional_args_rejects_invalid_sequencing_type(monkeypatch):
    monkeypatch.setattr(submit_dragen_batch, 'config_retrieve', lambda key, default=None: 'transcriptome')
    with pytest.raises(ValueError, match='must be'):
        submit_dragen_batch._build_additional_args()


def test_build_additional_args_rejects_preset_missing_cnv_segmentation_mode(monkeypatch):
    """A hand-edited preset missing `cnv_segmentation_mode` must produce a
    friendly ValueError naming the missing key, not a bare KeyError —
    matches the error style for missing-sequencing_type / missing-preset."""
    cfg = {
        ('workflow', 'sequencing_type'): 'genome',
        ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'genome'): {
            # cnv_segmentation_mode missing on purpose
            'additional_args': '',
            'additional_files': [],
        },
        ('dragen_align_pa', 'manage_dragen_pipeline', 'user'): {'additional_args': '', 'additional_files': []},
    }
    monkeypatch.setattr(
        submit_dragen_batch,
        'config_retrieve',
        lambda key, default=None: cfg.get(tuple(key), default),
    )
    with pytest.raises(ValueError, match='cnv_segmentation_mode'):
        submit_dragen_batch._build_additional_args()


def test_run_rejects_no_input_mode_before_any_io():
    """Calling run() with both CRAM and FASTQ paths None must raise BEFORE
    any GCS read. We verify by passing an analysis_output_fid_path that
    would raise on open() — if validation runs first, the ValueError is
    about input mode, not about the unreadable path."""

    class _BoomPath:
        def open(self, _mode='r'):
            raise AssertionError('analysis_output_fid_path was opened before input-mode validation')

    batch = IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
    with pytest.raises(ValueError, match='no valid input mode'):
        submit_dragen_batch.run(
            batch=batch,
            analysis_output_fid_path=_BoomPath(),  # type: ignore[arg-type]
            cram_state_paths=None,
            fastq_ids_path=None,
            per_sg_fastq_list_paths=None,
        )


def test_run_rejects_invalid_error_strategy_before_any_io():
    """A typo in error_strategy (e.g. 'CONTINUE', 'continue ', 'auto-retry') must
    raise BEFORE any GCS / secrets IO, so orchestrator-level misuse surfaces
    cheaply rather than as an obscure ICA pipeline-parameter rejection."""

    class _BoomPath:
        def open(self, _mode='r'):
            raise AssertionError('analysis_output_fid_path was opened before error_strategy validation')

    batch = IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
    with pytest.raises(ValueError, match='error_strategy'):
        submit_dragen_batch.run(
            batch=batch,
            analysis_output_fid_path=_BoomPath(),  # type: ignore[arg-type]
            cram_state_paths={'CPG_A': _BoomPath()},  # type: ignore[dict-item]
            fastq_ids_path=None,
            per_sg_fastq_list_paths=None,
            error_strategy='continue ',  # trailing whitespace
        )


def test_run_rejects_mixed_cram_and_fastq_inputs():
    """Passing both modes' paths is programmer error; the current code
    silently runs CRAM mode and ignores FASTQ args. Must raise instead."""

    class _BoomPath:
        def open(self, _mode='r'):
            raise AssertionError('analysis_output_fid_path was opened before mixed-input rejection')

    batch = IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
    with pytest.raises(ValueError, match='exactly one of'):
        submit_dragen_batch.run(
            batch=batch,
            analysis_output_fid_path=_BoomPath(),  # type: ignore[arg-type]
            cram_state_paths={'CPG_A': _BoomPath()},  # type: ignore[dict-item]
            fastq_ids_path=_BoomPath(),  # type: ignore[arg-type]
            per_sg_fastq_list_paths={'CPG_A': _BoomPath()},  # type: ignore[dict-item]
        )


def _stub_registry(monkeypatch, mapping: dict[str, str]) -> None:
    """Replace constants.ICA_FILE_IDS for the duration of one test so we don't
    couple the test to the production registry's current contents.

    resolve_ica_file_id() reads ICA_FILE_IDS from its own module at call time,
    so patching the name on `dragen_align_pa.constants` is enough — no matter
    how submit_dragen_batch imports the resolver."""
    monkeypatch.setattr('dragen_align_pa.constants.ICA_FILE_IDS', mapping)


def test_build_common_data_inputs_rejects_too_many_coverage_beds(monkeypatch):
    """Design spec §3 caps qc_coverage_region_beds at 3 entries. A
    misconfigured TOML with 4+ would silently send them all to ICA.
    Mock just enough config_retrieve to drive the validation path."""
    bed_names = [f'bed{i}.bed' for i in range(_MAX_COVERAGE_REGION_BEDS + 1)]
    _stub_registry(monkeypatch, {name: f'fil.{i:07d}' for i, name in enumerate(bed_names)})
    cfg = {
        ('ica', 'pipelines', 'dragen_ht_id'): 'fil.refref',
        ('ica', 'qc', 'genome', 'coverage_region_beds'): bed_names,
        ('ica', 'qc', 'cross_cont_vcf'): None,
        ('workflow', 'sequencing_type'): 'genome',
        ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'genome', 'additional_files'): [],
        ('dragen_align_pa', 'manage_dragen_pipeline', 'user', 'additional_files'): [],
    }
    monkeypatch.setattr(
        submit_dragen_batch,
        'config_retrieve',
        lambda key, default=None: cfg.get(tuple(key), default),
    )
    with pytest.raises(ValueError, match='coverage_region_beds'):
        submit_dragen_batch._build_common_data_inputs()


def test_build_common_data_inputs_accepts_max_coverage_beds(monkeypatch):
    """3 entries is the documented maximum — must not raise. Basenames in
    config are resolved to ICA file IDs via constants.ICA_FILE_IDS before
    being passed to ICA."""
    bed_names = [f'bed{i}.bed' for i in range(_MAX_COVERAGE_REGION_BEDS)]
    registry = {name: f'fil.{i:07d}' for i, name in enumerate(bed_names)}
    _stub_registry(monkeypatch, registry)
    cfg = {
        ('ica', 'pipelines', 'dragen_ht_id'): 'fil.refref',
        ('ica', 'qc', 'genome', 'coverage_region_beds'): bed_names,
        ('ica', 'qc', 'cross_cont_vcf'): None,
        ('workflow', 'sequencing_type'): 'genome',
        ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'genome', 'additional_files'): [],
        ('dragen_align_pa', 'manage_dragen_pipeline', 'user', 'additional_files'): [],
    }
    monkeypatch.setattr(
        submit_dragen_batch,
        'config_retrieve',
        lambda key, default=None: cfg.get(tuple(key), default),
    )
    inputs = submit_dragen_batch._build_common_data_inputs()
    coverage_inputs = [i for i in inputs if i['parameterCode'] == 'qc_coverage_region_beds']
    assert len(coverage_inputs) == 1
    # dataIds must be the resolved ICA file IDs, not the basenames from config.
    assert list(coverage_inputs[0]['dataIds']) == [registry[n] for n in bed_names]


def test_build_common_data_inputs_rejects_unregistered_bed_name(monkeypatch):
    """A typo or unstaged BED in coverage_region_beds must fail fast at
    submitter startup, not surface mid-run as an opaque ICA error."""
    _stub_registry(monkeypatch, {'registered.bed': 'fil.0000001'})
    cfg = {
        ('ica', 'pipelines', 'dragen_ht_id'): 'fil.refref',
        ('ica', 'qc', 'genome', 'coverage_region_beds'): ['registered.bed', 'typo_or_missing.bed'],
        ('ica', 'qc', 'cross_cont_vcf'): None,
        ('workflow', 'sequencing_type'): 'genome',
        ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'genome', 'additional_files'): [],
        ('dragen_align_pa', 'manage_dragen_pipeline', 'user', 'additional_files'): [],
    }
    monkeypatch.setattr(
        submit_dragen_batch,
        'config_retrieve',
        lambda key, default=None: cfg.get(tuple(key), default),
    )
    with pytest.raises(KeyError, match=r'typo_or_missing\.bed'):
        submit_dragen_batch._build_common_data_inputs()


def test_build_common_data_inputs_resolves_cross_cont_vcf_basename(monkeypatch):
    """cross_cont_vcf is a basename resolved to an ICA file ID via ICA_FILE_IDS;
    the qc_cross_cont_vcf data input must carry the resolved fil.… ID, not the
    basename from config."""
    _stub_registry(monkeypatch, {'SNP_NCBI_GRCh38.vcf': 'fil.crosscont'})
    cfg = {
        ('ica', 'pipelines', 'dragen_ht_id'): 'fil.refref',
        ('ica', 'qc', 'genome', 'coverage_region_beds'): [],
        ('ica', 'qc', 'cross_cont_vcf'): 'SNP_NCBI_GRCh38.vcf',
        ('workflow', 'sequencing_type'): 'genome',
        ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'genome', 'additional_files'): [],
        ('dragen_align_pa', 'manage_dragen_pipeline', 'user', 'additional_files'): [],
    }
    monkeypatch.setattr(
        submit_dragen_batch,
        'config_retrieve',
        lambda key, default=None: cfg.get(tuple(key), default),
    )
    inputs = submit_dragen_batch._build_common_data_inputs()
    cross_cont = [i for i in inputs if i['parameterCode'] == 'qc_cross_cont_vcf']
    assert len(cross_cont) == 1
    assert list(cross_cont[0]['dataIds']) == ['fil.crosscont']


def test_build_common_data_inputs_rejects_unregistered_cross_cont_vcf(monkeypatch):
    """A typo or unstaged cross_cont_vcf must fail fast at submitter startup."""
    _stub_registry(monkeypatch, {'SNP_NCBI_GRCh38.vcf': 'fil.crosscont'})
    cfg = {
        ('ica', 'pipelines', 'dragen_ht_id'): 'fil.refref',
        ('ica', 'qc', 'genome', 'coverage_region_beds'): [],
        ('ica', 'qc', 'cross_cont_vcf'): 'typo.vcf',
        ('workflow', 'sequencing_type'): 'genome',
        ('dragen_align_pa', 'manage_dragen_pipeline', 'presets', 'genome', 'additional_files'): [],
        ('dragen_align_pa', 'manage_dragen_pipeline', 'user', 'additional_files'): [],
    }
    monkeypatch.setattr(
        submit_dragen_batch,
        'config_retrieve',
        lambda key, default=None: cfg.get(tuple(key), default),
    )
    with pytest.raises(KeyError, match=r'typo\.vcf'):
        submit_dragen_batch._build_common_data_inputs()


def _make_fastq_ids_path(content: dict[str, str], tmp_path):
    p = tmp_path / 'COH0001_fastq_ids.txt'
    json.dump(content, p.open('w'))
    return p


def _make_per_sg_fastq_csv(tmp_path, sg_name: str, read1: list[str], read2: list[str]):
    p = tmp_path / f'{sg_name}_fastq_list.csv'
    df = pd.DataFrame(
        {
            'RGID': [f'rg{i}' for i in range(len(read1))],
            'RGSM': [sg_name] * len(read1),
            'RGLB': ['lib1'] * len(read1),
            'Lane': list(range(1, len(read1) + 1)),
            'Read1File': read1,
            'Read2File': read2,
        }
    )
    df.to_csv(p, index=False)
    return p


def test_load_per_sg_fastq_lists_rejects_filename_collision(tmp_path):
    """Two different SGs referencing the same FASTQ filename is a programmer
    error: same physical file shouldn't be R1 for two distinct samples.
    Silently dedup-via-set would pass the count-mismatch check downstream
    while DRAGEN sees a longer fastq_list CSV than the dataIds it was given —
    a hidden corruption mode. Detect explicitly and raise."""
    per_sg_csv_a = _make_per_sg_fastq_csv(
        tmp_path,
        'CPG_A',
        read1=['shared_R1.fastq.gz'],
        read2=['CPG_A_R2.fastq.gz'],
    )
    per_sg_csv_b = _make_per_sg_fastq_csv(
        tmp_path,
        'CPG_B',
        read1=['shared_R1.fastq.gz'],
        read2=['CPG_B_R2.fastq.gz'],
    )
    with pytest.raises(ValueError, match=r'shared_R1\.fastq\.gz'):
        submit_dragen_batch._load_per_sg_fastq_lists(
            sg_names=['CPG_A', 'CPG_B'],
            per_sg_fastq_list_paths={'CPG_A': per_sg_csv_a, 'CPG_B': per_sg_csv_b},
        )


def test_build_fastq_data_inputs_handles_duplicate_fastq_rows(tmp_path, monkeypatch):
    """A re-uploaded FASTQ may appear in {cohort}_fastq_ids.txt twice (old
    + new ICA file IDs). The submitter must deterministically pick the
    most recent (last) ID and not silently send both — and must not
    spuriously fail the count check."""
    fastq_ids_path = _make_fastq_ids_path(
        {
            'fil.OLD_R1': 'CPG_A_R1.fastq.gz',
            'fil.OLD_R2': 'CPG_A_R2.fastq.gz',
            'fil.NEW_R1': 'CPG_A_R1.fastq.gz',  # re-upload duplicate
            'fil.NEW_R2': 'CPG_A_R2.fastq.gz',
        },  # re-upload duplicate
        tmp_path,
    )
    per_sg_csv = _make_per_sg_fastq_csv(
        tmp_path,
        'CPG_A',
        read1=['CPG_A_R1.fastq.gz'],
        read2=['CPG_A_R2.fastq.gz'],
    )
    # Stub the upload helper — duplicate detection happens before upload, so the ICA
    # folder path (which reads BUCKET_NAME / output_folder) is never built here.
    monkeypatch.setattr(
        submit_dragen_batch,
        '_upload_per_batch_fastq_list',
        lambda **kwargs: 'fil.uploaded_csv',
    )

    batch = IcaBatch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
    data_inputs, _fastq_list_fid = submit_dragen_batch._build_fastq_data_inputs(
        api_instance=MagicMock(),
        project_id='proj',
        batch=batch,
        fastq_ids_path=fastq_ids_path,
        per_sg_fastq_list_paths={'CPG_A': per_sg_csv},
    )
    fastq_input = next(di for di in data_inputs if di['parameterCode'] == 'fastqs')
    ica_ids = list(fastq_input['dataIds'])
    # Most-recent-wins: NEW IDs preserved, OLD discarded.
    assert sorted(ica_ids) == ['fil.NEW_R1', 'fil.NEW_R2']
    assert len(set(ica_ids)) == len(ica_ids), 'duplicate ICA IDs leaked into dataIds'
