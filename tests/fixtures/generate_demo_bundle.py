"""Generate a synthetic ICA analysis-output folder matching the layout that
DRAGEN378-custom-unified-F2-v1 produces. Files are empty stubs; only the
tiny JSON manifests at the batch root carry real content. Use as a unit-test
fixture. Do NOT commit the generated output.

Can be imported (`generate_demo_bundle(...)`) or run as a script.
"""

import argparse
import json
from pathlib import Path

# Defaults match the production user_reference convention from Task 13:
# `f'{batch.name}_{ar_guid}_'` — ends with `_`. The analysis-folder path
# below uses `{user_reference}-{pipeline_id}` to match `get_ica_sample_folder`
# in utils.py (production), so the bundle layout is a faithful fixture.
DEFAULT_USER_REFERENCE = 'COH0001-batch0000_test-guid_'
DEFAULT_PIPELINE_ID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
DEFAULT_SAMPLES = ('SYN00001', 'SYN00002')

PER_SAMPLE_FILES = (
    '{sample}.cram',
    '{sample}.cram.crai',
    '{sample}.cram.md5sum',
    '{sample}.hard-filtered.gvcf.gz',
    '{sample}.hard-filtered.gvcf.gz.tbi',
    '{sample}.hard-filtered.gvcf.gz.md5sum',
    '{sample}.sv.vcf.gz',
    '{sample}.sv.vcf.gz.tbi',
    '{sample}.cnv_metrics.csv',
    '{sample}.sv_metrics.csv',
    '{sample}.mapping_metrics.csv',
    '{sample}.vc_metrics.csv',
    '{sample}.wgs_coverage_metrics.csv',
    '{sample}.target_bed_coverage_metrics.csv',
    '{sample}.fragment_length_hist.csv',
    '{sample}-replay.json',
)


def generate_demo_bundle(
    output_root: Path,
    samples: tuple[str, ...] = DEFAULT_SAMPLES,
    user_reference: str = DEFAULT_USER_REFERENCE,
    pipeline_id: str = DEFAULT_PIPELINE_ID,
    failed_samples: tuple[str, ...] = (),
) -> Path:
    """Materialise the synthetic bundle. Returns the analysis directory path."""
    failed_set = set(failed_samples)
    # Production convention (matches `get_ica_sample_folder` in utils.py):
    # `{user_reference}-{pipeline_id}`. Because `user_reference` ends with `_`,
    # the resulting folder name is `…_-{pipeline_id}/`.
    analysis_dir = output_root / 'analysis' / f'{user_reference}-{pipeline_id}'

    (analysis_dir / 'reports' / 'report_files' / 'samples').mkdir(parents=True, exist_ok=True)
    (analysis_dir / 'ica_logs' / 'analysis').mkdir(parents=True, exist_ok=True)
    (analysis_dir / 'ica_logs' / 'work').mkdir(parents=True, exist_ok=True)

    (analysis_dir / '_tags.json').write_text(
        json.dumps({'system.iap.timestamp': '2026-05-11T00:00:00Z', 'system.iap.tes': ''}),
    )

    passfail = {s: ('Fail' if s in failed_set else 'Success') for s in samples}
    (analysis_dir / 'passfail.json').write_text(json.dumps(passfail, indent=4))

    (analysis_dir / 'summary.json').write_text(
        json.dumps(
            {
                'num_samples_total': len(samples),
                'num_samples_completed': len(samples) - len(failed_set),
                'num_samples_failed': len(failed_set),
            },
            indent=4,
        ),
    )

    for sample in samples:
        sample_dir = analysis_dir / sample
        (sample_dir / 'logs').mkdir(parents=True, exist_ok=True)
        (sample_dir / 'sv' / 'workspace').mkdir(parents=True, exist_ok=True)
        (sample_dir / 'sv' / 'results').mkdir(parents=True, exist_ok=True)
        for pattern in PER_SAMPLE_FILES:
            (sample_dir / pattern.format(sample=sample)).touch()

    return analysis_dir


def _cli() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output_root', nargs='?', default='./tests/fixtures/ica-demo-bundle')
    parser.add_argument('--samples', nargs='+', default=list(DEFAULT_SAMPLES))
    parser.add_argument('--failed', nargs='+', default=[])
    parser.add_argument('--user-reference', default=DEFAULT_USER_REFERENCE)
    parser.add_argument('--pipeline-id', default=DEFAULT_PIPELINE_ID)
    args = parser.parse_args()

    path = generate_demo_bundle(
        output_root=Path(args.output_root),
        samples=tuple(args.samples),
        user_reference=args.user_reference,
        pipeline_id=args.pipeline_id,
        failed_samples=tuple(args.failed),
    )
    print(f'Generated: {path}')


if __name__ == '__main__':
    _cli()
