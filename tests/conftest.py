"""Test config bootstrap.

`dragen_align_pa.constants` reads `cpg_utils.config` at module-import time
(`config_retrieve` for `[workflow][reads_type]` and `[ica.pipelines][dragen_version]`,
plus `output_path` for the bucket root). To keep unit tests off GCS and free of a
real cpg-utils config bundle, we install in-memory stand-ins on `cpg_utils.config`
BEFORE any `dragen_align_pa.*` import happens — so when `constants.py` is loaded
later it sees the stubs and its module-level reads succeed locally.

Per-test overrides should monkeypatch `dragen_align_pa.utils.config_retrieve`
(the name bound inside utils.py at its own import time) — not these stand-ins.
"""

import cpg_utils.config

_TEST_CONFIG: dict[tuple, object] = {
    ('workflow', 'reads_type'): 'cram',
    # Match the production TOML value verbatim so any future test that doesn't
    # explicitly monkeypatch DRAGEN_VERSION still gets a path-shape match.
    ('ica', 'pipelines', 'dragen_version'): 'dragen_3_7_8',
}


def _test_config_retrieve(key, default=None):
    return _TEST_CONFIG.get(tuple(key), default)


def _test_output_path(suffix: str = '') -> str:
    return f'gs://cpg-test-dataset-test/{suffix}'


cpg_utils.config.config_retrieve = _test_config_retrieve
cpg_utils.config.output_path = _test_output_path


from pathlib import Path  # noqa: E402

import pytest  # noqa: E402

from tests.fixtures.generate_demo_bundle import generate_demo_bundle  # noqa: E402

DEMO_USER_REFERENCE = 'COH0001-batch0000_test-guid_'
DEMO_PIPELINE_ID = '00000000-1111-2222-3333-444444444444'
DEMO_SAMPLES = ('CPG00001', 'CPG00002')


@pytest.fixture
def demo_bundle(tmp_path: Path) -> Path:
    """Materialise a synthetic ICA analysis output bundle under tmp_path."""
    return generate_demo_bundle(
        output_root=tmp_path,
        samples=DEMO_SAMPLES,
        user_reference=DEMO_USER_REFERENCE,
        pipeline_id=DEMO_PIPELINE_ID,
    )


@pytest.fixture
def demo_bundle_with_failure(tmp_path: Path) -> Path:
    """Materialise a synthetic bundle where CPG00002 is marked Fail."""
    return generate_demo_bundle(
        output_root=tmp_path,
        samples=DEMO_SAMPLES,
        user_reference=DEMO_USER_REFERENCE,
        pipeline_id=DEMO_PIPELINE_ID,
        failed_samples=('CPG00002',),
    )
