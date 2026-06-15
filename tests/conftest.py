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

# Originals captured so pytest_sessionfinish can restore them at the end of
# the run. We MUST monkeypatch at module-import time (not in a session-scoped
# autouse fixture) because dragen_align_pa.constants reads cpg_utils.config
# at its own import time, which happens during pytest collection before any
# fixture can run. The trade-off is that the patch is not auto-bracketed by
# pytest's fixture machinery — pytest_sessionfinish below provides explicit
# bracketing instead.
_ORIGINAL_CONFIG_RETRIEVE = cpg_utils.config.config_retrieve
_ORIGINAL_OUTPUT_PATH = cpg_utils.config.output_path


_TEST_CONFIG: dict[tuple, object] = {
    ('workflow', 'reads_type'): 'cram',
    # Match the production TOML value verbatim so any future test that doesn't
    # explicitly monkeypatch DRAGEN_VERSION still gets a path-shape match.
    ('ica', 'pipelines', 'dragen_version'): 'dragen_3_7_8',
}


def _test_config_retrieve(key, default=None):
    return _TEST_CONFIG.get(tuple(key), default)


def _test_output_path(suffix: str = '', category: str | None = None) -> str:
    # `category` mirrors the real `cpg_utils.config.output_path` signature
    # (which selects between test/main/analysis buckets). For tests we don't
    # care about category-driven bucket selection; the stub returns a stable
    # path so callers can build deterministic assertions on the suffix.
    del category
    return f'gs://cpg-test-dataset-test/{suffix}'


# Monkey-patching a module attribute with a narrower signature; mypy's
# concern here doesn't apply at runtime because every test caller invokes
# config_retrieve / output_path through the same two-arg / one-arg shape
# that the stubs cover. Ignore the assignment-type check.
cpg_utils.config.config_retrieve = _test_config_retrieve  # type: ignore[assignment]
cpg_utils.config.output_path = _test_output_path  # type: ignore[assignment]


from pathlib import Path  # noqa: E402

import pytest  # noqa: E402

from tests.fixtures.generate_demo_bundle import generate_demo_bundle  # noqa: E402

DEMO_COHORT_NAME = 'COH0001'
DEMO_USER_REFERENCE = 'COH0001-batch0000_test-guid_'
DEMO_PIPELINE_ID = '00000000-1111-2222-3333-444444444444'
DEMO_SAMPLES = ('SYN00001', 'SYN00002')


@pytest.fixture
def demo_bundle(tmp_path: Path) -> Path:
    """Materialise a synthetic ICA analysis output bundle under tmp_path."""
    return generate_demo_bundle(
        output_root=tmp_path,
        samples=DEMO_SAMPLES,
        user_reference=DEMO_USER_REFERENCE,
        pipeline_id=DEMO_PIPELINE_ID,
        cohort_name=DEMO_COHORT_NAME,
    )


@pytest.fixture
def demo_bundle_with_failure(tmp_path: Path) -> Path:
    """Materialise a synthetic bundle where SYN00002 is marked Fail."""
    return generate_demo_bundle(
        output_root=tmp_path,
        samples=DEMO_SAMPLES,
        user_reference=DEMO_USER_REFERENCE,
        pipeline_id=DEMO_PIPELINE_ID,
        failed_samples=('SYN00002',),
        cohort_name=DEMO_COHORT_NAME,
    )


def pytest_sessionfinish(session, exitstatus):  # noqa: ARG001
    """Restore the originals captured at module import.

    This is cosmetic for a one-shot test run (the process exits anyway), but
    makes the monkeypatch boundary explicit and discoverable: anyone tracing
    `cpg_utils.config.config_retrieve` can find both the install and the
    teardown in this file.
    """
    cpg_utils.config.config_retrieve = _ORIGINAL_CONFIG_RETRIEVE  # type: ignore[assignment]
    cpg_utils.config.output_path = _ORIGINAL_OUTPUT_PATH  # type: ignore[assignment]
