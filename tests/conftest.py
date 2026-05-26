"""Test config bootstrap.

`dragen_align_pa.constants` reads `cpg_utils.config` at module-import time
(`config_retrieve` for `[workflow][reads_type]` and
`[ica.pipelines][dragen_version]`, plus `output_path` for the bucket root).
To keep unit tests off GCS and free of a real cpg-utils config bundle, we
install in-memory stand-ins on `cpg_utils.config` BEFORE any
`dragen_align_pa.*` import happens — so when `constants.py` is loaded later
it sees the stubs and its module-level reads succeed locally.
"""

import cpg_utils.config

_ORIGINAL_CONFIG_RETRIEVE = cpg_utils.config.config_retrieve
_ORIGINAL_OUTPUT_PATH = cpg_utils.config.output_path


_TEST_CONFIG: dict[tuple, object] = {
    ('workflow', 'reads_type'): 'cram',
    ('ica', 'pipelines', 'dragen_version'): 'dragen_3_7_8',
}


def _test_config_retrieve(key, default=None):  # noqa: ANN001, ANN202
    return _TEST_CONFIG.get(tuple(key), default)


def _test_output_path(suffix: str = '', category: str | None = None) -> str:
    del category
    return f'gs://cpg-test-dataset-test/{suffix}'


cpg_utils.config.config_retrieve = _test_config_retrieve  # type: ignore[assignment]
cpg_utils.config.output_path = _test_output_path  # type: ignore[assignment]


def pytest_sessionfinish(session, exitstatus):  # noqa: ARG001
    cpg_utils.config.config_retrieve = _ORIGINAL_CONFIG_RETRIEVE  # type: ignore[assignment]
    cpg_utils.config.output_path = _ORIGINAL_OUTPUT_PATH  # type: ignore[assignment]
