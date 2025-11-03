"""
Global pytest configuration and fixtures.
"""

from functools import reduce
from unittest import mock

# This is a minimal mock config based on dragen_align_pa_defaults.toml.
# It provides all the keys required by the application at import time,
# plus the keys needed for our unit tests.
MOCK_CONFIG = {
    'workflow': {
        'reads_type': 'fastq',  # A sensible default
    },
    'ica': {
        'pipelines': {
            'dragen_version': 'dragen_3_7_8',
            # Add a mock for the key that config_retrieve(cram) needs
            'cram': 'mock_cram_pipeline_id',
        },
        'cram_references': {
            'old_cram_reference': 'dragmap',
            'dragmap': 'ref_id_dragmap_123',
            'gatk': 'ref_id_gatk_456',
        },
        'mlr': {
            'analysis_instance_tier': 'test-tier',
        },
    },
}


mock_get_config = mock.patch('cpg_utils.config.get_config')
mock_get_config_instance = mock_get_config.start()
mock_get_config_instance.return_value = MOCK_CONFIG

# 2. Mock output_path()
# This is also called in constants.py at import time.
mock_path = mock.MagicMock()
mock_path.__str__ = mock.Mock(return_value='gs://mock-bucket/mock_output_path')
mock_output_path = mock.patch('cpg_utils.config.output_path', return_value=mock_path)
mock_output_path.start()


def pytest_sessionfinish(session):
    """
    A pytest hook that runs at the very end of the test session.
    We use it to stop the module-level mocks we started.
    """
    mock_get_config.stop()
    mock_output_path.stop()


# --- End of Module-Level Patching ---


def _mock_config_retrieve(keys, default=None):
    """
    A helper function that simulates the real config_retrieve
    by traversing the MOCK_CONFIG dictionary.

    NOTE: This function is no longer used by the conftest mock (which
    patches get_config directly), but it is a useful utility that
    could be imported by individual tests if needed.
    """
    try:
        # This traverses the dict: e.g., MOCK_CONFIG['ica']['mlr']['analysis_instance_tier']
        return reduce(lambda d, k: d[k], keys, MOCK_CONFIG)
    except (KeyError, TypeError):
        if default is not None:
            return default
        # Raise a realistic error to help with debugging tests
        raise KeyError(f'Mock config key not found in MOCK_CONFIG: {keys}')
