"""
Global pytest configuration and fixtures.
"""

from functools import reduce
from unittest import mock

import pytest

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


def _mock_config_retrieve(keys, default=None):
    """
    A helper function that simulates the real config_retrieve
    by traversing the MOCK_CONFIG dictionary.
    """
    try:
        # This traverses the dict: e.g., MOCK_CONFIG['ica']['mlr']['analysis_instance_tier']
        return reduce(lambda d, k: d[k], keys, MOCK_CONFIG)
    except (KeyError, TypeError):
        if default is not None:
            return default
        # Raise a realistic error to help with debugging tests
        raise KeyError(f'Mock config key not found in MOCK_CONFIG: {keys}')


@pytest.fixture(autouse=True)
def mock_cpg_utils_config():
    """
    Mocks cpg_utils.config functions that are called at import time.

    This fixture runs automatically for every test, ensuring that when
    modules like 'constants.py' are imported, they don't fail by trying
    to read a real config file.
    """

    with mock.patch('cpg_utils.config.get_config') as mock_get_config:
        # Set the return_value to our mock config.
        # Now, any call to get_config() will just return our dict.
        mock_get_config.return_value = MOCK_CONFIG

        # We also need to patch output_path, which is used in constants.py
        # It needs to return a mock object that can be converted to a string.
        mock_path = mock.MagicMock()
        # This is the correct way to mock a magic method like __str__
        # to satisfy Pylance.
        mock_path.__str__ = mock.Mock(return_value='gs://mock-bucket/mock_output_path')

        with mock.patch('cpg_utils.config.output_path', return_value=mock_path):
            # 'yield' allows the tests to run with these mocks active
            yield
