"""Unit tests for the StatusProvider Protocol and ParallelPerIdStatusProvider.

The provider runs the per-cycle ICA fan-out for `manage_ica_pipeline_loop`.
These tests stub `check_ica_pipeline_status` (mocking the SDK is covered
in test_ica_api_utils.py) so the focus stays on:
  - map population on success
  - partial-failure → 'UNKNOWN'
  - cycle wall-clock timeout
  - executor lifecycle
  - log-policy thresholds
"""

import logging
from unittest.mock import MagicMock, patch

import pytest

from dragen_align_pa.jobs.ica_status_provider import (
    ParallelPerIdStatusProvider,
    StatusProvider,
)


def test_status_provider_protocol_surface():
    """The Protocol is the contract the polling loop depends on; assert
    it exposes refresh and get_status. ParallelPerIdStatusProvider is the
    concrete implementation."""
    assert hasattr(StatusProvider, 'refresh')
    assert hasattr(StatusProvider, 'get_status')


def test_parallel_per_id_status_provider_returns_unknown_before_first_refresh():
    """Before refresh has ever been called, every id reads as 'UNKNOWN' —
    the loop's elif chain falls through, no transition that cycle."""
    with ParallelPerIdStatusProvider(concurrency=2, refresh_timeout_seconds=10) as provider:
        assert provider.get_status('any-id') == 'UNKNOWN'


def test_refresh_populates_map_with_per_id_statuses():
    """Happy path: every in-flight id has its status fetched and stored."""
    statuses = {'a': 'INPROGRESS', 'b': 'SUCCEEDED', 'c': 'FAILED'}

    def fake_check(api_instance, path_params):
        return statuses[path_params['analysisId']]

    with patch.object(
        ica_api_utils := __import__('dragen_align_pa.ica_api_utils', fromlist=['_']),
        'check_ica_pipeline_status',
        side_effect=fake_check,
    ), patch.object(
        ica_api_utils,
        'get_ica_api_client',
        return_value=MagicMock(__enter__=MagicMock(return_value=MagicMock()), __exit__=MagicMock(return_value=None)),
    ), patch.object(
        ica_api_utils,
        'get_ica_secrets',
        return_value={'projectID': 'proj', 'apiKey': 'k'},
    ):
        with ParallelPerIdStatusProvider(concurrency=4, refresh_timeout_seconds=10) as provider:
            provider.refresh({'a', 'b', 'c'})

            assert provider.get_status('a') == 'INPROGRESS'
            assert provider.get_status('b') == 'SUCCEEDED'
            assert provider.get_status('c') == 'FAILED'
