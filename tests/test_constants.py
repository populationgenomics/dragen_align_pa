"""Tests for the ICA_FILE_IDS registry and resolver in constants.py.

The registry maps human-readable BED basenames to ICA file IDs so that
config (`coverage_region_beds`, preset additional_args, etc.) can be
written in terms the user understands, and the submitter does one
indirection to recover the ICA ID for `additional_files`.
"""

import pytest

from dragen_align_pa import constants


def test_resolve_ica_file_id_returns_registered_id():
    """Known basename → its registered ICA file ID."""
    name, expected_id = next(iter(constants.ICA_FILE_IDS.items()))
    assert constants.resolve_ica_file_id(name) == expected_id


def test_resolve_ica_file_id_raises_on_unknown_name():
    """Unknown basename → clear error naming the unknown entry."""
    unknown = 'this_bed_is_not_registered.bed'
    with pytest.raises(KeyError, match=unknown):
        constants.resolve_ica_file_id(unknown)
