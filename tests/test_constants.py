"""Tests for the ICA_FILE_IDS registry and resolver in constants.py.

The registry maps human-readable BED basenames to ICA file IDs so that
config (`coverage_region_beds`, preset additional_args, etc.) can be
written in terms the user understands, and the submitter does one
indirection to recover the ICA ID for `additional_files`.
"""

import pytest

from dragen_align_pa import constants


def test_resolve_ica_file_id_returns_registered_id(monkeypatch):
    """Known basename mapped to a real fil.… ID → returns that ID."""
    monkeypatch.setattr(
        'dragen_align_pa.constants.ICA_FILE_IDS',
        {'real.bed': 'fil.0123456789abcdef'},
    )
    assert constants.resolve_ica_file_id('real.bed') == 'fil.0123456789abcdef'


def test_resolve_ica_file_id_raises_on_unknown_name():
    """Unknown basename → clear error naming the unknown entry."""
    unknown = 'this_bed_is_not_registered.bed'
    with pytest.raises(KeyError, match=r'this_bed_is_not_registered\.bed'):
        constants.resolve_ica_file_id(unknown)


def test_resolve_ica_file_id_rejects_placeholder_id(monkeypatch):
    """A registered basename whose value still starts with `fil.TODO_` is a
    sentinel for "no ICA upload yet" — submitting it to ICA produces an
    opaque "no such file" failure mid-run. resolve_ica_file_id must instead
    raise at submitter startup with a clear actionable message."""
    monkeypatch.setattr(
        'dragen_align_pa.constants.ICA_FILE_IDS',
        {'staged_but_not_uploaded.bed': 'fil.TODO_REPLACE_AFTER_ICA_UPLOAD'},
    )
    with pytest.raises(ValueError, match=r'staged_but_not_uploaded\.bed'):
        constants.resolve_ica_file_id('staged_but_not_uploaded.bed')
