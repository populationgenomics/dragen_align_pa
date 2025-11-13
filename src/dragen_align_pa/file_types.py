"""
This module defines shared data structures and types used across the pipeline.
"""

from dataclasses import dataclass


@dataclass
class FileTypeSpec:
    """Groups the file suffixes and prefixes for a downloadable file type."""

    gcs_prefix: str  # e.g., 'cram'
    data_suffix: str  # e.g., 'cram'
    index_suffix: str  # e.g., 'cram.crai'
    md5_suffix: str  # e.g., 'md5sum' or 'md5'
