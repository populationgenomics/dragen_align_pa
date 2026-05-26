"""Roundtrip tests for the dispatcher->worker manifest.

ManageDragenPipeline and ManageDragenMlr previously passed every per-SG
path dict inline through job.call(...), which embedded thousands of GS
paths in the Hail Batch PythonJob spec and blew the 1 MiB cap at ~2.2k
sequencing groups. The fix collapses those dicts into a single JSON
manifest on GCS and threads only the manifest path through the spec.
These tests pin the JSON schema so dispatcher and worker stay in sync
for both stage shapes (pipeline cram+fastq, mlr).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import cpg_utils

from dragen_align_pa.utils import load_manifest, write_manifest

if TYPE_CHECKING:
    from pathlib import Path


def _path(s: str) -> cpg_utils.Path:
    return cpg_utils.to_path(s)


def test_pipeline_manifest_cram_mode_roundtrip(tmp_path: Path) -> None:
    manifest_path = _path(str(tmp_path / 'manifest.json'))

    payload = {
        'outputs': {
            'sg_a_success': _path('gs://b/p/sg_a_pipeline_success.json'),
            'sg_a_pipeline_id_and_arguid': _path('gs://b/p/sg_a_pipeline_id_and_arguid.json'),
            'cohort_errors': _path('gs://b/p/cohort_errors.log'),
        },
        'cram_ica_fids_path': {'sg_a': _path('gs://b/prep/sg_a_fids.json')},
        'analysis_output_fids_path': {'sg_a': _path('gs://b/prep/sg_a_output_fid.json')},
        'fastq_ids_path': None,
        'fastq_list_fid_and_filenames_path': None,
    }
    write_manifest(manifest_path=manifest_path, payload=payload)

    loaded = load_manifest(manifest_path)

    assert loaded == payload


def test_pipeline_manifest_fastq_mode_roundtrip(tmp_path: Path) -> None:
    manifest_path = _path(str(tmp_path / 'manifest.json'))

    payload = {
        'outputs': {'cohort_errors': _path('gs://b/p/cohort_errors.log')},
        'cram_ica_fids_path': None,
        'analysis_output_fids_path': {'sg_a': _path('gs://b/prep/sg_a_output_fid.json')},
        'fastq_ids_path': _path('gs://b/prep/cohort_fastq_ids.txt'),
        'fastq_list_fid_and_filenames_path': {'sg_a': _path('gs://b/prep/sg_a_fastq_list_fid.json')},
    }
    write_manifest(manifest_path=manifest_path, payload=payload)

    loaded = load_manifest(manifest_path)

    assert loaded == payload


def test_mlr_manifest_roundtrip(tmp_path: Path) -> None:
    manifest_path = _path(str(tmp_path / 'manifest.json'))

    payload = {
        'outputs': {
            'sg_a_mlr_success': _path('gs://b/p/sg_a_mlr_pipeline_success.json'),
            'sg_a_mlr_pipeline_id': _path('gs://b/p/sg_a_mlr_pipeline_id.json'),
            'cohort_mlr_errors': _path('gs://b/p/cohort_mlr_errors.log'),
        },
        'pipeline_id_arguid_path_dict': {
            'sg_a_pipeline_id_and_arguid': _path('gs://b/p/sg_a_pipeline_id_and_arguid.json'),
        },
    }
    write_manifest(manifest_path=manifest_path, payload=payload)

    loaded = load_manifest(manifest_path)

    assert loaded == payload
