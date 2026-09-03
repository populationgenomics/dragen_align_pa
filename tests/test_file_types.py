"""The per-file download specs: the ICA-side names of the objects the per-file stages own.

The names come from listing every sequencing group's ICA output folder in three datasets
(PR #104, 2026-09-03): DRAGEN 3.7.8 writes an md5 companion for the cram and base gVCF
data files only, MLR writes one for the recal gVCF data file and its index.
"""

from unittest.mock import MagicMock

import pytest

from dragen_align_pa.file_types import BASE_GVCF, CRAM, PER_FILE_SPECS, RECAL_GVCF, FileTypeSpec
from dragen_align_pa.jobs import download_specific_files_from_ica


def test_a_per_file_job_rejects_a_spec_the_bulk_download_does_not_know():
    """A spec passed to a per-file stage but missing from PER_FILE_SPECS would have its files
    copied by the bulk download as well; fail at graph build instead."""
    stray = FileTypeSpec(data_suffix='sv.vcf.gz', index_suffix='sv.vcf.gz.tbi', md5_suffix='md5sum')

    with pytest.raises(ValueError, match='PER_FILE_SPECS'):
        download_specific_files_from_ica.make_download_job(
            job_name='DownloadSvFromIca',
            sequencing_group=MagicMock(),
            file_spec=stray,
            pipeline_id_arguid_path=MagicMock(),
            cohort_name='COH0001',
            gcs_output_dir=MagicMock(),
        )


def test_dragen_outputs_have_a_data_md5_but_no_index_md5():
    assert CRAM.ica_names('SG1') == frozenset({'SG1.cram', 'SG1.cram.crai', 'SG1.cram.md5sum'})
    assert BASE_GVCF.ica_names('SG1') == frozenset(
        {
            'SG1.hard-filtered.gvcf.gz',
            'SG1.hard-filtered.gvcf.gz.tbi',
            'SG1.hard-filtered.gvcf.gz.md5sum',
        },
    )


def test_mlr_output_has_md5_companions_for_data_and_index_under_the_ica_side_suffix():
    # MLR writes `.md5`, not `.md5sum`; the per-file stage renames the data md5 on the way
    # into GCS and the reheader job recomputes the index md5.
    assert RECAL_GVCF.ica_names('SG1') == frozenset(
        {
            'SG1.hard-filtered.recal.gvcf.gz',
            'SG1.hard-filtered.recal.gvcf.gz.tbi',
            'SG1.hard-filtered.recal.gvcf.gz.md5',
            'SG1.hard-filtered.recal.gvcf.gz.tbi.md5',
        },
    )


def test_the_bulk_exclusion_covers_every_name_the_per_file_job_fetches(monkeypatch):
    """`ica_names` is what the bulk download excludes; the per-file job must ask ICA for
    nothing outside it, or a file is fetched twice or by nobody."""
    for spec in PER_FILE_SPECS:
        orchestrate = MagicMock()
        monkeypatch.setattr(download_specific_files_from_ica, '_orchestrate_download', orchestrate)
        monkeypatch.setattr(download_specific_files_from_ica.storage, 'Client', MagicMock())
        api = MagicMock()
        api.__enter__ = MagicMock(return_value=(MagicMock(), {'projectId': 'p'}))
        api.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr(
            download_specific_files_from_ica.ica_api_utils,
            'ica_project_data_api',
            MagicMock(return_value=api),
        )
        sequencing_group = MagicMock()
        sequencing_group.name = 'SG1'

        download_specific_files_from_ica.run(
            sequencing_group=sequencing_group,
            file_spec=spec,
            ica_folder_path='/ica/folder/',
            gcs_output_dir='gs://bucket/ica/v/output/x',  # type: ignore[arg-type]
        )

        name_keys = ('main_file_name', 'index_file_name', 'md5_file_name')
        requested = {orchestrate.call_args.kwargs[key] for key in name_keys}
        assert requested <= spec.ica_names('SG1')
        assert orchestrate.call_args.kwargs['md5_gcp_name'] == f'{spec.data_name("SG1")}.md5sum'
