"""The per-file download specs: the ICA-side names of the objects the per-file stages own."""

from dragen_align_pa.file_types import BASE_GVCF, CRAM, PER_FILE_SPECS, RECAL_GVCF


def test_the_three_per_file_specs_are_the_module_constants():
    assert PER_FILE_SPECS == (CRAM, BASE_GVCF, RECAL_GVCF)
    assert [spec.gcs_prefix for spec in PER_FILE_SPECS] == ['cram', 'base_gvcf', 'recal_gvcf']


def test_ica_names_cover_data_index_and_both_md5_companions():
    assert CRAM.ica_names('SG1') == frozenset(
        {'SG1.cram', 'SG1.cram.crai', 'SG1.cram.md5sum', 'SG1.cram.crai.md5sum'},
    )
    assert BASE_GVCF.ica_names('SG1') == frozenset(
        {
            'SG1.hard-filtered.gvcf.gz',
            'SG1.hard-filtered.gvcf.gz.tbi',
            'SG1.hard-filtered.gvcf.gz.md5sum',
            'SG1.hard-filtered.gvcf.gz.tbi.md5sum',
        },
    )


def test_recal_gvcf_companions_use_the_ica_side_md5_suffix():
    # MLR writes `.md5`, not `.md5sum`; the per-file stage renames on the way into GCS.
    assert RECAL_GVCF.ica_names('SG1') == frozenset(
        {
            'SG1.hard-filtered.recal.gvcf.gz',
            'SG1.hard-filtered.recal.gvcf.gz.tbi',
            'SG1.hard-filtered.recal.gvcf.gz.md5',
            'SG1.hard-filtered.recal.gvcf.gz.tbi.md5',
        },
    )
