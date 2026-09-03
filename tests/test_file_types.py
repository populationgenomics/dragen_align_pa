"""The per-file download specs: the ICA-side names of the objects the per-file stages own.

The names come from the DRAGEN 3.7.8 output inventory (outputs RFC, Appendix A): DRAGEN
writes an md5 companion for the cram and base gVCF data files only, MLR writes one for the
recal gVCF data file and its index.
"""

from dragen_align_pa.file_types import BASE_GVCF, CRAM, PER_FILE_SPECS, RECAL_GVCF


def test_the_three_per_file_specs_are_the_module_constants():
    assert PER_FILE_SPECS == (CRAM, BASE_GVCF, RECAL_GVCF)


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
