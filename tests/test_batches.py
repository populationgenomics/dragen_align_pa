from dragen_align_pa.batches import Batch


def test_batch_name_zero_padded_four_digits():
    b = Batch(cohort_name='COH0001', batch_index=0, sg_names=['CPG_A'])
    assert b.name == 'COH0001-batch0000'


def test_batch_name_four_digit_index():
    b = Batch(cohort_name='COH0001', batch_index=12, sg_names=[])
    assert b.name == 'COH0001-batch0012'


def test_batch_name_handles_large_index():
    """Width 4 supports up to 9999 batches (= 49995 SGs at batch_size 5) without lex-sort
    breakage. Beyond that, names overflow the field but stay sortable for adjacent ranges."""
    b = Batch(cohort_name='COH0001', batch_index=1234, sg_names=[])
    assert b.name == 'COH0001-batch1234'
