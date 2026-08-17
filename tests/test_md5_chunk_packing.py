"""Unit tests for the MD5 pipeline's on-the-fly chunk planning.

Covers `compute_md5_chunk_size` (spreading N FASTQs over the ICA pod quota in
a fixed number of waves) and `pack_fastq_ids_by_size` (ordering the uploaded
ID list so the pipeline's sequential count-based split yields byte-balanced
chunks).
"""

import pytest

from dragen_align_pa.jobs import manage_md5_pipeline


@pytest.mark.parametrize(
    ('n_files', 'max_concurrent_pods', 'waves', 'expected'),
    [
        (1000, 25, 2, 20),  # divides evenly
        (3784, 25, 2, 76),  # remainder rounds up
        (5001, 25, 2, 101),  # one file over an even split still rounds up
        (30, 25, 2, 1),  # fewer files than pod slots -> one file per chunk
        (1, 25, 2, 1),
        (5000, 10, 4, 125),  # quota and waves both scale the divisor
    ],
)
def test_compute_md5_chunk_size_spreads_files_over_pod_waves(n_files, max_concurrent_pods, waves, expected):
    """Chunk size is ceil(n_files / (pods * waves)) so no wave exceeds the pod quota."""
    assert (
        manage_md5_pipeline.compute_md5_chunk_size(
            n_files=n_files, max_concurrent_pods=max_concurrent_pods, waves=waves
        )
        == expected
    )


@pytest.mark.parametrize(
    ('n_files', 'max_concurrent_pods', 'waves'),
    [(0, 25, 2), (100, 0, 2), (100, 25, 0)],
)
def test_compute_md5_chunk_size_rejects_non_positive_inputs(n_files, max_concurrent_pods, waves):
    """Zero files, pods, or waves is a caller bug and must raise, not return a size."""
    with pytest.raises(ValueError, match=r'must be positive'):
        manage_md5_pipeline.compute_md5_chunk_size(
            n_files=n_files, max_concurrent_pods=max_concurrent_pods, waves=waves
        )


def _block_byte_totals(ordered: list[str], id_to_size: dict[str, int], chunk_size: int) -> list[int]:
    """Byte total of each consecutive `chunk_size`-line block, as the pipeline would split it."""
    return [
        sum(id_to_size[file_id] for file_id in ordered[i : i + chunk_size])
        for i in range(0, len(ordered), chunk_size)
    ]


def test_pack_fastq_ids_by_size_balances_bytes_across_blocks():
    """Size-clustered input (manifest order) must not produce lopsided blocks.

    Ascending sizes mimic same-run files clustering together; left in that
    order, the last block would hold ~4x the bytes of the first. Packed, every
    block must sit within one max-file-size of the ideal mean.
    """
    id_to_size = {f'fil.{i:04d}': (i + 1) * 100_000_000 for i in range(100)}
    chunk_size = 25
    ordered = manage_md5_pipeline.pack_fastq_ids_by_size(id_to_size=id_to_size, chunk_size=chunk_size)
    blocks = _block_byte_totals(ordered, id_to_size, chunk_size)
    ideal = sum(id_to_size.values()) / len(blocks)
    assert max(blocks) <= ideal + max(id_to_size.values())


def test_pack_fastq_ids_by_size_returns_every_id_exactly_once():
    """Packing reorders the IDs but must not drop, duplicate, or invent any."""
    id_to_size = {f'fil.{i:04d}': (i % 7 + 1) * 1_000_000 for i in range(103)}
    ordered = manage_md5_pipeline.pack_fastq_ids_by_size(id_to_size=id_to_size, chunk_size=10)
    assert sorted(ordered) == sorted(id_to_size)


def test_pack_fastq_ids_by_size_keeps_blocks_balanced_with_a_remainder():
    """When n % chunk_size != 0, only the final block may be short.

    If the packer let interior blocks run short, the pipeline's strict
    count-based split would smear its buckets across block boundaries and
    re-create lopsided chunks.
    """
    big, small = 100_000_000, 1_000_000
    id_to_size = {f'fil.big{i}': big for i in range(4)} | {f'fil.small{i}': small for i in range(6)}
    chunk_size = 4
    ordered = manage_md5_pipeline.pack_fastq_ids_by_size(id_to_size=id_to_size, chunk_size=chunk_size)
    blocks = _block_byte_totals(ordered, id_to_size, chunk_size)
    assert len(ordered) == 10
    assert len(blocks) == 3
    ideal = sum(id_to_size.values()) / len(blocks)
    assert max(blocks) <= ideal + big


def test_pack_fastq_ids_by_size_rejects_non_positive_chunk_size():
    """A chunk size of zero or less is a caller bug and must raise, not divide by zero."""
    with pytest.raises(ValueError, match=r'chunk_size must be positive'):
        manage_md5_pipeline.pack_fastq_ids_by_size(id_to_size={'fil.a': 1}, chunk_size=0)


def _fastq_info(n: int) -> dict[str, manage_md5_pipeline.FastqFileDetails]:
    return {
        f'fil.{i:04d}': manage_md5_pipeline.FastqFileDetails(name=f'f{i}.fastq.gz', size_in_bytes=(i + 1) * 1_000)
        for i in range(n)
    }


def test_plan_md5_chunks_computes_chunk_size_from_the_pod_quota():
    """The chunk size comes from the fixed ICA pod quota and wave count, not config."""
    info = _fastq_info(151)
    ordered_ids, chunk_size = manage_md5_pipeline._plan_md5_chunks(info)
    # 25 pods x 2 waves -> ceil(151 / 50) = 4 files per chunk.
    assert chunk_size == 4
    assert sorted(ordered_ids) == sorted(info)
