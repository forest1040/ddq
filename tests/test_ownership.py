import pytest
from svsim.ownership import OwnershipMap, ExtentDescriptor


def test_partition_uses_high_rank_bits_and_global_ids():
    ownership = OwnershipMap(10, 3, 8, epoch=2)
    assert ownership.rank_bits == 3
    assert ownership.k == 4
    assert ownership.blocks_per_rank == 16
    blocks = []
    for rank in range(8):
        local = ownership.local_blocks(rank)
        assert all(ownership.shard_owner(h) == rank for h in local)
        blocks.extend(local)
    assert blocks == list(range(128))
    assert ownership.epoch == 2


@pytest.mark.parametrize('args', [(5,0,1),(5,6,1),(5,2,3),(5,2,16),(5,2,0),(5,2,1,-1)])
def test_invalid_ownership(args):
    with pytest.raises(ValueError): OwnershipMap(*args)


def test_owner_validation_and_extent_placement_independent():
    ownership = OwnershipMap(5,2,2)
    assert ownership.local_blocks(1) == range(4,8)
    extent = ExtentDescriptor('dataset','prefix','version',0,2,4096,'checksum')
    assert ownership.shard_owner(7) != extent.storage_rank
    for h in (-1,8,True):
        with pytest.raises(ValueError): ownership.shard_owner(h)
    with pytest.raises(ValueError): ownership.local_blocks(2)
    with pytest.raises(ValueError): ExtentDescriptor('dataset','prefix','version',0,2)
