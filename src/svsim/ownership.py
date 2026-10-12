"""Logical block ownership, independent of immutable extent placement."""
from dataclasses import dataclass


@dataclass(frozen=True)
class OwnershipMap:
    num_qubits: int
    block_bits: int
    size: int
    epoch: int = 0

    def __post_init__(self):
        values = (self.num_qubits, self.block_bits, self.size, self.epoch)
        if any(not isinstance(v, int) or isinstance(v, bool) for v in values):
            raise ValueError('Ownership dimensions must be integers')
        if not 1 <= self.block_bits <= self.num_qubits:
            raise ValueError('Invalid state/block width')
        if self.size < 1 or self.size & (self.size - 1):
            raise ValueError('Rank count must be a power of two')
        if self.size > self.num_blocks or self.epoch < 0:
            raise ValueError('Invalid rank count/ownership epoch')

    @property
    def num_blocks(self): return 1 << (self.num_qubits - self.block_bits)

    @property
    def rank_bits(self): return self.size.bit_length() - 1

    @property
    def k(self): return self.num_qubits - self.block_bits - self.rank_bits

    @property
    def blocks_per_rank(self): return 1 << self.k

    def shard_owner(self, h):
        if not isinstance(h, int) or isinstance(h, bool) or not 0 <= h < self.num_blocks:
            raise ValueError('Block ID out of range')
        return h >> self.k

    def local_blocks(self, rank):
        if not isinstance(rank, int) or isinstance(rank, bool) or not 0 <= rank < self.size:
            raise ValueError('Rank out of range')
        start = rank << self.k
        return range(start, start + self.blocks_per_rank)


@dataclass(frozen=True)
class ExtentDescriptor:
    dataset_id: str
    store_id: str
    version_id: str
    storage_rank: int
    segment: int | None = None
    offset: int | None = None
    checksum: str | None = None

    def __post_init__(self):
        if not all(isinstance(v, str) and v for v in (self.dataset_id, self.store_id, self.version_id)):
            raise ValueError('Extent identity must be nonempty')
        if not isinstance(self.storage_rank, int) or self.storage_rank < 0:
            raise ValueError('Invalid storage rank')
        if (self.segment is None) != (self.offset is None):
            raise ValueError('Extent segment and offset must both be present')
        if self.segment is not None and (self.segment < 0 or self.offset < 0):
            raise ValueError('Invalid extent address')
