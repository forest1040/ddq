"""Compact access rules and conservative task memory/I/O estimates."""
from dataclasses import dataclass
from .cache import BufferLease


@dataclass(frozen=True)
class MaterializationTask:
    task_id: str
    source_ops: tuple[int, ...]
    path: str
    upper_targets: tuple[int, ...]
    input_frontier: int
    output_frontier: int
    working_set_bytes: int


@dataclass(frozen=True)
class AccessPlan:
    circuit_hash: str
    profile_id: str
    layout_id: int
    tasks: tuple[MaterializationTask, ...]
    read_bytes_lower: int
    read_bytes_upper: int
    write_bytes_lower: int
    write_bytes_upper: int
    version: int = 1
    assumptions: str = 'Cold-cache upper bound; implicit-zero and metadata-only work may eliminate I/O'


def group_bits(gate, block_bits):
    if gate.diagonal: return ()
    return tuple(sorted({q for q in gate.targets if q >= block_bits}))


def working_set(config, group_count=1):
    return config.metadata_reserve_bytes + 2 * group_count * BufferLease.allocation_cost(config.slot_bytes,config.alignment)
