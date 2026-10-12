"""Logical wire mapping and physical storage transpositions are distinct."""
from dataclasses import dataclass
import numpy as np


@dataclass(frozen=True)
class QubitLayout:
    logical_to_physical: tuple[int, ...]
    layout_id: int = 0

    def __post_init__(self):
        if tuple(sorted(self.logical_to_physical)) != tuple(range(len(self.logical_to_physical))):
            raise ValueError('Layout must be a bit permutation')

    @classmethod
    def identity(cls, num_qubits): return cls(tuple(range(num_qubits)))

    def swapped_physical(self, a, b):
        n = len(self.logical_to_physical)
        if not 0 <= a < n or not 0 <= b < n: raise ValueError('Physical bit out of range')
        return QubitLayout(tuple(b if q == a else a if q == b else q for q in self.logical_to_physical), self.layout_id + 1)

    def logical_state(self, physical_state):
        indices = np.arange(len(physical_state), dtype=np.int64)
        physical = np.zeros(len(indices), dtype=np.int64)
        for logical, bit in enumerate(self.logical_to_physical): physical |= ((indices >> logical) & 1) << bit
        return np.array(physical_state[physical], copy=True)


def index_bit_swap(state, a, b):
    indices = np.arange(len(state), dtype=np.int64)
    toggle = (((indices >> a) ^ (indices >> b)) & 1)
    destination = indices ^ (toggle << a) ^ (toggle << b)
    out = np.empty_like(state)
    out[destination] = state
    return out
