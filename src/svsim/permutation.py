"""Symbolic upper-bit bijections; rules act on block labels, not layouts."""
from dataclasses import dataclass


@dataclass(frozen=True)
class PermutationEdge:
    kind: str
    targets: tuple[int,...]
    controls: tuple[int,...] = ()
    control_values: tuple[int,...] = ()

    def inverse(self,h):
        # X/controlled-X and SWAP are involutions, including open controls.
        if self.kind == 'MCX':
            if all((h >> q)&1 == v for q,v in zip(self.controls,self.control_values)):
                return h ^ (1 << self.targets[0])
            return h
        if self.kind == 'SWAP':
            a,b = self.targets
            toggle = ((h >> a) ^ (h >> b))&1
            return h ^ (toggle << a) ^ (toggle << b)
        raise ValueError('Unsupported symbolic permutation')

    @classmethod
    def from_gate(cls,gate,block_bits):
        if gate.kind not in {'MCX','SWAP'} or any(q < block_bits for q in gate.qubits):
            raise ValueError('Permutation must act on upper bits only')
        return cls(gate.kind,tuple(q-block_bits for q in gate.targets),
                   tuple(q-block_bits for q in gate.controls),gate.control_values)
