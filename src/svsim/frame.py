"""Canonical Clifford frames with an exact eighth-root scalar correction."""
from dataclasses import dataclass
import hashlib
import cmath
import operator
from .clifford import CliffordTableau, anchor_phase
from .ir import GateOp


@dataclass(frozen=True)
class FrameAsset:
    tableau: CliffordTableau
    word: tuple[GateOp, ...]
    correction: int

    def __post_init__(self):
        object.__setattr__(self, 'word', tuple(self.word))
        try: correction = operator.index(self.correction)
        except TypeError as exc: raise ValueError('Frame correction must be an integer Z8 exponent') from exc
        if not 0 <= correction < 8: raise ValueError('Frame correction must be between 0 and 7')
        object.__setattr__(self, 'correction', correction)

    @classmethod
    def identity(cls, b): return cls(CliffordTableau.identity(b), (), 0)

    @classmethod
    def from_tableau(cls, tableau):
        word=tableau.synthesis()
        return cls(tableau,word,anchor_phase(tableau.b,word,tableau))

    @property
    def content_id(self): return hashlib.sha256(self.tableau.canonical_bytes()).hexdigest()
    @property
    def asset_id(self): return self.content_id
    @property
    def block_bits(self): return self.tableau.b
    @property
    def multiplier(self): return cmath.exp(-1j*self.correction*cmath.pi/4)

    def left_update(self, gate):
        tableau=self.tableau.then(gate)
        # The bounded synthesis word represents W = omega^q C0, so subtract q.
        delta=(anchor_phase(self.tableau.b,self.word+(gate,),tableau)-self.correction)%8
        return FrameAsset.from_tableau(tableau),delta

    def conjugate_forward(self, pauli): return self.tableau.conjugate_forward(pauli)
    def conjugate_inverse(self, pauli): return self.tableau.conjugate_inverse(pauli)

    def apply(self, buffer, assets=None, binding=None):
        from .streaming_kernels import apply_local
        for gate in self.word: apply_local(buffer,gate,assets,binding,block_bits=self.block_bits)
        if self.correction: buffer *= self.multiplier
        return buffer


def frame_from_word(b, word):
    tableau=CliffordTableau.identity(b)
    for gate in word: tableau=tableau.then(gate)
    return FrameAsset.from_tableau(tableau),anchor_phase(b,tuple(word),tableau)
