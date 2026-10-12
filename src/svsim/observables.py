"""Pauli block-pair inner products and small NumPy adjoint reference."""
from dataclasses import dataclass
import numpy as np
from .layout import QubitLayout
from .kernels import apply_gate, apply_matrix, gate_matrix


@dataclass(frozen=True)
class PauliTerm:
    x_mask: int
    z_mask: int
    coefficient: complex = 1.0

    def __post_init__(self):
        if self.x_mask < 0 or self.z_mask < 0: raise ValueError('Negative Pauli mask')
        if not np.isfinite(self.coefficient): raise ValueError('Non-finite Pauli coefficient')

    @classmethod
    def from_string(cls, text, coefficient=1.0):
        """Character at index q denotes logical qubit q (LSB first)."""
        if any(c not in 'IXYZ' for c in text): raise ValueError('Invalid Pauli string')
        return cls(sum(1 << q for q, c in enumerate(text) if c in 'XY'),
                   sum(1 << q for q, c in enumerate(text) if c in 'ZY'), coefficient)


@dataclass(frozen=True)
class ObservableIR:
    terms: tuple[PauliTerm, ...]

    def __post_init__(self):
        object.__setattr__(self, 'terms', tuple(self.terms))
        if any(complex(t.coefficient).imag != 0 for t in self.terms):
            raise ValueError('Hermitian observables require real coefficients')


@dataclass(frozen=True)
class StateHandle:
    blocks: tuple[np.ndarray, ...]
    block_bits: int
    layout: QubitLayout
    frontier: int
    binding_id: str
    role: str = 'forward'

    @classmethod
    def from_vector(cls, vector, block_bits, *, layout=None, frontier=-1, binding_id='', role='forward'):
        vector = np.asarray(vector, dtype='<c16')
        n = (len(vector)-1).bit_length()
        if vector.shape != (1 << n,) or not 1 <= block_bits <= n: raise ValueError('Invalid state dimensions')
        if not np.all(np.isfinite(vector)): raise ValueError('Non-finite state')
        layout = layout or QubitLayout.identity(n)
        if len(layout.logical_to_physical) != n: raise ValueError('State/layout width mismatch')
        size = 1 << block_bits
        # Bytes-backed arrays are immutable, including against setflags(write=True).
        blocks = tuple(np.frombuffer(vector[i:i+size].tobytes(), dtype='<c16') for i in range(0, len(vector), size))
        return cls(blocks, block_bits, layout, frontier, binding_id, role)

    @classmethod
    def from_simulator(cls, simulator, *, role='forward'):
        """Capture the current logical cut, including deferred frames and phases."""
        amplitudes = [simulator.phases[h] * simulator.frames.materialize(simulator.frame_ids[h],
                      simulator.blocks[h], simulator.assets, simulator.binding) for h in simulator.blocks]
        return cls.from_vector(np.concatenate(amplitudes), simulator.block_bits, layout=simulator.layout,
                               frontier=simulator.frontier, binding_id=simulator.binding.binding_id if simulator.binding else '', role=role)


class BlockPauliInner:
    def evaluate(self, bra: StateHandle, ket: StateHandle, term: PauliTerm):
        if bra.layout != ket.layout or bra.block_bits != ket.block_bits or len(bra.blocks) != len(ket.blocks):
            raise ValueError('Block inner product needs aligned layouts and block widths')
        n = len(ket.layout.logical_to_physical)
        if (term.x_mask | term.z_mask) >> n: raise ValueError('Observable qubit out of range')
        def physical(mask):
            return sum(((mask >> q)&1) << bit for q, bit in enumerate(ket.layout.logical_to_physical))
        x, z = physical(term.x_mask), physical(term.z_mask)
        b = ket.block_bits
        upper_x, lower_x = x >> b, x & ((1 << b)-1)
        y_phase = 1j ** (x & z).bit_count()
        offset = np.arange(1 << b, dtype=np.int64)
        total = 0j
        for h, amplitudes in enumerate(ket.blocks):
            indices = (h << b) | offset
            sign = np.array([(-1) ** (int(i) & z).bit_count() for i in indices])
            partner = bra.blocks[h ^ upper_x][offset ^ lower_x]
            total += np.vdot(partner, y_phase * sign * amplitudes)
        return term.coefficient * total

    def expectation(self, state, observable):
        value = sum(self.evaluate(state, state, t) for t in observable.terms)
        if abs(complex(value).imag) > 1e-10: raise ArithmeticError('Expectation has unexpected imaginary residual')
        return float(complex(value).real)


def apply_pauli(vector, term):
    indices = np.arange(len(vector), dtype=np.int64)
    if (term.x_mask | term.z_mask) >= len(vector): raise ValueError('Observable qubit out of range')
    sign = np.array([(-1) ** (int(i) & term.z_mask).bit_count() for i in indices])
    out = np.empty_like(vector)
    out[indices ^ term.x_mask] = term.coefficient * (1j ** (term.x_mask & term.z_mask).bit_count()) * sign * vector
    return out


@dataclass(frozen=True)
class GradientPlanTemplate:
    circuit_hash: str
    observable: ObservableIR
    parameter_ids: tuple[str, ...]
    method: str = 'adjoint-reference'
    version: int = 1
    checkpoint_policy: str = 'store-all-reference'


def adjoint_gradient(gates, num_qubits, assets, binding, observable, *, block_bits=2, initial_state=None):
    """Store-all Phase 0 adjoint oracle; no distributed/checkpoint scheduler yet."""
    if not 1 <= block_bits <= num_qubits: raise ValueError('Invalid block width')
    state = np.zeros(1 << num_qubits, complex) if initial_state is None else np.array(initial_state, complex, copy=True)
    if initial_state is None: state[0] = 1
    forward = [state]
    for gate in gates: forward.append(apply_gate(forward[-1], gate, assets, binding))
    lam = sum((apply_pauli(forward[-1], t) for t in observable.terms), start=np.zeros_like(state))
    parameters = frozenset().union(*(g.dependencies for g in gates))
    gradients = dict.fromkeys(sorted(parameters), 0.0)
    paulis = {'RX': np.array([[0,1],[1,0]], complex), 'RY': np.array([[0,-1j],[1j,0]], complex), 'RZ': np.diag([1,-1])}
    inner = BlockPauliInner()
    for i in range(len(gates)-1, -1, -1):
        gate = gates[i]
        if gate.dependencies:
            derivative = (-0.5j * paulis[gate.kind]) @ gate_matrix(gate, assets, binding)
            dstate = apply_matrix(forward[i], derivative, gate.targets, gate.controls, gate.control_values)
            if gate.controls:
                indices = np.arange(len(dstate))
                selected = np.ones(len(dstate), bool)
                for q, v in zip(gate.controls, gate.control_values): selected &= ((indices >> q)&1) == v
                dstate[~selected] = 0  # Inactive identity branch has zero derivative.
            bra = StateHandle.from_vector(lam, block_bits, frontier=i, binding_id=binding.binding_id, role='adjoint')
            ket = StateHandle.from_vector(dstate, block_bits, frontier=i, binding_id=binding.binding_id)
            local = 2 * inner.evaluate(bra, ket, PauliTerm(0,0)).real
            for parameter in gate.dependencies: gradients[parameter] += local * gate.angle.derivative(parameter).evaluate(binding)
        if gate.kind == 'MCX': lam = apply_gate(lam, gate, assets, binding)
        else: lam = apply_matrix(lam, gate_matrix(gate, assets, binding).conj().T, gate.targets, gate.controls, gate.control_values)
    return gradients
