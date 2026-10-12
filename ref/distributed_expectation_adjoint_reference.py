"""Small, self-contained reference for block-wise Pauli expectation and adjoint gradients.

This is a correctness oracle, NOT an MPI or out-of-core implementation.
Run: python distributed_expectation_adjoint_reference.py
Requires: numpy.
Qubit q is bit q of the computational basis index (little-endian).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Union, Iterable
import numpy as np


@dataclass(frozen=True)
class Pauli:
    x: int
    z: int
    ny: int

    @staticmethod
    def from_labels(labels: str) -> "Pauli":
        """labels[q] in I, X, Y, Z, with character 0 denoting qubit 0."""
        x = z = ny = 0
        for q, p in enumerate(labels.upper()):
            if p in "XY":
                x |= 1 << q
            if p in "ZY":
                z |= 1 << q
            if p == "Y":
                ny += 1
            if p not in "IXYZ":
                raise ValueError(p)
        return Pauli(x, z, ny)


def parity_signs(indices: np.ndarray, mask: int) -> np.ndarray:
    # Reference code: use a scalar parity loop; native implementation uses bit operations.
    return np.fromiter((1.0 if (int(i) & mask).bit_count() % 2 == 0 else -1.0
                        for i in indices), dtype=np.float64, count=len(indices))


def pauli_apply_full(state: np.ndarray, pauli: Pauli) -> np.ndarray:
    idx = np.arange(len(state), dtype=np.int64)
    src = idx ^ pauli.x
    return (1j ** pauli.ny) * parity_signs(src, pauli.z) * state[src]


def block_pauli_inner(bra: np.ndarray, ket: np.ndarray, pauli: Pauli,
                      block_bits: int, *, count_partner_block_visits: bool = False,
                      owned_blocks: Iterable[int] | None = None):
    """Exact <bra|P|ket> as sum of independent source-block contributions.

    In distributed execution, bra partner blocks must be fetched via storage/MPI.
    Only the final complex partial sums need a collective reduction.
    """
    assert bra.shape == ket.shape and bra.ndim == 1
    n = (len(ket).bit_length() - 1)
    assert (1 << n) == len(ket) and 0 < block_bits <= n
    block_len = 1 << block_bits
    block_count = len(ket) // block_len
    x_local = pauli.x & (block_len - 1)
    x_high = pauli.x >> block_bits
    offsets = np.arange(block_len, dtype=np.int64)
    partner_offsets = offsets ^ x_local
    bra_blocks = bra.reshape(block_count, block_len)
    ket_blocks = ket.reshape(block_count, block_len)
    acc = 0j
    visits = set()
    for h in (range(block_count) if owned_blocks is None else owned_blocks):
        hp = h ^ x_high
        global_indices = (h << block_bits) | offsets
        phase = (1j ** pauli.ny) * parity_signs(global_indices, pauli.z)
        acc += np.vdot(bra_blocks[hp, partner_offsets], phase * ket_blocks[h])
        visits.add((h, hp))
    if count_partner_block_visits:
        return acc, visits
    return acc


def pauli_rotate(state: np.ndarray, pauli: Pauli, angle: float) -> np.ndarray:
    return (np.cos(angle / 2.0) * state
            - 1j * np.sin(angle / 2.0) * pauli_apply_full(state, pauli))


def hadamard(state: np.ndarray, qubit: int) -> np.ndarray:
    idx0 = np.arange(len(state), dtype=np.int64)
    idx0 = idx0[(idx0 & (1 << qubit)) == 0]
    idx1 = idx0 | (1 << qubit)
    out = state.copy()
    out[idx0] = (state[idx0] + state[idx1]) / np.sqrt(2.0)
    out[idx1] = (state[idx0] - state[idx1]) / np.sqrt(2.0)
    return out


def cnot(state: np.ndarray, control: int, target: int) -> np.ndarray:
    assert control != target
    idx0 = np.arange(len(state), dtype=np.int64)
    idx0 = idx0[((idx0 & (1 << control)) != 0) &
                ((idx0 & (1 << target)) == 0)]
    idx1 = idx0 | (1 << target)
    out = state.copy()
    out[idx0] = state[idx1]
    out[idx1] = state[idx0]
    return out


@dataclass(frozen=True)
class Rotation:
    pauli: Pauli
    parameter: str
    multiplier: float = 1.0
    offset: float = 0.0


@dataclass(frozen=True)
class HGate:
    qubit: int


@dataclass(frozen=True)
class CXGate:
    control: int
    target: int


Gate = Union[Rotation, HGate, CXGate]


def run_gate(state: np.ndarray, gate: Gate, values: dict[str, float],
             inverse: bool = False) -> np.ndarray:
    if isinstance(gate, Rotation):
        angle = gate.multiplier * values[gate.parameter] + gate.offset
        return pauli_rotate(state, gate.pauli, -angle if inverse else angle)
    if isinstance(gate, HGate):
        return hadamard(state, gate.qubit)
    if isinstance(gate, CXGate):
        return cnot(state, gate.control, gate.target)
    raise TypeError(gate)


def forward(circuit: list[Gate], initial: np.ndarray,
            values: dict[str, float]) -> np.ndarray:
    s = initial.copy()
    for gate in circuit:
        s = run_gate(s, gate, values)
    return s


def observable_apply(state: np.ndarray, terms: list[tuple[float, Pauli]]) -> np.ndarray:
    return sum((weight * pauli_apply_full(state, p) for weight, p in terms),
               start=np.zeros_like(state))


def block_expectation(state: np.ndarray, terms: list[tuple[float, Pauli]],
                      block_bits: int) -> float:
    return float(np.real(sum(weight * block_pauli_inner(state, state, p, block_bits)
                             for weight, p in terms)))


def adjoint_block_gradient(circuit: list[Gate], initial: np.ndarray,
                           values: dict[str, float],
                           terms: list[tuple[float, Pauli]],
                           block_bits: int) -> tuple[float, dict[str, float]]:
    """Reversible adjoint prototype, with block-pair partial gradients.

    ψ_j and λ_j are distributed block vectors at the same logical frontier.
    Materialization/MPI/Clifford-frame alignment are NOT implemented here.
    """
    psi = forward(circuit, initial, values)
    energy = block_expectation(psi, terms, block_bits)
    lam = observable_apply(psi, terms)
    gradients = dict.fromkeys(values, 0.0)
    for gate in reversed(circuit):
        if isinstance(gate, Rotation):
            local_contribution = block_pauli_inner(lam, psi, gate.pauli, block_bits)
            gradients[gate.parameter] += gate.multiplier * float(np.imag(local_contribution))
        psi = run_gate(psi, gate, values, inverse=True)
        lam = run_gate(lam, gate, values, inverse=True)
    if not np.allclose(psi, initial, atol=2e-12):
        raise AssertionError("Reverse reconstruction failed")
    return energy, gradients


def _reference_tests():
    rng = np.random.default_rng(20261012)
    # Test arbitrary X/Y/Z masks, and local/global partner relationships.
    checks = 0
    for n in range(2, 9):
        size = 1 << n
        state = rng.normal(size=size) + 1j * rng.normal(size=size)
        state /= np.linalg.norm(state)
        bra = rng.normal(size=size) + 1j * rng.normal(size=size)
        for block_bits in range(1, n + 1):
            for _ in range(25):
                labels = "".join(rng.choice(list("IXYZ"), size=n))
                p = Pauli.from_labels(labels)
                expected = np.vdot(bra, pauli_apply_full(state, p))
                observed = block_pauli_inner(bra, state, p, block_bits)
                if not np.allclose(observed, expected, atol=1e-11, rtol=1e-11):
                    raise AssertionError((n, block_bits, labels, expected, observed))
                checks += 1

    # Simulate rank-sharded ownership: each rank computes only its source blocks,
    # then a scalar Allreduce (sum) yields the exact global result. No real MPI.
    rank_checks = 0
    for n in range(3, 9):
        state = rng.normal(size=1 << n) + 1j * rng.normal(size=1 << n)
        bra = rng.normal(size=1 << n) + 1j * rng.normal(size=1 << n)
        for block_bits in range(1, n):
            block_count = 1 << (n - block_bits)
            for nranks in (2, 4):
                if nranks > block_count:
                    continue
                for _ in range(12):
                    p = Pauli.from_labels("".join(rng.choice(list("IXYZ"), size=n)))
                    pieces = [block_pauli_inner(bra, state, p, block_bits,
                                                 owned_blocks=range(rank, block_count, nranks))
                              for rank in range(nranks)]
                    ref = np.vdot(bra, pauli_apply_full(state, p))
                    np.testing.assert_allclose(sum(pieces), ref, atol=1e-10, rtol=1e-10)
                    rank_checks += 1

    # Test adjoint gradients of multi-Pauli rotations, repeated parameter
    # symbols, local/global bits, Hadamard and cross-block CNOT.
    grad_checks = 0
    for n in (3, 5, 7):
        size = 1 << n
        state = rng.normal(size=size) + 1j * rng.normal(size=size)
        state /= np.linalg.norm(state)
        terms = [(0.7, Pauli.from_labels("Z" + "I" * (n-1))),
                 (-0.43, Pauli.from_labels("Y" + "I" * (n-2) + "X")),
                 (0.13, Pauli.from_labels("X" * n))]
        circuit: list[Gate] = [
            HGate(0),
            Rotation(Pauli.from_labels("X" + "I" * (n-1)), "alpha"),
            CXGate(0, n-1),
            Rotation(Pauli.from_labels("Y" + "I" * (n-2) + "Z"), "beta", -0.7),
            HGate(n-1),
            Rotation(Pauli.from_labels("Z" + "I" * (n-1)), "alpha", 0.25),
            Rotation(Pauli.from_labels("X" * n), "gamma", 1.2),
        ]
        values = {"alpha": 0.36, "beta": -0.43, "gamma": 0.21}
        for block_bits in range(1, n+1):
            got_e, got_grad = adjoint_block_gradient(circuit, state, values, terms, block_bits)
            ref_state = forward(circuit, state, values)
            ref_e = float(np.real(np.vdot(ref_state, observable_apply(ref_state, terms))))
            np.testing.assert_allclose(got_e, ref_e, atol=1e-11)
            for name in values:
                epsilon = 1e-6
                plus = dict(values); minus = dict(values)
                plus[name] += epsilon; minus[name] -= epsilon
                eplus = block_expectation(forward(circuit, state, plus), terms, block_bits)
                eminus = block_expectation(forward(circuit, state, minus), terms, block_bits)
                finite_diff = (eplus - eminus) / (2 * epsilon)
                np.testing.assert_allclose(got_grad[name], finite_diff, atol=1e-8, rtol=1e-7,
                                           err_msg=f"n={n} b={block_bits} param={name}")
                grad_checks += 1
    print(f"PASS: {checks} block Pauli inner products; "
          f"{rank_checks} simulated rank partial reductions; "
          f"{grad_checks} adjoint gradients")


if __name__ == "__main__":
    _reference_tests()
