"""Bounded exact structured lowering for dense gates of up to four targets."""
from dataclasses import replace

import numpy as np

from .ir import GateOp


class StructuredExactDecomposer:
    """Factor a small unitary into controlled U(2) rotations and phase gates.

    The returned operations are derived candidates. The semantic source gate
    remains the journal authority and must not also be executed.
    """

    def __init__(self, assets, *, tolerance=1e-10, max_targets=4, max_operations=4096):
        self.assets = assets
        self.tolerance = tolerance
        self.max_targets = max_targets
        self.max_operations = max_operations
        if max_operations < 1: raise ValueError('max_operations must be positive')

    def decompose(self, gate):
        if gate.kind != 'DENSE_UNITARY': raise ValueError('expected DENSE_UNITARY')
        k = len(gate.targets)
        if not 1 <= k <= self.max_targets: raise ValueError('lowering width exceeds limit')
        u = np.asarray(self.assets.get(gate.matrix_asset_ref).matrix, dtype=complex)
        n = len(u)
        # Eliminate below-diagonal entries with unitary two-row rotations.
        work = u.copy()
        rotations = []
        for col in range(n):
            for row in range(n-1, col, -1):
                a, b = work[row-1, col], work[row, col]
                norm = np.hypot(abs(a), abs(b))
                if norm == 0: continue
                g = np.array([[np.conj(a)/norm, np.conj(b)/norm],
                              [-b/norm, a/norm]], complex)
                left = np.eye(n, dtype=complex)
                left[np.ix_([row-1, row], [row-1, row])] = g
                work = left @ work
                rotations.append((row-1, row, g.conj().T))
        diagonal = np.diag(work).copy()
        if np.linalg.norm(work - np.diag(diagonal)) > self.tolerance * max(1, n):
            raise ValueError('Givens factorization residual exceeds tolerance')
        ops = []

        def add_dense(targets, controls, values, matrix):
            ref = self.assets.register(matrix, len(targets))
            diagonal = bool(np.array_equal(matrix, np.diag(np.diag(matrix))))
            ops.append(GateOp('DENSE_UNITARY', tuple(targets), tuple(controls), tuple(values),
                              matrix_asset_ref=ref, source_op_id=gate.op_id,
                              diagonal=diagonal, approximation_error=gate.approximation_error if not ops else 0.0))

        outer = list(zip(gate.controls, gate.control_values))
        # Diagonal basis phases: each is a controlled scalar identity. The
        # target's two entries are equal, so its value is immaterial.
        base_target = gate.targets[0]
        for basis, phase in enumerate(diagonal):
            controls = list(outer)
            for bit, q in enumerate(gate.targets[1:], start=1): controls.append((q, (basis >> bit) & 1))
            # Encode basis bit zero on target through a conjugating X around a
            # diagonal two-level action when needed.
            if basis & 1:
                add_dense((base_target,), tuple(q for q, _ in controls), tuple(v for _, v in controls), np.array([[1, 0], [0, phase]], complex))
            else:
                add_dense((base_target,), tuple(q for q, _ in controls), tuple(v for _, v in controls), np.array([[phase, 0], [0, 1]], complex))
        if len(ops) > self.max_operations:
            raise ValueError(f'structured lowering exceeds operation limit {self.max_operations}')
        # Apply factors in reverse elimination order: U = G1^dag ... Gm^dag D.
        for a, b, rot in reversed(rotations):
            # Gray path swaps take |a>,|b> to states differing only in LSB.
            gray = [a]
            x = a ^ b
            for bit in range(k):
                if (x >> bit) & 1: gray.append(gray[-1] ^ (1 << bit))
            path = []
            for x0, x1 in zip(gray[:-2], gray[1:-1]):
                bit = (x0 ^ x1).bit_length()-1
                target = gate.targets[bit]
                controls = list(outer)
                for j, q in enumerate(gate.targets):
                    if j != bit: controls.append((q, (x0 >> j) & 1))
                path.append((target, tuple(q for q, _ in controls), tuple(v for _, v in controls)))
            for target, cs, vs in path:
                ops.append(GateOp('MCX', (target,), cs, vs, source_op_id=gate.op_id,
                                  permutation=True, clifford=len(cs) <= 1))
            # The endpoint pair after the path differs on the first changed bit.
            bit = (gray[-2] ^ gray[-1]).bit_length()-1
            target = gate.targets[bit]
            controls = list(outer)
            for j, q in enumerate(gate.targets):
                if j != bit: controls.append((q, (gray[-2] >> j) & 1))
            add_dense((target,), tuple(q for q, _ in controls), tuple(v for _, v in controls), rot)
            for target, cs, vs in reversed(path):
                ops.append(GateOp('MCX', (target,), cs, vs, source_op_id=gate.op_id,
                                  permutation=True, clifford=len(cs) <= 1))
            if len(ops) > self.max_operations:
                raise ValueError(f'structured lowering exceeds operation limit {self.max_operations}')
        # Verify by composing the represented operation matrices on the small
        # target register; reject a candidate instead of publishing bad work.
        rebuilt = _compose_local(ops, gate.targets, self.assets)
        residual = np.linalg.norm(rebuilt-u, 'fro') / max(1.0, np.linalg.norm(u, 'fro'))
        if residual > self.tolerance * 10:
            raise ValueError(f'structured lowering reconstruction residual {residual}')
        if ops:
            ops[0] = replace(ops[0], approximation_error=gate.approximation_error + residual)
        return tuple(ops)


def _compose_local(ops, targets, assets):
    n = 1 << len(targets)
    positions = {q: i for i, q in enumerate(targets)}
    total = np.eye(n, dtype=complex)
    for op in ops:
        step = np.zeros((n, n), complex)
        mat = assets.get(op.matrix_asset_ref).matrix if op.kind == 'DENSE_UNITARY' else None
        for col in range(n):
            local_controls = [(positions[q], v) for q, v in zip(op.controls, op.control_values) if q in positions]
            if not all(((col >> q) & 1) == v for q, v in local_controls):
                step[col, col] = 1
            elif op.kind == 'MCX': step[col ^ (1 << positions[op.targets[0]]), col] = 1
            else:
                t = positions[op.targets[0]]
                for bit in range(2): step[(col & ~(1 << t)) | (bit << t), col] = mat[bit, (col >> t) & 1]
        total = step @ total
    return total
