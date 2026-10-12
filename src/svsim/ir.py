"""Semantic gates are the source of truth; lowering creates derived operations."""
from dataclasses import dataclass, replace
import operator
import math
from .parameters import Expr, expression
from .unitary import MatrixAssetRegistry


@dataclass(frozen=True)
class GateOp:
    kind: str
    targets: tuple[int, ...]
    controls: tuple[int, ...] = ()
    control_values: tuple[int, ...] = ()
    angle: Expr | None = None
    matrix_asset_ref: str | None = None
    op_id: int = -1
    diagonal: bool = False
    permutation: bool = False
    clifford: bool = False
    approximation_error: float = 0.0
    source_op_id: int | None = None
    paulis: tuple[str, ...] = ()

    def __post_init__(self):
        for name in ('targets', 'controls', 'control_values', 'paulis'):
            object.__setattr__(self, name, tuple(getattr(self, name)))

    @property
    def dependencies(self): return self.angle.dependencies if self.angle else frozenset()
    @property
    def qubits(self): return self.targets + self.controls


class GateNormalizer:
    def __init__(self, num_qubits, assets=None):
        if num_qubits < 1: raise ValueError('At least one qubit required')
        self.num_qubits = num_qubits
        self.assets = assets or MatrixAssetRegistry()

    def gate(self, kind, targets, *, controls=(), control_values=None, angle=None,
             matrix=None, approximation_error=0.0, paulis=()):
        name = kind.upper()
        targets = tuple(operator.index(q) for q in targets)
        controls = tuple(operator.index(q) for q in controls)
        if name in {'CX', 'CNOT', 'CCX', 'TOFFOLI', 'CCCX'}:
            count = {'CX': 1, 'CNOT': 1, 'CCX': 2, 'TOFFOLI': 2, 'CCCX': 3}[name]
            if not controls:
                if len(targets) != count + 1: raise ValueError('Invalid controlled X arity')
                controls, targets = targets[:-1], targets[-1:]
            if len(controls) != count: raise ValueError('Invalid controlled X controls')
            name = 'MCX'
        if name == 'X': name = 'MCX'
        if name not in {'H', 'S', 'SDG', 'Z', 'T', 'TDG', 'RX', 'RY', 'RZ', 'CZ', 'SWAP', 'MCX', 'DENSE_UNITARY', 'PAULI_ROT'}:
            raise ValueError(f'Unsupported gate {kind}')
        if not targets or len(set(targets + controls)) != len(targets + controls):
            raise ValueError('Targets and controls must be distinct')
        if any(q < 0 or q >= self.num_qubits for q in targets + controls): raise ValueError('Qubit out of range')
        required = 2 if name in {'CZ', 'SWAP'} else 1
        if name not in {'DENSE_UNITARY','PAULI_ROT'} and len(targets) != required: raise ValueError('Invalid target arity')
        paulis = tuple(str(p).upper() for p in paulis)
        if name == 'PAULI_ROT':
            if len(paulis) != len(targets) or any(p not in {'I','X','Y','Z'} for p in paulis):
                raise ValueError('One valid Pauli letter per target required')
            if controls: raise ValueError('Controlled PAULI_ROT is unsupported')
        elif paulis: raise ValueError('Paulis only allowed for PAULI_ROT')
        if controls and name in {'CZ', 'SWAP'}: raise ValueError('Extra controls unsupported for this gate')
        values = tuple(1 for _ in controls) if control_values is None else tuple(operator.index(v) for v in control_values)
        if len(values) != len(controls) or any(v not in (0, 1) for v in values): raise ValueError('Invalid control values')
        if name in {'RX', 'RY', 'RZ', 'PAULI_ROT'}:
            if angle is None: raise ValueError('Rotation needs angle')
            angle = expression(angle)
        elif angle is not None: raise ValueError('Angle only allowed for rotations')
        if approximation_error < 0 or not math.isfinite(approximation_error): raise ValueError('Invalid error budget')
        asset = None
        if name == 'DENSE_UNITARY':
            if matrix is None: raise ValueError('Missing matrix')
            asset = self.assets.register(matrix, len(targets))
        elif matrix is not None: raise ValueError('Matrix only allowed for DENSE_UNITARY')
        diagonal = name in {'S', 'SDG', 'Z', 'T', 'TDG', 'RZ', 'CZ'} or name == 'PAULI_ROT' and all(p in {'I','Z'} for p in paulis)
        permutation = name in {'MCX', 'SWAP'}
        clifford = name in {'H', 'S', 'SDG', 'Z', 'CZ', 'SWAP'} or name == 'MCX' and len(controls) <= 1
        if controls and name != 'MCX': clifford = False
        if asset:
            import numpy as np
            u = self.assets.get(asset).matrix
            diagonal = np.array_equal(u, np.diag(np.diag(u)))
            permutation = bool(np.all((u == 0) | (u == 1)) and np.all(np.sum(u == 1, axis=0) == 1))
        return GateOp(name, targets, controls, values, angle, asset, diagonal=diagonal,
                      permutation=permutation, clifford=clifford, approximation_error=approximation_error, paulis=paulis)


class GlobalSequencer:
    def __init__(self): self.operations = []
    def append(self, gate):
        if gate.op_id != -1: raise ValueError('Gate already sequenced')
        op = replace(gate, op_id=len(self.operations))
        self.operations.append(op)
        return op


class NativeGateSelector:
    def select(self, gate, layout, block_bits):
        targets = tuple(layout.logical_to_physical[q] for q in gate.targets)
        if gate.kind == 'MCX': return 'NATIVE_MCX_CONDITIONAL_SWAP'
        if gate.diagonal: return 'NATIVE_DIAGONAL'
        if all(q < block_bits for q in targets): return 'NATIVE_LOCAL'
        return 'NATIVE_JOINT'


class FallbackDecomposer:
    """Exact lowering for the initial named gate set; no approximate U synthesis."""
    def decompose(self, gate):
        if gate.controls and not (gate.kind == 'MCX' and len(gate.controls) <= 1):
            raise NotImplementedError('Controlled gates and multi-control X remain native')
        def g(kind, targets=gate.targets, angle=None, controls=(), values=()):
            return GateOp(kind, targets, controls, values, angle, source_op_id=gate.op_id,
                          approximation_error=0.0)
        k = gate.kind
        if k in {'H', 'S', 'T', 'RZ'}: result = [replace(gate, op_id=-1, source_op_id=gate.op_id)]
        elif k in {'SDG', 'TDG', 'Z'}:
            result = [g('S' if k != 'TDG' else 'T')] * {'SDG': 3, 'TDG': 7, 'Z': 2}[k]
        elif k == 'MCX':
            if not gate.controls: result = [g('H'), g('S'), g('S'), g('H')]
            else:
                # Negative control is conjugation by X on the control.
                c = gate.controls[0]
                flips = [g('H', (c,)), g('S', (c,)), g('S', (c,)), g('H', (c,))]
                result = ([] if gate.control_values[0] else flips) + [g('MCX', controls=(c,), values=(1,))] + ([] if gate.control_values[0] else flips)
        elif k == 'CZ':
            c, t = gate.targets
            result = [g('H', (t,)), g('MCX', (t,), controls=(c,), values=(1,)), g('H', (t,))]
        elif k == 'SWAP':
            a, b = gate.targets
            result = [g('MCX', (b,), controls=(a,), values=(1,)), g('MCX', (a,), controls=(b,), values=(1,)), g('MCX', (b,), controls=(a,), values=(1,))]
        elif k == 'RX': result = [g('H'), g('RZ', angle=gate.angle), g('H')]
        elif k == 'RY': result = [g('SDG'), g('H'), g('RZ', angle=gate.angle), g('H'), g('S')]
        else: raise NotImplementedError(f'No exact initial fallback for {k}')
        # Recursively remove helper SDG, retaining original provenance/error budget.
        out = []
        for op in result:
            if op.kind == 'SDG': out.extend([g('S')] * 3)
            else: out.append(op)
        if out: out[0] = replace(out[0], approximation_error=gate.approximation_error)
        return tuple(out)
