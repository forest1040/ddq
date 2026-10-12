"""Small native kernels, with MCX implemented as predicate-selected swaps."""
import numpy as np
from .parameters import ParameterBinding


def gate_matrix(gate, assets, binding):
    k = gate.kind
    if k == 'DENSE_UNITARY': return assets.get(gate.matrix_asset_ref).matrix
    if k == 'H': return np.array([[1, 1], [1, -1]], complex) / np.sqrt(2)
    if k in {'S', 'SDG', 'Z', 'T', 'TDG'}:
        phase = {'S': 1j, 'SDG': -1j, 'Z': -1, 'T': np.exp(1j*np.pi/4), 'TDG': np.exp(-1j*np.pi/4)}[k]
        return np.diag([1, phase])
    if k in {'RX', 'RY', 'RZ'}:
        theta = gate.angle.evaluate(binding)
        c, s = np.cos(theta/2), np.sin(theta/2)
        if k == 'RX': return np.array([[c, -1j*s], [-1j*s, c]])
        if k == 'RY': return np.array([[c, -s], [s, c]])
        return np.diag([np.exp(-1j*theta/2), np.exp(1j*theta/2)])
    if k == 'CZ': return np.diag([1, 1, 1, -1])
    if k == 'SWAP': return np.array([[1,0,0,0], [0,0,1,0], [0,1,0,0], [0,0,0,1]], complex)
    raise ValueError(f'No dense matrix kernel for {k}')


def apply_matrix(vector, matrix, targets, controls=(), control_values=()):
    """Target tuple defines matrix bit order, targets[0] is its LSB."""
    out = vector.copy()
    target_mask = sum(1 << q for q in targets)
    control_mask = sum(1 << q for q in controls)
    control_value = sum(v << q for q, v in zip(controls, control_values))
    bases = np.arange(len(vector), dtype=np.int64)
    bases = bases[((bases & target_mask) == 0) & ((bases & control_mask) == control_value)]
    offsets = np.array([sum(((i >> bit) & 1) << q for bit, q in enumerate(targets))
                        for i in range(1 << len(targets))])
    indices = bases[:, None] | offsets
    out[indices] = vector[indices] @ matrix.T
    return out


def apply_gate(vector, gate, assets, binding=None):
    binding = binding or ParameterBinding('')
    if gate.kind == 'MCX':
        indices = np.arange(len(vector), dtype=np.int64)
        cm = sum(1 << c for c in gate.controls)
        cv = sum(v << c for c, v in zip(gate.controls, gate.control_values))
        tm = 1 << gate.targets[0]
        selected = indices[((indices & tm) == 0) & ((indices & cm) == cv)]
        partners = selected | tm
        out = vector.copy()
        out[selected], out[partners] = vector[partners], vector[selected]
        return out
    return apply_matrix(vector, gate_matrix(gate, assets, binding), gate.targets,
                        gate.controls, gate.control_values)


def eager_reference(gates, num_qubits, assets, binding=None, initial_state=None):
    """Independent scalar indexing oracle for the blocked execution path."""
    state = np.zeros(1 << num_qubits, complex) if initial_state is None else np.array(initial_state, complex, copy=True)
    if initial_state is None: state[0] = 1
    for gate in gates:
        out = state.copy()
        if gate.kind == 'MCX':
            for i in range(len(state)):
                if all(((i >> q) & 1) == v for q, v in zip(gate.controls, gate.control_values)):
                    out[i ^ (1 << gate.targets[0])] = state[i]
        else:
            u = gate_matrix(gate, assets, binding or ParameterBinding(''))
            for i in range(len(state)):
                if not all(((i >> q) & 1) == v for q, v in zip(gate.controls, gate.control_values)): continue
                column = sum(((i >> q) & 1) << j for j, q in enumerate(gate.targets))
                base = i & ~sum(1 << q for q in gate.targets)
                # Each output is assigned once per target group.
                if column: continue
                for row in range(len(u)):
                    dest = base | sum(((row >> j) & 1) << q for j, q in enumerate(gate.targets))
                    out[dest] = sum(u[row, col] * state[base | sum(((col >> j) & 1) << q for j, q in enumerate(gate.targets))]
                                    for col in range(len(u)))
        state = out
    return state
