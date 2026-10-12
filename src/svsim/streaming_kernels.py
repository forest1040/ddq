"""Kernels for caller-owned blocks and compact upper-bit groups.

No kernel constructs a state-sized index array or copies an input block.  The
caller owns input/output buffers and may publish outputs only after success.
"""
import numpy as np

from .kernels import gate_matrix
from .parameters import ParameterBinding


def _control_match(index, controls, values):
    return all(((index >> q) & 1) == v for q, v in zip(controls, values))


def apply_local(buffer, gate, assets, binding=None, *, block_id=0, block_bits=None):
    """Apply a gate in-place to one immutable-sized block buffer.

    Targets/controls are physical bit positions.  Bits below ``block_bits``
    index the buffer; higher bits are read from ``block_id``.  Upper target
    gates must be diagonal.  MCX permits upper controls and a local target.
    """
    binding = binding or ParameterBinding('')
    if block_bits is None:
        block_bits = (len(buffer)-1).bit_length()
    if len(buffer) != 1 << block_bits:
        raise ValueError('buffer length must equal 2**block_bits')
    upper_targets = [q for q in gate.targets if q >= block_bits]
    if gate.kind == 'MCX':
        target = gate.targets[0]
        if target >= block_bits: raise ValueError('upper MCX target requires a group kernel')
        if any(((block_id >> (q-block_bits)) & 1) != v
               for q, v in zip(gate.controls, gate.control_values) if q >= block_bits):
            return
        cm = sum(1 << q for q in gate.controls if q < block_bits)
        cv = sum(v << q for q, v in zip(gate.controls, gate.control_values) if q < block_bits)
        tm = 1 << target
        for i in range(len(buffer)):
            if i & tm == 0 and i & cm == cv:
                j = i | tm
                buffer[i], buffer[j] = buffer[j], buffer[i]
        return buffer
    for q, v in zip(gate.controls, gate.control_values):
        if q >= block_bits and ((block_id >> (q-block_bits)) & 1) != v:
            return buffer
    if gate.diagonal:
        mat = gate_matrix(gate, assets, binding)
        diagonal = np.diag(mat)
        local_controls = tuple(q for q in gate.controls if q < block_bits)
        local_values = tuple(v for q, v in zip(gate.controls, gate.control_values) if q < block_bits)
        for i in range(len(buffer)):
            if not _control_match(i, local_controls, local_values):
                continue
            col = 0
            for j, q in enumerate(gate.targets):
                bit = ((i >> q) & 1) if q < block_bits else ((block_id >> (q-block_bits)) & 1)
                col |= bit << j
            buffer[i] *= diagonal[col]
        return buffer
    if upper_targets: raise ValueError('upper targets require a diagonal gate')
    mat = gate_matrix(gate, assets, binding)
    n = len(gate.targets)
    offsets = [sum(((x >> j) & 1) << q for j, q in enumerate(gate.targets)) for x in range(1 << n)]
    target_mask = sum(1 << q for q in gate.targets)
    controls = tuple(q for q in gate.controls if q < block_bits)
    values = tuple(v for q, v in zip(gate.controls, gate.control_values) if q < block_bits)
    for base in range(len(buffer)):
        if base & target_mask or not _control_match(base, controls, values): continue
        src = [buffer[base | off] for off in offsets]
        for row, off in enumerate(offsets):
            buffer[base | off] = sum(mat[row, col] * src[col] for col in range(len(src)))
    return buffer


def apply_group(inputs, outputs, gate, block_bits, upper_targets, assets, binding=None):
    """Apply a gate across a compact group of blocks.

    ``inputs`` and ``outputs`` are sequences of ``2**len(upper_targets)``
    equally sized arrays.  Group bit j corresponds to physical upper bit
    ``upper_targets[j]``. Inputs are read-only by contract; every output slot
    is assigned from those inputs. All gate targets must be local or listed
    in upper_targets. Controls outside this compact group are expected to have
    been filtered by the caller.
    """
    binding = binding or ParameterBinding('')
    upper_targets = tuple(upper_targets)
    count = 1 << len(upper_targets)
    if len(inputs) != count or len(outputs) != count: raise ValueError('group size mismatch')
    size = 1 << block_bits
    if any(len(x) != size for x in (*inputs, *outputs)): raise ValueError('block size mismatch')
    if any(q >= block_bits and q not in upper_targets for q in gate.targets):
        raise ValueError('missing upper target block')
    if any(q >= block_bits and q not in upper_targets for q in gate.controls):
        raise ValueError('upper control must be filtered by caller')
    local_targets = tuple(q for q in gate.targets if q < block_bits)
    target_positions = tuple((q if q < block_bits else block_bits + upper_targets.index(q)) for q in gate.targets)
    control_positions = tuple(q if q < block_bits else block_bits + upper_targets.index(q) for q in gate.controls)
    if gate.kind == 'MCX':
        t = target_positions[0]
        for g in range(count):
            for i in range(size):
                full = (g << block_bits) | i
                dest = full
                if all(((full >> q) & 1) == v for q, v in zip(control_positions, gate.control_values)):
                    dest ^= 1 << t
                outputs[dest >> block_bits][dest & (size-1)] = inputs[g][i]
        return outputs
    mat = gate_matrix(gate, assets, binding)
    offsets = [sum(((x >> j) & 1) << q for j, q in enumerate(target_positions)) for x in range(1 << len(target_positions))]
    mask = sum(1 << q for q in target_positions)
    for g in range(count):
        for i in range(size):
            full = (g << block_bits) | i
            controls_match = all(((full >> q) & 1) == v for q, v in zip(control_positions, gate.control_values))
            if full & mask:
                if not controls_match: outputs[g][i] = inputs[g][i]
                continue  # a matching target-zero base assigns this tuple
            if not controls_match:
                outputs[g][i] = inputs[g][i]
                continue
            src = []
            for off in offsets:
                fg = full | off
                og, oi = fg >> block_bits, fg & (size-1)
                src.append(inputs[og][oi])
            for row, off in enumerate(offsets):
                fg = full | off
                outputs[fg >> block_bits][fg & (size-1)] = sum(mat[row, col] * src[col] for col in range(len(src)))
    return outputs
