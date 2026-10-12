"""Small block-pair kernels used by distributed execution and adjoints.

Block IDs contain the physical bits above ``block_bits``; local amplitude
indices use the ordinary little-endian physical bit convention.
"""
from __future__ import annotations

import math
import operator
from dataclasses import replace

import numpy as np


def _bits(value, name):
    try:
        value = operator.index(value)
    except TypeError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 0:
        raise ValueError(f"{name} must be nonnegative")
    return value


def _controls(block_bits, controls, control_values):
    b = _bits(block_bits, "block_bits")
    cs = tuple(_bits(c, "control") for c in controls)
    vs = tuple(operator.index(v) for v in control_values)
    if len(cs) != len(vs) or any(v not in (0, 1) for v in vs):
        raise ValueError("controls and binary control_values must have equal length")
    if len(set(cs)) != len(cs) or any(c >= b for c in cs):
        raise ValueError("controls must be distinct lower bits")
    return b, cs, vs


def active_offsets(block_bits, controls=(), control_values=()):
    """Yield local indices matching controls, in ascending order, lazily."""
    b, cs, vs = _controls(block_bits, controls, control_values)
    mask = sum(1 << c for c in cs)
    value = sum(v << c for c, v in zip(cs, vs))
    for i in range(1 << b):
        if i & mask == value:
            yield i


def _vector(a, n, name):
    arr = np.asarray(a)
    if arr.ndim != 1 or len(arr) != n:
        raise ValueError(f"{name} must be a one-dimensional block of length {n}")
    return arr


def pack_active(input, output, offsets):
    """Copy selected elements into the caller-owned output prefix; return count."""
    src = np.asarray(input)
    dst = np.asarray(output)
    if src.ndim != 1 or dst.ndim != 1:
        raise ValueError("input and output must be one-dimensional")
    count = 0
    for offset in offsets:
        i = _bits(offset, "offset")
        if i >= len(src):
            raise ValueError("active offset outside input")
        if count >= len(dst):
            raise ValueError("output buffer is too small")
        dst[count] = src[i]
        count += 1
    return count


def _no_alias(inputs, outputs):
    if any(np.shares_memory(o, i) for o in outputs for i in inputs):
        raise ValueError("outputs must not alias inputs")
    if len(outputs) == 2 and np.shares_memory(outputs[0], outputs[1]):
        raise ValueError("pair outputs must not alias each other")


def partial_mcx_pair(input_a, input_b, out_a, out_b, controls=(), control_values=()):
    """Exchange amplitudes at lower-control offsets between an upper-bit pair."""
    a = np.asarray(input_a); b = np.asarray(input_b)
    oa = np.asarray(out_a); ob = np.asarray(out_b)
    if a.ndim != 1 or b.shape != a.shape or oa.shape != a.shape or ob.shape != a.shape:
        raise ValueError("all buffers must be equal-length one-dimensional blocks")
    if a.size == 0 or a.size & (a.size - 1):
        raise ValueError("block size must be a positive power of two")
    block_bits = a.size.bit_length() - 1
    _, cs, vs = _controls(block_bits, controls, control_values)
    _no_alias((a, b), (oa, ob))
    oa[:] = a
    ob[:] = b
    for i in active_offsets(block_bits, cs, vs):
        oa[i], ob[i] = b[i], a[i]
    return oa, ob


def _phase(x_mask, z_mask, source_index):
    y_count = (x_mask & z_mask).bit_count()
    parity = (z_mask & source_index).bit_count() & 1
    return (1j ** y_count) * (-1 if parity else 1)


def _projected(source_index, controls, control_values):
    return all(((source_index >> c) & 1) == v for c, v in zip(controls, control_values))


def pauli_inner(bra_partner, ket, x_mask, z_mask, source_block, block_bits,
                controls=(), control_values=()):
    """Compensated complex sum ``<bra|P|ket>`` over one source block.

    ``bra_partner`` is the block at ``source_block XOR upper(X)``. Optional
    controls project on the full computational source index.
    """
    x = _bits(x_mask, "x_mask"); z = _bits(z_mask, "z_mask")
    b = _bits(block_bits, "block_bits"); h = _bits(source_block, "source_block")
    lowmask = (1 << b) - 1
    xs, zs = x & lowmask, z
    ket = np.asarray(ket); bra = np.asarray(bra_partner)
    if ket.ndim != 1 or bra.shape != ket.shape or len(ket) != 1 << b:
        raise ValueError("bra and ket must be blocks matching block_bits")
    cs = tuple(_bits(c, "control") for c in controls)
    vs = tuple(operator.index(v) for v in control_values)
    if len(cs) != len(vs) or any(v not in (0, 1) for v in vs) or len(set(cs)) != len(cs):
        raise ValueError("invalid controls")
    total = 0j; correction = 0j
    upper_parity_base = ((z >> b) & h).bit_count() & 1
    y_phase = 1j ** (x & z).bit_count()
    for i, kval in enumerate(ket):
        source = (h << b) | i
        if not _projected(source, cs, vs):
            continue
        parity = (upper_parity_base + (z & i).bit_count()) & 1
        term = np.conjugate(bra[i ^ xs]) * y_phase * (-1 if parity else 1) * kval
        adjusted = term - correction
        updated = total + adjusted
        correction = (updated - total) - adjusted
        total = updated
    return total


def apply_pauli_action(input_partner, output, x_mask, z_mask, destination_block,
                       block_bits, coefficient=1, accumulate=False):
    """Write one destination block of ``coefficient * P |input>``.

    The input is the source partner block and remains read-only. In accumulate
    mode the action is added to the existing destination buffer.
    """
    x = _bits(x_mask, "x_mask"); z = _bits(z_mask, "z_mask")
    b = _bits(block_bits, "block_bits"); dest = _bits(destination_block, "destination_block")
    src_block = dest ^ (x >> b)
    src = _vector(input_partner, 1 << b, "input_partner")
    out = _vector(output, 1 << b, "output")
    if np.shares_memory(src, out):
        raise ValueError("output must not alias input_partner")
    coeff = complex(coefficient)
    if not (math.isfinite(coeff.real) and math.isfinite(coeff.imag)):
        raise ValueError("coefficient must be finite")
    lowmask = (1 << b) - 1
    xlow = x & lowmask
    for i in range(1 << b):
        source_local = i ^ xlow
        source_index = (src_block << b) | source_local
        value = coeff * _phase(x, z, source_index) * src[source_local]
        if accumulate:
            out[i] += value
        else:
            out[i] = value
    return out


def inverse_gate(gate, assets):
    """Return a semantic inverse GateOp, registering a daggered dense asset."""
    k = gate.kind
    pairs = {"S": "SDG", "SDG": "S", "T": "TDG", "TDG": "T"}
    if k in pairs:
        return replace(gate, kind=pairs[k])
    if k in {"RX", "RY", "RZ", "PAULI_ROT"}:
        return replace(gate, angle=-gate.angle)
    if k == "DENSE_UNITARY":
        matrix = assets.get(gate.matrix_asset_ref).matrix
        asset_ref = assets.register(matrix.conj().T, len(gate.targets))
        return replace(gate, matrix_asset_ref=asset_ref)
    if k in {"H", "Z", "CZ", "SWAP", "MCX"}:
        return replace(gate)
    raise ValueError(f"No semantic inverse for gate {k}")
