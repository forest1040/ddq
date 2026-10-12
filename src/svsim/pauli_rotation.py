"""Scalar-loop kernels for rotations about signed Pauli strings."""
from __future__ import annotations

from dataclasses import dataclass
import math
import operator
import numpy as np


@dataclass(frozen=True, slots=True)
class SignedPauli:
    """Hermitian Pauli ``(-1)^r i^popcount(x&z) X^x Z^z``.

    Bit zero is the least significant physical qubit.
    """

    x_mask: int
    z_mask: int
    sign_bit: int = 0

    def __post_init__(self):
        for name in ("x_mask", "z_mask"):
            value = getattr(self, name)
            try:
                value = operator.index(value)
            except TypeError as exc:
                raise ValueError(f"{name} must be a nonnegative integer") from exc
            if value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
            object.__setattr__(self, name, value)
        try:
            sign = operator.index(self.sign_bit)
        except TypeError as exc:
            raise ValueError("sign_bit must be 0 or 1") from exc
        if sign not in (0, 1):
            raise ValueError("sign_bit must be 0 or 1")
        object.__setattr__(self, "sign_bit", sign)


def _pauli(pauli):
    # Keep the kernel interoperable with an equivalent immutable type owned by
    # another module (notably tableau implementations).
    try:
        x, z, r = operator.index(pauli.x_mask), operator.index(pauli.z_mask), operator.index(pauli.sign_bit)
    except (AttributeError, TypeError) as exc:
        raise TypeError("pauli must provide integer x_mask, z_mask, and sign_bit") from exc
    if x < 0 or z < 0 or r not in (0, 1):
        raise ValueError("invalid signed Pauli")
    return x, z, r


def _shape(buffer, block_bits):
    if block_bits is None:
        block_bits = (len(buffer) - 1).bit_length()
    block_bits = operator.index(block_bits)
    if block_bits < 0 or len(buffer) != 1 << block_bits:
        raise ValueError("buffer length must equal 2**block_bits")
    return block_bits


def _phase(sign, y_count, z_parity):
    # (-1)^sign i^y_count (-1)^z_parity
    return (-1 if (sign + z_parity) & 1 else 1) * (1j ** y_count)


def apply_pauli_rotation(array, pauli, angle, *, block_id=0, block_bits=None):
    """Apply ``exp(-i angle P/2)`` in-place to one block.

    The Pauli X support must be local. Upper Z support is evaluated from
    ``block_id``; an upper X requires :func:`apply_pauli_pair`.
    """
    if not math.isfinite(angle): raise ValueError('Angle must be finite')
    bits = _shape(array, block_bits)
    x, z, sign = _pauli(pauli)
    low_mask = (1 << bits) - 1
    x_low = x & low_mask
    if x >> bits:
        raise ValueError("upper X support requires apply_pauli_pair")
    block_id = operator.index(block_id)
    if block_id < 0:
        raise ValueError("block_id must be nonnegative")
    upper_z_parity = ((z >> bits) & block_id).bit_count() & 1
    y_count = (x & z).bit_count()
    c, s = math.cos(angle / 2), math.sin(angle / 2)
    if x_low == 0:
        for i in range(len(array)):
            parity = ((z & i).bit_count() + upper_z_parity) & 1
            array[i] *= c - 1j * s * _phase(sign, y_count, parity)
        return array
    for i in range(len(array)):
        j = i ^ x_low
        if i > j:
            continue
        ai, aj = array[i], array[j]
        pi = _phase(sign, y_count, ((z & i).bit_count() + upper_z_parity) & 1)
        pj = _phase(sign, y_count, ((z & j).bit_count() + upper_z_parity) & 1)
        array[i] = c * ai - 1j * s * pj * aj
        array[j] = c * aj - 1j * s * pi * ai
    return array


def apply_pauli_pair(input_a, input_b, output_a, output_b, pauli, angle,
                     block_a, block_b, block_bits):
    """Apply a Pauli rotation across two upper-X partner blocks.

    Inputs are read-only by contract. Outputs are assigned completely and
    may not alias either input. ``block_a``/``block_b`` are upper-bit IDs.
    """
    bits = _shape(input_a, block_bits)
    if not math.isfinite(angle): raise ValueError('Angle must be finite')
    if _shape(input_b, bits) != bits or _shape(output_a, bits) != bits or _shape(output_b, bits) != bits:
        raise ValueError("all block buffers must have equal size")
    if any(out is inp or np.shares_memory(out,inp) for out in (output_a, output_b) for inp in (input_a, input_b)):
        raise ValueError("pair outputs must not alias inputs")
    if output_a is output_b or np.shares_memory(output_a,output_b): raise ValueError('Pair outputs must not alias each other')
    x, z, sign = _pauli(pauli)
    x_upper, x_low = x >> bits, x & ((1 << bits) - 1)
    if not x_upper:
        raise ValueError("Pauli must have upper X support")
    block_a, block_b = operator.index(block_a), operator.index(block_b)
    if block_a < 0 or block_b < 0 or (block_a ^ block_b) != x_upper:
        raise ValueError("blocks must be partners under the Pauli upper X mask")
    y_count = (x & z).bit_count()
    z_upper = z >> bits
    upper_parity_a = (z_upper & block_a).bit_count() & 1
    upper_parity_b = (z_upper & block_b).bit_count() & 1
    c, s = math.cos(angle / 2), math.sin(angle / 2)
    for i in range(len(input_a)):
        j = i ^ x_low
        pa = _phase(sign, y_count, (upper_parity_b + (z & j).bit_count()) & 1)
        pb = _phase(sign, y_count, (upper_parity_a + (z & j).bit_count()) & 1)
        output_a[i] = c * input_a[i] - 1j * s * pa * input_b[j]
        output_b[i] = c * input_b[i] - 1j * s * pb * input_a[j]
    return output_a, output_b
