"""Small, dependency-free CPU calibration for Phase 2 frame operations."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import math
import operator
import platform
import sys
from time import perf_counter

import numpy as np

from .clifford import CliffordTableau
from .frame import FrameAsset
from .ir import GateOp
from .pauli_rotation import SignedPauli, apply_pauli_rotation
from .streaming_kernels import apply_local


@dataclass(frozen=True, slots=True)
class FrameCalibration:
    profile_id: str
    version: int
    block_bits: int
    frame_gate_seconds: float
    pauli_rotation_seconds: float
    conjugation_seconds: float
    synthesis_seconds: float
    tableau_update_seconds: float

    def __post_init__(self):
        if not self.profile_id:
            raise ValueError("profile_id must be nonempty")
        if self.version != 1:
            raise ValueError("Unsupported frame calibration version")
        if not 1 <= self.block_bits <= 8:
            raise ValueError("block_bits must be in [1, 8]")
        for name in ("frame_gate_seconds", "pauli_rotation_seconds", "conjugation_seconds",
                     "synthesis_seconds", "tableau_update_seconds"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive and finite")
            object.__setattr__(self, name, value)

    def cost(self, operation: str, count: int = 1) -> float:
        """Return a measured CPU-cost estimate for a named operation count."""
        try:
            count = operator.index(count)
        except TypeError as exc:
            raise ValueError("count must be a positive integer") from exc
        if count < 1:
            raise ValueError("count must be a positive integer")
        key = operation.removesuffix("_seconds")
        field = f"{key}_seconds"
        if field not in {"frame_gate_seconds", "pauli_rotation_seconds", "conjugation_seconds",
                         "synthesis_seconds", "tableau_update_seconds"}:
            raise ValueError(f"Unknown frame operation {operation!r}")
        return getattr(self, field) * count

    def data(self) -> dict[str, object]:
        return asdict(self)


def _cpu_profile_id() -> str:
    identity = (platform.system(), platform.machine(), platform.processor(),
                platform.python_version(), str(__import__("os").cpu_count()))
    digest = hashlib.sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()[:16]
    return f"cpu-{digest}"


def _elapsed(callable_, repetitions: int) -> float:
    start = perf_counter()
    for _ in range(repetitions):
        callable_()
    elapsed = (perf_counter() - start) / repetitions
    # Keep very fast operations representable as finite positive estimates.
    return max(elapsed, sys.float_info.min)


def calibrate_frames(block_bits: int = 4, repetitions: int = 3) -> FrameCalibration:
    """Measure small-block frame and Pauli operations with bounded memory.

    The temporary state contains at most 256 complex amplitudes. Calibration
    is local CPU work and does not inspect or modify simulator storage.
    """
    try:
        block_bits = operator.index(block_bits)
        repetitions = operator.index(repetitions)
    except TypeError as exc:
        raise ValueError("block_bits and repetitions must be integers") from exc
    if not 1 <= block_bits <= 8:
        raise ValueError("block_bits must be in [1, 8]")
    if not 1 <= repetitions <= 100:
        raise ValueError("repetitions must be in [1, 100]")

    gates = [GateOp("H", (0,), clifford=True)]
    if block_bits >= 2:
        gates.extend((GateOp("S", (1,), clifford=True, diagonal=True),
                      GateOp("MCX", (1,), controls=(0,), control_values=(1,), clifford=True)))
    tableau = CliffordTableau.identity(block_bits)
    for gate in gates:
        tableau = tableau.then(gate)
    asset = FrameAsset.from_tableau(tableau)
    pauli = SignedPauli(1 | (2 if block_bits >= 2 else 0), 1)
    buffer = np.ones(1 << block_bits, dtype=np.complex128)

    def frame_gates():
        for gate in gates:
            apply_local(buffer, gate, None, block_bits=block_bits)

    frame_gate_seconds = _elapsed(frame_gates, repetitions) / len(gates)
    pauli_rotation_seconds = _elapsed(
        lambda: apply_pauli_rotation(buffer, pauli, 0.37, block_bits=block_bits), repetitions)
    conjugation_seconds = _elapsed(lambda: tableau.conjugate_inverse(pauli), repetitions)
    synthesis_seconds = _elapsed(tableau.synthesis, repetitions)
    tableau_update_seconds = _elapsed(lambda: asset.left_update(gates[0]), repetitions)
    return FrameCalibration(_cpu_profile_id(), 1, block_bits, frame_gate_seconds,
                            pauli_rotation_seconds, conjugation_seconds,
                            synthesis_seconds, tableau_update_seconds)
