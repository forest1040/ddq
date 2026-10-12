"""Validated immutable matrix assets with explicit little-endian target order."""
from dataclasses import dataclass
from hashlib import sha256
import numpy as np


@dataclass(frozen=True)
class MatrixAsset:
    asset_id: str
    dimension: int
    data: bytes
    dtype: str = '<c16'
    target_order: str = 'targets[0] is the least significant matrix bit'

    @property
    def matrix(self):
        return np.frombuffer(self.data, dtype=self.dtype).reshape(self.dimension, self.dimension)


class UnitaryValidator:
    def __init__(self, tolerance=1e-12, max_qubits=4, max_bytes=1 << 20):
        if tolerance < 0 or not np.isfinite(tolerance): raise ValueError('Invalid tolerance')
        self.tolerance, self.max_qubits, self.max_bytes = tolerance, max_qubits, max_bytes

    def validate(self, matrix, num_targets):
        if not 1 <= num_targets <= self.max_qubits: raise ValueError('Dense unitary width exceeds limit')
        size = 1 << num_targets
        if size * size * 16 > self.max_bytes: raise ValueError('Matrix memory limit exceeded')
        u = np.asarray(matrix, dtype='<c16')
        if u.shape != (size, size): raise ValueError('Matrix shape does not match targets')
        if not np.all(np.isfinite(u)): raise ValueError('Matrix contains non-finite values')
        residual = np.linalg.norm(u.conj().T @ u - np.eye(size), 'fro') / np.sqrt(size)
        if residual > self.tolerance: raise ValueError(f'Matrix is not unitary: residual={residual}')
        return u


class MatrixAssetRegistry:
    def __init__(self, validator=None):
        self.validator = validator or UnitaryValidator()
        self.assets = {}

    def register(self, matrix, num_targets):
        u = self.validator.validate(matrix, num_targets)
        data = u.tobytes(order='C')
        key = sha256(str(u.shape).encode() + b'<c16:lsb-first' + data).hexdigest()
        self.assets.setdefault(key, MatrixAsset(key, len(u), data))
        return key

    def get(self, asset_id): return self.assets[asset_id]
