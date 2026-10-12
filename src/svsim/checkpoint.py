"""Immutable committed cuts and storage-backed logical state handles."""
from dataclasses import dataclass
from .layout import QubitLayout


@dataclass(frozen=True)
class SnapshotManifest:
    snapshot_id: str
    root: str
    frontier: int
    circuit_hash: str
    initial_state_hash: str
    layout: tuple[int, ...]
    layout_id: int
    binding_values: tuple[tuple[str, float], ...]
    parameter_dependencies: tuple[str, ...]
    journal_cursors: dict[str, int]
    assets: tuple[str, ...]
    prefix_fingerprint: str | None = None
    schema_version: int = 1
    binding_witnesses: tuple = ()
    semantics_version: int = 1

    @classmethod
    def from_data(cls, data):
        data = dict(data)
        if data.get('schema_version') not in {1,2}: raise ValueError('Unsupported snapshot schema')
        if data.get('semantics_version',1) not in {1,2}: raise ValueError('Unsupported snapshot semantics')
        for key in ('layout','parameter_dependencies','assets'): data[key] = tuple(data[key])
        data['binding_values'] = tuple(tuple(x) for x in data['binding_values'])
        data['binding_witnesses'] = tuple(tuple(x) for x in data.get('binding_witnesses',()))
        return cls(**data)


@dataclass(frozen=True)
class StoredStateHandle:
    snapshot_id: str
    root: str
    frontier: int
    binding_id: str
    circuit_hash: str
    layout: QubitLayout
    role: str = 'forward'
