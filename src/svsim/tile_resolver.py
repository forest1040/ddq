"""Index and metadata checks for the bounded single-rank tile route."""
from dataclasses import dataclass
import math

from .fused_swap import permute_index


@dataclass(frozen=True)
class TileRouteCertificate:
    pairs: tuple
    num_qubits: int
    block_bits: int
    tile_bytes: int
    alignment: int
    inverse_witness: str = 'reverse-ordered-self-inverse-swaps'
    frame_class: str = 'identity-only'
    rank_count: int = 1


def validate_pairs(pairs, n):
    pairs = tuple(tuple(pair) for pair in pairs)
    if not pairs or len(pairs) > 2:
        raise ValueError('Tile SWAP accepts one or two ordered bit pairs')
    if any(len(p) != 2 or any(type(q) is not int or not 0 <= q < n for q in p) for p in pairs):
        raise ValueError('Invalid physical bit pair')
    return pairs


def source_index(destination, pairs):
    return permute_index(destination, tuple(reversed(pairs)))


def certify(sim, pairs, tile_bytes):
    pairs = validate_pairs(pairs, sim.num_qubits)
    align = sim.config.alignment
    if type(tile_bytes) is not int or tile_bytes < align or tile_bytes % align:
        raise ValueError('tile_bytes must be a positive multiple of storage alignment')
    if sim.store.__class__.__name__ != 'FileBlockStore':
        raise ValueError('UNSUPPORTED_TILE_STORE: only FileBlockStore is supported')
    rank_count = getattr(sim, 'size', 1)
    return TileRouteCertificate(pairs, sim.num_qubits, sim.block_bits, tile_bytes, align,
                                rank_count=rank_count)


def require_identity_view(view, sim=None):
    if view.frame_id or view.base_log:
        raise ValueError('UNSUPPORTED_FRAME_TILE_ROUTE: tile route requires identity Frame and empty log')
    if view.frame_asset_id:
        # Phase2's canonical identity adaptation records its source coordinate.
        # Accept only that exact identity asset and matching source version.
        if view.base_coordinate_id != view.version_id:
            raise ValueError('UNSUPPORTED_FRAME_TILE_ROUTE: unresolved coordinate transform')
        identity = getattr(getattr(sim, 'phase2', None), 'identity', None)
        if identity is None or view.frame_asset_id != identity.content_id:
            raise ValueError('UNSUPPORTED_FRAME_TILE_ROUTE: non-identity Frame')
    phase = complex(view.phase)
    if not math.isfinite(phase.real) or not math.isfinite(phase.imag):
        raise ValueError('UNSUPPORTED_FRAME_TILE_ROUTE: unresolved scalar phase')
    if view.phase_value is not None and complex(view.phase_value.evaluate()) != phase:
        raise ValueError('UNSUPPORTED_FRAME_TILE_ROUTE: unresolved scalar phase')
