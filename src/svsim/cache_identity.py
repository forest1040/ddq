"""Conservative identities for raw and resolved cache values.

Resolved-buffer sharing is deliberately unsupported until reader lifetime is
tracked. These identities prove value equivalence only; they grant no lifetime.
The frozen container representation is a comparison aid for current metadata,
not a canonical wire format or authorization to share resolved buffers.
"""
from dataclasses import dataclass
import math
from dataclasses import fields, is_dataclass
from collections.abc import Mapping


def _id(value, label):
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValueError(f'{label} must be a nonempty bounded string')
    return value


def _freeze(value):
    """Convert supported metadata into a stable, recursively immutable value."""
    if value is None or isinstance(value, (str, int, bool)): return value
    if isinstance(value, float):
        if not math.isfinite(value): raise ValueError('Identity values must be finite')
        return value
    if isinstance(value, complex):
        if not math.isfinite(value.real) or not math.isfinite(value.imag):
            raise ValueError('Identity values must be finite')
        return ('complex', value.real, value.imag)
    if isinstance(value, Mapping):
        return tuple(sorted((_freeze(k), _freeze(v)) for k, v in value.items()))
    if isinstance(value, (tuple, list)):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, (set, frozenset)):
        return tuple(sorted(_freeze(v) for v in value))
    if is_dataclass(value) and getattr(type(value), '__dataclass_params__').frozen:
        return (type(value).__qualname__, tuple((f.name, _freeze(getattr(value, f.name))) for f in fields(value)))
    raise TypeError(f'Unsupported mutable identity value: {type(value).__name__}')


@dataclass(frozen=True)
class RawCacheIdentity:
    dataset: str
    store: str
    version: str
    owner: str
    format: str
    dtype: str

    def __post_init__(self):
        for name in ('dataset', 'store', 'version', 'owner', 'format', 'dtype'):
            _id(getattr(self, name), name)


@dataclass(frozen=True)
class ResolvedCacheIdentity:
    raw: RawCacheIdentity
    source_coordinate: str
    layout: str
    placement: str
    frame: str
    log: tuple
    phase: object
    parameter_values: tuple
    exact_witnesses: tuple

    def __post_init__(self):
        if not isinstance(self.raw, RawCacheIdentity): raise TypeError('raw must be RawCacheIdentity')
        for name in ('source_coordinate', 'layout', 'placement', 'frame'):
            _id(getattr(self, name), name)
        object.__setattr__(self, 'log', _freeze(self.log))
        object.__setattr__(self, 'phase', _freeze(self.phase))
        object.__setattr__(self, 'parameter_values', _freeze(self.parameter_values))
        object.__setattr__(self, 'exact_witnesses', _freeze(self.exact_witnesses))

    @classmethod
    def build(cls, raw, *, source_coordinate, layout, placement, frame, log=(), phase=None,
              dependencies=(), binding=None):
        """Build an identity from only relevant parameter IDs and exact witnesses.

        ``dependencies`` is the set of parameter IDs that affect this resolved
        value. Missing values are rejected. Binding ID alone is never evidence.
        """
        if binding is None and dependencies:
            raise ValueError('Cannot resolve parameter dependencies without a binding')
        values = dict(getattr(binding, 'values', ())) if binding is not None else {}
        witnesses = dict(getattr(binding, 'exact_witnesses', ())) if binding is not None else {}
        deps = tuple(sorted(set(map(str, dependencies))))
        if any(dep not in values for dep in deps):
            raise ValueError('Unresolved parameter dependency')
        return cls(raw, str(source_coordinate), str(layout), str(placement), str(frame),
                   log, phase, tuple((dep, values[dep]) for dep in deps),
                   tuple((dep, witnesses[dep]) for dep in deps if dep in witnesses))
