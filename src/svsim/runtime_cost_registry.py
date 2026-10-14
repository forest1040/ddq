"""One runtime cost registry for ETA, Guard, cache, gradient and replan consumers.

Every cost source is registered with a kind, a provenance and a resolver.
Consumers resolve by purpose, and the purpose decides which provenances may
answer: production decisions accept only measured, calibrated sources;
evaluation additionally accepts injected evidence; health may use legacy
baselines; diagnostic views accept everything but stay labelled. A missing or
rejected source yields ``None`` with a reason; nothing is ever defaulted.
"""
from __future__ import annotations

from dataclasses import dataclass, field

PROVENANCES = ('measured', 'derived', 'injected', 'manual', 'legacy')
ACCEPTED = {
    'production': {'measured'},
    'evaluation': {'measured', 'injected'},
    'health': {'measured', 'legacy'},
    'diagnostic': set(PROVENANCES),
}
REJECTION = {'derived': 'DERIVED_NOT_PRODUCTION', 'injected': 'INJECTED_NOT_PRODUCTION',
             'manual': 'MANUAL_NOT_PRODUCTION', 'legacy': 'LEGACY_NOT_PRODUCTION', 'measured': 'MEASURED_REJECTED'}
MAX_SOURCES = 64


@dataclass(frozen=True)
class Resolution:
    value: object
    reason: str
    provenance: str | None = None
    source_id: str | None = None
    units: str | None = None
    rejected: tuple = ()

    @property
    def known(self): return self.value is not None


@dataclass
class CostSource:
    kind: str
    source_id: str
    provenance: str
    resolver: object
    units: str | None = None
    order: int = 0


@dataclass
class RuntimeCostRegistry:
    sources: dict = field(default_factory=dict)
    version: int = 0
    resolutions: dict = field(default_factory=dict)

    def register(self, kind, source_id, provenance, resolver, *, units=None):
        """Add or replace the source ``source_id`` of ``kind``; returns the registry version."""
        if provenance not in PROVENANCES: raise ValueError(f'unknown cost provenance {provenance!r}')
        if not callable(resolver): raise TypeError('cost resolver must be callable')
        if not isinstance(kind, str) or not kind or not isinstance(source_id, str) or not source_id:
            raise ValueError('cost kind and source id are required')
        if (kind, source_id) not in self.sources and len(self.sources) >= MAX_SOURCES:
            raise ValueError('cost registry source limit exceeded')
        self.version += 1
        self.sources[(kind, source_id)] = CostSource(kind, source_id, provenance, resolver, units, self.version)
        return self.version

    def unregister(self, kind, source_id):
        if self.sources.pop((kind, source_id), None) is not None: self.version += 1

    def kinds(self):
        return sorted({kind for kind, _ in self.sources})

    def describe(self):
        return [{'kind': s.kind, 'source_id': s.source_id, 'provenance': s.provenance, 'units': s.units}
                for s in sorted(self.sources.values(), key=lambda s: s.order)]

    def resolve(self, kind, context=None, condition=None, *, purpose='production', units=None, select='latest',
                source_id=None):
        """First acceptable answer among sources of ``kind`` (newest first unless ``select='first'``)."""
        if purpose not in ACCEPTED: raise ValueError(f'unknown cost purpose {purpose!r}')
        candidates = sorted((s for s in self.sources.values() if s.kind == kind and
                             (source_id is None or s.source_id == source_id)),
                            key=lambda s: s.order, reverse=select == 'latest')
        rejected = []
        for source in candidates:
            if source.provenance not in ACCEPTED[purpose]:
                rejected.append((source.source_id, REJECTION[source.provenance]))
                continue
            if units is not None and source.units is not None and source.units != units:
                rejected.append((source.source_id, 'UNITS_MISMATCH'))
                continue
            value = source.resolver(context, condition)
            if value is None:
                rejected.append((source.source_id, 'NO_MATCHING_COST'))
                continue
            result = Resolution(value, 'RESOLVED', source.provenance, source.source_id, source.units, tuple(rejected))
            self._count(kind, purpose, 'RESOLVED')
            return result
        reason = rejected[0][1] if rejected else 'UNKNOWN_COST'
        self._count(kind, purpose, reason)
        return Resolution(None, reason, rejected=tuple(rejected))

    def _count(self, kind, purpose, reason):
        key = f'{kind}|{purpose}|{reason}'
        if key in self.resolutions or len(self.resolutions) < 256:
            self.resolutions[key] = self.resolutions.get(key, 0) + 1
