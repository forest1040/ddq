"""Bounded future hints and same-unit cache eviction cost records."""
from dataclasses import dataclass
from dataclasses import asdict
import json
import math

MAX_HINTS = 64
MAX_WINDOW = 32
MAX_CONTEXT_ITEMS = 16
MAX_ID_LENGTH = 256


def _identifier(value, label):
    if not isinstance(value, str) or not value or len(value) > MAX_ID_LENGTH:
        raise ValueError(f'{label} must be a nonempty bounded string')
    return value


def _cost_value(value):
    if value is None: return
    if isinstance(value, bool): raise ValueError('Costs must be numeric, not bool')
    try: finite = math.isfinite(value)
    except (TypeError, OverflowError, ValueError): finite = False
    if not finite or value < 0: raise ValueError('Costs must be finite and nonnegative')


def _bounded_pairs(values):
    source = values.items() if hasattr(values, 'items') else iter(values)
    pairs = []
    for pair in source:
        if len(pairs) == MAX_CONTEXT_ITEMS:
            raise ValueError('Too many context versions')
        pairs.append(pair)
    return tuple(pairs)


@dataclass(frozen=True)
class FutureUseHint:
    version: str
    next_use: int
    reuse_count: int = 1
    raw_read: float | None = None
    rematerialize: float | None = None
    dirty_writeback: float | None = None
    whole_fetch: float | None = None
    component_fetch: bool = False
    confidence: float = 1.0
    cost_evidence: str = 'unknown'
    cost_validated: bool = False
    cost_units: str = 'seconds'
    cost_source: str = ''
    cost_scope: str = ''

    def __post_init__(self):
        _identifier(self.version, 'Version')
        if isinstance(self.next_use, bool) or not isinstance(self.next_use, int) or self.next_use < 0:
            raise ValueError('Invalid future-use distance')
        if isinstance(self.reuse_count, bool) or not isinstance(self.reuse_count, int) or self.reuse_count < 1:
            raise ValueError('Invalid future-use distance or reuse count')
        try: confidence_finite = math.isfinite(self.confidence)
        except (TypeError, OverflowError, ValueError): confidence_finite = False
        if (isinstance(self.confidence, bool) or not isinstance(self.confidence, (int, float))
                or not confidence_finite or not 0 <= self.confidence <= 1):
            raise ValueError('Invalid future-use confidence')
        for value in (self.raw_read, self.rematerialize, self.dirty_writeback, self.whole_fetch):
            _cost_value(value)
        if not isinstance(self.component_fetch, bool): raise TypeError('component_fetch must be bool')
        if not isinstance(self.cost_validated, bool): raise TypeError('cost_validated must be bool')
        _identifier(self.cost_evidence, 'Cost evidence')
        _identifier(self.cost_units, 'Cost units')
        if self.cost_source: _identifier(self.cost_source, 'Cost source')
        if self.cost_scope: _identifier(self.cost_scope, 'Cost scope')


@dataclass(frozen=True)
class FutureUseSummary:
    hints: tuple
    snapshot_version: str
    context_versions: tuple
    window: int = MAX_WINDOW

    def __post_init__(self):
        if isinstance(self.window, bool) or not isinstance(self.window, int) or not 0 <= self.window <= MAX_WINDOW or len(self.hints) > MAX_HINTS:
            raise ValueError('Future-use summary exceeds bounded limits')
        _identifier(self.snapshot_version, 'Snapshot version')
        if any(not isinstance(h, FutureUseHint) for h in self.hints):
            raise TypeError('hints must contain FutureUseHint values')
        if any(h.next_use >= self.window for h in self.hints):
            raise ValueError('Hint lies beyond the summary window')
        if len({h.version for h in self.hints}) != len(self.hints):
            raise ValueError('Duplicate future-use version')
        object.__setattr__(self, 'hints', tuple(self.hints))
        pairs = _bounded_pairs(self.context_versions)
        context = {}
        for pair in pairs:
            if not isinstance(pair, (tuple, list)) or len(pair) != 2:
                raise ValueError('Context versions must be key/value pairs')
            key, value = pair
            if key in context: raise ValueError('Duplicate context version key')
            context[key] = value
        for key, value in context.items():
            _identifier(key, 'Context key'); _identifier(value, 'Context version')
        object.__setattr__(self, 'context_versions', tuple(sorted(context.items())))

    @classmethod
    def from_versions(cls, versions, *, snapshot_version='manual', context_versions=(), window=MAX_WINDOW):
        if isinstance(window, bool) or not isinstance(window, int) or not 0 <= window <= MAX_WINDOW:
            raise ValueError('Invalid future window')
        hints = []
        for i, v in enumerate(versions):
            if i >= min(MAX_HINTS, window): break
            if v not in {h.version for h in hints}: hints.append(FutureUseHint(str(v), i))
        return cls(tuple(hints), str(snapshot_version), _bounded_pairs(context_versions), window)

    @classmethod
    def from_pending_versions(cls, versions, *, snapshot_version, context_versions=(), window=MAX_WINDOW):
        """Summarize an ordered known-future sequence, bounded to its first window."""
        if isinstance(window, bool) or not isinstance(window, int) or not 0 <= window <= MAX_WINDOW:
            raise ValueError('Invalid future window')
        first, counts = {}, {}
        iterator = iter(versions)
        for ordinal in range(window):
            try:
                version = next(iterator)
            except StopIteration:
                break
            _identifier(version, 'Future-use version')
            first.setdefault(version, ordinal)
            counts[version] = counts.get(version, 0) + 1
        hints = tuple(FutureUseHint(version, ordinal, reuse_count=counts[version])
                      for version, ordinal in first.items())
        return cls(hints, snapshot_version, _bounded_pairs(context_versions), window)

    @property
    def managed_bytes(self):
        """Conservative capacity estimate; it is not a process-RSS measurement."""
        encoded = json.dumps(asdict(self), sort_keys=True, separators=(',', ':'),
                             ensure_ascii=True, allow_nan=False).encode('ascii')
        return 4096 + 8 * len(encoded)

    def is_current(self, context):
        return tuple(sorted(dict(context or {}).items())) == self.context_versions

    def get(self, version):
        return next((hint for hint in self.hints if hint.version == str(version)), None)


@dataclass(frozen=True)
class CacheCost:
    """Cost measurements use declared units; only validated measurements drive eviction."""
    raw_read: float | None = None
    rematerialize: float | None = None
    dirty_writeback: float | None = None
    whole_fetch: float | None = None
    component_fetch: bool = False
    evidence: str = 'unknown'
    validated: bool = False
    units: str = 'seconds'
    source: str = ''
    scope: str = ''

    def __post_init__(self):
        for value in (self.raw_read, self.rematerialize, self.dirty_writeback, self.whole_fetch):
            _cost_value(value)
        if not isinstance(self.component_fetch, bool): raise TypeError('component_fetch must be bool')
        if not isinstance(self.validated, bool): raise TypeError('validated must be bool')
        _identifier(self.evidence, 'Cost evidence'); _identifier(self.units, 'Cost units')
        if self.source: _identifier(self.source, 'Cost source')
        if self.scope: _identifier(self.scope, 'Cost scope')

    def eviction_score(self, dirty, reuse_count=1):
        if not isinstance(dirty, bool): raise ValueError('dirty must be bool')
        if isinstance(reuse_count, bool) or not isinstance(reuse_count, int) or reuse_count < 1:
            raise ValueError('reuse_count must be a positive integer')
        if not self.validated or self.evidence != 'measured' or not self.source or not self.scope:
            return None
        # A whole-fetch measurement already includes its components.
        if self.whole_fetch is not None:
            retained = self.whole_fetch
        elif self.raw_read is not None and self.rematerialize is not None:
            retained = self.raw_read + self.rematerialize
        else:
            return None
        if self.component_fetch and self.whole_fetch is not None:
            retained = self.whole_fetch
        writeback = self.dirty_writeback if dirty else 0.0
        if dirty and writeback is None: return None
        try: score = retained * reuse_count + writeback
        except OverflowError: return None
        return score if math.isfinite(score) else None


def resolve_cost(hint, resolver, version, entry):
    if resolver is None:
        return CacheCost(raw_read=hint.raw_read, rematerialize=hint.rematerialize,
                         dirty_writeback=hint.dirty_writeback, whole_fetch=hint.whole_fetch,
                         component_fetch=hint.component_fetch, evidence=hint.cost_evidence,
                         validated=hint.cost_validated, units=hint.cost_units,
                         source=hint.cost_source, scope=hint.cost_scope)
    value = resolver(version, entry, hint)
    if isinstance(value, CacheCost): return value
    if isinstance(value, dict): return CacheCost(**value)
    return None
