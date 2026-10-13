"""Bounded, main-thread collection of validated runtime completion events."""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, field
from copy import deepcopy
import hashlib
import json
import math
import threading
from typing import Any


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


@dataclass(frozen=True)
class CompletionObservation:
    event_id: str
    context: dict[str, Any]
    condition: dict[str, Any]
    parent_id: str | None = None
    group_id: str | None = None
    stage: str = "operation"
    units: str = "calls"
    quantity: int = 1
    queue_seconds: float | None = None
    service_seconds: float | None = None
    protocol_seconds: float | None = None
    cpu_seconds: float | None = None
    payload_bytes: int | None = None
    storage_bytes: int | None = None
    success: bool = True
    validated: bool = False
    evidence: str = "measured"
    scope: str = "same-host"
    warmup: bool = False
    raw_ref: str | None = None
    context_hash: str = field(init=False)
    condition_hash: str = field(init=False)

    def __post_init__(self):
        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValueError("event_id is required")
        for name in ('event_id','parent_id','group_id','raw_ref'):
            value=getattr(self,name)
            if value is not None and (not isinstance(value,str) or len(value.encode())>256):
                raise ValueError(f'{name} exceeds bounded identifier length')
        if not isinstance(self.context, dict) or not isinstance(self.condition, dict):
            raise ValueError("context and condition must be dictionaries")
        if any(type(value) is not bool for value in (self.success, self.validated, self.warmup)):
            raise ValueError("success, validated, and warmup flags must be boolean")
        for name in ("stage", "units", "evidence", "scope"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} is required")
        _canonical(self.context); _canonical(self.condition)
        if isinstance(self.quantity, bool) or not isinstance(self.quantity, int) or self.quantity < 0:
            raise ValueError("quantity must be a nonnegative integer")
        for name in ("payload_bytes", "storage_bytes"):
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                raise ValueError(f"{name} must be a nonnegative integer")
        for name in ("queue_seconds", "service_seconds", "protocol_seconds", "cpu_seconds"):
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or not math.isfinite(value) or value < 0):
                raise ValueError(f"{name} must be finite and nonnegative")
        if self.scope not in {"same-host", "cross-host", "runtime-calibration", "unknown"}:
            raise ValueError("unsupported observation scope")
        object.__setattr__(self, "context", deepcopy(self.context))
        object.__setattr__(self, "condition", deepcopy(self.condition))
        object.__setattr__(self, "context_hash", _hash(self.context))
        object.__setattr__(self, "condition_hash", _hash(self.condition))

    def to_dict(self):
        return asdict(self)


class RuntimeMeasurementCollector:
    """Keeps bounded summaries; only successful validated events enter cost observations."""

    def __init__(self, *, max_conditions: int = 256, per_condition: int = 64,
                 max_bytes: int = 64 * 1024 * 1024, clock=None, enabled: bool = True):
        if type(enabled) is not bool:
            raise ValueError("enabled must be boolean")
        if any(type(value) is not int or value<1 for value in (max_conditions,per_condition,max_bytes)):
            raise ValueError("collector limits must be positive")
        self.max_conditions, self.per_condition, self.max_bytes = max_conditions, per_condition, max_bytes
        self._enabled = enabled
        self._clock = clock
        self._thread = threading.get_ident()
        self._rings: dict[tuple[str, str], deque[CompletionObservation]] = {}
        self._events: set[str] = set()
        self._event_capacity=min(max_conditions*per_condition,max(1,max_bytes//256))
        self._event_order: deque[str] = deque(maxlen=self._event_capacity)
        self._failures: deque[CompletionObservation] = deque(maxlen=max_conditions * per_condition)
        self._failure_sizes: deque[int] = deque(maxlen=max_conditions * per_condition)
        self._failure_bytes = 0
        self._bytes = 0
        self.dropped_conditions = self.dropped_bytes = self.duplicate_events = 0
        self.failures = 0
        self._children: dict[str, set[str]] = {}
        self._child_bytes = 0
        self.dropped_children = 0
        self._group_stack: list[str] = []
        self._group_status: dict[str, bool] = {}
        self._group_parent: dict[str, str | None] = {}
        self._group_status_order: deque[str] = deque(maxlen=self._event_capacity)

    def _check_thread(self):
        if threading.get_ident() != self._thread:
            raise RuntimeError("measurement collector is main-thread only")

    @property
    def enabled(self):
        return self._enabled

    def submit(self, event_id: str, **fields):
        self._check_thread()
        if not self.enabled:
            return False
        if event_id in self._events:
            self.duplicate_events += 1
            return False
        observation = CompletionObservation(event_id=event_id, **{
            **fields,'parent_id':fields.get('parent_id') or (self._group_stack[-1] if self._group_stack else None)})
        # Reserve the id even for failures so a retry cannot double count it.
        if len(self._event_order) == self._event_order.maxlen:
            self._events.discard(self._event_order[0])
        self._event_order.append(event_id)
        self._events.add(event_id)
        if fields.get("parent_id") is None and self._group_stack:
            fields["parent_id"] = self._group_stack[-1]
        if observation.group_id:
            if observation.group_id not in self._group_status and len(self._group_status_order) == self._group_status_order.maxlen:
                expired = self._group_status_order[0]
                self._group_status.pop(expired, None)
                self._group_parent.pop(expired, None)
            if observation.group_id not in self._group_status:
                self._group_status_order.append(observation.group_id)
            self._group_status[observation.group_id] = bool(observation.success and observation.validated)
            self._group_parent[observation.group_id] = observation.parent_id
        encoded_size = len(_canonical(asdict(observation)).encode())
        if encoded_size > self.max_bytes:
            self.dropped_bytes += 1
            return False
        if not observation.success or not observation.validated:
            self.failures += 1
            while self._bytes + self._failure_bytes + self._child_bytes + encoded_size > self.max_bytes and self._failure_sizes:
                self._failure_bytes -= self._failure_sizes.popleft()
                self._failures.popleft()
            if self._bytes + self._failure_bytes + self._child_bytes + encoded_size > self.max_bytes:
                self.dropped_bytes += 1
                return False
            if len(self._failures) == self._failures.maxlen:
                self._failure_bytes -= self._failure_sizes.popleft()
            self._failure_sizes.append(encoded_size)
            self._failure_bytes += encoded_size
            self._failures.append(observation)
            if observation.parent_id:
                self._add_child(observation.parent_id, observation.event_id)
            return False
        key = (observation.context_hash, observation.condition_hash)
        if key not in self._rings and len(self._rings) >= self.max_conditions:
            self.dropped_conditions += 1
            return False
        ring = self._rings.get(key)
        if ring is not None and len(ring) == self.per_condition:
            old = ring.popleft()
            self._bytes -= len(_canonical(asdict(old)).encode())
        if self._bytes + self._failure_bytes + self._child_bytes + encoded_size > self.max_bytes:
            self.dropped_bytes += 1
            return False
        ring = self._rings.setdefault(key, deque())
        ring.append(observation)
        self._bytes += encoded_size
        if observation.parent_id:
            self._add_child(observation.parent_id, observation.event_id)
        return True

    def _add_child(self, group_id, event_id):
        children = self._children.get(group_id)
        if children is None and len(self._children) < self._event_capacity:
            size = len(group_id.encode()) + 32
            if self._bytes + self._failure_bytes + self._child_bytes + size > self.max_bytes:
                self.dropped_children += 1
                return
            children = self._children.setdefault(group_id, set())
            self._child_bytes += size
        if children is not None and event_id not in children and len(children) < self.per_condition:
            size = len(event_id.encode()) + 8
            if self._bytes + self._failure_bytes + self._child_bytes + size > self.max_bytes:
                self.dropped_children += 1
                return
            children.add(event_id)
            self._child_bytes += size

    def begin_group(self, group_id: str):
        self._check_thread()
        if not self.enabled:
            return
        if not isinstance(group_id, str) or not group_id:
            raise ValueError("group_id is required")
        if len(self._group_stack) >= 64:
            raise RuntimeError("measurement group nesting exceeds limit")
        self._group_stack.append(group_id)

    def end_group(self, group_id: str):
        self._check_thread()
        if not self.enabled:
            return
        if not self._group_stack or self._group_stack[-1] != group_id:
            raise RuntimeError("measurement groups must end in stack order")
        self._group_stack.pop()

    def complete(self, event_id: str, *, validator=None, **fields):
        """Complete after caller validation. A raising validator becomes failed evidence."""
        self._check_thread()
        if not self.enabled:
            return False
        validated = False
        error = None
        if fields.get("success", True):
            try:
                validated = bool(validator()) if validator is not None else bool(fields.get("validated", False))
            except Exception as exc:
                error = exc
        fields["validated"] = validated
        if error is not None:
            fields["success"] = False
        return self.submit(event_id, **fields)

    def snapshot(self):
        self._check_thread()
        rows = [item for ring in self._rings.values() for item in ring]
        rows.sort(key=lambda item: item.event_id)
        return {"observations": [item.to_dict() for item in deepcopy(rows)],
                "collection_enabled": self.enabled,
                "failed_observations": [item.to_dict() for item in deepcopy(self._failures)],
                "condition_count": len(self._rings),
                "bytes": self._bytes, "dropped_conditions": self.dropped_conditions,
                "dropped_bytes": self.dropped_bytes, "dropped_children": self.dropped_children,
                "duplicate_events": self.duplicate_events,
                "failures": self.failures}

    def cost_observations(self):
        """Return only successful, validated, non-warmup observations."""
        self._check_thread()
        candidates = [item for ring in self._rings.values() for item in ring
                      if not item.warmup and item.success and item.validated and item.evidence == 'measured']
        result = []
        for item in candidates:
            parent = item.parent_id
            usable = True
            seen = set()
            while parent and parent not in seen:
                seen.add(parent)
                if self._group_status.get(parent) is not True:
                    usable = False
                    break
                parent = self._group_parent.get(
                    parent, next((row.parent_id for row in candidates if row.event_id == parent), None))
            if usable:
                result.append(deepcopy(item))
        return tuple(result)

    def consumed_children(self, group_id: str) -> frozenset[str]:
        self._check_thread()
        return frozenset(self._children.get(group_id, ()))
