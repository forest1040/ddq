"""Deterministic, bounded performance degradation for a caller's own work.

This module models service constraints in-process. It never changes operating
system or network settings and never declares a real operation complete.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import math
import random
import time
from typing import Callable


def _finite_nonnegative(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be finite and nonnegative")
    try:
        result = float(value)
    except (OverflowError, ValueError):
        raise ValueError(f"{name} must be finite and nonnegative") from None
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{name} must be finite and nonnegative")
    return result


@dataclass(frozen=True)
class InjectionSpec:
    """Immutable injection law shared identically by comparison arms.

    ``trigger`` is ``"work"`` or ``"time"``. Work cases activate at the named
    semantic barrier and inject for ``start`` through ``end`` matching work
    units. Time cases use seconds from :meth:`PerformanceInjection.start`.
    ``per_bytes`` makes delay proportional to bytes; otherwise it is per call.
    The maximums cap cumulative planned delay and event count.
    """
    seed: int
    resource: str
    trigger: str
    start: float
    end: float
    delay_seconds: float
    operations: tuple[str, ...]
    max_seconds: float
    max_events: int
    barrier: str | None = None
    per_bytes: float | None = None
    scope: str = "runtime-calibration"

    def __post_init__(self):
        if isinstance(self.seed, bool) or not isinstance(self.seed, int) or self.seed < 0:
            raise ValueError("seed must be a nonnegative integer")
        if self.resource not in {"storage", "mpi", "compute"}:
            raise ValueError("unsupported resource")
        if self.trigger not in {"work", "time"}:
            raise ValueError("trigger must be work or time")
        start = _finite_nonnegative(self.start, "start")
        end = _finite_nonnegative(self.end, "end")
        if end <= start:
            raise ValueError("end must be greater than start")
        delay = _finite_nonnegative(self.delay_seconds, "delay_seconds")
        maximum = _finite_nonnegative(self.max_seconds, "max_seconds")
        if type(self.max_events) is not int or self.max_events < 1:
            raise ValueError("max_events must be a positive integer")
        if self.max_events > 256:
            raise ValueError("max_events exceeds the absolute 256-event cap")
        if not isinstance(self.operations, tuple) or not self.operations or len(self.operations) > 64:
            raise ValueError("operations must be a nonempty bounded tuple")
        if any(not isinstance(op, str) or not op or len(op) > 64 for op in self.operations):
            raise ValueError("operation names must be nonempty bounded strings")
        if len(set(self.operations)) != len(self.operations):
            raise ValueError("operations must be unique")
        if self.trigger == "work" and (not isinstance(self.barrier, str) or not self.barrier or len(self.barrier) > 128):
            raise ValueError("work trigger requires a bounded semantic barrier")
        if self.trigger == "time" and self.barrier is not None:
            raise ValueError("time trigger cannot specify a barrier")
        if self.per_bytes is not None:
            _finite_nonnegative(self.per_bytes, "per_bytes")
            if self.resource == "compute":
                raise ValueError("compute injection is bounded per call, not per byte")
        if self.scope != "runtime-calibration":
            raise ValueError("injection scope must be runtime-calibration")
        if delay == 0 or maximum == 0:
            raise ValueError("delay and max_seconds must be positive")
        try:
            encoded = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        except (TypeError, ValueError, OverflowError):
            raise ValueError("spec must be finite JSON data") from None
        if len(encoded) > 4096:
            raise ValueError("serialized spec exceeds 4 KiB")


@dataclass
class InjectionTicket:
    """One planned service delay; readiness is polled without blocking."""
    event_id: str
    operation: str
    planned_delay: float
    ready_at: float
    created_at: float
    actual_service_seconds: float | None = None
    actual_delay_seconds: float = 0.0
    state: str = "pending"
    _complete_callback: Callable | None = field(default=None, repr=False, compare=False)

    def complete(self, *, service_seconds: float = 0.0):
        if self._complete_callback is None:
            raise RuntimeError("ticket has no owning injection scheduler")
        return self._complete_callback(self, service_seconds=service_seconds)

    def _mark_completed(self, service_seconds):
        if self.state == "cancelled":
            raise ValueError("cancelled ticket cannot complete")
        if self.state != "ready":
            raise RuntimeError("ticket can complete only after injection readiness")
        self.actual_service_seconds = _finite_nonnegative(service_seconds, "service_seconds")
        self.state = "completed"
        return self

    def cancel(self):
        if self.state in {"completed", "cancelled"}:
            return False
        self.state = "cancelled"
        return True


class PerformanceInjection:
    """Fake-clock ready-time scheduler with bounded ticket and event storage."""
    def __init__(self, spec: InjectionSpec, *, clock: Callable[[], float] | None = None):
        if not isinstance(spec, InjectionSpec):
            raise TypeError("spec must be InjectionSpec")
        self.spec = spec
        self.clock = clock or time.monotonic
        self._rng = random.Random(spec.seed)
        self._run_start: float | None = None
        self._last_now: float | None = None
        self._barrier_seen = False
        self._work = 0.0
        self._planned = 0.0
        self._events: dict[str, InjectionTicket] = {}
        self._ready: list[str] = []
        self._serial = 0
        self.counters = {"scheduled": 0, "ready": 0, "completed": 0, "cancelled": 0, "ignored": 0}

    def start(self, *, now: float | None = None):
        if self._run_start is not None:
            raise RuntimeError("injection run already started")
        t = _finite_nonnegative(self.clock() if now is None else now, "now")
        self._run_start = t
        self._last_now = t
        return t

    def barrier(self, semantic_id: str, *, now: float | None = None):
        """Activate work-trigger schedules at the identical named prefix."""
        if not isinstance(semantic_id, str) or not semantic_id or len(semantic_id) > 128:
            raise ValueError("semantic_id must be a bounded nonempty string")
        if self._run_start is None:
            raise RuntimeError("start the run before barriers")
        if self.spec.trigger == "work" and semantic_id == self.spec.barrier:
            self._barrier_seen = True
            if now is not None:
                self._check_time(now)
            return True
        return False

    def _check_time(self, now):
        t = _finite_nonnegative(now, "now")
        if self._run_start is None:
            raise RuntimeError("injection run has not started")
        if t < self._run_start:
            raise ValueError("clock moved before run start")
        if self._last_now is not None and t < self._last_now:
            raise ValueError("clock moved backwards")
        self._last_now = t
        return t

    def request(self, operation: str, *, work: float = 1, nbytes: int = 0,
                event_id: str | None = None, token: str | None = None,
                now: float | None = None) -> InjectionTicket | None:
        """Schedule an injection delay; caller continues real work to completion."""
        t = self._check_time(self.clock() if now is None else now)
        if not isinstance(operation, str) or not operation:
            raise ValueError("operation is required")
        work = _finite_nonnegative(work, "work")
        if type(nbytes) is not int or nbytes < 0:
            raise ValueError("nbytes must be a nonnegative integer")
        if operation not in self.spec.operations:
            self.counters["ignored"] += 1
            return None
        if token is not None:
            if event_id is not None and event_id != token:
                raise ValueError("event_id and token disagree")
            event_id = token
        if event_id is None:
            event_id = f"inject-{self._serial}"
        if not isinstance(event_id, str) or not event_id or len(event_id) > 128:
            raise ValueError("event_id must be bounded nonempty string")
        if event_id in self._events:
            raise ValueError("duplicate injection event")
        if len(self._events) >= self.spec.max_events:
            return None
        active = True
        if self.spec.trigger == "work":
            if not self._barrier_seen:
                active = False
            else:
                relative = self._work
                next_work = self._work + work
                if not math.isfinite(next_work):
                    raise ValueError("work cursor overflow")
                self._work = next_work
                active = relative >= self.spec.start and relative < self.spec.end
        else:
            elapsed = t - self._run_start
            active = elapsed >= self.spec.start and elapsed < self.spec.end
        if not active:
            ticket = InjectionTicket(event_id, operation, 0.0, t, t,
                                     actual_delay_seconds=0.0, state="ready",
                                     _complete_callback=self.finish)
            self._events[event_id] = ticket
            self._serial += 1
            self.counters["scheduled"] += 1
            self.counters["ready"] += 1
            return ticket
        per_unit = self.spec.per_bytes
        try:
            delay = self.spec.delay_seconds * (nbytes * per_unit if per_unit is not None else 1.0)
        except OverflowError:
            raise ValueError("injection delay overflow") from None
        if not math.isfinite(delay):
            raise ValueError("injection delay overflow")
        delay = self._rng.uniform(0.9, 1.1) * delay
        if not math.isfinite(delay):
            raise ValueError("injection delay overflow")
        delay = min(delay, self.spec.max_seconds - self._planned)
        if delay <= 0:
            return None
        planned_total = self._planned + delay
        ready_at = t + delay
        if not math.isfinite(planned_total) or not math.isfinite(ready_at):
            raise ValueError("injection timing overflow")
        self._planned = planned_total
        ticket = InjectionTicket(event_id, operation, delay, ready_at, t,
                                 _complete_callback=self.finish)
        self._events[event_id] = ticket
        self._serial += 1
        self.counters["scheduled"] += 1
        return ticket

    def progress(self, *, now: float | None = None):
        """Return newly ready tickets; no sleeping or implicit completion occurs."""
        t = self._check_time(self.clock() if now is None else now)
        ready = []
        for event_id, ticket in self._events.items():
            if ticket.state == "pending" and t >= ticket.ready_at:
                ticket.state = "ready"
                ticket.actual_delay_seconds = t - ticket.created_at
                ready.append(ticket)
                self.counters["ready"] += 1
        return tuple(ready)

    def finish(self, ticket: InjectionTicket, *, service_seconds: float = 0.0):
        if self._events.get(ticket.event_id) is not ticket:
            raise ValueError("unknown ticket")
        if ticket.state == "cancelled":
            raise ValueError("cancelled ticket cannot complete")
        if ticket.state == "completed":
            raise ValueError("duplicate completion")
        ticket._mark_completed(service_seconds)
        self.counters["completed"] += 1
        return True

    complete = finish

    def cancel(self, ticket: InjectionTicket):
        if self._events.get(ticket.event_id) is not ticket:
            raise ValueError("unknown ticket")
        if ticket.cancel():
            self.counters["cancelled"] += 1
            return True
        return False

    def snapshot(self):
        """Bounded calibration evidence; actual service and added delay stay split."""
        return {
            "scope": self.spec.scope,
            "evidence": "injected",
            "resource": self.spec.resource,
            "trigger": self.spec.trigger,
            "planned_injection_seconds": self._planned,
            "actual_injection_seconds": sum(x.actual_delay_seconds for x in self._events.values()),
            "actual_service_seconds": sum(x.actual_service_seconds or 0.0 for x in self._events.values()),
            "counters": dict(self.counters),
            "events": tuple({"event_id": x.event_id, "operation": x.operation,
                             "planned_delay_seconds": x.planned_delay,
                             "actual_delay_seconds": x.actual_delay_seconds,
                             "actual_service_seconds": x.actual_service_seconds,
                             "state": x.state} for x in self._events.values()),
        }
