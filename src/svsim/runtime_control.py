"""Collective runtime policy changes at drained, versioned prefix cuts."""
from dataclasses import asdict, dataclass, replace
import math
from time import monotonic

from .journal import content_hash
from .lifecycle import BenchmarkCondition, BenchmarkSample, StaticCircuitOptimizer
from .telemetry import ObservePerformanceGuard


@dataclass(frozen=True)
class ReplanEvidence:
    continue_lower_seconds: float
    candidate_upper_seconds: float
    planning_seconds: float = 0
    drain_seconds: float = 0
    migration_seconds: float = 0
    validation_seconds: float = 0
    checkpoint_seconds: float = 0
    condition_hash: str = ''
    measured: bool = False

    def __post_init__(self):
        for key, value in asdict(self).items():
            if key.endswith('seconds') and (not math.isfinite(value) or value < 0):
                raise ValueError('Replan costs must be finite and nonnegative')

    @property
    def gain_lower(self):
        return self.continue_lower_seconds - sum(
            value for key, value in asdict(self).items()
            if key.endswith('seconds') and key != 'continue_lower_seconds')


@dataclass(frozen=True)
class PlanCandidate:
    candidate_id: str
    source_cut: str
    plan_version: int
    policy_epoch: int
    updates: tuple
    classification: str
    gain_lower_seconds: float | None
    eligible: bool
    reason: str
    expires_at: float


class RuntimeControl:
    def __init__(self, sim):
        self.sim = sim
        self.plan_version = 1
        self.policy_epoch = 1
        self.profile_version = 1
        self.guard = None
        self.health = 'INSUFFICIENT_BASELINE'
        self.candidates = {}
        self.last_change = -math.inf
        self.transitions = []
        self.condition_hash = ''
        self.remaining_seconds = None
        from .runtime_cost import RuntimeCostModel
        self.cost_model = RuntimeCostModel()
        self._measurement_budget=0
        self.measurements=None
        self.configure_measurements(max_bytes=max(1024,min(64*1024,sim.config.metadata_reserve_bytes//16)))
        self.profile_adapter = None

    def configure_measurements(self, *, max_bytes, max_conditions=256, per_condition=64):
        """Reserve bounded observer storage, including indices and snapshot copies."""
        from .runtime_measurement import RuntimeMeasurementCollector
        collector=RuntimeMeasurementCollector(max_bytes=max_bytes,max_conditions=max_conditions,per_condition=per_condition)
        if self.measurements is not None and self.measurements._events:
            raise ValueError('Measurement limits must be set before collecting events')
        capacity=min(max_conditions*per_condition,max(1,max_bytes//256))
        pool=8*max_bytes+256*capacity
        extra=max(0,pool-self._measurement_budget)
        self.sim.cache.ensure(extra)
        self.sim.budget.reserve(extra)
        if pool<self._measurement_budget:self.sim.budget.release(self._measurement_budget-pool)
        self._measurement_budget=pool
        self.measurements=collector

    def close_measurements(self):
        self.measurements=None
        if self._measurement_budget:
            self.sim.budget.release(self._measurement_budget)
            self._measurement_budget=0

    def attach_profile_document(self, document, *, scope='same-host'):
        """Attach an immutable profile for exact-condition, exact-build lookup."""
        from .runtime_profile_adapter import RuntimeProfileAdapter
        from .profile_document import probe_profile
        sim=self.sim
        actual=probe_profile({'scope':'local','ranks':sim.size,
            'nodes':len({row['host'] for row in sim.environment}),
            'threads':sim.policy.io_workers},(sim.directory,))
        if document.configuration_fingerprint!=actual.configuration_fingerprint:
            raise ValueError('Profile configuration differs from the current runtime allocation/build')
        self.profile_adapter = RuntimeProfileAdapter(document,
            configuration_fingerprint=actual.configuration_fingerprint,scope=scope)
        return self.profile_adapter

    def record_cost(self, sample):
        """Register a measured matched task cost, never an assumed link rate."""
        self.cost_model.record(sample)
        self.profile_version = max(self.profile_version, self.cost_model.version)

    def estimate_eta(self, tasks):
        estimate = self.cost_model.estimate(tasks)
        self.remaining_seconds = estimate.expected_seconds
        self.sim.trace.record('eta', performance_class='eta', metadata={
            'path': 'RUNTIME_ETA', **asdict(estimate), 'profile_version': self.profile_version})
        return estimate

    def configure_guard(self, hardware_profile, *, window_size=8, consecutive_windows=3):
        sim = self.sim
        sim.transport.agree({'guard_profile': hardware_profile.to_dict() if hasattr(hardware_profile, 'to_dict')
                             else asdict(hardware_profile), 'window': window_size,
                             'consecutive': consecutive_windows})
        plan = StaticCircuitOptimizer().create_plan(sim.circuit_hash, hardware_profile,
                                                   plan_id=sim.run_id+'-async')
        self.guard = ObservePerformanceGuard(hardware_profile, plan, window_size=window_size,
                                             consecutive_windows=consecutive_windows)

    def observe(self, sample):
        if self.guard is None:
            return None
        report = self.guard.observe(sample)
        self.health = report.health_state
        self.profile_version = self.guard.runtime_profile.version
        if self.guard.hardware_profile.matching_benchmark(sample.condition) is not None:
            self.condition_hash = content_hash(asdict(sample.condition))
        self.sim.trace.record('async-health', performance_class='health', health_state=self.health,
                              metadata={'path': 'ASYNC_GUARD', 'report': asdict(report),
                                        'profile_version': self.profile_version})
        return report

    def io_condition(self, operation, request=None):
        sim = self.sim
        actual=getattr(request,'storage_metadata',{})
        return BenchmarkCondition('async-storage-'+operation, block_bytes=sim.store.slot_bytes,
                                  io_depth=sim.policy.max_io_requests, ranks=sim.size,
                                  details={'backend': actual.get('resolved_backend',sim.config.backend),
                                           'requested_backend':sim.config.backend,
                                           'device_id':actual.get('device_id','unknown'),
                                           'alignment':actual.get('alignment'),
                                           'submitted_in_flight':actual.get('submitted_in_flight'),
                                           'payload_bytes':sim.store.payload_bytes,
                                           'cache_state':'unknown','working_set':'unknown',
                                           'engine': sim.io_engine.identity,
                                           'prefetch_blocks': sim.policy.prefetch_blocks,
                                           'scope': 'same-host' if len({r['host'] for r in sim.environment}) == 1 else 'multi-host',
                                           'flush': 'deferred'})

    def io_completion(self, request):
        elapsed = getattr(request, 'wall_seconds', request.elapsed_seconds)
        successful = request.error is None and request.operation in {'read', 'write'} and bool(getattr(request,'validated',False))
        condition = self.io_condition(request.operation,request) if request.operation in {'read', 'write'} else None
        if condition is not None:
            sim = self.sim
            same_host = len({r['host'] for r in sim.environment}) == 1
            self.measurements.complete(
                f'io:{request.request_id}',
                context={'configuration_fingerprint': getattr(self.profile_adapter, 'fingerprint', None),
                         'backend': sim.config.backend, 'engine': sim.io_engine.identity,
                         'ranks': sim.size},
                condition=asdict(condition), stage='storage-' + request.operation,
                units='calls', quantity=1,
                queue_seconds=getattr(request, 'queue_seconds', None),
                service_seconds=request.elapsed_seconds, protocol_seconds=elapsed,
                payload_bytes=sim.store.slot_bytes, storage_bytes=sim.store.slot_bytes,
                success=successful, validator=lambda: bool(getattr(request, 'validated', False)),
                warmup=bool(getattr(sim,'_measurement_warmup',False)),
                scope='same-host' if same_host else 'cross-host')
        if successful and elapsed > 0:
            self.observe(BenchmarkSample(condition,
                          bandwidth_bytes_per_second=self.sim.store.slot_bytes/elapsed,
                          latency_ms=elapsed*1000))

    def mpi_completion(self, request, elapsed):
        """Record one source-side whole transfer after protocol acknowledgment."""
        if request.source != self.sim.rank or request.source == request.destination:
            return False
        metadata = request.metadata if isinstance(request.metadata, dict) else {}
        condition = {'operation': 'mpi-transfer', 'payload_bytes': request.count,
                     'source_rank': request.source, 'destination_rank': request.destination,
                     'engine': 'typed-nonblocking', 'scope': metadata.get('scope', 'unknown'),
                     'packing': metadata.get('packing', 'unknown'),
                     'peer_class': metadata.get('peer_class', 'unknown'),
                     'concurrency': metadata.get('concurrency', 'unknown')}
        scope = metadata.get('scope')
        if scope not in {'same-host', 'cross-host'}:
            scope = 'unknown'
        return self.measurements.complete(
            f'mpi:{request.slot}:{request.generation}',
            context={'configuration_fingerprint': getattr(self.profile_adapter, 'fingerprint', None),
                     'ranks': self.sim.size, 'rank': self.sim.rank},
            condition=condition, parent_id=metadata.get('group_id'), stage='mpi-transfer',
            units='calls', quantity=1, protocol_seconds=elapsed,
            payload_bytes=request.count, success=request.error is None,
            validated=request.error is None,
            warmup=bool(getattr(self.sim,'_measurement_warmup',False)),
            scope=scope)

    def set_mode(self, mode):
        return self.apply_policy({'mode': mode}, classification='LIVE_TUNE')

    def request_replan(self, updates=None, *, evidence=None, apply=False):
        sim = self.sim
        updates = dict(updates or {'prefetch_blocks': 0})
        # Validate the proposed policy without mutating active state.
        replace(sim.policy, **updates)
        classification = 'FRONTIER_REPLAN' if any(k in updates for k in ('swap_policy', 'lookahead_gates')) else 'LIVE_TUNE'
        gain = evidence.gain_lower if evidence else None
        eligible = (evidence is not None and evidence.measured and
                    bool(evidence.condition_hash) and evidence.condition_hash == self.condition_hash and
                    gain > max(0, evidence.continue_lower_seconds*sim.policy.minimum_gain_fraction))
        reason = 'measured-positive-gain' if eligible else 'insufficient-baseline-or-gain'
        payload = {'cut': sim.root, 'plan': self.plan_version, 'policy': self.policy_epoch,
                   'updates': updates, 'classification': classification,
                   'evidence': asdict(evidence) if evidence else None}
        sim.transport.agree(payload)
        candidate = PlanCandidate(content_hash(payload), sim.root, self.plan_version, self.policy_epoch,
                                  tuple(sorted(updates.items())), classification, gain, eligible, reason,
                                  monotonic()+sim.policy.candidate_ttl_seconds)
        # Keep only the current bounded set of proposals.
        if len(self.candidates) >= 16:
            self.candidates.pop(next(iter(self.candidates)))
        self.candidates[candidate.candidate_id] = candidate
        sim.trace.record(candidate.candidate_id, performance_class='planning',
                         metadata={'path': 'REPLAN_DRY_RUN', 'candidate': asdict(candidate),
                                   'amplitude_migration_bytes': 0})
        if apply:
            self.apply_candidate(candidate.candidate_id)
        return candidate

    def apply_candidate(self, candidate_id):
        sim = self.sim
        candidate = self.candidates.get(candidate_id)
        error = None
        if candidate is None:
            error = 'Unknown candidate'
        elif (candidate.source_cut, candidate.plan_version, candidate.policy_epoch) != (sim.root, self.plan_version, self.policy_epoch):
            error = 'Stale candidate source cut or policy'
        elif monotonic() > candidate.expires_at:
            error = 'Expired candidate'
        elif not candidate.eligible:
            error = candidate.reason
        elif sim.policy.mode == 'observe':
            error = 'observe mode cannot apply candidates'
        sim.transport.vote(error)
        return self.apply_policy(dict(candidate.updates), classification=candidate.classification,
                                 candidate_id=candidate_id)

    def apply_policy(self, updates, *, classification='LIVE_TUNE', candidate_id=None):
        sim = self.sim
        sim.transport.agree({'updates': updates, 'classification': classification,
                             'candidate': candidate_id, 'cut': sim.root})
        error = None
        try:
            if getattr(sim, '_private_epoch', False):
                raise RuntimeError('Policy changes require a published prefix cut')
            proposed = replace(sim.policy, **updates)
        except Exception as exc:
            error = str(exc)
        sim.transport.vote(error)
        sim.drain()
        old = sim.policy
        old_versions = self.plan_version, self.policy_epoch
        sim._checkpoint_decision_durable = False
        try:
            error = None
            installed = False
            try:
                sim._install_policy(proposed)
                installed = True
            except Exception as exc:
                error = str(exc)
            try:
                sim.transport.vote(error)
            except BaseException:
                if installed:
                    sim._install_policy(old)
                raise
            self.policy_epoch += 1
            if classification == 'FRONTIER_REPLAN':
                self.plan_version += 1
            # Plan changes and their source cut use the same durable protocol.
            if classification == 'FRONTIER_REPLAN':
                sim.commit_snapshot()
        except BaseException:
            # A durable decision written during commit cannot be rolled back.
            if not getattr(sim, '_checkpoint_decision_durable', False):
                sim._install_policy(old)
                self.plan_version, self.policy_epoch = old_versions
            raise
        self.last_change = monotonic()
        transition = {'policy_epoch': self.policy_epoch, 'plan_version': self.plan_version,
                      'classification': classification, 'updates': updates, 'cut': sim.root}
        self.transitions.append(transition)
        if len(self.transitions) > 64:
            del self.transitions[0]
        sim.trace.record('policy-change', performance_class='control', plan_version=self.plan_version,
                         metadata={'path': classification, **transition})
        return transition

    def frontier(self):
        sim = self.sim
        states = sim.transport.control.allgather(self.health)
        if sim.policy.mode != 'auto' or 'DEGRADED' not in states:
            return
        ready = sim.transport.broadcast_json(
            monotonic()-self.last_change >= sim.policy.cooldown_seconds if sim.rank == 0 else None)
        if not ready:
            return
        if sim.policy.prefetch_blocks:
            # Credit reduction is the first bounded operational response. It
            # changes no posted buffer and claims no improvement before VERIFY.
            self.apply_policy({'prefetch_blocks': 0}, classification='LIVE_TUNE')
            self.health = 'VERIFYING'

    def state(self):
        return {'plan_version': self.plan_version, 'policy_epoch': self.policy_epoch,
                'profile_version': self.profile_version, 'health': self.health,
                'policy': asdict(self.sim.policy)}
