"""Collective runtime policy changes at drained, versioned prefix cuts."""
from dataclasses import asdict, dataclass, replace
import json
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


@dataclass(frozen=True)
class _GroupMark:
    event_id: str
    resource_before: float


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
        self._eta_baseline_adapter = None
        self._eta_budget=0
        self._eta_endpoint=None
        self._eta_cost_views={}
        self.latest_eta=None
        self.generated_candidates={}
        from .runtime_cost_registry import RuntimeCostRegistry
        # The single source of runtime costs for ETA, Guard, cache, gradient and replan.
        self.costs=RuntimeCostRegistry()
        # Rolling measured completions of this run (validated, non-warmup, non-injected).
        from .runtime_observed_costs import observed_cache_cost, observed_task_cost
        self.cost_decisions={}  # last freshness/quality reason per rolling cost class
        scope=lambda:'same-host' if len({r['host'] for r in sim.environment})==1 else 'cross-host'
        for suffix,diagnostic,provenance in (('-diagnostic',True,'derived'),('',False,'measured')):  # measured registered last: newest
            self.costs.register('observed-task-seconds','runtime-collector'+suffix,provenance,
                lambda _context,condition,d=diagnostic:observed_task_cost(self.measurements,condition,
                    self.policy_epoch,self.plan_version,diagnostic=d,decisions=self.cost_decisions),units='seconds/call')
            self.costs.register('cache-entry','runtime-collector-io'+suffix,provenance,
                lambda _key,_condition,d=diagnostic:observed_cache_cost(self.measurements,scope(),
                    self.policy_epoch,self.plan_version,diagnostic=d,decisions=self.cost_decisions),units='seconds')
        self.condition_provenance=None
        self.gradient_cost_reason=None
        self.evaluation_estimate=None
        self.monitoring={'eta':True,'epoch_summary':True}
        self.checkpoint_walls=[]
        self.max_switches=4
        self.switch_count=0
        self.verifying=None
        self.replan_decisions=[]
        self._eta_last_progress=0
        self.performance_injection=None
        self._executor_observer=None
        self._injection_budget=0
        self._gradient_profile_budget=0
        self.gradient_calibration=None
        self.latest_gradient=None
        self.configure_eta(max_bytes=max(1024,min(8192,sim.config.metadata_reserve_bytes//32)))

    def configure_measurements(self, *, max_bytes, max_conditions=256, per_condition=64,
                               enabled=True):
        """Reserve bounded observer storage, including indices and snapshot copies."""
        from .runtime_measurement import RuntimeMeasurementCollector
        collector=RuntimeMeasurementCollector(max_bytes=max_bytes,max_conditions=max_conditions,
            per_condition=per_condition,enabled=enabled)
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
        spill=getattr(self,'_raw_spill',None)
        if spill is not None:
            try:spill['stream'].close()
            finally:
                spill['lease'].release();self._raw_spill=None
        if self._measurement_budget:
            self.sim.budget.release(self._measurement_budget)
            self._measurement_budget=0
        if getattr(self,'_eta_budget',0):
            self.sim.budget.release(self._eta_budget)
            self._eta_budget=0
        if hasattr(self,'remaining_extractor'):self.remaining_extractor.invalidate()
        for name in ('_injection_budget','_gradient_profile_budget'):
            amount=getattr(self,name,0)
            if amount:self.sim.budget.release(amount)
            setattr(self,name,0)
        self.performance_injection=None
        self._executor_observer=None
        self.gradient_calibration=None

    def configure_performance_injection(self,spec,*,observer=None):
        """Opt-in, in-process delays for evaluation only.

        compute delays each executor-group call; storage delays each storage
        read/write request (optionally per byte); mpi delays each posted
        source-side transfer. Storage and MPI waits are rank-local (no
        collective) and are paid inside the executor group's measured service.
        """
        RESOURCE_OPERATIONS={'compute':{'executor-group'},'storage':{'storage-read','storage-write'},
                             'mpi':{'mpi-transfer'}}
        from .performance_injection import InjectionSpec,PerformanceInjection
        sim=self.sim
        error=None
        try:
            if sim._private_epoch:raise ValueError('Injection requires a published cut')
            if not isinstance(spec,InjectionSpec):raise TypeError('InjectionSpec required')
            if spec.trigger!='work' or not set(spec.operations)<=RESOURCE_OPERATIONS[spec.resource]:
                raise ValueError('Actual runtime injection needs a work trigger and operations of its resource')
            if spec.resource=='compute' and (spec.operations!=('executor-group',) or spec.per_bytes is not None):
                raise ValueError('Compute injection delays executor-group calls only')
            if observer is not None and not callable(observer):raise TypeError('Observer must be callable')
        except Exception as exc:error=str(exc)
        sim.transport.vote(error)
        sim.transport.agree({'performance_injection':asdict(spec)})
        pool=8*4096+512*spec.max_events
        extra=max(0,pool-self._injection_budget)
        admitted=False;error=None
        try:sim.budget.reserve(extra);admitted=True
        except Exception as exc:error=str(exc)
        try:sim.transport.vote(error)
        except BaseException:
            if admitted:sim.budget.release(extra)
            raise
        if pool<self._injection_budget:sim.budget.release(self._injection_budget-pool)
        self._injection_budget=pool
        self.performance_injection=PerformanceInjection(spec)
        self.performance_injection.start()
        self._executor_observer=observer
        self.resource_injected_seconds=0.0
        hook=self.inject_resource if spec.resource!='compute' else None
        sim.store.injection_hook=hook
        sim.transport.injection_hook=hook
        sim._measurement_injected=True
        self.invalidate_eta('EVALUATION_INJECTION_ATTACHED')
        return self.performance_injection

    def inject_resource(self,operation,nbytes=0):
        """Rank-local evaluation wait for one storage request or MPI transfer."""
        from time import sleep
        driver=self.performance_injection
        if driver is None:return 0.0
        ticket=driver.request(operation,work=1,nbytes=nbytes)
        if ticket is None:return 0.0
        started=monotonic()
        while ticket.state=='pending':
            driver.progress()
            if ticket.state=='pending':sleep(min(.001,max(0,ticket.ready_at-monotonic())))
        ticket.complete(service_seconds=0.0)
        waited=monotonic()-started
        self.resource_injected_seconds+=waited
        return waited

    def before_executor(self,event_id,source_count):
        from time import sleep
        driver=self.performance_injection
        if driver is None:return None
        if driver.spec.resource!='compute':
            # Observe groups under resource injection; their waits are inside service time.
            return _GroupMark(event_id,self.resource_injected_seconds)
        ticket=None;error=None
        # Progress actual requests while waiting for a bounded service barrier.
        try:
            ticket=driver.request('executor-group',work=source_count,token=event_id)
            while ticket is not None and ticket.state=='pending':
                driver.progress()
                if ticket.state=='pending':
                    self.sim._progress_io()
                    self.sim.transport.poll()
                    sleep(min(.001,max(0,ticket.ready_at-monotonic())))
        except Exception as exc:
            error=str(exc)
            if ticket is not None:driver.cancel(ticket)
        self.sim.transport.vote(error or (str(self.sim._async_errors[0]) if self.sim._async_errors else None))
        return ticket

    def after_executor(self,ticket,*,service_seconds,source_ids,success,work_class=None):
        if ticket is None:return
        if isinstance(ticket,_GroupMark):
            if self._executor_observer is not None:
                try:self._executor_observer({'event_id':ticket.event_id,'source_ids':source_ids,
                    'actual_service_seconds':service_seconds,'injected_delay_seconds':0.0,
                    'resource_injected_seconds':self.resource_injected_seconds-ticket.resource_before,
                    'resource':self.performance_injection.spec.resource,'success':success,'evidence':'injected',
                    'work_class':work_class})
                except Exception as exc:self.sim._async_errors.append(exc)
            return
        if success:self.performance_injection.complete(ticket,service_seconds=service_seconds)
        else:self.performance_injection.cancel(ticket)
        if self._executor_observer is not None:
            try:self._executor_observer({'event_id':ticket.event_id,'source_ids':source_ids,
                'actual_service_seconds':service_seconds,'injected_delay_seconds':ticket.actual_delay_seconds,
                'planned_delay_seconds':ticket.planned_delay,'success':success,'evidence':'injected','work_class':work_class})
            except Exception as exc:self.sim._async_errors.append(exc)

    def attach_gradient_calibration(self,report):
        """Store bounded explicit evidence; attaching never runs a pilot."""
        from copy import deepcopy
        from .journal import canonical
        sim=self.sim
        data=None;error=None
        try:
            if sim._private_epoch:raise ValueError('Gradient profile requires a published cut')
            data=report.to_dict() if hasattr(report,'to_dict') else dict(report)
            size=len(canonical(data).encode())
            if size>65536:raise ValueError('Gradient calibration exceeds runtime profile limit')
        except Exception as exc:error=str(exc)
        sim.transport.vote(error)
        sim.transport.agree({'gradient_calibration':content_hash(data)})
        pool=8*size;extra=max(0,pool-self._gradient_profile_budget)
        admitted=False;error=None
        try:sim.budget.reserve(extra);admitted=True
        except Exception as exc:error=str(exc)
        try:sim.transport.vote(error)
        except BaseException:
            if admitted:sim.budget.release(extra)
            raise
        if pool<self._gradient_profile_budget:sim.budget.release(self._gradient_profile_budget-pool)
        self._gradient_profile_budget=pool
        self.gradient_calibration=deepcopy(data)
        self.costs.register('gradient-method','gradient-calibration','measured',self._gradient_costs,
                            units='seconds/gradient')
        self.invalidate_eta('GRADIENT_PROFILE_ATTACHED')
        return content_hash(data)

    def _gradient_costs(self,context,_condition):
        """Validated held-out method costs for this exact context, or None (reason kept)."""
        from .gradient_cost_adapter import GradientCostAdapter
        if context.get('injected'):
            self.gradient_cost_reason='INJECTED_EVALUATION';return None
        if not context.get('build'):
            self.gradient_cost_reason='INSUFFICIENT_BUILD_BASELINE';return None
        adapter=GradientCostAdapter()
        costs=adapter.adapt(self.gradient_calibration,context)
        self.gradient_cost_reason=adapter.last_reason
        return costs or None

    def configure_eta(self, *, max_bytes=65536,scan_items=256,scan_seconds=.02):
        from .remaining_work import CaptureLimits,RemainingWorkExtractor
        limits=CaptureLimits(max_bytes=max_bytes,scan_items=scan_items,scan_seconds=scan_seconds)
        pool=8*max_bytes
        extra=max(0,pool-self._eta_budget)
        # Monitoring must not cause foreground I/O just to make room.
        try:self.sim.budget.reserve(extra)
        except MemoryError:
            if self._eta_budget:raise
            limits=CaptureLimits(max_bytes=1024,scan_items=1,scan_seconds=.001)
            pool=0
        if pool<self._eta_budget:self.sim.budget.release(self._eta_budget-pool)
        self._eta_budget=pool
        self.remaining_extractor=RemainingWorkExtractor(limits)
        self.latest_eta=None

    def set_eta_cost_view(self,name,resolver):
        """Explicit diagnostic views never certify an automatic switch."""
        if name not in {'baseline','observed','adapted'}:raise ValueError('Unknown ETA view')
        self._eta_cost_views[name]=resolver
        self.costs.register('eta-view:'+name,'explicit:'+name,'manual',lambda task,_condition,r=resolver:r(task))
        self.invalidate_eta('DIAGNOSTIC_COST_VIEW_CHANGED')

    def invalidate_eta(self, reason, *, reset_endpoint=False):
        self.remaining_extractor.invalidate()
        self.generated_candidates.clear()
        if reset_endpoint:self._eta_endpoint=None
        self.remaining_seconds=None
        if self.latest_eta is not None:
            old=dict(self.latest_eta)
            old['stale']=True
            old['invalidation_reason']=reason
            old['rank_max_expected_seconds']={name:None for name in old['views']}
            old['views']={name:{**row,'lower_seconds':None,'expected_seconds':None,'upper_seconds':None,
                'reason':'STALE_SNAPSHOT','unknown_reasons':('STALE_SNAPSHOT',reason)}
                for name,row in old['views'].items()}
            self.latest_eta=old

    def update_eta(self, *, endpoint_frontier=None, collective=False):
        from .runtime_eta import RuntimeETA
        sim=self.sim
        if not self.monitoring['eta']:
            if endpoint_frontier is not None:self._eta_endpoint=endpoint_frontier
            return None
        error=None
        try:
            if endpoint_frontier is not None:self._eta_endpoint=endpoint_frontier
            snapshot=self.remaining_extractor.capture(sim,endpoint_frontier=self._eta_endpoint)
            if getattr(sim,'_measurement_injected',False):
                snapshot=replace(snapshot,unknown_reasons=snapshot.unknown_reasons+('EVALUATION_INJECTION_ACTIVE',))
            if not self._eta_budget:
                snapshot=replace(snapshot,complete=False,unknown_reasons=snapshot.unknown_reasons+('ETA_MONITOR_BUDGET',))
            reports={}
            sources={}
            costs=self.costs
            profiled='task-seconds' in costs.kinds()
            for name in ('baseline','observed','adapted'):
                explicit=name in self._eta_cost_views
                resolver=None;label='unavailable'
                if explicit:
                    resolver=lambda task,name=name:costs.resolve('eta-view:'+name,task,purpose='diagnostic').value
                    label='diagnostic-explicit'
                elif profiled and name=='baseline':
                    # The baseline stays the first immutable profile.
                    resolver=lambda task:costs.resolve('task-seconds',task.context,task.condition,
                        purpose='production',select='first').value
                    label='conditional-envelope'
                elif name=='observed':
                    # Observed: newest profile cell, else this run's rolling measured completions.
                    def resolver(task):
                        cell=costs.resolve('task-seconds',task.context,task.condition,purpose='production').value
                        return cell if cell is not None else costs.resolve('observed-task-seconds',task.context,
                            task.condition,purpose='production').value
                    label='conditional-envelope+rolling-observed' if profiled else 'rolling-observed'
                sources[name]=label
                reports[name]=asdict(RuntimeETA().evaluate(snapshot,resolver))
            if 'eta-remaining' in costs.kinds():
                from .runtime_time_model_eta import VIEW_NAME
                resolved=costs.resolve('eta-remaining',snapshot,purpose='diagnostic')
                if resolved.known:
                    reports[VIEW_NAME]=resolved.value
                    sources[VIEW_NAME]=resolved.source_id
            result={'schema_version':1,'kind':'runtime-eta','snapshot_id':snapshot.snapshot_id,
                'endpoint':snapshot.endpoint,'scope':'rank-local','published_frontier':snapshot.published_frontier,
                'endpoint_frontier':snapshot.endpoint_frontier,'remaining_sources':snapshot.remaining_sources,
                'staged_sources':snapshot.staged_sources,'scanned_sources':snapshot.scanned_sources,
                'unscanned_sources':snapshot.unscanned_sources,'private':snapshot.private,'stale':snapshot.stale,
                'complete':snapshot.complete,'metadata_bytes':snapshot.metadata_bytes,
                'extraction_seconds':snapshot.extraction_seconds,'views':reports,'cost_view_sources':sources,
                'eligible_for_control':False}
        except Exception as exc:
            error=exc
        if collective:sim.transport.vote(str(error) if error else None)
        elif error is not None:raise error
        if collective:
            error=None
            try:sim.trace.record('runtime-eta',performance_class='eta',metadata=result)
            except Exception as exc:error=str(exc)
            sim.transport.vote(error)
            values=sim.transport.control.allgather({name:row['expected_seconds'] for name,row in reports.items()})
            result['scope']='rank-max'
            result['rank_max_expected_seconds']={name:max(row[name] for row in values) if all(row[name] is not None for row in values) else None for name in reports}
        self.latest_eta=result
        self.remaining_seconds=(result.get('rank_max_expected_seconds',{}).get('observed') if collective else reports['observed']['expected_seconds'])
        return result

    def configure_monitoring(self,*,eta=True,epoch_summary=True):
        """Switch monitor components for overhead comparison; agreed by every rank.

        Completion collection itself is switched by configure_measurements(enabled=...).
        Disabling ETA leaves remaining_seconds unknown; disabling the epoch
        summary leaves raw lines queued (bounded) without spill or exchange.
        """
        if type(eta) is not bool or type(epoch_summary) is not bool:raise TypeError('monitoring switches are boolean')
        self.sim.transport.agree({'monitoring':{'eta':eta,'epoch_summary':epoch_summary}})
        self.monitoring={'eta':eta,'epoch_summary':epoch_summary}
        if not eta:self.invalidate_eta('ETA_MONITORING_DISABLED')

    def progress_eta(self):
        now=monotonic()
        if not self.monitoring['eta']:return
        if self.latest_eta is None or now-self._eta_last_progress<1:return
        self._eta_last_progress=now
        # Monitoring failures must not turn successful request callbacks into
        # failed foreground I/O. The next agreed boundary captures again.
        try:self.update_eta()
        except Exception:self.invalidate_eta('LOCAL_MONITOR_FAILURE')

    def cache_context(self):
        sim=self.sim
        values={'root':sim.root,'layout':sim.layout.logical_to_physical,
                'binding':sim.binding.binding_id if sim.binding else None,
                'placement':sim.placement_epoch,'policy':self.policy_epoch,'profile':self.profile_version,
                'source_length':len(sim.sequencer.operations)}
        return {key:content_hash(value) for key,value in values.items()}

    def search_candidates(self,*,collective=False):
        from .replan_candidates import generate,RuntimeCapabilityRegistry
        sim=self.sim
        error=None
        try:
            snapshot=self.remaining_extractor.capture(sim,endpoint_frontier=self._eta_endpoint)
            proposals=generate(sim,snapshot)
            rows=[asdict(p) for p in proposals]
        except Exception as exc:error=exc
        if collective:sim.transport.vote(str(error) if error else None)
        elif error is not None:raise error
        if collective:
            rows=sim.transport.broadcast_json(rows if sim.rank==0 else None)
            capabilities=RuntimeCapabilityRegistry.capture(sim)
            local=[capabilities.check(sim.policy,dict(row['updates'])) for row in rows]
            reasons=sim.transport.control.allgather(local)
            for index,row in enumerate(rows):
                rejected=sorted({rank[index] for rank in reasons if rank[index] is not None})
                if rejected:
                    row['capability_reason']='|'.join(rejected)
                    row['reason']=row['capability_reason']
        self.generated_candidates={row['proposal_id']:row for row in rows}
        return tuple(rows)

    def set_cache_cost_hints(self,summary,*,cost_resolver=None,provenance='manual'):
        """Eviction costs resolve through the registry; only measured sources reach production.

        A caller-supplied resolver is registered with the provenance the caller
        declares (manual by default). Hint-declared fields alone are manual.
        """
        from .cache_cost import resolve_cost
        if getattr(self.sim,'_private_epoch',False):raise ValueError('Cache hints require a published cut')
        if not hasattr(self.sim.cache,'set_cost_hints'):raise ValueError('Cache does not support cost hints')
        source=cost_resolver if cost_resolver is not None else (lambda version,entry,hint:resolve_cost(hint,None,version,entry))
        self.costs.register('cache-entry','cache-resolver' if cost_resolver is not None else 'cache-hints',
            provenance if cost_resolver is not None else 'manual',lambda key,_condition,f=source:f(*key))
        costs=self.costs
        return self.sim.cache.set_cost_hints(summary,context=self.cache_context(),
            cost_resolver=lambda version,entry,hint:costs.resolve('cache-entry',(version,entry,hint),
                purpose='production').value,context_getter=self.cache_context)

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
        if self._eta_baseline_adapter is None:self._eta_baseline_adapter=self.profile_adapter
        self.costs.register('task-seconds','profile:'+document.profile_id,'measured',
            lambda context,condition,a=self.profile_adapter:a.cost_sample(context,condition),units='seconds/call')
        self.profile_version+=1
        self.invalidate_eta('PROFILE_ATTACHED')
        return self.profile_adapter

    def attach_time_model(self, model, *, sync_seconds=None, configuration_fingerprint=None):
        """Attach a fitted Phase 5 time model as a separate gates-only ETA view.

        The model must have been fitted for this host/build/allocation and this
        executor condition; mismatches are reported by the view, never forced.
        A caller that already verified a profile against the current host may
        pass its fingerprint; otherwise the runtime directory is probed.
        """
        from .profile_document import probe_profile
        from .runtime_time_model_eta import runtime_condition
        sim=self.sim
        nodes=len({row['host'] for row in sim.environment})
        if configuration_fingerprint is None:
            configuration_fingerprint=probe_profile({'scope':'local','ranks':sim.size,'nodes':nodes,
                'threads':sim.policy.io_workers},(sim.directory,)).configuration_fingerprint
        from .runtime_time_model_eta import evaluate as time_model_view
        self.time_model=model
        # Fitted (derived) remaining-time view: diagnostic, never a production cost.
        for kind,source in list(self.costs.sources):
            if kind=='eta-remaining':self.costs.unregister(kind,source)
        self.costs.register('eta-remaining','time-model:'+model.model_id,'derived',
            lambda snapshot,_condition:time_model_view(snapshot,self.time_model,**self._time_model_context),
            units='seconds')
        self._time_model_context={'condition':runtime_condition(sim,nodes=nodes,threads=sim.policy.io_workers),
            'configuration_fingerprint':configuration_fingerprint,'sync_seconds':sync_seconds,
            'block_bits':sim.block_bits,'blocks_per_rank':sim.ownership.blocks_per_rank,
            'rank_boundary':sim.num_qubits-sim.ownership.rank_bits}
        self.invalidate_eta('TIME_MODEL_ATTACHED')

    def set_time_model_sync_latency(self, seconds):
        """Evaluate the time-model view at a freshly probed device sync latency (None: calibration median)."""
        if getattr(self,'time_model',None) is None:raise ValueError('No time model is attached')
        self._time_model_context['sync_seconds']=seconds

    def record_cost(self, sample):
        """Register a measured matched task cost, never an assumed link rate."""
        self.cost_model.record(sample)
        self.profile_version = max(self.profile_version, self.cost_model.version)
        self.invalidate_eta('COST_PROFILE_CHANGED')

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
        # HardwareProfile benchmarks carry no build/fingerprint/quality provenance: legacy, health only.
        self.costs.register('guard-baseline','hardware-profile','legacy',
            lambda _context,condition,p=hardware_profile:p.matching_benchmark(condition))

    def observe(self, sample):
        if self.guard is None:
            return None
        report = self.guard.observe(sample)
        self.health = report.health_state
        self.profile_version = self.guard.runtime_profile.version
        baseline = self.costs.resolve('guard-baseline', None, sample.condition, purpose='health')
        if baseline.known:
            self.condition_hash = content_hash(asdict(sample.condition))
            self.condition_provenance = baseline.provenance
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
        if condition is not None and self.measurements.enabled:
            sim = self.sim
            same_host = len({r['host'] for r in sim.environment}) == 1
            self.measurements.complete(
                f'io:{request.request_id}',
                context={'configuration_fingerprint': getattr(self.profile_adapter, 'fingerprint', None),
                         'backend': sim.config.backend, 'engine': sim.io_engine.identity,
                         'ranks': sim.size, 'policy_epoch': self.policy_epoch, 'plan_version': self.plan_version},
                condition=asdict(condition), stage='storage-' + request.operation,
                units='calls', quantity=1,
                queue_seconds=getattr(request, 'queue_seconds', None),
                service_seconds=(request.elapsed_seconds if
                    getattr(request,'storage_metadata',{}).get('service_seconds_known',True) else None),
                protocol_seconds=elapsed,
                payload_bytes=sim.store.slot_bytes, storage_bytes=sim.store.slot_bytes,
                evidence='injected' if getattr(sim,'_measurement_injected',False) else 'measured',
                success=successful, validator=lambda: bool(getattr(request, 'validated', False)),
                warmup=bool(getattr(sim,'_measurement_warmup',False)),
                scope='same-host' if same_host else 'cross-host')
        if successful and elapsed > 0 and not getattr(self.sim,'_measurement_injected',False):
            self.observe(BenchmarkSample(condition,
                          bandwidth_bytes_per_second=self.sim.store.slot_bytes/elapsed,
                          latency_ms=elapsed*1000))

    def mpi_completion(self, request, elapsed):
        """Record one source-side whole transfer after protocol acknowledgment."""
        if not self.measurements.enabled:
            return False
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
            evidence='injected' if getattr(self.sim,'_measurement_injected',False) else 'measured',
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
        if getattr(self, 'time_model', None) is not None:
            # The time-model condition hashes the executor policy; price the new one.
            from .runtime_time_model_eta import runtime_condition
            context = self._time_model_context
            context['condition'] = runtime_condition(sim, nodes=context['condition']['nodes'],
                                                     threads=context['condition']['threads'])
        self.invalidate_eta('POLICY_CHANGED')
        self.last_change = monotonic()
        transition = {'policy_epoch': self.policy_epoch, 'plan_version': self.plan_version,
                      'classification': classification, 'updates': updates, 'cut': sim.root}
        self.transitions.append(transition)
        if len(self.transitions) > 64:
            del self.transitions[0]
        sim.trace.record('policy-change', performance_class='control', plan_version=self.plan_version,
                         metadata={'path': classification, **transition})
        return transition

    def record_stage(self, stage, condition, *, seconds, cpu_seconds=None, payload_bytes=None, quantity=1,
                     units='calls', success=True):
        """Completion record for one sub-stage (Frame resolve, packing, catalog/checkpoint commit).

        The event is a child of the innermost open executor/epoch group so a group
        sample and its stages are never both added by a consumer.
        """
        collector=self.measurements
        if collector is None or not collector.enabled:return False
        sim=self.sim
        self._stage_serial=getattr(self,'_stage_serial',0)+1
        same_host=len({row['host'] for row in sim.environment})==1
        return collector.complete(f'{sim.run_id}:{stage}:{self._stage_serial}',
            context=sim._measurement_context(),condition={'operation':stage,**condition},
            parent_id=collector.current_group(),stage=stage,units=units,quantity=quantity,
            protocol_seconds=seconds,cpu_seconds=cpu_seconds,payload_bytes=payload_bytes,
            success=success,validated=success,
            evidence='injected' if getattr(sim,'_measurement_injected',False) else 'measured',
            warmup=bool(getattr(sim,'_measurement_warmup',False)),
            scope='same-host' if same_host else 'cross-host')

    def observe_evaluation(self, row):
        """Accept one agreed evaluation row (health, and evidence when degraded)."""
        self.health = row['health']
        if row.get('estimate') is not None:
            self.evaluation_estimate = dict(row['estimate'])
            # Injected evaluation evidence: usable for evaluation, never for production replans.
            self.costs.register('replan-per-gate','evaluation-estimate','injected',
                                lambda _c,_d,e=dict(row['estimate']):e)
        if row['health'] in {'STABLE','VERIFY_FAILED'} and self.verifying is not None:
            self.verifying = {**self.verifying, 'result': row['health'], 'reason': row.get('reason')}
            self.replan_decisions.append({'kind':'verify', **self.verifying})
            self.verifying = None

    def record_checkpoint_wall(self, seconds):
        self.checkpoint_walls = (self.checkpoint_walls + [seconds])[-16:]

    RAW_SPILL_LIMIT_BYTES = 16 * 1024 * 1024
    EPOCH_SUMMARY_LIMIT = 64

    def epoch_boundary(self):
        """Spill raw completions to bounded local disk and exchange a rank summary (collective)."""
        from collections import deque
        sim=self.sim
        collector=self.measurements
        if collector is None or not collector.enabled or not self.monitoring['epoch_summary']:return None
        started=monotonic();error=None;written=0
        lines=collector.drain_raw()
        try:written=self._spill_raw(lines)
        except Exception as exc:error=str(exc)
        sim.transport.vote(error)
        self.record_stage('monitor-raw-flush',{'target':'rank-local-jsonl'},seconds=monotonic()-started,
                          quantity=max(1,len(lines)),units='lines',payload_bytes=written)
        started=monotonic()
        local={}
        for line in lines:
            row=json.loads(line)
            if row.get('evidence')!='measured' or row.get('warmup'):continue
            seconds=row.get('protocol_seconds') if row.get('protocol_seconds') is not None else row.get('service_seconds')
            stat=local.setdefault(row['stage'],[0,0.0,0.0])
            stat[0]+=1
            if seconds is not None:stat[1]+=seconds;stat[2]=max(stat[2],seconds)
        if len(local)>32:local=dict(sorted(local.items())[:32])
        ranks=sim.transport.control.allgather(local)
        stages={}
        for stage in sorted({name for rank in ranks for name in rank}):
            totals=[rank.get(stage,[0,0.0,0.0])[1] for rank in ranks]
            mean=sum(totals)/len(totals)
            stages[stage]={'count_max':max(rank.get(stage,[0])[0] for rank in ranks),
                'rank_max_seconds':max(totals),'rank_mean_seconds':mean,
                'imbalance':max(totals)/mean if mean>0 else None,
                'slowest_rank':totals.index(max(totals)),
                'event_max_seconds':max(rank.get(stage,[0,0.0,0.0])[2] for rank in ranks)}
        if not hasattr(self,'epoch_summaries'):self.epoch_summaries=deque(maxlen=self.EPOCH_SUMMARY_LIMIT)
        summary={'frontier':sim.frontier,'ranks':sim.size,'stages':stages,
                 'dropped_raw':sim.transport.control.allreduce(collector.dropped_raw),
                 'raw_spill_bytes':sim.transport.control.allreduce(getattr(self,'raw_spill_bytes',0)),
                 'raw_spill_truncated':any(sim.transport.control.allgather(getattr(self,'raw_spill_truncated',False)))}
        self.epoch_summaries.append(summary)
        self.record_stage('monitor-summary-exchange',{'ranks':sim.size},seconds=monotonic()-started)
        return summary

    def _spill_raw(self,lines):
        """Append raw lines under a chunked disk reservation; stop (counted) at the total limit."""
        from .disk_budget import AppendReservation
        if not lines:return 0
        sim=self.sim
        spill=getattr(self,'_raw_spill',None)
        if spill is None:
            path=sim.directory/f'rank-{sim.rank:04d}'/'measurements'/f'raw-{sim.run_id}.jsonl'
            budget=lambda:getattr(sim,'disk_budget',None) or getattr(sim.store,'disk_budget',None)
            lease=AppendReservation(budget,operation='measurement-append')
            lease.admit(1)
            path.parent.mkdir(parents=True,exist_ok=True)
            spill=self._raw_spill={'path':path,'stream':path.open('ab',buffering=0),'lease':lease}
            self.raw_spill_bytes=0;self.raw_spill_truncated=False
        written=0
        for line in lines:
            if self.raw_spill_bytes+len(line)>self.RAW_SPILL_LIMIT_BYTES:
                self.raw_spill_truncated=True;self.measurements.dropped_raw+=1;continue
            spill['lease'].admit(len(line))
            if spill['stream'].write(line)!=len(line):raise OSError('Incomplete raw measurement write')
            spill['lease'].wrote(len(line))
            self.raw_spill_bytes+=len(line);written+=len(line)
        return written

    def frontier(self):
        sim = self.sim
        self.epoch_boundary()
        self.update_eta(collective=True)
        states = sim.transport.control.allgather(self.health)
        if sim.policy.mode not in {'auto','recommend'} or 'DEGRADED' not in states or self.verifying is not None:
            return
        ready = sim.transport.broadcast_json(
            monotonic()-self.last_change >= sim.policy.cooldown_seconds and self.switch_count < self.max_switches
            if sim.rank == 0 else None)
        if not ready:
            return
        decision = self._evaluate_candidates()
        if decision['selected'] is not None and sim.policy.mode == 'auto':
            self._apply_selected(decision)

    def _evaluate_candidates(self):
        """Price every generated proposal from runtime evidence; rank-conservative selection."""
        from .replan_candidates import ReplanProposal
        from .replan_cost import evaluate
        from .replan_cost_producer import costs_for
        sim = self.sim
        started = monotonic()
        rows = self.search_candidates(collective=True)
        planning = sim.transport.max_value(monotonic()-started)
        started = monotonic(); sim.drain()
        drain = sim.transport.max_value(monotonic()-started)
        walls = sim.transport.control.allgather(max(self.checkpoint_walls) if self.checkpoint_walls else None)
        checkpoint = None if any(w is None for w in walls) else max(walls)
        end = self._eta_endpoint if self._eta_endpoint is not None else len(sim.sequencer.operations)-1
        purpose = 'evaluation' if getattr(sim, '_measurement_injected', False) else 'production'
        evidence = self.costs.resolve('replan-per-gate', purpose=purpose)
        local = []
        for row in rows:
            proposal = ReplanProposal(**{**row, 'updates': tuple(tuple(item) for item in row['updates'])})
            costs, counts = costs_for(sim, row, evidence.value, end=end, planning_seconds=planning,
                                      drain_seconds=drain, checkpoint_seconds=checkpoint,
                                      qualified=evidence.known)
            result = evaluate(proposal, costs, minimum_gain_fraction=sim.policy.minimum_gain_fraction)
            local.append({'proposal_id': row['proposal_id'], 'updates': row['updates'],
                          'classification': row['classification'], 'gain_lower_seconds': result.gain_lower_seconds,
                          'candidate_upper_seconds': result.total_candidate_upper_seconds,
                          'continue_lower_seconds': costs.continue_lower_seconds,
                          'eligible': result.eligible, 'reason': result.reason, **counts})
        ranks = sim.transport.control.allgather(local)
        merged = []
        for index, row in enumerate(local):
            peers = [rank[index] for rank in ranks]
            gains = [p['gain_lower_seconds'] for p in peers]
            merged.append({**row, 'gain_lower_seconds': None if any(g is None for g in gains) else min(gains),
                           'eligible': all(p['eligible'] for p in peers),
                           'reason': row['reason'] if all(p['eligible'] for p in peers) else
                                     next((p['reason'] for p in peers if not p['eligible']), row['reason'])})
        eligible = [r for r in merged if r['eligible'] and r['classification'] != 'reference']
        selected = max(eligible, key=lambda r: (r['gain_lower_seconds'], r['proposal_id'])) if eligible else None
        for row in merged: self.generated_candidates.setdefault(row['proposal_id'], {}).update(row)
        decision = {'kind': 'evaluate', 'frontier': sim.frontier, 'candidates': merged,
                    'selected': selected['proposal_id'] if selected else None,
                    'estimate': evidence.value, 'cost_purpose': purpose, 'cost_source': evidence.source_id,
                    'cost_reason': evidence.reason, 'planning_seconds': planning, 'drain_seconds': drain,
                    'checkpoint_seconds': checkpoint, 'mode': sim.policy.mode}
        self.replan_decisions.append(decision)
        sim.trace.record('replan-search', performance_class='planning', metadata={
            'path': 'BOUNDED_REPLAN_SEARCH', 'candidate_count': len(merged), 'eligible_count': len(eligible),
            'selected': decision['selected'],
            'reason': 'MEASURED_POSITIVE_GAIN' if selected else
                      sorted({r['reason'] for r in merged if r['classification'] != 'reference'})})
        return decision

    def _apply_selected(self, decision):
        sim = self.sim
        row = next(r for r in decision['candidates'] if r['proposal_id'] == decision['selected'])
        updates = tuple(sorted(tuple(item) for item in row['updates']))
        candidate = PlanCandidate(row['proposal_id'], sim.root, self.plan_version, self.policy_epoch, updates,
                                  row['classification'], row['gain_lower_seconds'], True, row['reason'],
                                  monotonic()+sim.policy.candidate_ttl_seconds)
        if len(self.candidates) >= 16:
            self.candidates.pop(next(iter(self.candidates)))
        self.candidates[candidate.candidate_id] = candidate
        self.apply_candidate(candidate.candidate_id)
        self.switch_count += 1
        estimate = decision['estimate']
        self.verifying = {'proposal_id': row['proposal_id'], 'predicted_gain_lower_seconds': row['gain_lower_seconds'],
                          'service_upper_per_gate': estimate['service_upper_per_gate'],
                          'extra_upper_per_call': estimate['extra_upper_per_call'],
                          'policy_epoch': self.policy_epoch, 'plan_version': self.plan_version}
        self.health = 'VERIFYING'
        self.replan_decisions.append({'kind': 'apply', **self.verifying})

    def state(self):
        return {'plan_version': self.plan_version, 'policy_epoch': self.policy_epoch,
                'profile_version': self.profile_version, 'health': self.health,
                'policy': asdict(self.sim.policy)}
