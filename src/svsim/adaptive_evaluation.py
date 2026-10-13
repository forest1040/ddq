"""Bounded observation of injected compute; no production cost certification."""
from dataclasses import replace
import math
from statistics import median

from .lifecycle import BenchmarkCondition,BenchmarkSample,StaticCircuitOptimizer
from .telemetry import ObservePerformanceGuard


class AdaptiveEvaluation:
    """Use an actual noninjected prefix as an evaluation-only Guard baseline.

    The caller supplies rank-MAX completed group times in an agreed order.
    This evaluator never creates ReplanEvidence or certifies a positive gain.
    """
    def __init__(self,*,baseline_groups=4,window_size=2,consecutive_windows=2,max_events=64):
        for value in (baseline_groups,window_size,consecutive_windows,max_events):
            if type(value) is not int or not 0<=value<=256:raise ValueError('Invalid bounded evaluation limit')
        if not window_size or not consecutive_windows or not max_events:raise ValueError('Positive limits required')
        self.baseline_groups=baseline_groups
        self.window_size=window_size;self.consecutive_windows=consecutive_windows
        self.max_events=max_events
        self.prefix=[];self.events=[];self.guard=None;self.dropped_events=0
        self.condition=None
        self.work_class=None

    def observe(self,event,*,profile,circuit_hash,num_qubits,block_bits,ranks):
        service=event['actual_service_seconds'];delay=event['injected_delay_seconds']
        if any(isinstance(t,bool) or not math.isfinite(t) or t<0 for t in (service,delay)):
            raise ValueError('Invalid evaluation timing')
        if len(self.events)>=self.max_events:
            self.dropped_events+=1
            return {'health':'INSUFFICIENT_METRICS','reason':'EVALUATION_EVENT_LIMIT'}
        work_class=event.get('work_class')
        if self.events and self.work_class!=work_class:
            row={'health':'INSUFFICIENT_BASELINE','reason':'EXECUTOR_CLASS_MISMATCH'}
        elif event.get('success') is not True:
            row={'health':'INSUFFICIENT_METRICS','reason':'FAILED_EXECUTOR_GROUP'}
        elif self.guard is None and self.baseline_groups and len(self.prefix)<self.baseline_groups and delay==0:
            self.work_class=work_class
            self.prefix.append(service)
            row={'health':'OBSERVING','reason':'EVALUATION_BASELINE_PREFIX'}
            if len(self.prefix)==self.baseline_groups:
                latency=median(self.prefix)
                if latency>0:
                    self.condition=BenchmarkCondition('injected-compute-evaluation',ranks=ranks,
                        details={'n':num_qubits,'b':block_bits,'endpoint':'executor-group','work_class':work_class,'evidence':'injected'})
                    baseline=BenchmarkSample(self.condition,latency_ms=latency*1000)
                    reference=replace(profile,benchmarks=(baseline,))
                    plan=StaticCircuitOptimizer().create_plan(circuit_hash,reference,plan_id='injected-evaluation')
                    self.guard=ObservePerformanceGuard(reference,plan,window_size=self.window_size,
                        consecutive_windows=self.consecutive_windows)
                    row={'health':'BASELINED','reason':'ACTUAL_PREFIX_EVALUATION_ONLY'}
        elif self.guard is None:
            row={'health':'INSUFFICIENT_BASELINE','reason':'NO_NONINJECTED_PREFIX_BASELINE'}
        else:
            report=self.guard.observe(BenchmarkSample(self.condition,latency_ms=(service+delay)*1000))
            row={'health':report.health_state,'reason':report.reason,'ratio':report.degradation_ratio}
        row.update({'ordinal':len(self.events),'source_ids':tuple(event.get('source_ids',())),
            'actual_service_seconds':service,'injected_delay_seconds':delay,'scope':'injected',
            'production_profile':False})
        self.events.append(row)
        return row

    def report(self,*,transitions=(),candidates=()):
        events=tuple(self.events)
        return {'schema_version':1,'kind':'adaptive-evaluation','scope':'injected',
            'events':events,'dropped_events':self.dropped_events,
            'degraded_observed':any(e['health']=='DEGRADED' for e in events),
            'switch_count':len(transitions),'candidate_count':len(candidates),
            'eligible_candidate_count':sum(bool(c.get('eligible')) for c in candidates),
            'positive_gain_verified':False,'status':'observation-and-safe-refusal',
            'reason':'AUTOMATIC_TRANSITION_COST_AND_VERIFY_PENDING'}
