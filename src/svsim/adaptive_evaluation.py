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
        self.verify=None
        self.verification=None
        self.rebaseline=None
        self._context=None
        self.resources=set()

    def observe(self,event,*,profile,circuit_hash,num_qubits,block_bits,ranks):
        service=event['actual_service_seconds'];delay=event['injected_delay_seconds']
        if any(isinstance(t,bool) or not math.isfinite(t) or t<0 for t in (service,delay)):
            raise ValueError('Invalid evaluation timing')
        if len(self.events)>=self.max_events:
            self.dropped_events+=1
            return {'health':'INSUFFICIENT_METRICS','reason':'EVALUATION_EVENT_LIMIT'}
        work_class=event.get('work_class')
        # The injected resource lets layout candidates charge the excess only to calls that use it.
        self.resources.add(event.get('resource'))
        if self.verify is not None:
            row=self._verify(event,service,delay)
            if row['health']=='STABLE':self._rebaseline(work_class)
        elif self.rebaseline is not None and work_class==self.work_class and self.guard is None:
            self.rebaseline.append(service+delay)
            row={'health':'STABLE','reason':'POST_SWITCH_REBASELINE'}
            if len(self.rebaseline)>=self.baseline_groups:self._install_guard(median(self.rebaseline),work_class,'post-switch')
        elif self.events and self.work_class!=work_class:
            row={'health':'INSUFFICIENT_BASELINE','reason':'EXECUTOR_CLASS_MISMATCH'}
        elif event.get('success') is not True:
            row={'health':'INSUFFICIENT_METRICS','reason':'FAILED_EXECUTOR_GROUP'}
        elif self.guard is None and self.baseline_groups and len(self.prefix)<self.baseline_groups and delay==0:
            self.work_class=work_class
            self.prefix.append(service)
            row={'health':'OBSERVING','reason':'EVALUATION_BASELINE_PREFIX'}
            if len(self.prefix)==self.baseline_groups:
                self._context=(profile,circuit_hash,num_qubits,block_bits,ranks)
                if self._install_guard(median(self.prefix),work_class,'prefix'):
                    row={'health':'BASELINED','reason':'ACTUAL_PREFIX_EVALUATION_ONLY'}
        elif self.guard is None:
            row={'health':'INSUFFICIENT_BASELINE','reason':'NO_NONINJECTED_PREFIX_BASELINE'}
        else:
            report=self.guard.observe(BenchmarkSample(self.condition,latency_ms=(service+delay)*1000))
            row={'health':report.health_state,'reason':report.reason,'ratio':report.degradation_ratio}
            if row['health']=='DEGRADED':row['estimate']=self.estimate()
        row.update({'ordinal':len(self.events),'source_ids':tuple(event.get('source_ids',())),
            'actual_service_seconds':service,'injected_delay_seconds':delay,'scope':'injected',
            'production_profile':False})
        self.events.append(row)
        return row

    def _install_guard(self,latency,work_class,origin):
        if latency<=0:return False
        profile,circuit_hash,num_qubits,block_bits,ranks=self._context
        self.condition=BenchmarkCondition('injected-compute-evaluation',ranks=ranks,
            details={'n':num_qubits,'b':block_bits,'endpoint':'executor-group','work_class':work_class,
                     'evidence':'injected','baseline_origin':origin})
        reference=replace(profile,benchmarks=(BenchmarkSample(self.condition,latency_ms=latency*1000),))
        plan=StaticCircuitOptimizer().create_plan(circuit_hash,reference,plan_id='injected-evaluation')
        self.guard=ObservePerformanceGuard(reference,plan,window_size=self.window_size,
            consecutive_windows=self.consecutive_windows)
        self.rebaseline=None
        return True

    def _rebaseline(self,work_class):
        """After a verified switch, re-judge later calls against the new executor class itself."""
        self.guard=None;self.work_class=work_class;self.rebaseline=[]

    def estimate(self):
        """Per-gate service and per-call excess from this evaluation's own measured events.

        The baseline prefix holds single-gate calls without injection; the excess
        is (service+delay) minus the baseline median over the post-baseline
        events of the same class. Unknown until a full Guard window exists.
        """
        if self.guard is None or any(len(e['source_ids'])!=1 for e in self.events[:len(self.prefix)]):return None
        center=median(self.prefix)
        recent=[e for e in self.events[len(self.prefix):] if e['health'] not in {'INSUFFICIENT_BASELINE','INSUFFICIENT_METRICS'}]
        recent=recent[-self.window_size*self.consecutive_windows:]
        if len(recent)<self.window_size:return None
        extra=[max(0.0,e['actual_service_seconds']+e['injected_delay_seconds']-center) for e in recent]
        return {'scope':'injected','service_lower_per_gate':min(self.prefix),'service_upper_per_gate':max(self.prefix),
                'extra_lower_per_call':min(extra),'extra_upper_per_call':max(extra),'excess_sample_count':len(extra),
                'resource':next(iter(self.resources)) if len(self.resources)==1 else None}

    def begin_verify(self,*,service_upper_per_gate,extra_upper_per_call,required=None,slack=.1):
        """After a switch, check each new call against the predicted per-call upper bound."""
        self.verify={'service':service_upper_per_gate,'extra':extra_upper_per_call,'slack':slack,
                     'required':required or self.window_size,'passed':0}

    def _verify(self,event,service,delay):
        state=self.verify
        if event.get('success') is not True:
            return {'health':'VERIFYING','reason':'FAILED_EXECUTOR_GROUP'}
        predicted=len(event.get('source_ids',()))*state['service']+state['extra']
        observed=service+delay
        if observed>predicted*(1+state['slack']):
            self.verify=None
            self.verification={'verified':False,'observed':observed,'predicted_upper':predicted}
            return {'health':'VERIFY_FAILED','reason':'CALL_EXCEEDED_PREDICTED_UPPER','predicted_upper':predicted}
        state['passed']+=1
        if state['passed']>=state['required']:
            self.verify=None
            self.verification={'verified':True,'calls':state['passed']}
            return {'health':'STABLE','reason':'VERIFIED_AGAINST_PREDICTED_UPPER','predicted_upper':predicted}
        return {'health':'VERIFYING','reason':'VERIFY_WINDOW','predicted_upper':predicted}

    def report(self,*,transitions=(),candidates=()):
        events=tuple(self.events)
        return {'schema_version':1,'kind':'adaptive-evaluation','scope':'injected',
            'events':events,'dropped_events':self.dropped_events,
            'degraded_observed':any(e['health']=='DEGRADED' for e in events),
            'switch_count':len(transitions),'candidate_count':len(candidates),
            'eligible_candidate_count':sum(bool(c.get('eligible')) for c in candidates),
            'verification':self.verification,
            'status':('switched-and-verified' if self.verification and self.verification['verified'] else
                      'switched-verify-failed' if self.verification else
                      'switched-verify-incomplete' if transitions else 'observation-or-safe-refusal'),
            'positive_gain_verified':False,
            'positive_gain_note':'set only by a paired fixed-vs-auto comparison whose interval excludes zero'}
