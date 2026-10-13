"""Read-only, bounded capture of published work and private request deltas."""
from dataclasses import asdict,dataclass,replace
from time import monotonic,perf_counter
from itertools import islice

from .journal import canonical,content_hash,gate_data
from .runtime_eta import CountedTask,InFlightWork
from .s2_planner import plan_window


@dataclass(frozen=True)
class CaptureLimits:
    window: int = 32
    max_groups: int = 256
    scan_items: int = 256
    scan_seconds: float = .02
    max_bytes: int = 65536

    def __post_init__(self):
        import math
        for name,limit in (('window',32),('max_groups',256),('scan_items',4096),('max_bytes',8*1024**2)):
            value=getattr(self,name)
            if type(value) is not int or not 1<=value<=limit:raise ValueError('Invalid capture '+name)
        if self.max_bytes<1024:raise ValueError('Capture max_bytes must hold a 1024-byte status envelope')
        if isinstance(self.scan_seconds,bool) or not math.isfinite(self.scan_seconds) or not 0<self.scan_seconds<=1:
            raise ValueError('Invalid scan deadline')


@dataclass(frozen=True)
class RemainingWork:
    snapshot_id: str
    context: dict
    window: tuple[CountedTask,...]
    tail: tuple[CountedTask,...]
    in_flight: tuple[InFlightWork,...]
    complete: bool
    stale: bool
    unknown_reasons: tuple[str,...]
    published_frontier: int
    endpoint_frontier: int
    remaining_sources: int
    staged_sources: int
    scanned_sources: int
    unscanned_sources: int
    private: bool
    extraction_seconds: float
    metadata_bytes: int
    endpoint: str = 'gates-only'


def executor_condition(sim,operations,layout=None):
    layout=layout or sim.layout
    physical=[replace(g,targets=tuple(layout.logical_to_physical[q] for q in g.targets),
                      controls=tuple(layout.logical_to_physical[q] for q in g.controls)) for g in operations]
    return {'operation':'executor-group' if len(operations)>1 else 'executor-gate',
        'executor':'fused-local' if len(operations)>1 else 'single-gate',
        'operations':[gate_data(g) for g in physical], 'n':sim.num_qubits,
        'block_bits':sim.block_bits,'backend':sim.config.backend,'engine':sim.io_engine.identity,
        'swap_policy':sim.policy.swap_policy,'layout':layout.logical_to_physical}


def publication_condition(sim):
    return {'operation':'epoch-publication','n':sim.num_qubits,'block_bits':sim.block_bits,
        'ranks':sim.size,'backend':sim.config.backend,'engine':sim.io_engine.identity,
        'durability':'runtime-global-cut','catalog_scale':'unknown','cache_state':'unknown'}


def runtime_context(sim):
    runtime=sim.runtime
    binding=sim.binding
    return {'root':sim.root,'frontier':sim.frontier,'n':sim.num_qubits,'b':sim.block_bits,
        'ranks':sim.size,'rank':sim.rank,'layout':sim.layout.logical_to_physical,
        'placement_epoch':sim.placement_epoch,'source_length':len(sim.sequencer.operations),
        'binding_id':binding.binding_id if binding else None,
        # binding_id includes values and ExactPi witnesses; avoid copying a
        # potentially large parameter vector into every bounded snapshot.
        'plan_version':runtime.plan_version,'policy_epoch':runtime.policy_epoch,
        'profile_version':runtime.profile_version,'policy':asdict(sim.policy),
        'configuration_fingerprint':getattr(runtime.profile_adapter,'fingerprint',None),
        'frame_class':'uninspected-block-views','scope':'rank-local'}


def cost_context(sim):
    context=sim._measurement_context()
    # This initial adapter requires the same Binding, not guessed angle classes.
    context['binding_id']=sim.binding.binding_id if sim.binding else None
    return context


def task(task_id,condition,context,*,count=1,dependencies=(),resource='main',kind='executor',source_ids=(),unknown=()):
    return CountedTask(task_id,condition,context,count,dependencies,resource,'groups',kind,
                       source_ids,content_hash({'context':context,'condition':condition}),unknown)


class RemainingWorkExtractor:
    def __init__(self,limits=None):
        self.limits=limits or CaptureLimits()
        self._scan_key=None;self._scan_cursor=0;self._tail={};self._tail_overflow=0
        self._published=None

    def invalidate(self):
        self._scan_key=None;self._tail.clear();self._published=None

    def _flights(self,sim):
        rows=[]
        for req in sim.io_engine.pending.values():
            rows.append(InFlightWork(f'io:{req.request_id}',req.operation,False,('UNCALIBRATED_IO_RESIDUAL',)))
            if len(rows)>128:raise ValueError('Request summary exceeds limit')
        for req in sim.transport.slots.values():
            if not req.acknowledged:
                rows.append(InFlightWork(f'mpi:{req.slot}:{req.generation}','mpi',False,('UNCALIBRATED_MPI_RESIDUAL',)))
                if len(rows)>128:raise ValueError('Request summary exceeds limit')
        if len(rows)>128:raise ValueError('Request summary exceeds limit')
        return tuple(rows)

    def capture(self,sim,*,endpoint_frontier=None):
        started=perf_counter()
        end=len(sim.sequencer.operations)-1 if endpoint_frontier is None else endpoint_frontier
        if type(end) is not int or not -1<=end<len(sim.sequencer.operations):raise ValueError('Invalid ETA endpoint')
        flights=self._flights(sim)
        if sim._private_epoch:
            base=self._published
            if base is None:
                return self._minimal(sim,end,flights,('NO_PUBLISHED_SNAPSHOT',),started,private=True)
            ledger=getattr(sim,'_active_epoch_ledger',None)
            # Only inspect the source IDs already present in the bounded window.
            ids={sid for t in base.window for sid in t.source_ids}
            staged={sid for sid in ids if sid in ledger.tasks and ledger.tasks[sid].state.value in {'staged','committed'}} if ledger else set()
            window=tuple(t for t in base.window if not t.source_ids or not all(sid in staged for sid in t.source_ids))
            old={t.task_id:t for t in base.window};kept={t.task_id for t in window}
            def predecessors(key):
                if key in kept:return (key,)
                return tuple(p for dep in old[key].dependencies for p in predecessors(dep))
            window=tuple(replace(t,dependencies=tuple(dict.fromkeys(p for dep in t.dependencies for p in predecessors(dep)))) for t in window)
            current=runtime_context(sim)
            stale=end!=base.endpoint_frontier or any(current.get(k)!=v for k,v in base.context.items()
                if k not in {'root','frontier','layout','placement_epoch','endpoint_frontier'})
            snap=replace(base,snapshot_id=content_hash({'base':base.snapshot_id,'staged':sorted(staged),
                'requests':[r.request_id for r in flights]}),window=window,in_flight=flights,
                private=True,staged_sources=len(staged),stale=stale,
                unknown_reasons=tuple(dict.fromkeys(base.unknown_reasons+('PRIVATE_EPOCH_RESIDUAL_UNCALIBRATED',))),
                extraction_seconds=perf_counter()-started)
            size=len(canonical(asdict(snap)).encode())
            if size+8>self.limits.max_bytes:
                return self._minimal(sim,end,flights,('CAPTURE_METADATA_LIMIT','PRIVATE_EPOCH_RESIDUAL_UNCALIBRATED'),started,private=True)
            snap=replace(snap,metadata_bytes=size)
            return replace(snap,metadata_bytes=len(canonical(asdict(snap)).encode()))
        context=runtime_context(sim)
        if end<sim.frontier:
            return self._minimal(sim,end,flights,('ENDPOINT_ALREADY_PASSED',),started)
        context['endpoint_frontier']=end
        key=content_hash(context)
        first=sim.frontier+1
        width=min(self.limits.window,sim.policy.lookahead_gates)
        stop=min(end+1,first+width)
        source=sim.sequencer.operations[first:stop]
        # Bound the plan input before building its beam/group metadata.
        if sum(len(canonical(gate_data(g)).encode())+128 for g in source)>self.limits.max_bytes//2:
            snap=self._minimal(sim,end,flights,('CAPTURE_METADATA_LIMIT',),started)
            self._published=snap
            return snap
        plan=plan_window(source,sim.layout,sim.block_bits,sim.policy.beam_width)
        order=plan.source_ids if sim.policy.swap_policy=='S2' else tuple(g.op_id for g in source)
        groups={g[0].op_id:g for g in plan.groups if len(g)>1} if sim.policy.swap_policy=='S2' else {}
        by_id={g.op_id:g for g in source}
        window=[];cursor=0;previous=();unknown=[]
        ccontext=cost_context(sim)
        while cursor<len(order):
            sid=order[cursor];group=groups.get(sid)
            ops=tuple(group) if group and tuple(g.op_id for g in group)==tuple(order[cursor:cursor+len(group)]) else (by_id[sid],)
            reasons=()
            if width<min(sim.policy.lookahead_gates,end-first+1):reasons+=('EXECUTOR_WINDOW_TRUNCATED',)
            if sim.policy.swap_policy=='S1':reasons+=('FUTURE_S1_LAYOUT_UNKNOWN',)
            if any(by_id[k].kind=='SWAP' for k in order[:cursor]):reasons+=('POST_SWAP_LAYOUT_UNKNOWN',)
            if any(g.dependencies for g in ops) and sim.binding is None:reasons+=('UNBOUND_PARAMETERS',)
            tid=f'window:{len(window)}'
            window.append(task(tid,executor_condition(sim,ops),ccontext,dependencies=previous,
                               source_ids=tuple(g.op_id for g in ops),unknown=reasons))
            previous=(tid,);cursor+=len(ops)
        if window:
            window.append(task('window:publish',publication_condition(sim),ccontext,
                               dependencies=previous,resource='main',kind='publication'))
            previous=('window:publish',)
        if self._scan_key!=key:
            self._scan_key=key;self._scan_cursor=stop;self._tail={};self._tail_overflow=0
        deadline=monotonic()+self.limits.scan_seconds
        scanned=0
        while self._scan_cursor<=end and scanned<self.limits.scan_items and monotonic()<deadline:
            op=sim.sequencer.operations[self._scan_cursor]
            data=gate_data(op);data.pop('op_id',None);data.pop('source_op_id',None)
            signature=content_hash(data)
            if signature in self._tail:self._tail[signature][1]+=1
            elif len(self._tail)<self.limits.max_groups:
                # Count semantics exactly; future fusion/Frame/layout are not guessed.
                size=len(canonical(data).encode())+128
                if sum(v[2] for v in self._tail.values())+size<=self.limits.max_bytes//2:
                    self._tail[signature]=[data,1,size]
                else:self._tail_overflow+=1
            else:self._tail_overflow+=1
            self._scan_cursor+=1;scanned+=1
        tail=[]
        for i,(data,count,_) in enumerate(self._tail.values()):
            condition={'operation':'semantic-suffix','class':data}
            tid=f'tail:{i}'
            tail.append(task(tid,condition,ccontext,count=count,dependencies=previous,
                kind='semantic-count',unknown=('FUTURE_EXECUTOR_SHAPE_UNKNOWN',)))
            previous=(tid,)
        if self._tail_overflow:
            tail.append(task('tail:overflow',{'operation':'unclassified-suffix'},ccontext,
                count=self._tail_overflow,dependencies=previous,kind='semantic-count',unknown=('SUMMARY_CLASS_LIMIT',)))
            unknown.append('SUMMARY_CLASS_LIMIT')
        pending=max(0,end-self._scan_cursor+1)
        complete=not pending
        if not window and not tail:
            # The gate endpoint excludes a durable checkpoint, but pending
            # dirty/writeback/error work still prevents a false quiescent zero.
            entries=getattr(sim.cache,'entries',{})
            if len(entries)>64:unknown.append('CACHE_QUIESCENCE_UNINSPECTED')
            if any(e.dirty for e in islice(entries.values(),64)):unknown.append('DIRTY_WRITEBACK_REMAINS')
            if getattr(sim,'_async_errors',()) or getattr(sim.cache,'errors',()):unknown.append('FAILED_WORK_REMAINS')
        snap=RemainingWork(content_hash({'context':context,'scan':self._scan_cursor}),context,
            tuple(window),tuple(tail),flights,complete,False,tuple(unknown),sim.frontier,end,
            max(0,end-sim.frontier),0,self._scan_cursor-stop,pending,False,perf_counter()-started,0)
        size=len(canonical(asdict(snap)).encode())
        if size+8>self.limits.max_bytes:
            snap=self._minimal(sim,end,flights,('CAPTURE_METADATA_LIMIT',),started)
        else:
            snap=replace(snap,metadata_bytes=size)
            snap=replace(snap,metadata_bytes=len(canonical(asdict(snap)).encode()))
        self._published=snap
        return snap

    def _minimal(self,sim,end,flights,reasons,started,private=False):
        context={'root':getattr(sim,'_rollback_root',sim.root) if private else sim.root,
                 'frontier':self._published.published_frontier if private and self._published else sim.frontier,
                 'endpoint_frontier':end,'scope':'rank-local'}
        # Budget fallback keeps the unresolved-request reason, not a possibly
        # oversized request descriptor array. Never report a quiescent zero.
        if flights:reasons=tuple(dict.fromkeys(reasons+('REQUEST_DETAILS_OMITTED',)))
        snap=RemainingWork(content_hash(context),context,(),(),(),False,False,reasons,
            context['frontier'],end,max(0,end-context['frontier']),0,0,max(0,end-context['frontier']),
            private,perf_counter()-started,0)
        snap=replace(snap,metadata_bytes=len(canonical(asdict(snap)).encode()))
        return replace(snap,metadata_bytes=len(canonical(asdict(snap)).encode()))
