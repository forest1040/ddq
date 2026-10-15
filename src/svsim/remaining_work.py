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
    # Exact executor shape of the epochs after the window (one class per
    # executor condition, plus one publication and one boundary per epoch).
    future: tuple[CountedTask,...] = ()
    future_complete: bool = False
    future_reasons: tuple[str,...] = ()
    # Private epoch: the running call is a measured residual task that covers
    # the outstanding requests, so they do not each leave the total unknown.
    in_flight_covered: bool = False
    # Explicit end-to-end endpoint work after the gates (expectation, gradient, durable checkpoint).
    endpoint_tasks: tuple[CountedTask,...] = ()


def executor_groups(window,layout,block_bits,policy):
    """Executor calls (operation tuples, in order) of one epoch window, exactly as the async scheduler runs it."""
    if policy.swap_policy=='S2':
        plan=plan_window(window,layout,block_bits,policy.beam_width)
        order=plan.source_ids;groups={g[0].op_id:g for g in plan.groups if len(g)>1}
    else:order,groups=tuple(g.op_id for g in window),{}
    by_id={g.op_id:g for g in window};cursor=0
    while cursor<len(order):
        group=groups.get(order[cursor])
        ops=tuple(group) if group and tuple(g.op_id for g in group)==tuple(order[cursor:cursor+len(group)]) else (by_id[order[cursor]],)
        yield ops;cursor+=len(ops)


S1_LAYOUT_MEMO=4096


def s1_moves(sim):
    """Whether S1 can move layout bits in this run (multi-rank with a matching network profile and storage bandwidth)."""
    import math
    profile,bandwidth=getattr(sim,'network_profile',None),getattr(sim,'storage_bandwidth',None)
    return (sim.policy.swap_policy=='S1' and sim.size>1 and profile is not None
            and profile.block_bytes==16*(1<<sim.block_bits) and bandwidth is not None
            and math.isfinite(bandwidth) and bandwidth>0)


def epoch_calls(sim,window,layout):
    """(operations, layout of the call) of one epoch window and the layout after it.

    S1 replays the scheduler's per-gate decision (``choose_s1`` over the rest of
    the epoch, deterministic for this layout and profile); the swap runs inside
    the gate's measured call, so it is not a separate task.
    """
    if not s1_moves(sim):
        return [(ops,layout) for ops in executor_groups(window,layout,sim.block_bits,sim.policy)],layout
    from .distributed_plan import choose_s1
    calls=[]
    for index,op in enumerate(window):
        decision=choose_s1(sim.num_qubits,sim.block_bits,sim.size,layout,window[index:],sim.network_profile,
                           sim.storage_bandwidth)
        if decision.bits:layout=layout.swapped_physical(*decision.bits)
        calls.append(((op,),layout))
    return calls,layout


def future_shape_reasons(sim):
    """Why the executor shape after the current epoch is not determined by the current layout and policy."""
    # Source SWAPs run as data operations and never relabel the layout; S1
    # decisions are replayed (epoch_calls); an explicit physical swap or replan
    # invalidates the snapshot.
    return ()


def semantic_class(op):
    data=gate_data(op);data.pop('op_id',None);data.pop('source_op_id',None)
    return data,content_hash(data)


POOLED_ANGLE={'op':'pooled-angle'}


def future_key(condition):
    """(shape signature with angles removed, exact signature) of a future executor condition."""
    shape={**condition,'operations':[{k:v for k,v in op.items() if k!='angle'} for op in condition['operations']]}
    return content_hash(shape),content_hash(condition)


def pool_angles(condition):
    # Members differ only in angle: priced by the angle-pooled measured class.
    return {**condition,'operations':[{**op,'angle':POOLED_ANGLE} if op.get('angle') is not None else op
                                      for op in condition['operations']]}


def strip_ids(condition):
    return {**condition,'operations':[{k:v for k,v in op.items() if k not in {'op_id','source_op_id'}}
                                      for op in condition['operations']]}


def boundary_condition(sim):
    return {'operation':'epoch-boundary','n':sim.num_qubits,'block_bits':sim.block_bits,'ranks':sim.size,
        'backend':sim.config.backend,'engine':sim.io_engine.identity,'mode':sim.policy.mode}


ENDPOINT_STAGES=('expectation','gradient','checkpoint-commit')


def _endpoint_base(sim):
    return {'n':sim.num_qubits,'block_bits':sim.block_bits,'ranks':sim.size,'backend':sim.config.backend,
            'engine':sim.io_engine.identity,'store':sim.config.amplitude_store}


def observable_id(observables):
    """Cost identity of one ObservableIR / term sequence or a sequence of them: Pauli masks, not coefficients."""
    from .observables import ObservableIR
    if isinstance(observables,ObservableIR) or not isinstance(observables,(list,tuple)) or not observables or \
            not isinstance(observables[0],ObservableIR):
        observables=(observables if isinstance(observables,ObservableIR) else ObservableIR(tuple(observables)),)
    return content_hash([[[t.x_mask,t.z_mask] for t in o.terms] for o in observables])


def expectation_condition(sim,observables):
    return {'operation':'expectation','observable':observable_id(observables),**_endpoint_base(sim)}


def gradient_condition(sim,observable,parameters,method):
    return {'operation':'gradient','method':method,'observable':observable_id(observable),
            'parameters':content_hash(sorted(str(p) for p in parameters)),'circuit':sim.circuit_hash,**_endpoint_base(sim)}


def checkpoint_condition(sim):
    return {'operation':'checkpoint-commit',**_endpoint_base(sim)}


def endpoint_tasks(sim,spec,context):
    """Counted endpoint tasks for an ``RuntimeControl.set_eta_endpoint`` spec, and the endpoint label."""
    tasks=[];labels=['gates']
    if spec.get('expectation') is not None:
        tasks.append(task('endpoint:expectation',expectation_condition(sim,spec['expectation']),context,kind='endpoint'))
        labels.append('expectation')
    if spec.get('gradient') is not None:
        g=spec['gradient']
        parameters=g.get('parameters')
        if parameters is None:parameters=sorted(set().union(*(op.dependencies for op in sim.sequencer.operations)))
        tasks.append(task('endpoint:gradient',gradient_condition(sim,g['observable'],parameters,g.get('method','auto')),
                          context,kind='endpoint'))
        labels.append('gradient')
    if spec.get('checkpoint'):
        tasks.append(task('endpoint:checkpoint',checkpoint_condition(sim),context,kind='endpoint'))
        labels.append('checkpoint')
    return tuple(tasks),'+'.join(labels) if tasks else 'gates-only'


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
        self._tail_start=0;self._future_origin=0
        self._future_cursor=0;self._future={};self._future_epochs=0;self._future_bytes=0;self._future_overflow=False
        self._future_layout=None;self._layouts={}

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
            # The call executing now is priced as a measured residual given its
            # elapsed time; its requests (and earlier writebacks drained by the
            # publication) are inside the calibrated call/publication times.
            # Between calls nothing runs and outstanding writebacks are drained
            # inside the measured publication.
            residual=True
            running=getattr(sim,'_executor_in_flight',None)
            if running is not None and not (running[0]!=('publish',) and all(i in staged for i in running[0])):
                ids,begin=running
                match=next((i for i,t in enumerate(window) if (t.kind=='publication' and ids==('publish',))
                            or (t.source_ids and t.source_ids==ids)),None)
                if match is None or window[match].unknown_reasons:residual=False
                else:
                    t=window[match]
                    condition=strip_ids(t.condition) if 'operations' in t.condition else t.condition
                    window=window[:match]+(replace(t,condition={'operation':'call-residual','class':condition,
                        'elapsed_seconds':max(0.,perf_counter()-begin)},kind='residual'),)+window[match+1:]
            current=runtime_context(sim)
            stale=end!=base.endpoint_frontier or any(current.get(k)!=v for k,v in base.context.items()
                if k not in {'root','frontier','layout','placement_epoch','endpoint_frontier'})
            snap=replace(base,snapshot_id=content_hash({'base':base.snapshot_id,'staged':sorted(staged),
                'requests':[r.request_id for r in flights]}),window=window,in_flight=flights,
                private=True,staged_sources=len(staged),stale=stale,in_flight_covered=residual,
                unknown_reasons=tuple(dict.fromkeys(base.unknown_reasons+(() if residual else ('PRIVATE_EPOCH_RESIDUAL_UNCALIBRATED',)))),
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
        # Scans survive frontier advances; everything else that shapes the work resets them.
        # S1 moves the layout during the run; its scans check the replayed layout instead.
        moving=s1_moves(sim)
        key=content_hash({k:v for k,v in context.items() if k not in {'root','frontier'}|({'layout','placement_epoch'} if moving else set())})
        first=sim.frontier+1
        width=min(self.limits.window,sim.policy.lookahead_gates)
        stop=min(end+1,first+width)
        source=sim.sequencer.operations[first:stop]
        # Bound the plan input before building its beam/group metadata.
        if sum(len(canonical(gate_data(g)).encode())+128 for g in source)>self.limits.max_bytes//2:
            snap=self._minimal(sim,end,flights,('CAPTURE_METADATA_LIMIT',),started)
            self._published=snap
            return snap
        truncated=width<min(sim.policy.lookahead_gates,end-first+1)
        shape=future_shape_reasons(sim)
        window=[];previous=();unknown=[]
        ccontext=cost_context(sim)
        calls,window_layout=epoch_calls(sim,source,sim.layout)
        for ops,call_layout in calls:
            reasons=shape
            if truncated:reasons+=('EXECUTOR_WINDOW_TRUNCATED',)
            if any(g.dependencies for g in ops) and sim.binding is None:reasons+=('UNBOUND_PARAMETERS',)
            tid=f'window:{len(window)}'
            window.append(task(tid,executor_condition(sim,ops,call_layout),ccontext,dependencies=previous,
                               source_ids=tuple(g.op_id for g in ops),unknown=reasons))
            previous=(tid,)
        if window:
            window.append(task('window:publish',publication_condition(sim),ccontext,
                               dependencies=previous,resource='main',kind='publication'))
            previous=('window:publish',)
        window_tail=previous
        if self._scan_key!=key or not self._advance(sim,first,stop,end):
            self._scan_key=key;self._scan_cursor=self._tail_start=stop;self._tail={};self._tail_overflow=0
            self._future_cursor=self._future_origin=first+sim.policy.lookahead_gates;self._future={};self._future_epochs=0
            self._future_bytes=0;self._future_overflow=False
            self._future_layout=window_layout;self._layouts={}
        deadline=monotonic()+self.limits.scan_seconds
        self._scan_future(sim,end,shape+(('EXECUTOR_WINDOW_TRUNCATED',) if truncated else ()),deadline)
        scanned=0
        # Each class serializes as a task carrying the cost context and key.
        overhead=len(canonical(ccontext).encode())+256
        while self._scan_cursor<=end and scanned<self.limits.scan_items and monotonic()<deadline:
            op=sim.sequencer.operations[self._scan_cursor]
            data,signature=semantic_class(op)
            if signature in self._tail:self._tail[signature][1]+=1
            elif not self._tail_overflow and len(self._tail)<self.limits.max_groups and (
                    sum(v[2] for v in self._tail.values())+len(canonical(data).encode())+overhead<=self.limits.max_bytes//2):
                # Count semantics exactly; future fusion/Frame/layout are not guessed.
                self._tail[signature]=[data,1,len(canonical(data).encode())+overhead]
            # After the first overflow no class is added until a rescan, so a
            # source is in its class exactly when its signature has one.
            else:self._tail_overflow+=1
            self._scan_cursor+=1;scanned+=1
        tail=[]
        for i,(data,count,_) in enumerate(self._tail.values()):
            condition={'operation':'semantic-suffix','class':data}
            tid=f'tail:{i}'
            tail.append(task(tid,condition,ccontext,count=count,dependencies=previous,kind='semantic-count',
                # The semantic tail assumes the current layout; S1 moves it (the exact future replays it).
                unknown=('FUTURE_EXECUTOR_SHAPE_UNKNOWN',)+(('FUTURE_S1_LAYOUT_UNKNOWN',) if moving else ())))
            previous=(tid,)
        if self._tail_overflow:
            tail.append(task('tail:overflow',{'operation':'unclassified-suffix'},ccontext,
                count=self._tail_overflow,dependencies=previous,kind='semantic-count',unknown=('SUMMARY_CLASS_LIMIT',)))
        pending=max(0,end-self._scan_cursor+1)
        complete=not pending
        if not window and not tail:
            # The gate endpoint excludes a durable checkpoint, but pending
            # dirty/writeback/error work still prevents a false quiescent zero.
            entries=getattr(sim.cache,'entries',{})
            if len(entries)>64:unknown.append('CACHE_QUIESCENCE_UNINSPECTED')
            if any(e.dirty for e in islice(entries.values(),64)):unknown.append('DIRTY_WRITEBACK_REMAINS')
            if getattr(sim,'_async_errors',()) or getattr(sim.cache,'errors',()):unknown.append('FAILED_WORK_REMAINS')
        future,future_complete,future_reasons=self._future_tasks(sim,end,ccontext,window_tail,bool(window))
        snap=RemainingWork(content_hash({'context':context,'scan':self._scan_cursor,'future':self._future_cursor}),context,
            tuple(window),tuple(tail),flights,complete,False,tuple(unknown),sim.frontier,end,
            max(0,end-sim.frontier),0,self._scan_cursor-stop,pending,False,perf_counter()-started,0,
            future=future,future_complete=future_complete,future_reasons=future_reasons)
        size=len(canonical(asdict(snap)).encode())
        if size+8>self.limits.max_bytes and future:
            # The exact future is an optional refinement of the semantic tail.
            snap=replace(snap,future=(),future_complete=False,future_reasons=('FUTURE_METADATA_LIMIT',))
            size=len(canonical(asdict(snap)).encode())
        if size+8>self.limits.max_bytes:
            snap=self._minimal(sim,end,flights,('CAPTURE_METADATA_LIMIT',),started)
        else:
            snap=replace(snap,metadata_bytes=size)
            snap=replace(snap,metadata_bytes=len(canonical(asdict(snap)).encode()))
        self._published=snap
        return snap

    def _advance(self,sim,first,stop,end):
        """Move the scanned tail and future past work the frontier advance put in the window; False to rescan."""
        lookahead=sim.policy.lookahead_gates
        origin=first+lookahead
        if stop<self._tail_start or origin<self._future_origin or (origin-self._future_origin)%lookahead:return False
        moving=s1_moves(sim)
        # S1: the epoch now in the window must start from the layout the scan replayed for it.
        if moving and first>=self._future_origin and (self._future_cursor<origin or
                self._layouts.get(first)!=sim.layout.logical_to_physical):return False
        ops=sim.sequencer.operations
        for index in range(self._tail_start,min(stop,self._scan_cursor)):
            signature=semantic_class(ops[index])[1]
            entry=self._tail.get(signature)
            if entry is None:
                if not self._tail_overflow:return False
                self._tail_overflow-=1
                continue
            entry[1]-=1
            if not entry[1]:del self._tail[signature]
        self._tail_start=stop;self._scan_cursor=max(self._scan_cursor,stop)
        if not self._future_overflow:
            start=self._future_origin
            while start<origin and start<self._future_cursor:
                from .layout import QubitLayout
                begin=QubitLayout(self._layouts.pop(start)) if moving else sim.layout
                for group,call_layout in epoch_calls(sim,ops[start:min(end,start+lookahead-1)+1],begin)[0]:
                    signature=future_key(strip_ids(executor_condition(sim,group,call_layout)))[0]
                    entry=self._future.get(signature)
                    if entry is None:return False
                    entry[1]-=1
                    if not entry[1]:
                        self._future_bytes-=entry[4]
                        del self._future[signature]
                self._future_epochs-=1;start+=lookahead
        self._future_origin=origin;self._future_cursor=max(self._future_cursor,origin)
        return True

    def _scan_future(self,sim,end,reasons,deadline):
        """Advance the epoch-aligned executor-shape scan by whole epochs within the scan budget."""
        if reasons or self._future_overflow:return
        ops,scanned=sim.sequencer.operations,0
        overhead=len(canonical(cost_context(sim)).encode())+256
        while self._future_cursor<=end and scanned<self.limits.scan_items and monotonic()<deadline:
            stop=min(end,self._future_cursor+sim.policy.lookahead_gates-1)
            begin=self._future_layout
            calls,after=epoch_calls(sim,ops[self._future_cursor:stop+1],begin)
            if s1_moves(sim):
                if len(self._layouts)>=S1_LAYOUT_MEMO:self._future_overflow=True;return
                self._layouts[self._future_cursor]=begin.logical_to_physical
            for group,call_layout in calls:
                condition=strip_ids(executor_condition(sim,group,call_layout))
                signature,exact=future_key(condition)
                entry=self._future.get(signature)
                if entry is not None:
                    entry[1]+=1
                    if entry[3] not in (None,exact):entry[0]=pool_angles(entry[0]);entry[3]=None
                else:
                    size=len(canonical(condition).encode())+overhead
                    if len(self._future)>=self.limits.max_groups-2 or self._future_bytes+size>self.limits.max_bytes//4:
                        self._future_overflow=True;return
                    self._future[signature]=[condition,1,any(g.dependencies for g in group),exact,size];self._future_bytes+=size
            self._future_epochs+=1;scanned+=stop-self._future_cursor+1;self._future_cursor=stop+1
            self._future_layout=after

    def _future_tasks(self,sim,end,ccontext,previous,current_epoch):
        reasons=future_shape_reasons(sim)
        if self.limits.window<min(sim.policy.lookahead_gates,end-sim.frontier):reasons+=('EXECUTOR_WINDOW_TRUNCATED',)
        if self._future_overflow:reasons+=('FUTURE_CLASS_LIMIT',)
        if sim.binding is None and any(v[2] for v in self._future.values()):reasons+=('UNBOUND_PARAMETERS',)
        if reasons:return (),False,reasons
        tasks=[]
        for i,(condition,count,*_) in enumerate(self._future.values()):
            tid=f'future:{i}'
            tasks.append(task(tid,condition,ccontext,count=count,dependencies=previous,kind='executor-count'))
            previous=(tid,)
        if self._future_epochs:
            tasks.append(task('future:publish',publication_condition(sim),ccontext,count=self._future_epochs,
                              dependencies=previous,kind='publication'))
            previous=('future:publish',)
        epochs=self._future_epochs+current_epoch
        if epochs:
            tasks.append(task('future:boundary',boundary_condition(sim),ccontext,count=epochs,
                              dependencies=previous,kind='epoch-boundary'))
        return tuple(tasks),self._future_cursor>end,()

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
