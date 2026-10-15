"""Single-process out-of-core deferred state-vector execution.

Every amplitude buffer is owned by the MemoryBudget/BlockCache. Semantic gates
remain the journal authority; checkpoints publish a durable immutable root.
"""
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, replace
from hashlib import sha256
import json
import os
import sqlite3
from pathlib import Path
from time import perf_counter
from uuid import uuid4
import numpy as np
from .cache import BlockCache, MemoryBudget
from .checkpoint import SnapshotManifest, StoredStateHandle
from .ir import GateNormalizer, GlobalSequencer, FallbackDecomposer, GateOp
from .journal import Journal, canonical, content_hash, gate_data, decode_gate
from .kernels import gate_matrix
from .layout import QubitLayout
from .lifecycle import HardwareProfiler, StaticCircuitOptimizer
from .lowering import StructuredExactDecomposer
from .parameters import ParameterBinding, ExactPi
from .planner import AccessPlan, MaterializationTask, group_bits, working_set
from .storage import FileBlockStore, StoredBlockView, StorageConfig
from .streaming_kernels import apply_local, apply_group
from .telemetry import StreamingExecutionTrace, ObservePerformanceGuard
from .unitary import MatrixAssetRegistry
from .admitted_files import atomic_write_bytes


class StoredSimulator:
    def __init__(self, num_qubits, storage: StorageConfig, *, initial_state=None,
                 initial_blocks=None, profile=None, execution='auto', mode='per-block-frame',
                 transport_factory=None, frame_policy='auto', alignment_policy='auto', frame_profile=None, _snapshot=None, _create=True, disk_budget=None):
        if execution not in {'auto','fallback','structured-exact'}: raise ValueError('Unknown gate execution mode')
        if mode not in {'per-block-frame','deferred-no-frame','batching','eager'}: raise ValueError('Unknown materialization mode')
        if frame_policy not in {'auto','keep','materialize'}: raise ValueError('Unknown frame policy')
        if alignment_policy not in {'auto','input-a','input-b','identity'}: raise ValueError('Unknown alignment policy')
        self.frame_policy,self.alignment_policy = frame_policy,alignment_policy
        self.frame_profile = frame_profile
        if initial_state is not None and initial_blocks is not None: raise ValueError('Choose one initial-state source')
        self.num_qubits, self.config = num_qubits, storage
        self.block_bits, self.execution, self.mode = storage.block_bits, execution, mode
        if frame_profile is not None and frame_profile.block_bits != self.block_bits:
            raise ValueError('Frame calibration block width must match execution width')
        self.normalizer = GateNormalizer(num_qubits)
        self.assets = self.normalizer.assets
        self.sequencer = GlobalSequencer()
        self.layout = QubitLayout.identity(num_qubits)
        self.profile = profile or HardwareProfiler([storage.directory if Path(storage.directory).exists() else Path(storage.directory).parent]).discover()
        self.plan = None
        self.plan_version = 0
        self.binding = None
        self.frontier = -1
        self.initial_state_hash = content_hash({'state':'zero','num_qubits':num_qubits})
        self.closed = False
        self._readonly_circuit = _snapshot is not None
        self._store_kwargs = {'transport_factory':transport_factory} if transport_factory else {}
        if disk_budget is not None:self._store_kwargs['disk_budget']=disk_budget
        self.store = FileBlockStore(storage,num_qubits,create=_create,**self._store_kwargs)
        from .phase2_runtime import Phase2Catalog
        try: self.phase2 = Phase2Catalog(self)
        except BaseException:
            self.store.close()
            raise
        self.store.phase2 = self.phase2
        self.budget = MemoryBudget(storage.ram_budget_bytes,storage.metadata_reserve_bytes)
        self.cache = BlockCache(self.store,self.budget)
        disk_kwargs={'disk_budget_getter':lambda:self.store.disk_budget,
                     'drain_getter':lambda:getattr(self.store,'_disk_drain',False)}
        self.journal = Journal(self.store.directory/'journal',**disk_kwargs)
        self.journal.protected_cursor = lambda kind: self.store.db.execute(
            'SELECT max(json_extract(data,?)) FROM snapshots',(f'$.journal_cursors.{kind}',)).fetchone()[0] or 0
        self.store.journal = self.journal
        self.run_id = uuid4().hex
        self.trace = StreamingExecutionTrace(self.store.directory/'traces'/f'{self.run_id}.jsonl',profile_id=self.profile.profile_id,plan_id='unregistered',**disk_kwargs)
        self.store.trace = self.trace
        self.guard = None
        try:
            self.root = self.store.new_root()
            if _snapshot is not None: self._restore(_snapshot)
            elif initial_state is not None or initial_blocks is not None:
                size = 1 << storage.block_bits
                if initial_state is not None:
                    vector = np.asarray(initial_state)
                    if vector.shape != (1 << num_qubits,): raise ValueError('Invalid initial state dimensions')
                    initial_blocks = (vector[i:i+size] for i in range(0,len(vector),size))
                self._import_initial(initial_blocks)
            self._check_metadata()
        except BaseException:
            self.close()
            raise

    def _open(self):
        if self.closed: raise ValueError('Simulator is closed')

    @property
    def root(self): return self._root

    @root.setter
    def root(self, value):
        self._root = value
        self.store.db.execute('INSERT OR REPLACE INTO active_runs VALUES(?,?)',(self.run_id,value))
        self.store.db.commit()

    @property
    def circuit_hash(self):
        return content_hash({'schema_version':1,'num_qubits':self.num_qubits,'bit_order':'lsb-first',
                             'gates':[gate_data(g) for g in self.sequencer.operations]})

    def _check_metadata(self):
        # Admission keeps semantic objects and small matrix assets bounded.
        estimated = len(canonical([gate_data(g) for g in self.sequencer.operations]).encode())*4
        estimated += sum(len(a.data) for a in self.assets.assets.values()) + 8192
        if estimated > self.config.metadata_reserve_bytes:
            raise MemoryError(f'Circuit/assets need at least {estimated} metadata bytes; reserve is {self.config.metadata_reserve_bytes}')

    def _persist_assets(self):
        for key, asset in self.assets.assets.items():
            path = self.store.directory/'assets'/f'{key}.bin'
            if not path.exists():
                atomic_write_bytes(path,asset.data,disk_budget=self.store.disk_budget,
                                   drain=getattr(self.store,'_disk_drain',False),operation='matrix-asset')
            else:
                with open(path,'rb') as stream:
                    digest = sha256()
                    while chunk := stream.read(4096): digest.update(chunk)
                if digest.digest() != sha256(asset.data).digest(): raise OSError('Existing matrix asset checksum mismatch')

    def append_gate(self, gate):
        self._open()
        if self._readonly_circuit: raise ValueError('Fork/resume keeps the registered circuit immutable')
        if gate.op_id != -1: raise ValueError('Gate already sequenced')
        self.sequencer.operations.append(replace(gate,op_id=len(self.sequencer.operations)))
        try: self._check_metadata()
        except BaseException:
            self.sequencer.operations.pop(); raise
        op = self.sequencer.operations[-1]
        cursors = self.journal.cursors()
        try:
            self._persist_assets()
            self.journal.append('gates',{'op_id':op.op_id,'gate':gate_data(op)})
            physical = self._physical(op)
            kind = 'joint' if group_bits(physical,self.block_bits) else 'shared'
            self.journal.append(kind,{'op_id':op.op_id,'layout_id':self.layout.layout_id,
                                     'controls':op.controls,'control_values':op.control_values})
        except BaseException:
            self.sequencer.operations.pop()
            self.journal.rollback(cursors)
            raise
        if self.binding is not None: self.binding = ParameterBinding(self.circuit_hash,self.binding.values,self.binding.exact_witnesses)
        self.plan = None
        return op

    def gate(self, kind, targets, **kwargs):
        self._open()
        before = set(self.assets.assets)
        try: return self.append_gate(self.normalizer.gate(kind,targets,**kwargs))
        except BaseException:
            for key in set(self.assets.assets)-before: self.assets.assets.pop(key)
            raise

    def bind(self, values=()):
        return ParameterBinding(self.circuit_hash,tuple(values.items()) if hasattr(values,'items') else tuple(values))

    def _physical(self, gate):
        return replace(gate,targets=tuple(self.layout.logical_to_physical[q] for q in gate.targets),
                       controls=tuple(self.layout.logical_to_physical[q] for q in gate.controls))

    def _register(self):
        self.plan_version += 1
        self.plan = StaticCircuitOptimizer().create_plan(self.circuit_hash,self.profile,
            plan_id=f'{self.run_id}-v{self.plan_version}',layout_id=str(self.layout.layout_id),
            frontier=len(self.sequencer.operations)-1,
            task_dependencies=((str(g.op_id),(str(g.op_id-1),) if g.op_id else ()) for g in self.sequencer.operations),
            expected_conditions=(s.condition for s in self.profile.benchmarks if s.condition.block_bytes == self.store.slot_bytes))
        self.plan = replace(self.plan,version=self.plan_version,metadata={'ram_budget_bytes':self.budget.limit,
            'block_bits':self.block_bits,'slot_bytes':self.store.slot_bytes,'mode':self.mode,'execution':self.execution})
        self.trace.plan_id, self.trace.plan_version = self.plan.plan_id,self.plan.version
        self.guard = ObservePerformanceGuard(self.profile,self.plan)
        self.store.guard = self.guard

    def _import_initial(self, iterator):
        digest = sha256()
        count = 0
        try:
            for h, block in enumerate(iterator):
                if h >= self.store.num_blocks: raise ValueError('Too many initial Blocks')
                with self.cache.outputs(1) as buffers:
                    array = buffers[0].array(self.store.payload_bytes)
                    try:
                        incoming = np.asarray(block)
                        if incoming.shape != array.shape or not np.all(np.isfinite(incoming)): raise ValueError('Invalid initial Block')
                        array[:] = incoming
                        # Padding stays zero from anonymous mmap initialization.
                        digest.update(buffers[0].view[:self.store.payload_bytes])
                        version = self.cache.adopt(buffers[0])
                        self.store.publish(self.root,((h,StoredBlockView(version)),))
                    finally: del array
                count += 1
        finally:
            self.trace.record('initial-import',metadata={'path':'INITIAL_IMPORT','blocks':count})
        if count != self.store.num_blocks: raise ValueError('Not enough initial Blocks')
        self.initial_state_hash = digest.hexdigest()

    def _compiled(self, start, last, binding):
        originals = self.sequencer.operations[start:last+1]
        out = []
        decomposer = StructuredExactDecomposer(self.assets)
        for gate in originals:
            if gate.angle is not None: gate.angle.evaluate(binding)
            if self.execution == 'fallback' and gate.kind != 'PAULI_ROT': candidates = FallbackDecomposer().decompose(gate)
            elif self.execution == 'structured-exact' and gate.kind == 'DENSE_UNITARY': candidates = decomposer.decompose(gate)
            else: candidates = (gate,)
            out.extend((self._physical(op),(gate.op_id,)) for op in candidates)
        # Fusion preserves boundaries: only adjacent original RZ and small
        # local non-Clifford gates with identical targets/control predicates.
        fused = []
        for gate, sources in out:
            if self.execution == 'auto' and self.mode != 'eager' and fused:
                prev, ids = fused[-1]
                same = (prev.targets,prev.controls,prev.control_values) == (gate.targets,gate.controls,gate.control_values)
                if same and prev.kind == gate.kind == 'RZ':
                    fused[-1] = (replace(prev,angle=prev.angle+gate.angle,
                                         approximation_error=prev.approximation_error+gate.approximation_error),ids+sources)
                    continue
                can_matrix = same and len(gate.targets) <= 2 and all(q < self.block_bits for q in gate.targets)
                if can_matrix and not prev.clifford and not gate.clifford and prev.kind not in {'MCX','PAULI_ROT'} and gate.kind not in {'MCX','PAULI_ROT'} and self.mode != 'per-block-frame':
                    matrix = gate_matrix(gate,self.assets,binding) @ gate_matrix(prev,self.assets,binding)
                    key = self.assets.register(matrix,len(gate.targets))
                    diagonal = prev.diagonal and gate.diagonal
                    fused[-1] = (replace(prev,kind='DENSE_UNITARY',angle=None,matrix_asset_ref=key,diagonal=diagonal,
                                         approximation_error=prev.approximation_error+gate.approximation_error),ids+sources)
                    continue
            fused.append((gate,sources))
        self._check_metadata()
        return fused

    def access_plan(self, binding=None, frontier=None):
        self._open()
        binding = binding or self.binding or self.bind()
        last = len(self.sequencer.operations)-1 if frontier is None else frontier
        compiled = self._compiled(self.frontier+1,last,binding)
        tasks = []
        for gate, sources in compiled:
            bits = group_bits(gate,self.block_bits)
            count = 2 if gate.kind == 'PAULI_ROT' and bits else 1 << len(bits)
            tasks.append(MaterializationTask(str(sources),sources,'joint' if bits else 'local',bits,
                                             min(sources)-1,max(sources),working_set(self.config,count)))
        visits = len(tasks)*self.store.num_blocks*self.store.slot_bytes
        pending = sum(e.dirty for e in self.cache.entries.values())*self.store.slot_bytes
        return AccessPlan(self.circuit_hash,self.profile.profile_id,self.layout.layout_id,tuple(tasks),0,visits,0,visits+pending)

    def _validate_binding(self, binding, last):
        if binding.circuit_hash != self.circuit_hash: raise ValueError('Binding belongs to a different circuit')
        if self.binding is not None and self.frontier >= 0:
            dependencies = frozenset().union(*(g.dependencies for g in self.sequencer.operations[:self.frontier+1]))
            for key in dependencies:
                if self.binding.value(key) != binding.value(key): raise ValueError('Cannot change an executed parameter; fork its prefix snapshot')
                if dict(self.binding.exact_witnesses).get(key) != dict(binding.exact_witnesses).get(key):
                    raise ValueError('Cannot change an executed parameter witness; fork its prefix snapshot')
        for gate in self.sequencer.operations[:last+1]:
            if gate.angle is not None: gate.angle.evaluate(binding)

    def materialize(self, frontier=None, binding=None):
        self._open()
        last = len(self.sequencer.operations)-1 if frontier is None else frontier
        if not self.frontier <= last < len(self.sequencer.operations):
            if last == self.frontier == -1: return self
            raise ValueError('Invalid or past frontier')
        binding = binding or self.binding or self.bind()
        self._validate_binding(binding,last)
        compiled = self._compiled(self.frontier+1,last,binding)
        # Refuse impossible tasks before changing any logical state.
        rotation_bound = self.store.log_bound(self.root)
        for gate, sources in compiled:
            bits = group_bits(gate,self.block_bits)
            if self.mode == 'per-block-frame':
                if gate.kind in {'MCX','SWAP'} and all(q >= self.block_bits for q in gate.qubits): continue
                if not bits and self.frame_policy != 'materialize':
                    rule = self.phase2.make_rule(gate,sources,binding)
                    if rule is not None:
                        if rule['kind'] != 'rotation': continue
                        rotation_bound += 1
                        if self.frame_policy == 'keep' and rotation_bound <= self.phase2.max_log: continue
            needed = working_set(self.config,2 if gate.kind == 'PAULI_ROT' and bits else 1 << len(bits))
            if needed > self.budget.limit: raise MemoryError(f'Task requires at least {needed} managed bytes; budget is {self.budget.limit}')
        prior = self.root,self.frontier,self.binding
        self.binding = binding
        try:
            if self.plan is None: self._register()
            local = []
            for gate, sources in compiled:
                bits = group_bits(gate,self.block_bits)
                virtual = self.mode == 'per-block-frame' and gate.kind in {'MCX','SWAP'} and all(q >= self.block_bits for q in gate.qubits)
                if virtual:
                    if local: self._epoch(local); local = []
                    from .permutation import PermutationEdge
                    from dataclasses import asdict
                    self.phase2.add_rule({'kind':'permutation','permutation':asdict(PermutationEdge.from_gate(gate,self.block_bits)),
                                         'frontier':max(sources),'sources':sources})
                    continue
                if not bits:
                    local.append((gate,sources))
                    if self.mode == 'eager': self._epoch(local); local = []
                else:
                    if local: self._epoch(local); local = []
                    self._joint_phase2(gate,sources,bits)
            if local: self._epoch(local)
            self.frontier = last
        except BaseException:
            self.root,self.frontier,self.binding = prior
            self.trace.record('materialization-abort',metadata={'path':'ABORT','restored_frontier':self.frontier})
            raise
        return self

    def _upper_ok(self, gate, h):
        return all(((h >> (q-self.block_bits))&1) == v for q,v in zip(gate.controls,gate.control_values) if q >= self.block_bits)

    def _local(self, gate):
        controls = [(q,v) for q,v in zip(gate.controls,gate.control_values) if q < self.block_bits]
        return replace(gate,controls=tuple(q for q,_ in controls),control_values=tuple(v for _,v in controls))

    def _frame_clifford(self, gate):
        if any(q >= self.block_bits for q in gate.targets): return False
        if gate.kind == 'MCX': return len(gate.controls) <= 1
        return not gate.controls and gate.kind in {'H','S','SDG','Z','CZ','SWAP'}

    def _resolve_array(self, array, view, h):
        start = perf_counter()
        self.phase2.resolve(array,view,h)
        if view.frame_id or view.frame_asset_id or view.phase != 1 or view.base_log:
            self.trace.record(f'frame-{h}',performance_class='frame',transfer_ms=(perf_counter()-start)*1000,
                              metadata={'path':'MATERIALIZE_FRAME','frame_id':view.frame_id,'block_id':h})

    def _epoch(self, operations):
        return self._epoch_phase2(operations)

    def _epoch_phase2(self, operations):
        pending = []
        for gate,sources in operations:
            rule = self.phase2.make_rule(gate,sources) if self.mode == 'per-block-frame' and self.frame_policy != 'materialize' else None
            if rule is not None and rule['kind'] == 'rotation':
                from .frame_planner import choose_local
                estimate = sum(len(op.get('word',())) for op in pending)
                for ancestor in self.store.root_chain(self.root):
                    row = self.store.db.execute('SELECT data FROM root_edges WHERE root=?',(ancestor,)).fetchone()
                    if row:
                        data = self.phase2.rule(ancestor,row[0]); estimate += sum(len(op.get('word',())) for op in data.get('ops',()))
                decision = choose_local(self.frame_policy,working_set(self.config),self.profile.profile_id,
                    calibration=self.frame_profile,frame_gate_estimate=min(estimate,2*self.block_bits**2),block_bits=self.block_bits)
                self.trace.record(str(sources),performance_class='frame-plan',metadata={'path':'LOCAL_FRAME_DECISION',
                    'decision':decision.data(),'source_ops':sources,'binding_id':self.binding.binding_id,'layout_id':self.layout.layout_id})
                if decision.chosen == 'materialize': rule = None
            if rule is not None:
                if pending and len(canonical(pending+[rule]).encode())*4 > self.config.metadata_reserve_bytes//2-1024:
                    self._publish_local_rules(pending,max(max(op['sources']) for op in pending)); pending = []
                pending.append(rule)
                if len(pending) >= min(32,self.phase2.max_log):
                    self._publish_local_rules(pending,max(sources)); pending = []
                continue
            if pending:
                self._publish_local_rules(pending,max(max(op['sources']) for op in pending)); pending = []
            parent = self.root
            new = self.store.new_root(parent)
            end = max(sources)
            for h in range(self.store.num_blocks):
                if not self._upper_ok(gate,h): continue
                view = self.store.view(parent,h)
                self.cache.ensure(2*self.cache.buffer_cost)
                with self.cache.outputs(1) as buffers:
                    with self.cache.pin(view.version_id) as source: buffers[0].view[:] = source.view
                    array = buffers[0].array(self.store.payload_bytes)
                    try:
                        self._resolve_array(array,view,h)
                        local = self._local(gate)
                        if local.kind == 'PAULI_ROT':
                            from .phase2_runtime import pauli_of
                            from .pauli_rotation import apply_pauli_rotation
                            apply_pauli_rotation(array,pauli_of(gate),gate.angle.evaluate(self.binding),block_id=h,block_bits=self.block_bits)
                        else: apply_local(array,local,self.assets,self.binding,block_id=h,block_bits=self.block_bits)
                        version = self.cache.adopt(buffers[0])
                        self.store.publish(new,((h,StoredBlockView(version,end,end)),))
                    finally: del array
            self.phase2.progress(new,end)
            self.store.seal(parent)
            self.root,self.frontier = new,end
            self.journal.append('local',{'source_ops':sources,'input_root':parent,'output_root':new,'frontier':end,'semantics_version':2})
            self.trace.record(str(sources),performance_class='compute',metadata={'path':'LOCAL_EPOCH','source_ops':sources,
                'execution_gate_count':1,'execution':self.execution,'binding_id':self.binding.binding_id,'layout_id':self.layout.layout_id})
        if pending: self._publish_local_rules(pending,max(max(op['sources']) for op in pending))

    def _publish_local_rules(self,ops,end):
        # Conservatively flush before appending enough rules to exceed any
        # block's bounded log. The shared registry remains disk resident.
        rotations = sum(op['kind'] == 'rotation' for op in ops)
        prior = self.store.log_bound(self.root)
        if prior+rotations > self.phase2.max_log: self.flush_base_logs()
        self.phase2.add_rule({'kind':'local','ops':ops,'frontier':end,'sources':sorted({s for op in ops for s in op['sources']})})

    def flush_base_logs(self):
        """Explicit bounded-buffer checkpoint of base logs, retaining Frames."""
        self._open()
        parent = self.root
        new = self.store.new_root()  # Flatten labels as an explicit scan task.
        for h in range(self.store.num_blocks):
            view = self.store.view(parent,h)
            if view.base_log:
                self.cache.ensure(2*self.cache.buffer_cost)
                with self.cache.outputs(1) as buffers:
                    with self.cache.pin(view.version_id) as source: buffers[0].view[:] = source.view
                    array = buffers[0].array(self.store.payload_bytes)
                    try:
                        self.phase2.resolve_base(array,view,h)
                        version = self.cache.adopt(buffers[0])
                        view = replace(view,version_id=version,base_log=(),base_frontier=self.frontier)
                    finally: del array
            self.store.publish(new,((h,view),))
        self.store.seal(parent); self.root = new
        self.journal.append('commits',{'kind':'flush-base-logs','input_root':parent,'root':new,'frontier':self.frontier})
        self.trace.record('flush-base-logs',metadata={'path':'FLUSH_BASE_LOGS','blocks':self.store.num_blocks})
        return self

    def compact_metadata(self):
        """Explicit disk-catalog compaction, with one resolved view at a time."""
        self._open()
        parent = self.root; new = self.store.new_root()
        for h in range(self.store.num_blocks): self.store.publish(new,((h,self.store.view(parent,h)),))
        self.store.db.execute('UPDATE root_log_bounds SET rotations=? WHERE root=?',(self.store.log_bound(parent),new))
        self.store.db.commit()
        self.store.seal(parent); self.root = new
        self.journal.append('commits',{'kind':'compact-metadata','input_root':parent,'root':new,'frontier':self.frontier})
        self.trace.record('compact-metadata',metadata={'path':'COMPACT_METADATA','blocks':self.store.num_blocks})
        return self

    def _joint_phase2(self,gate,sources,bits):
        from .phase2_runtime import pauli_of, inverse_word
        from .frame_planner import choose_alignment
        from .pauli_rotation import SignedPauli, apply_pauli_pair
        from .phase import PhaseValue
        import cmath
        pauli_gate = gate.kind in {'PAULI_ROT','RX','RY'}
        if pauli_gate and any(q < self.block_bits for q in gate.controls):
            if self.alignment_policy in {'input-a','input-b'}: raise ValueError('Forced alignment is not legal for lower controls')
            return self._joint(gate,sources,bits)
        if not pauli_gate and (self.mode != 'per-block-frame' or self.frame_policy == 'materialize' or
                              any(q < self.block_bits for q in gate.qubits) or self.alignment_policy == 'identity'):
            if self.alignment_policy in {'input-a','input-b'} and self.mode == 'per-block-frame':
                raise ValueError('Forced common-frame alignment is not legal for this Joint predicate/support')
            return self._joint(gate,sources,bits)
        parent = self.root; new = self.store.new_root(parent)
        p = pauli_of(gate) if pauli_gate else None
        mask = (p.x_mask >> self.block_bits) if p else sum(1 << (q-self.block_bits) for q in bits)
        count = 2 if p else 1 << len(bits)
        for base in range(self.store.num_blocks):
            if p:
                if base > (base ^ mask): continue
                partners = [base,base ^ mask]
            else:
                if base & mask or not self._upper_ok(gate,base): continue
                partners = [base | sum(((i >> j)&1) << (q-self.block_bits) for j,q in enumerate(bits)) for i in range(count)]
            views = [self.store.view(parent,h) for h in partners]
            assets = [self.phase2.asset(v.frame_asset_id) for v in views]
            policy = 'identity' if self.frame_policy == 'materialize' or self.mode != 'per-block-frame' else self.alignment_policy
            decision = choose_alignment(assets,policy,working_set(self.config,count),self.profile.profile_id,self.frame_profile)
            identity = decision.chosen == 'identity'
            reference = None if identity else assets[0 if decision.chosen == 'input-a' else min(1,count-1)]
            phase = PhaseValue() if identity else views[0 if decision.chosen == 'input-a' else min(1,count-1)].phase_value
            if not identity and phase.evaluate() == 0:
                if policy != 'auto': raise ValueError('Common-frame phase anchor must be nonzero')
                decision = choose_alignment(assets,'identity',working_set(self.config,count),self.profile.profile_id,self.frame_profile)
                identity,reference,phase = True,None,PhaseValue()
            frame_id = '' if identity else self.phase2.intern(reference)
            self.cache.ensure(2*count*self.cache.buffer_cost)
            with self.cache.outputs(count) as aligned:
                arrays = [buf.array(self.store.payload_bytes) for buf in aligned]
                try:
                    for i,h in enumerate(partners):
                        with self.cache.pin(views[i].version_id) as source: aligned[i].view[:] = source.view
                        if identity: self._resolve_array(arrays[i],views[i],h)
                        else:
                            self.phase2.resolve_base(arrays[i],views[i],h)
                            if assets[i].tableau != reference.tableau:
                                assets[i].apply(arrays[i],self.assets,self.binding)
                                for op in inverse_word(reference.word): apply_local(arrays[i],op,self.assets,self.binding,block_bits=self.block_bits)
                                arrays[i] *= cmath.exp(1j*np.pi*reference.correction/4)
                            arrays[i] *= views[i].phase_value.evaluate()/phase.evaluate()
                    with self.cache.outputs(count) as outputs:
                        results = [buf.array(self.store.payload_bytes) for buf in outputs]
                        try:
                            if p:
                                low_mask = (1 << self.block_bits)-1
                                lower = SignedPauli(p.x_mask&low_mask,p.z_mask&low_mask)
                                transformed = lower if identity else reference.tableau.conjugate_inverse(lower)
                                pp = SignedPauli((p.x_mask&~low_mask)|transformed.x_mask,(p.z_mask&~low_mask)|transformed.z_mask,transformed.sign_bit)
                                # Controlled upper RX/RY requires source-pair
                                # predicate filtering; lower controls fall back.
                                if gate.controls:
                                    if any(q < self.block_bits for q in gate.controls):
                                        raise ValueError('Controlled Joint rotation requires native fallback')
                                    if not self._upper_ok(gate,base):
                                        for i in range(count): results[i][:] = arrays[i]
                                    else: apply_pauli_pair(*arrays,*results,pp,gate.angle.evaluate(self.binding),*partners,self.block_bits)
                                else: apply_pauli_pair(*arrays,*results,pp,gate.angle.evaluate(self.binding),*partners,self.block_bits)
                            else: apply_group(arrays,results,self._local(gate),self.block_bits,bits,self.assets,self.binding)
                            versions = [self.cache.adopt(buf) for buf in outputs]
                            end = max(sources)
                            coordinate = uuid4().hex
                            self.store.publish(new,((h,StoredBlockView(v,end,end,phase=phase.evaluate(),frame_asset_id=frame_id,
                                phase_value=phase,base_coordinate_id=coordinate)) for h,v in zip(partners,versions)))
                        finally: results.clear()
                finally: arrays.clear()
            self.trace.record(str(sources),performance_class='frame-plan',metadata={'path':'ALIGN_FRAME' if not identity else 'MATERIALIZE_FRAME',
                'decision':decision.data(),'source_ops':sources,'partner_blocks':partners,'binding_id':self.binding.binding_id,'layout_id':self.layout.layout_id})
        self.phase2.progress(new,max(sources))
        self.store.seal(parent); self.root,self.frontier = new,max(sources)
        self.journal.append('joint',{'source_ops':sources,'input_root':parent,'output_root':new,'semantics_version':2,'frontier':self.frontier})

    def _joint(self, gate, sources, bits):
        parent = self.root
        new = self.store.new_root(parent)
        mask = sum(1 << (q-self.block_bits) for q in bits)
        begin = perf_counter()
        count = 1 << len(bits)
        for base in range(self.store.num_blocks):
            if base & mask or not self._upper_ok(gate,base): continue
            partners = [base | sum(((i >> j)&1) << (q-self.block_bits) for j,q in enumerate(bits)) for i in range(count)]
            self.cache.ensure(2*count*self.cache.buffer_cost)
            with self.cache.outputs(count) as aligned:
                arrays = [b.array(self.store.payload_bytes) for b in aligned]
                try:
                    # Pin each input only while copying; alignment buffers remain
                    # immutable while old cache inputs may be evicted for outputs.
                    for i,h in enumerate(partners):
                        view = self.store.view(parent,h)
                        with self.cache.pin(view.version_id) as source:
                            aligned[i].view[:] = source.view
                        self._resolve_array(arrays[i],view,h)
                    with self.cache.outputs(count) as results:
                        result_arrays = [b.array(self.store.payload_bytes) for b in results]
                        try:
                            apply_group(arrays,result_arrays,self._local(gate),self.block_bits,bits,self.assets,self.binding)
                            versions = [self.cache.adopt(b) for b in results]
                            end = max(sources)
                            self.store.publish(new,((h,StoredBlockView(v,end,end)) for h,v in zip(partners,versions)))
                        finally: result_arrays.clear()
                finally: arrays.clear()
        self.phase2.progress(new,max(sources))
        self.store.seal(parent)
        self.root = self.store.flatten(new)
        self.frontier = max(sources)
        self.journal.append('joint',{'source_ops':sources,'input_root':parent,'output_root':self.root,'layout_id':self.layout.layout_id})
        self.trace.record(str(sources),performance_class='compute',transfer_ms=(perf_counter()-begin)*1000,
                          metadata={'path':'JOINT','source_ops':sources,'execution':self.execution,'upper_targets':bits,
                                    'binding_id':self.binding.binding_id,'layout_id':self.layout.layout_id,
                                    'execution_gate_count':1,'lowered_count':int(self.execution != 'auto'),
                                    'approximation_error':gate.approximation_error})

    def block_view(self,h):
        self._open()
        dependencies = frozenset().union(*(g.dependencies for g in self.sequencer.operations[:self.frontier+1]))
        return replace(self.store.view(self.root,h),binding_id=self.binding.binding_id if self.binding else '',
                       layout_id=self.layout.layout_id,parameter_dependencies=dependencies)

    @contextmanager
    def _logical_buffers(self, root, partners):
        count = len(partners)
        self.cache.ensure(2*count*self.cache.buffer_cost)
        with ExitStack() as stack:
            inputs = [stack.enter_context(self.cache.pin(self.store.view(root,h).version_id)) for h in partners]
            outputs = stack.enter_context(self.cache.outputs(count))
            arrays = [b.array(self.store.payload_bytes) for b in outputs]
            try:
                for i,h in enumerate(partners):
                    outputs[i].view[:] = inputs[i].view
                    self._resolve_array(arrays[i],self.store.view(root,h),h)
                yield arrays,outputs
            finally: arrays.clear()

    def state(self,binding=None):
        self.materialize(binding=binding)
        size = 1 << self.num_qubits
        needed = size*16
        self.cache.ensure(needed+2*self.cache.buffer_cost)
        self.budget.reserve(needed)
        try:
            out = np.empty(size,dtype=complex)
            for h in range(self.store.num_blocks):
                with self._logical_buffers(self.root,[h]) as (arrays,_):
                    for offset, amplitude in enumerate(arrays[0]):
                        physical = (h << self.block_bits)|offset
                        logical = sum(((physical >> p)&1) << q for q,p in enumerate(self.layout.logical_to_physical))
                        out[logical] = amplitude
            return out
        finally: self.budget.release(needed)

    def physical_index_bit_swap(self,a,b):
        self._open()
        layout = self.layout.swapped_physical(a,b)
        if a == b: return self
        count = 2 if (a < self.block_bits) != (b < self.block_bits) else 1
        needed = working_set(self.config,count)+count*self.cache.buffer_cost
        if needed > self.budget.limit: raise MemoryError(f'Layout task requires {needed} managed bytes')
        self.materialize(binding=self.binding)
        parent = self.root
        new = self.store.new_root() # All outputs fully materialized, no implicit inherited slots.
        begin = perf_counter()
        lower_upper = (a < self.block_bits) != (b < self.block_bits)
        for h in range(self.store.num_blocks):
            if lower_upper:
                lower, upper = (a,b) if a < self.block_bits else (b,a)
                delta = 1 << (upper-self.block_bits)
                if h & delta: continue
                partners = [h,h|delta]
            else: partners = [h]
            with self._logical_buffers(parent,partners) as (arrays,buffers):
                # Scalar swaps need no state-sized index/scratch arrays.
                if a < self.block_bits and b < self.block_bits:
                    for i in range(len(arrays[0])):
                        j = i ^ ((((i >> a) ^ (i >> b))&1) << a) ^ ((((i >> a) ^ (i >> b))&1) << b)
                        if i < j: arrays[0][i],arrays[0][j] = arrays[0][j],arrays[0][i]
                    destinations = partners
                elif lower_upper:
                    for i in range(len(arrays[0])):
                        if (i >> lower)&1:
                            j = i ^ (1 << lower)
                            arrays[0][i],arrays[1][j] = arrays[1][j],arrays[0][i]
                    destinations = partners
                else:
                    pa,pb = a-self.block_bits,b-self.block_bits
                    toggle = ((h >> pa) ^ (h >> pb))&1
                    destinations = [h ^ (toggle << pa) ^ (toggle << pb)]
                versions = [self.cache.adopt(buf) for buf in buffers]
                self.store.publish(new,((d,StoredBlockView(v,self.frontier,self.frontier)) for d,v in zip(destinations,versions)))
        self.store.seal(parent)
        self.root, self.layout = new,layout
        self.plan = None
        self.journal.append('commits',{'kind':'layout','bits':(a,b),'input_root':parent,'output_root':new,'layout_id':layout.layout_id,'frontier':self.frontier})
        self.trace.record('layout-swap',performance_class='layout',transfer_ms=(perf_counter()-begin)*1000,
                          metadata={'path':'PHYSICAL_INDEX_BIT_SWAP','bits':(a,b),'layout_id':layout.layout_id})
        return self

    def commit_snapshot(self, *, prefix_fingerprint=None):
        self._open()
        self.phase2.validate_root(self.root)
        self._persist_assets()
        self.cache.flush()
        self.journal.flush()
        self.store.flush()
        dependencies = sorted(frozenset().union(*(g.dependencies for g in self.sequencer.operations[:self.frontier+1])))
        manifest = SnapshotManifest(uuid4().hex,self.root,self.frontier,self.circuit_hash,self.initial_state_hash,
            self.layout.logical_to_physical,self.layout.layout_id,self.binding.values if self.binding else (),tuple(dependencies),
            self.journal.cursors(),tuple(sorted(self.assets.assets)),prefix_fingerprint,schema_version=2,
            binding_witnesses=tuple((k,v.numerator,v.denominator) for k,v in (self.binding.exact_witnesses if self.binding else ())),semantics_version=2)
        data = canonical(asdict(manifest))
        with self.store.db:
            self.store.db.execute('UPDATE roots SET sealed=1 WHERE id=?',(self.root,))
            self.store.db.execute('INSERT INTO snapshots VALUES(?,?)',(manifest.snapshot_id,data))
        self.root = self.store.new_root(manifest.root)
        self.trace.record(manifest.snapshot_id,metadata={'path':'SNAPSHOT_COMMIT','frontier':self.frontier,'root':manifest.root})
        export = self.store.directory/'manifests'/f'snapshot-{manifest.snapshot_id}.json'
        raw=(data+'\n').encode()
        atomic_write_bytes(export,raw,disk_budget=self.store.disk_budget,operation='snapshot-export')
        return manifest

    def commit_prefix(self, variable_parameters, *, binding=None):
        ids = frozenset(p.parameter_id if hasattr(p,'parameter_id') else str(p) for p in variable_parameters)
        first = next((g.op_id for g in self.sequencer.operations if g.dependencies & ids),len(self.sequencer.operations))
        if self.frontier >= first: raise ValueError('Variable-dependent gates already executed')
        bound = binding or self.binding or self.bind()
        self.materialize(frontier=first-1,binding=bound)
        dependencies = frozenset().union(*(g.dependencies for g in self.sequencer.operations[:first]))
        fixed = tuple((k,bound.value(k)) for k in sorted(dependencies))
        fixed_witnesses = tuple((k,v.numerator,v.denominator) for k,v in bound.exact_witnesses if k in dependencies)
        fingerprint = content_hash({'schema_version':1,'prefix':[gate_data(g) for g in self.sequencer.operations[:first]],
                                    'initial':self.initial_state_hash,'fixed':fixed,'layout':self.layout.logical_to_physical,
                                    'frontier':first-1,'dtype':'<c16','semantics':2,'witnesses':fixed_witnesses})
        return self.commit_snapshot(prefix_fingerprint=fingerprint)

    def _load_manifest(self, snapshot):
        key = snapshot.snapshot_id if isinstance(snapshot,SnapshotManifest) else str(snapshot)
        row = self.store.db.execute('SELECT data FROM snapshots WHERE id=?',(key,)).fetchone()
        if row is None: raise ValueError('Snapshot is not committed in this store')
        manifest = SnapshotManifest.from_data(json.loads(row[0]))
        if isinstance(snapshot,SnapshotManifest) and manifest != snapshot: raise ValueError('Snapshot fingerprint/content mismatch')
        return manifest

    def _restore(self, snapshot):
        manifest = self._load_manifest(snapshot)
        for kind,cursor in manifest.journal_cursors.items():
            for _ in self.journal.replay(kind,cursor): pass
        for record in self.journal.replay('gates',manifest.journal_cursors.get('gates',0)):
            gate = decode_gate(record['gate'])
            if gate.op_id != len(self.sequencer.operations): raise OSError('Duplicate/nonsequential semantic op ID')
            self.sequencer.operations.append(gate)
        for key in manifest.assets:
            path = self.store.directory/'assets'/f'{key}.bin'
            data = path.read_bytes()
            dimension = int((len(data)//16)**.5)
            if dimension < 2 or dimension&(dimension-1) or dimension*dimension*16 != len(data): raise OSError('Invalid matrix asset shape')
            matrix = np.frombuffer(data,dtype='<c16').reshape(dimension,dimension)
            if self.assets.register(matrix,dimension.bit_length()-1) != key: raise OSError('Matrix asset checksum mismatch')
        if self.circuit_hash != manifest.circuit_hash: raise OSError('Snapshot circuit hash mismatch')
        self.layout = QubitLayout(manifest.layout,manifest.layout_id)
        self.root = self.store.new_root(manifest.root)
        self.frontier, self.initial_state_hash = manifest.frontier,manifest.initial_state_hash
        self.binding = ParameterBinding(self.circuit_hash,manifest.binding_values,
            tuple((k,ExactPi(n,d)) for k,n,d in manifest.binding_witnesses))
        # Validate referenced frames/extents lazily on load; committed root exists now.
        for _ in self.store.root_chain(manifest.root): pass
        self.phase2.validate_root(manifest.root)

    def fork(self, snapshot, *, binding):
        self._open()
        manifest = self._load_manifest(snapshot)
        if manifest.circuit_hash != self.circuit_hash: raise ValueError('Prefix belongs to another registered circuit')
        if binding.circuit_hash != self.circuit_hash: raise ValueError('Binding belongs to another circuit')
        fixed = dict(manifest.binding_values)
        witnesses = {k:ExactPi(n,d) for k,n,d in manifest.binding_witnesses}
        for key in manifest.parameter_dependencies:
            if binding.value(key) != fixed[key]: raise ValueError('Prefix fixed-parameter fingerprint mismatch')
            if dict(binding.exact_witnesses).get(key) != witnesses.get(key):
                raise ValueError('Prefix fixed-parameter witness fingerprint mismatch')
        run = StoredSimulator(self.num_qubits,self.config,profile=self.profile,execution=self.execution,mode=self.mode,
                              frame_policy=self.frame_policy,alignment_policy=self.alignment_policy,
                              frame_profile=self.frame_profile,
                              _snapshot=manifest,_create=False,**({'transport_factory':self._store_kwargs['transport_factory']} if 'transport_factory' in self._store_kwargs else {}))
        run.binding = binding
        run.trace.record('prefix-fork',metadata={'path':'PREFIX_FORK','snapshot_id':manifest.snapshot_id,'payload_copy_bytes':0,'binding_id':binding.binding_id})
        return run

    @classmethod
    def resume(cls, storage, snapshot, *, profile=None, execution='auto', mode='per-block-frame',frame_policy='auto',alignment_policy='auto'):
        catalog=(Path(storage.directory)/'metadata/catalog.sqlite3').resolve()
        db = sqlite3.connect(catalog.as_uri()+'?mode=ro',uri=True)
        try: data = json.loads(db.execute("SELECT data FROM settings WHERE key='config'").fetchone()[0])
        finally: db.close()
        return cls(data['num_qubits'],storage,profile=profile,execution=execution,mode=mode,frame_policy=frame_policy,alignment_policy=alignment_policy,_snapshot=snapshot,_create=False)

    def handle(self, snapshot, *, role='forward'):
        manifest = self._load_manifest(snapshot)
        binding = ParameterBinding(manifest.circuit_hash,manifest.binding_values,tuple((k,ExactPi(n,d)) for k,n,d in manifest.binding_witnesses))
        return StoredStateHandle(manifest.snapshot_id,manifest.root,manifest.frontier,binding.binding_id,manifest.circuit_hash,
                                 QubitLayout(manifest.layout,manifest.layout_id),role)

    def expectation(self, observables, *, handle=None, binding=None):
        from .observable_plan import streaming_expectation
        if handle is None:
            self.materialize(binding=binding)
            # Immutable logical handle does not require a durable disk snapshot.
            self.store.seal(self.root)
            handle = StoredStateHandle('',self.root,self.frontier,self.binding.binding_id if self.binding else '',self.circuit_hash,self.layout)
        return streaming_expectation(self,observables,handle)

    def iter_blocks(self, handle=None):
        self._open()
        root = handle.root if handle else self.root
        for h in range(self.store.num_blocks):
            self.cache.ensure(2*self.cache.buffer_cost+self.store.payload_bytes)
            self.budget.reserve(self.store.payload_bytes)
            try:
                with self._logical_buffers(root,[h]) as (arrays,_):
                    block = arrays[0].copy()
                    block.setflags(write=False)
            finally: self.budget.release(self.store.payload_bytes)
            # Returned block is caller-owned; retaining output copies is outside
            # this runtime's cache budget, just like retaining state() output.
            yield h,block

    def export_state(self,path,*,order='logical',binding=None):
        import struct
        if order not in {'logical','physical'}: raise ValueError('Invalid export order')
        from .state_export import admitted_export_file, write_exact
        with admitted_export_file(path,(1 << self.num_qubits)*16,budget=self.store.disk_budget,
                                  dataset=self.store.directory,owner=self.run_id) as stream:
            self.materialize(binding=binding)
            for h in range(self.store.num_blocks):
                with self._logical_buffers(self.root,[h]) as (arrays,_):
                    if order == 'physical':
                        stream.seek(h*self.store.payload_bytes)
                        view = memoryview(arrays[0])
                        try: write_exact(stream,view)
                        finally: view.release()
                    else:
                        for offset,amplitude in enumerate(arrays[0]):
                            physical = (h << self.block_bits)|offset
                            logical = sum(((physical >> p)&1) << q for q,p in enumerate(self.layout.logical_to_physical))
                            stream.seek(logical*16); write_exact(stream,struct.pack('<dd',amplitude.real,amplitude.imag))
        return Path(path)

    def collect_garbage(self):
        """Quiescent, conservative reclamation of whole unreferenced segments.

        Extents within a live segment are retained; no live snapshot is moved.
        Other active runtimes (including suffix forks) prohibit maintenance.
        """
        self._open()
        active = self.store.db.execute('SELECT id FROM active_runs').fetchall()
        if active != [(self.run_id,)]: raise RuntimeError('Garbage collection requires a single active runtime')
        self.cache.flush()
        self.cache.clear()
        db = self.store.db
        db.execute('CREATE TEMP TABLE IF NOT EXISTS live_roots(root TEXT PRIMARY KEY)')
        db.execute('DELETE FROM live_roots')
        def mark(root):
            for ancestor in self.store.root_chain(root):
                db.execute('INSERT OR IGNORE INTO live_roots VALUES(?)',(ancestor,))
        mark(self.root)
        for row in db.execute('SELECT data FROM snapshots'):
            mark(json.loads(row[0])['root'])
        with db:
            db.execute('DELETE FROM views WHERE root NOT IN (SELECT root FROM live_roots)')
            db.execute('DELETE FROM views_v2 WHERE root NOT IN (SELECT root FROM live_roots)')
            db.execute('DELETE FROM root_edges WHERE root NOT IN (SELECT root FROM live_roots)')
            db.execute('DELETE FROM root_log_bounds WHERE root NOT IN (SELECT root FROM live_roots)')
            db.execute('DELETE FROM catalog_checksums WHERE substr(key,6,32) NOT IN (SELECT root FROM live_roots)')
            db.execute('DELETE FROM roots WHERE id NOT IN (SELECT root FROM live_roots)')
            db.execute('DELETE FROM versions WHERE id NOT IN (SELECT version FROM views)')
        removed_bytes = 0
        for path in (self.store.directory/'state').glob('segment-*.bin'):
            segment = int(path.stem.split('-')[1])
            if db.execute('SELECT 1 FROM versions WHERE segment=? LIMIT 1',(segment,)).fetchone(): continue
            handle = self.store.files.pop(segment,None)
            if handle: handle.close()
            removed_bytes += path.stat().st_size
            path.unlink()
        self.trace.record('gc',metadata={'path':'GARBAGE_COLLECTION','reclaimed_bytes':removed_bytes})
        return removed_bytes

    def close(self):
        if not self.closed:
            self.cache.discard_unwritten()
            self.store.db.execute('DELETE FROM active_runs WHERE id=?',(self.run_id,))
            self.store.db.commit()
            self.journal.close()
            self.store.close()
            self.trace.close()
            self.closed = True

    def __enter__(self): self._open(); return self
    def __exit__(self,*_): self.close()
