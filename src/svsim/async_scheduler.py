"""Mac-first bounded async runtime over the exact distributed block kernels."""
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from time import perf_counter, process_time

from .async_cache import AsyncBlockCache
from .async_io import WorkerIOEngine, BackpressureError
from .distributed import DistributedSimulator
from .execution_tasks import EpochLedger
from .fused_swap import execute_fused_swap
from .journal import content_hash, decode_gate, gate_data
from .mpi_progress import MPIProgressEngine
from .runtime_control import RuntimeControl
from .remaining_work import boundary_condition, publication_condition
from .s2_planner import plan_window
from .storage import StoredBlockView
from .streaming_kernels import apply_local


@dataclass(frozen=True)
class AsyncPolicy:
    mode: str = 'observe'
    io_engine: str = 'auto'
    max_io_requests: int = 2
    io_workers: int = 2
    max_mpi_requests: int = 4
    prefetch_blocks: int = 2
    lookahead_gates: int = 32
    beam_width: int = 4
    swap_policy: str = 'S0'
    max_active_jobs: int = 1
    cooldown_seconds: float = 120
    minimum_gain_fraction: float = 0.1
    candidate_ttl_seconds: float = 120

    def __post_init__(self):
        if self.mode not in {'observe', 'recommend', 'auto'}:
            raise ValueError('Unknown runtime mode')
        if self.io_engine not in {'auto', 'worker', 'cooperative', 'io_uring'}:
            raise ValueError('Unknown I/O engine')
        if self.swap_policy not in {'S0', 'S1', 'S2'}:
            raise ValueError('Unknown swap policy')
        for name in ('max_io_requests', 'io_workers', 'max_mpi_requests', 'lookahead_gates', 'beam_width', 'max_active_jobs'):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError('Positive integer limits required')
        if self.lookahead_gates > 32 or self.beam_width > 16:
            raise ValueError('DAG window/beam exceed bounded search limits')
        if self.max_io_requests > 64 or self.max_mpi_requests > 64 or self.max_active_jobs > 16:
            raise ValueError('Request/job count exceeds bounded runtime limits')
        if type(self.prefetch_blocks) is not int or self.prefetch_blocks < 0:
            raise ValueError('Invalid prefetch limit')
        import math
        if any(not math.isfinite(v) or v < 0 for v in (self.cooldown_seconds, self.candidate_ttl_seconds)):
            raise ValueError('Invalid runtime interval')
        if not 0 <= self.minimum_gain_fraction < 1:
            raise ValueError('Invalid gain threshold')


def _stage_condition(condition):
    return {k: v for k, v in condition.items() if k != 'operation'}


class AsyncDistributedSimulator(DistributedSimulator):
    transport_class = MPIProgressEngine

    def __init__(self, num_qubits, config, *, policy=None, **kwargs):
        self.policy = policy or AsyncPolicy()
        self.io_engine = None
        self._private_epoch = False
        self._executor_in_flight = None
        self._saved_runtime_state = None
        self._gradient_origin = None
        self._angle_overrides = {}
        self._async_errors = []
        self._measurement_sequence = 0
        self._measurement_warmup = False
        # Phase 4 uses owner-local lifecycle even on a single Mac.
        config = replace(config, storage_scope='node-local')
        super().__init__(num_qubits, config, **kwargs)
        error=None
        try: self.runtime = RuntimeControl(self)
        except Exception as exc:error=str(exc)
        try:self.transport.vote(error)
        except Exception:
            if hasattr(self,'runtime'):self.runtime.close_measurements()
            super().close()
            raise
        saved = self._saved_runtime_state
        if saved and 'policy' in saved:
            self.policy = AsyncPolicy(**saved['policy'])
            self.runtime.plan_version = saved['plan_version']
            self.runtime.policy_epoch = saved['policy_epoch']
            self.runtime.profile_version = saved['profile_version']
            self._gradient_origin = saved.get('gradient_origin')
        error = None
        try:
            self.transport.agree(asdict(self.policy))
            self.cache.clear()
            self.io_engine = WorkerIOEngine(max_requests=self.policy.max_io_requests,
                              workers=self.policy.io_workers, engine=self.policy.io_engine,
                              mpi_thread_level=self.transport.thread_level, budget=self.budget)
            if self.config.amplitude_store == 'file':
                self.cache = self.local.cache = AsyncBlockCache(self.store, self.budget, self.io_engine,
                                                  prefetch_blocks=self.policy.prefetch_blocks)
            self.transport.max_requests = self.policy.max_mpi_requests
            self.transport.generations = [0]*self.policy.max_mpi_requests
            self.transport.progress_callback = self._progress_io
            self.transport.completion_observer = self.runtime.mpi_completion
            self._original_io_poll = self.io_engine.poll
            self.io_engine.poll = self._poll_io
            self.trace.record('async-engine', metadata={'path': 'ASYNC_RUNTIME',
                              'io_engine': self.io_engine.identity, 'policy': asdict(self.policy),
                              'storage_scope': self.config.storage_scope})
        except Exception as exc:
            error = str(exc)
        try:
            self.transport.vote(error)
        except BaseException:
            try:
                if self.io_engine is not None:
                    self.io_engine.close()
            finally:
                try:super().close()
                finally:self.runtime.close_measurements()
            raise

    def _poll_io(self):
        completed = self._original_io_poll()
        for request in completed:
            self.runtime.io_completion(request)
            if request.error is not None:
                self._async_errors.append(request.error)
        if completed and hasattr(self,'runtime'):self.runtime.progress_eta()
        return completed

    def _progress_io(self):
        # Defer ordinary local I/O errors to the next ordered collective vote.
        try:
            self.io_engine.poll()
        except Exception as exc:
            self._async_errors.append(exc)

    def _measurement_context(self):
        return {'configuration_fingerprint': getattr(self.runtime.profile_adapter, 'fingerprint', None),
                'n': self.num_qubits, 'block_bits': self.block_bits,
                'backend': self.config.backend, 'engine': self.io_engine.identity,
                'ranks': self.size, 'threads': self.policy.io_workers,
                'binding_id':self.binding.binding_id if self.binding else None,
                'policy_epoch':self.runtime.policy_epoch,'plan_version':self.runtime.plan_version}

    def _record_executor_group(self, event_id, group_id, condition, quantity,
                               wall_seconds, cpu_seconds, *, success, parent_id=None):
        if not self.runtime.measurements.enabled:
            return False
        same_host = len({row['host'] for row in self.environment}) == 1
        return self.runtime.measurements.complete(
            event_id, context=self._measurement_context(), condition=condition,
            parent_id=parent_id, group_id=group_id, stage=condition['operation'],
            units='groups', quantity=1, protocol_seconds=wall_seconds,
            service_seconds=None, cpu_seconds=cpu_seconds,
            success=success, validated=success,
            evidence='injected' if getattr(self,'_measurement_injected',False) else 'measured',
            warmup=self._measurement_warmup,
            scope='same-host' if same_host else 'cross-host')

    def _admit(self, *args, **kwargs):
        if self.io_engine is not None:
            self._progress_io()
            self.transport.vote(str(self._async_errors[0]) if self._async_errors else None)
        return super()._admit(*args, **kwargs)

    def _publish(self, *args, **kwargs):
        super()._publish(*args, **kwargs)
        if isinstance(self.cache, AsyncBlockCache):
            for version, entry in self.cache.entries.items():
                if entry.dirty and not entry.pins:
                    try:
                        self.cache.writeback(version)
                    except BackpressureError:
                        break

    def _install_policy(self, policy):
        if policy.io_engine != self.policy.io_engine or policy.io_workers != self.policy.io_workers:
            raise ValueError('Changing I/O engine/worker count requires restart')
        if hasattr(self.io_engine, 'capacity') and policy.max_io_requests > self.io_engine.capacity:
            raise ValueError('Growing native queue capacity requires restart')
        tag_ub = self.transport.data.Get_attr(self.transport._MPI.TAG_UB) or 32767
        if policy.max_mpi_requests-1 > tag_ub:
            raise ValueError('MPI slots exceed TAG_UB')
        difference = (policy.max_io_requests-self.policy.max_io_requests)*512
        if difference > 0:
            self.budget.reserve(difference)
        elif difference < 0:
            self.budget.release(-difference)
        self.io_engine.metadata_bytes += difference
        if policy.max_mpi_requests != self.policy.max_mpi_requests:
            self.transport.generations = [0]*policy.max_mpi_requests
            self.transport.max_requests = policy.max_mpi_requests
        self.io_engine.max_requests = policy.max_io_requests
        if isinstance(self.cache, AsyncBlockCache):
            self.cache.prefetch_blocks = policy.prefetch_blocks
        self.policy = policy

    def _fork_kwargs(self):
        return {'policy': self.policy}

    def gate(self,*args,**kwargs):
        op=super().gate(*args,**kwargs)
        self.runtime.invalidate_eta('SOURCE_APPENDED',reset_endpoint=True)
        return op

    def tile_index_bit_swap(self,pairs,*,tile_bytes=4096):
        """Explicit P=1 identity-Frame tile route; never an implicit fallback."""
        if self._private_epoch:raise ValueError('Tile transpose requires a published cut')
        self.drain()
        from .tile_transpose import execute_tile_swap
        result=execute_tile_swap(self,pairs,tile_bytes)
        self.runtime.invalidate_eta('TILE_LAYOUT_CHANGED')
        return result

    def _checkpoint_state(self):
        if self._angle_overrides:
            raise ValueError('Shifted occurrence workspace cannot be committed as semantic source')
        state = self.runtime.state() if hasattr(self, 'runtime') else {
            'policy': asdict(self.policy), 'plan_version': 1, 'policy_epoch': 1, 'profile_version': 1}
        state['gradient_origin'] = self._gradient_origin
        return state

    def _restore_checkpoint_state(self, state):
        self._saved_runtime_state = state

    def _prefetch_local(self):
        if not isinstance(self.cache, AsyncBlockCache) or not self.policy.prefetch_blocks:
            return
        error = None
        try:
            versions = []
            for index, h in enumerate(self.ownership.local_blocks(self.rank)):
                if index >= self.policy.prefetch_blocks:
                    break
                data = self._locate(self.root, h)
                view = data.get('view')
                if view and view['storage_rank'] == self.rank and view['store_id'] == self.store_id:
                    version = view['version']
                    versions.append(version)
                    if version not in self.cache.entries:
                        self.cache.prefetch(version)
            self.cache.set_future_versions(versions)
        except Exception as exc:
            error = str(exc)
        self.transport.vote(error)

    def _physical(self, gate):
        if gate.op_id in self._angle_overrides:
            from .parameters import expression
            gate = replace(gate, angle=expression(self._angle_overrides[gate.op_id]))
        return replace(gate, targets=tuple(self.layout.logical_to_physical[q] for q in gate.targets),
                       controls=tuple(self.layout.logical_to_physical[q] for q in gate.controls))

    def _execute_local_fused(self, originals, end):
        self._admit(4, output_blocks=self.ownership.blocks_per_rank)
        parent = self.root
        new = self._new_root(parent)
        physical = [self._physical(g) for g in originals]
        for h in range(self.ownership.num_blocks):
            owner = self.ownership.shard_owner(h)
            with self.fetch(h, owner, root=parent) as fetched:
                error = None
                if fetched is not None:
                    arr = fetched.array(self.store.payload_bytes)
                    try:
                        for gate in physical:
                            if self.local._upper_ok(gate, h):
                                apply_local(arr, self.local._local(gate), self.assets, self.binding,
                                            block_id=h, block_bits=self.block_bits)
                        self._publish(new, h, StoredBlockView(self.cache.adopt(fetched), end, end))
                    except Exception as exc:
                        error = str(exc)
                    finally:
                        del arr
                self.transport.vote(error)
        self.local.phase2.progress(new, end)
        self._commit_stage(new, end)
        for original, gate in zip(originals, physical):
            self.history.append({'kind': 'gate', 'source': original.op_id,
                                 'physical': gate_data(gate), 'layout': self.layout.logical_to_physical,
                                 'fused_group': [g.op_id for g in originals]})
        self.trace.record(str(originals[0].op_id), performance_class='fusion',
                          metadata={'path': 'LOCAL_GATE_FUSION', 'source_ids': [g.op_id for g in originals],
                                    'state_passes': 1, 'sequential_passes': len(originals)})

    def _execute(self, gate, end, *, physical_swap=False):
        from .planner import group_bits
        bits = group_bits(gate, self.block_bits)
        if not bits or gate.kind in {'MCX', 'SWAP'} or physical_swap:
            return super()._execute(gate, end, physical_swap=physical_swap)
        from .async_exchange import fetch_partners
        from .pauli_rotation import apply_pauli_pair
        from .phase2_runtime import pauli_of
        from .streaming_kernels import apply_group
        b = self.block_bits
        p = pauli_of(gate) if gate.kind == 'PAULI_ROT' else None
        count = 2 if p else 1 << len(bits)
        self._admit(2*count+3, output_blocks=self.ownership.blocks_per_rank)
        parent = self.root
        new = self._new_root(parent)
        mask = (p.x_mask >> b) if p else sum(1 << (q-b) for q in bits)
        for base in range(self.ownership.num_blocks):
            if p:
                if base > (base ^ mask):
                    continue
                partners = [base, base ^ mask]
            else:
                if base & mask or not self.local._upper_ok(gate, base):
                    continue
                partners = [base | sum(((j >> i) & 1) << (q-b) for i, q in enumerate(bits))
                            for j in range(count)]
            owner = self.ownership.shard_owner(partners[0])
            with self._buffers(count, owner) as buffers:
                with fetch_partners(self, partners, owner, root=parent) as inputs:
                    error = None
                    arrays = [v.array(self.store.payload_bytes) for v in inputs if v is not None]
                    outputs = [v.array(self.store.payload_bytes) for v in buffers]
                    try:
                        if self.rank == owner:
                            if p:
                                apply_pauli_pair(*arrays, *outputs, p, gate.angle.evaluate(self.binding),
                                                 *partners, b)
                            else:
                                apply_group(arrays, outputs, self.local._local(gate), b, bits,
                                            self.assets, self.binding)
                    except Exception as exc:
                        error = str(exc)
                    finally:
                        arrays.clear()
                        outputs.clear()
                    self.transport.vote(error)
                for index, h in enumerate(partners):
                    destination = self.ownership.shard_owner(h)
                    with self._buffers(1, destination) as final:
                        if owner == destination:
                            if final:
                                final[0].view[:] = buffers[index].view
                        else:
                            self.transport.transfer(buffers[index].view[:self.store.payload_bytes] if self.rank == owner else None,
                                    final[0].view[:self.store.payload_bytes] if final else None, owner, destination,
                                    metadata={'root': new, 'h': h, 'stream': 'joint-output'})
                        error = None
                        if final:
                            try:
                                self._publish(new, h, StoredBlockView(self.cache.adopt(final[0]), end, end))
                            except Exception as exc:
                                error = str(exc)
                        self.transport.vote(error)
        self.local.phase2.progress(new, end)
        self._commit_stage(new, end)

    def materialize(self, frontier=None, binding=None):
        self._open()
        last = len(self.sequencer.operations)-1 if frontier is None else frontier
        if last < self.frontier or last >= len(self.sequencer.operations):
            raise ValueError('Invalid frontier')
        self.transport.agree({'last': last, 'policy': asdict(self.policy)})
        self._set_binding(binding or self.binding or self.bind(), last)
        self.runtime.update_eta(endpoint_frontier=last,collective=True)
        if self.frontier == -1 and self._gradient_origin is None and any(g.dependencies for g in self.sequencer.operations):
            self._gradient_origin = self.commit_snapshot()['snapshot_id']
        while self.frontier < last:
            start = self.frontier+1
            end = min(last, start+self.policy.lookahead_gates-1)
            original = self.sequencer.operations[start:end+1]
            prior = (self.root, self.frontier, self.layout, self.placement_epoch, len(self.history))
            self._rollback_root = self.root
            self._private_epoch = True
            begin = perf_counter()
            cpu_begin = process_time()
            epoch_number = self._measurement_sequence
            self._measurement_sequence += 1
            epoch_event = f'{self.run_id}:executor-epoch:{epoch_number}'
            epoch_group = epoch_event + ':children'
            epoch_condition = {'operation': 'executor-epoch', 'n': self.num_qubits,
                               'block_bits': self.block_bits, 'backend': self.config.backend,
                               'engine': self.io_engine.identity, 'policy': asdict(self.policy),
                               'layout': self.layout.logical_to_physical,
                               'source_operations': [gate_data(g) for g in original]}
            epoch_success = False
            measurements = self.runtime.measurements
            measurements.begin_group(epoch_group)
            ledger = None
            try:
                error = None
                try:
                    self.cache.ensure(len(original)*256)
                    ledger = EpochLedger(tuple(g.op_id for g in original), budget=self.budget,
                                 binding_id=self.binding.binding_id, layout_id=self.layout.layout_id,
                                 placement_epoch=self.placement_epoch, plan_version=self.runtime.plan_version,
                                 policy_version=self.runtime.policy_epoch)
                    self._active_epoch_ledger=ledger
                except Exception as exc:
                    error = str(exc)
                self.transport.vote(error)
                plan = plan_window(original, self.layout, self.block_bits, self.policy.beam_width)
                order = plan.source_ids if self.policy.swap_policy == 'S2' else tuple(g.op_id for g in original)
                groups = {g[0].op_id: g for g in plan.groups if len(g) > 1} if self.policy.swap_policy == 'S2' else {}
                self.transport.agree({'order': order, 'groups': [tuple(g.op_id for g in group) for group in groups.values()]})
                cursor = 0
                while cursor < len(order):
                    source = order[cursor]
                    self._prefetch_local()
                    input_root = self.root
                    group = groups.get(source)
                    is_fused = bool(group and tuple(g.op_id for g in group) ==
                                    tuple(order[cursor:cursor+len(group)]))
                    ops = tuple(group) if is_fused else (self.sequencer.operations[source],)
                    gate_number = self._measurement_sequence
                    self._measurement_sequence += 1
                    gate_event = f'{self.run_id}:executor-group:{gate_number}'
                    gate_group = gate_event + ':children'
                    gate_begin = perf_counter()
                    gate_cpu = process_time()
                    gate_success = False
                    injection_ticket=None
                    service_begin=gate_begin
                    measurements.begin_group(gate_group)
                    # Lets a private-epoch ETA price the rest of this call as a measured residual.
                    self._executor_in_flight = (tuple(g.op_id for g in ops), gate_begin)
                    try:
                        injection_ticket=self.runtime.before_executor(gate_event,len(ops))
                        service_begin=perf_counter()
                        with self._catalog_batch():
                            if is_fused:
                                self._execute_local_fused(group, prior[1])
                                ids = tuple(g.op_id for g in group)
                                cursor += len(group)
                                executor = 'fused-local'
                            else:
                                gate = self.sequencer.operations[source]
                                if self.policy.swap_policy == 'S1':
                                    from .distributed_plan import choose_s1
                                    decision = choose_s1(self.num_qubits, self.block_bits, self.size, self.layout,
                                                         self.sequencer.operations[source:end+1], self.network_profile,
                                                         self.storage_bandwidth)
                                    self.transport.agree(asdict(decision))
                                    if decision.bits:
                                        self.physical_index_bit_swap(*decision.bits, _pending=True)
                                physical = self._physical(gate)
                                self._execute(physical, prior[1])
                                self.history.append({'kind': 'gate', 'source': source,
                                                     'physical': gate_data(physical), 'layout': self.layout.logical_to_physical})
                                ids = (source,)
                                cursor += 1
                                executor = 'single-gate'
                        ledger.stage(ids, input_root, self.root)
                        gate_success = True
                    finally:
                        injection_class=None
                        if injection_ticket is not None:
                            descriptors=[]
                            for g in ops:
                                data=gate_data(self._physical(g));data.pop('op_id',None);data.pop('source_op_id',None)
                                descriptors.append(data)
                            injection_class=content_hash({'operations':descriptors,'layout':self.layout.logical_to_physical,
                                'n':self.num_qubits,'b':self.block_bits,'P':self.size,'binding':self.binding.binding_id})
                        self.runtime.after_executor(injection_ticket,service_seconds=perf_counter()-service_begin,
                            source_ids=tuple(g.op_id for g in ops),success=gate_success,work_class=injection_class)
                        self._executor_in_flight = None
                        measurements.end_group(gate_group)
                        condition = {'operation': 'executor-group' if len(ops) > 1 else 'executor-gate',
                                     'executor': executor if gate_success else ('fused-local' if is_fused else 'single-gate'),
                                     'operations': [gate_data(self._physical(g)) for g in ops],
                                     'n': self.num_qubits, 'block_bits': self.block_bits,
                                     'backend': self.config.backend, 'engine': self.io_engine.identity,
                                     'swap_policy': self.policy.swap_policy,
                                     'layout': self.layout.logical_to_physical}
                        self._record_executor_group(gate_event, gate_group, condition, len(ops),
                                                    perf_counter()-gate_begin, process_time()-gate_cpu,
                                                    success=gate_success, parent_id=epoch_group)
                ledger.validate_coverage()
                publish_begin, publish_cpu = perf_counter(), process_time()
                self._executor_in_flight = (('publish',), publish_begin)
                with self._catalog_batch():
                    self.drain()
                    published = self._new_root(self.root)
                    self.local.phase2.progress(published, end)
                    self._commit_stage(published, end)
                ledger.commit()
                self._executor_in_flight = None
                # Priced per epoch by the remaining-work ETA (child of the epoch group).
                self._record_stage('epoch-publication', _stage_condition(publication_condition(self)),
                                   seconds=perf_counter()-publish_begin, cpu_seconds=process_time()-publish_cpu)
                epoch_success = True
                self.trace.record('async-epoch', performance_class='scheduler',
                                  metadata={'path': 'ASYNC_EPOCH', 'source_range': [start, end],
                                            'actual_order': order, 'elapsed_seconds': perf_counter()-begin,
                                            'private_cut': False, 'plan_version': self.runtime.plan_version})
            except BaseException:
                self._executor_in_flight = None
                if ledger is not None and not ledger.committed:
                    ledger.fail('epoch-failed')
                self.root, self.frontier, self.layout, self.placement_epoch, length = prior
                self.local.root, self.local.frontier, self.local.layout = self.root, self.frontier, self.layout
                del self.history[length:]
                raise
            finally:
                self._active_epoch_ledger=None
                try:
                    if ledger is not None:
                        ledger.close()
                except BaseException:
                    epoch_success = False
                    raise
                finally:
                    try:
                        measurements.end_group(epoch_group)
                        self._record_executor_group(epoch_event, epoch_group, epoch_condition,
                                                    len(original), perf_counter()-begin,
                                                    process_time()-cpu_begin,
                                                    success=epoch_success)
                    finally:
                        self._private_epoch = False
                        self._rollback_root = self.root
            boundary_begin, boundary_cpu = perf_counter(), process_time()
            self.runtime.frontier()
            self._record_stage('epoch-boundary', _stage_condition(boundary_condition(self)),
                               seconds=perf_counter()-boundary_begin, cpu_seconds=process_time()-boundary_cpu)
        return self

    def drain(self):
        error = None
        try:
            self.transport.drain()
            if self.io_engine is not None:
                self.io_engine.drain()
            if self._async_errors:
                raise self._async_errors[0]
        except Exception as exc:
            error = str(exc)
        self.transport.vote(error)

    def commit_snapshot(self, **kwargs):
        if self._private_epoch:
            raise ValueError('Cannot checkpoint an unpublished epoch')
        self.drain()
        started=perf_counter()
        snapshot=super().commit_snapshot(**kwargs)
        # Measured durable-cut cost prices FRONTIER_REPLAN candidates later in the run.
        self.runtime.record_checkpoint_wall(self.transport.max_value(perf_counter()-started))
        self.runtime.invalidate_eta('CHECKPOINT_COMMITTED')
        return snapshot

    def physical_index_bit_swap(self,a,b,*,_pending=False):
        from .executor_profile import swap_condition
        if not _pending:self.materialize(binding=self.binding)  # outside the measured swap
        begin,cpu=perf_counter(),process_time()
        result=super().physical_index_bit_swap(a,b,_pending=_pending)
        # Measured migration cost; a pre-calibrated profile prices layout candidates from it.
        if a!=b:self._record_stage('physical-bit-swap',_stage_condition(swap_condition(self,a,b)),
                                   seconds=perf_counter()-begin,cpu_seconds=process_time()-cpu)
        if not _pending:self.runtime.invalidate_eta('PHYSICAL_LAYOUT_CHANGED')
        return result

    def fused_index_bit_swap(self, pairs):
        self.materialize(binding=self.binding)
        self.drain()
        result=execute_fused_swap(self, pairs)
        self.runtime.invalidate_eta('FUSED_LAYOUT_CHANGED')
        return result

    def collect_garbage(self):
        self.drain()
        return super().collect_garbage()

    def gradient(self, observable, *, parameters=None, binding=None, method='auto', costs=None):
        from .gradient_scheduler import GradientScheduler
        from .phase4_gradient import execute_gradient
        from .gradient_cost_adapter import GradientCostAdapter
        from .gradient_runtime_context import gradient_context
        from .observables import ObservableIR
        if not isinstance(observable,ObservableIR):observable=ObservableIR(tuple(observable))
        self.materialize(binding=binding)
        requested = list(parameters) if parameters is not None else sorted(
            set().union(*(g.dependencies for g in self.sequencer.operations)))
        error = None
        try:
            context=gradient_context(self,observable,requested)
            adapter=GradientCostAdapter()
            cost_origin='manual-reference' if costs is not None else 'unavailable'
            if method=='auto' and costs is None and self.runtime.gradient_calibration is not None:
                resolved=self.runtime.costs.resolve('gradient-method',context,purpose='production')
                costs=resolved.value or {}
                cost_origin=self.runtime.gradient_cost_reason or resolved.reason
            schedule = GradientScheduler().choose(self.sequencer.operations, requested, self.binding,
                        method, costs, available_ram_bytes=self.budget.limit,
                        available_disk_bytes=self.config.disk_quota_bytes)
        except Exception as exc:
            error = str(exc)
        self.transport.vote(error)
        self.transport.agree(asdict(schedule))
        self.trace.record('gradient-schedule', metadata={'path': 'GRADIENT_SCHEDULE', **asdict(schedule),
            'cost_origin':cost_origin,'context_hash':content_hash(context)})
        self.runtime.invalidate_eta('GRADIENT_JOB_STARTED')
        started=perf_counter();success=False;result=None
        try:
            result=execute_gradient(self, observable, requested, schedule)
            success=True
            return result
        finally:
            self.runtime.latest_gradient={'method':schedule.method,'cost_origin':cost_origin,
                'context_hash':content_hash(context),'wall_seconds':perf_counter()-started,
                'success':success,'estimated_seconds':schedule.estimated_seconds,
                'resource_baseline':'execution-admission-only',
                'evidence':'injected' if getattr(self,'_measurement_injected',False) else 'measured'}
            self.runtime.invalidate_eta('GRADIENT_JOB_FINISHED')

    def close(self):
        if self.closed:
            return
        error = None
        try:
            self.drain()
        except BaseException as exc:
            error = exc
        try:
            self._async_errors.clear()
            if isinstance(self.cache, AsyncBlockCache):
                self.cache.errors.clear()
            super().close()
        finally:
            try:
                if self.io_engine is not None:self.io_engine.close()
            finally:self.runtime.close_measurements()
        if error:
            raise error
