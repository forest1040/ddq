"""Phase 0 RAM reference runtime: ordered journal, local frames and joint DAG.

Frames are immutable sequences of Clifford gates, rather than production
stabilizer tableaux. Materialization conservatively closes a global frontier.
"""
from dataclasses import dataclass, replace
from hashlib import sha256
from time import perf_counter
import numpy as np
from .ir import GateNormalizer, GlobalSequencer, NativeGateSelector, FallbackDecomposer
from .kernels import apply_gate
from .layout import QubitLayout, index_bit_swap
from .parameters import ParameterBinding
from .lifecycle import HardwareProfiler, StaticCircuitOptimizer, ExecutionTrace


@dataclass(frozen=True)
class BlockVersion:
    block_id: int
    frontier: int
    binding_id: str
    layout_id: int
    parameter_dependencies: frozenset[str] = frozenset()


@dataclass(frozen=True)
class BlockView:
    block_id: int
    base_version: BlockVersion
    logical_frontier: int
    frame_id: int
    relative_phase: complex


@dataclass(frozen=True)
class LogRecord:
    op_id: int
    kind: str
    upper_control_mask: int = 0
    upper_control_value: int = 0
    layout_id: int = 0

    def includes(self, block_id): return block_id & self.upper_control_mask == self.upper_control_value


class PerBlockLog:
    """One physical record per operation, with predicate-based logical views."""
    def __init__(self): self.records = []
    def append(self, record): self.records.append(record)
    def view(self, block_id, frontier=None):
        return tuple(r for r in self.records if r.includes(block_id) and (frontier is None or r.op_id <= frontier))


@dataclass(frozen=True)
class JointTask:
    op_id: int
    input_frontier: int
    output_frontier: int
    predecessors: tuple[int, ...]
    parameter_dependencies: frozenset[str]


class JointTaskDAG:
    def __init__(self): self.tasks = {}
    def append(self, gate):
        # A conservative chain is a valid dependency DAG for the initial runtime.
        self.tasks[gate.op_id] = JointTask(gate.op_id, gate.op_id-1, gate.op_id,
                                         (gate.op_id-1,) if gate.op_id else (), gate.dependencies)


class FrameGroupRegistry:
    """Hash-consed immutable local Clifford sequences provide sharing and COW."""
    def __init__(self):
        self.frames = {0: ()}
        self._ids = {(): 0}
    def apply_rule(self, frame_id, gate):
        if not gate.clifford: raise ValueError('Only Clifford gates belong in frames')
        sequence = self.frames[frame_id] + (gate,)
        if sequence not in self._ids:
            new_id = len(self.frames)
            self._ids[sequence] = new_id
            self.frames[new_id] = sequence
        return self._ids[sequence]
    def materialize(self, frame_id, amplitudes, assets, binding):
        for gate in self.frames[frame_id]: amplitudes = apply_gate(amplitudes, gate, assets, binding)
        return amplitudes


class DeferredSimulator:
    def __init__(self, num_qubits, block_bits, *, initial_state=None, profile=None, execution='auto'):
        if not 1 <= block_bits <= num_qubits: raise ValueError('Invalid block width')
        if execution not in {'auto', 'fallback'}: raise ValueError('Unknown execution mode')
        self.num_qubits, self.block_bits = num_qubits, block_bits
        self.execution = execution
        self.normalizer = GateNormalizer(num_qubits)
        self.assets = self.normalizer.assets
        self.sequencer = GlobalSequencer()
        self.block_log, self.dag = PerBlockLog(), JointTaskDAG()
        self.frames = FrameGroupRegistry()
        self.layout = QubitLayout.identity(num_qubits)
        self.selector, self.decomposer = NativeGateSelector(), FallbackDecomposer()
        initial = np.zeros(1 << num_qubits, complex) if initial_state is None else np.array(initial_state, complex, copy=True)
        if initial.shape != (1 << num_qubits,) or not np.all(np.isfinite(initial)): raise ValueError('Invalid initial state')
        if initial_state is None: initial[0] = 1
        self._initial = initial.copy()
        size = 1 << block_bits
        self.blocks = {h: initial[h*size:(h+1)*size].copy() for h in range(1 << (num_qubits-block_bits))}
        self.frame_ids = {h: 0 for h in self.blocks}
        self.phases = {h: 1+0j for h in self.blocks}
        self.frontier = -1
        self.binding = None
        self.dependencies = frozenset()
        self.base_versions = {h: BlockVersion(h, -1, '', 0) for h in self.blocks}
        self.profile = profile or HardwareProfiler().discover()
        self.plan = None
        self.trace = None

    def append_gate(self, gate):
        op = self.sequencer.append(gate)
        upper = [(self.layout.logical_to_physical[q] - self.block_bits, v)
                 for q, v in zip(op.controls, op.control_values)
                 if self.layout.logical_to_physical[q] >= self.block_bits]
        joint = any(self.layout.logical_to_physical[q] >= self.block_bits for q in op.targets) and not op.diagonal
        kind = 'JoinRef' if joint else 'SharedOpRef'
        self.block_log.append(LogRecord(op.op_id, kind, sum(1 << q for q, _ in upper), sum(v << q for q, v in upper), self.layout.layout_id))
        if joint: self.dag.append(op)
        self.plan = None
        if self.binding is not None:
            # Append extends this circuit while preserving its executed prefix.
            self.binding = ParameterBinding(self.circuit_hash, self.binding.values)
        return op

    def gate(self, kind, targets, **kwargs): return self.append_gate(self.normalizer.gate(kind, targets, **kwargs))

    @property
    def circuit_hash(self):
        return sha256(repr((self.num_qubits, tuple(self.sequencer.operations))).encode()).hexdigest()

    def bind(self, values=()):
        return ParameterBinding(self.circuit_hash, tuple(values.items()) if hasattr(values, 'items') else tuple(values))

    def _register_plan(self):
        previous_trace = self.trace
        self.plan = StaticCircuitOptimizer().create_plan(self.circuit_hash, self.profile,
            plan_id='plan-'+self.circuit_hash[:16], layout_id=str(self.layout.layout_id),
            frontier=len(self.sequencer.operations)-1,
            task_dependencies=((str(g.op_id), (str(g.op_id-1),) if g.op_id else ()) for g in self.sequencer.operations),
            alternatives=('native', 'fallback'), replanning_frontiers=range(len(self.sequencer.operations)))
        self.trace = ExecutionTrace('trace-'+self.plan.plan_id, 1, self.profile.profile_id, self.plan.plan_id)
        if previous_trace is not None:
            self.trace.events.extend(previous_trace.events)

    def _flush(self, h):
        self.blocks[h] = self.phases[h] * self.frames.materialize(self.frame_ids[h], self.blocks[h], self.assets, self.binding)
        self.frame_ids[h], self.phases[h] = 0, 1+0j
        self.base_versions[h] = BlockVersion(h, self.frontier, self.binding.binding_id,
                                            self.layout.layout_id, self.dependencies)

    def _physical(self, gate):
        return replace(gate, targets=tuple(self.layout.logical_to_physical[q] for q in gate.targets),
                       controls=tuple(self.layout.logical_to_physical[q] for q in gate.controls))

    def _apply(self, gate):
        gate = self._physical(gate)
        b = self.block_bits
        upper_controls = [(q-b, v) for q, v in zip(gate.controls, gate.control_values) if q >= b]
        local_controls = [(q, v) for q, v in zip(gate.controls, gate.control_values) if q < b]
        eligible = [h for h in self.blocks if all(((h >> q) & 1) == v for q, v in upper_controls)]
        local_gate = replace(gate, controls=tuple(q for q, _ in local_controls), control_values=tuple(v for _, v in local_controls))
        if all(q < b for q in gate.targets):
            local_clifford = gate.clifford or gate.kind == 'MCX' and len(local_controls) <= 1
            if local_clifford:
                local_gate = replace(local_gate, clifford=True)
                for h in eligible: self.frame_ids[h] = self.frames.apply_rule(self.frame_ids[h], local_gate)
                return 'FRAME_UPDATE'
            for h in eligible:
                self._flush(h)
                self.blocks[h] = apply_gate(self.blocks[h], local_gate, self.assets, self.binding)
            return 'NATIVE_MCX_CONDITIONAL_SWAP' if gate.kind == 'MCX' else 'NATIVE_LOCAL'
        # A diagonal upper 1Q gate is a relative phase, preserving each block's norm.
        if gate.kind != 'PAULI_ROT' and gate.diagonal and len(gate.targets) == 1 and not local_controls:
            from .kernels import gate_matrix
            u = gate_matrix(local_gate, self.assets, self.binding)
            bit = gate.targets[0]-b
            for h in eligible: self.phases[h] *= u[(h >> bit)&1, (h >> bit)&1]
            return 'PHASE_UPDATE'
        # Gather only partner groups induced by upper target bits. Flush different
        # local frames/phases before mixing, retaining unnormalized amplitudes.
        upper_targets = tuple(sorted({q-b for q in gate.targets if q >= b}))
        umask = sum(1 << q for q in upper_targets)
        selected = set(eligible)
        for base in eligible:
            if base & umask: continue
            partners = [base | sum(((i >> j)&1) << q for j, q in enumerate(upper_targets))
                        for i in range(1 << len(upper_targets))]
            if not all(h in selected for h in partners): raise RuntimeError('Incomplete joint partner group')
            for h in partners: self._flush(h)
            packed = np.concatenate([self.blocks[h] for h in partners])
            remap = lambda q: q if q < b else b + upper_targets.index(q-b)
            compact = replace(local_gate, targets=tuple(remap(q) for q in gate.targets))
            out = apply_gate(packed, compact, self.assets, self.binding)
            size = 1 << b
            for i, h in enumerate(partners): self.blocks[h] = out[i*size:(i+1)*size].copy()
        return 'NATIVE_MCX_CONDITIONAL_SWAP' if gate.kind == 'MCX' else 'NATIVE_JOINT'

    def materialize(self, blocks=None, frontier=None, binding=None):
        """Close an ordered global frontier, conservatively including all blocks."""
        if blocks is not None and any(h not in self.blocks for h in blocks): raise ValueError('Unknown block')
        last = len(self.sequencer.operations)-1 if frontier is None else frontier
        if not -1 <= last < len(self.sequencer.operations): raise ValueError('Frontier out of range')
        if last < self.frontier: raise ValueError('Cannot rewind materialized state')
        binding = binding or self.binding or self.bind()
        if binding.circuit_hash != self.circuit_hash: raise ValueError('Binding belongs to a different circuit')
        if self.binding and self.binding.binding_id != binding.binding_id: raise ValueError('Use a new simulator for another binding')
        # Validate all required parameters before mutating state.
        for gate in self.sequencer.operations[:last+1]:
            if gate.angle is not None: gate.angle.evaluate(binding)
        if self.execution == 'fallback':
            for gate in self.sequencer.operations[self.frontier+1:last+1]: self.decomposer.decompose(gate)
        self.binding = binding
        if self.plan is None: self._register_plan()
        i = self.frontier + 1
        while i <= last:
            gate = self.sequencer.operations[i]
            end, fused = i, gate
            if self.execution == 'auto' and gate.kind == 'RZ':
                while end < last:
                    nxt = self.sequencer.operations[end+1]
                    if (nxt.kind, nxt.targets, nxt.controls, nxt.control_values) != (gate.kind, gate.targets, gate.controls, gate.control_values): break
                    fused = replace(fused, angle=fused.angle + nxt.angle,
                                    approximation_error=fused.approximation_error+nxt.approximation_error)
                    end += 1
            start = perf_counter()
            operations = self.decomposer.decompose(fused) if self.execution == 'fallback' else (fused,)
            paths = [self._apply(op) for op in operations]
            self.dependencies |= frozenset().union(*(g.dependencies for g in self.sequencer.operations[i:end+1]))
            self.frontier = end
            self.trace.record(str(i), performance_class='compute', transfer_ms=(perf_counter()-start)*1000,
                              metadata={'path': 'FALLBACK' if self.execution == 'fallback' else 'FUSED_RZ' if end > i else paths[0],
                                        'source_ops': tuple(range(i, end+1)), 'binding_id': binding.binding_id,
                                        'layout_id': self.layout.layout_id, 'approximation_error': fused.approximation_error,
                                        'plan_id': self.plan.plan_id,
                                        'native_candidate': self.selector.select(fused, self.layout, self.block_bits)})
            i = end + 1
        for h in self.blocks: self._flush(h)
        return self

    def state(self, binding=None):
        self.materialize(binding=binding)
        return self.layout.logical_state(np.concatenate([self.blocks[h] for h in self.blocks]))

    def block_view(self, h):
        return BlockView(h, self.base_versions[h], self.frontier, self.frame_ids[h], self.phases[h])

    def physical_index_bit_swap(self, a, b):
        layout = self.layout.swapped_physical(a, b)
        self.materialize(binding=self.binding)
        swapped = index_bit_swap(np.concatenate([self.blocks[h] for h in self.blocks]), a, b)
        size = 1 << self.block_bits
        for h in self.blocks: self.blocks[h] = swapped[h*size:(h+1)*size].copy()
        self.layout = layout
        for h in self.blocks: self._flush(h)
        self.trace.record('layout-swap', metadata={'path': 'PHYSICAL_INDEX_BIT_SWAP', 'bits': (a,b), 'layout_id': layout.layout_id})
        self.plan = None
        return self
