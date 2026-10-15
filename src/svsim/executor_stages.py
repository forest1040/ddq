"""Bounded executor stage plans; no catalog traversal or block enumeration.

A plan describes the inside of ONE measured inclusive executor call. Stage
nodes are audit/dependency/work descriptors, never additional priced tasks.
Unknown cache, placement and Frame state stays conditional. The enclosing
measured call must match the route contract before it can cover these nodes.
"""
from dataclasses import dataclass
from typing import NamedTuple
from .planner import group_bits

CONTRACT = 'inclusive-executor-stages-v1'


class ExecutorStage(NamedTuple):
    stage: str
    dependencies: tuple[str, ...]
    resource: str
    count_lower: int
    count_upper: int
    unknown_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExecutorRoute:
    route: str
    stages: tuple[ExecutorStage, ...]
    payload_bytes: int | None
    kernel_work: int | None
    unknown_reasons: tuple[str, ...] = ()
    payload_bytes_lower: int = 0
    payload_bytes_upper: int | None = None

    # Shared contract semantics are properties, not repeated snapshot metadata.
    @property
    def contract(self): return CONTRACT

    @property
    def pricing(self): return 'included-once-in-parent-protocol-seconds'

    @property
    def work_unit(self): return 'rank-local-main-kernel-amplitude-visits; excludes Frame and packing'

    @property
    def payload_scope(self): return 'MPI amplitude send payload; excludes storage and control traffic'


def _matching_blocks(sim, fixed):
    """Number of labels satisfying fixed bits in this rank, using prefix algebra."""
    k = sim.ownership.k
    rank = sim.rank
    for bit, value in fixed.items():
        if bit >= k and ((rank >> (bit-k)) & 1) != value:
            return 0
    return 1 << (k - sum(bit < k for bit in fixed))


def route_kind(sim, physical):
    """Mirror the explicit branches of async/distributed _execute, without I/O."""
    if len(physical) > 1:
        return 'fused-local'
    gate = physical[0]; b = sim.block_bits
    bits = group_bits(gate, b)
    if gate.kind in {'MCX', 'SWAP'} and all(q >= b for q in gate.qubits):
        return 'virtual-permutation'
    if not bits and sim.config.frame_policy == 'keep':
        local = sim.local._local(gate)
        upper_scalar = (gate.kind in {'S','SDG','Z','T','TDG','RZ','CZ'}
                        and all(q >= b for q in gate.targets) and not local.controls)
        if upper_scalar or (gate.kind == 'CZ' and any(q >= b for q in gate.targets)) or sim.local._frame_clifford(local):
            return 'virtual-local-rule'
        if gate.kind in {'RX','RY','RZ','T','TDG','PAULI_ROT'} and not local.controls:
            from .parameters import ParameterBinding, exact_pi_coefficient, expression, ExactPi
            binding = getattr(sim,'binding',None) or ParameterBinding('',())
            expr = gate.angle if gate.angle is not None else expression(ExactPi(1 if gate.kind == 'T' else -1,4))
            coefficient = exact_pi_coefficient(expr,binding)
            if coefficient is not None and (2*coefficient).denominator == 1:
                return 'virtual-local-rule'
            return 'virtual-rotation-rule'
    if not bits:
        return 'operation-shipping'
    if gate.kind == 'MCX':
        from .distributed_plan import choose_mcx
        active = 16 * (1 << (b-sum(q < b for q in gate.controls)))
        return 'mcx-' + choose_mcx(sim.network_profile,sim.config,sim.store.payload_bytes,active)
    return 'joint-async' if gate.kind != 'SWAP' else 'joint-full'


def executor_route(sim, physical):
    """DAG of real route stages, bounded by O(source gates + qubits)."""
    kind = route_kind(sim, physical)
    b = sim.block_bits; local_blocks = sim.ownership.blocks_per_rank
    stages = []
    previous = ()
    def add(name, resource, lo=1, hi=None, unknown=()):
        nonlocal previous
        stages.append(ExecutorStage(name, previous, resource, lo, lo if hi is None else hi, unknown))
        previous = (name,)
    # S1 physical swaps are inside the timed call. The post-swap layout alone
    # cannot establish whether a swap happened, so do not assert exact work.
    s1 = sim.policy.swap_policy == 'S1' and sim.size > 1
    if s1:
        add('layout-relocation','main',0,sim.ownership.num_blocks,('S1_RELOCATION_CONDITIONAL',))
    kernel = 0
    virtual = kind.startswith('virtual-')
    reasons = []
    if virtual:
        if kind == 'virtual-rotation-rule':
            for name,resource in (('read','storage'),('frame-resolve','main'),('write','storage')):
                add(name,resource,0,sim.ownership.num_blocks,('LOG_BOUND_COMPACTION_CONDITIONAL',))
            kernel = None
            reasons.append('LOG_BOUND_COMPACTION_CONDITIONAL')
        add('metadata','main')
        add('commit','main')
        payload = None if s1 or (kind == 'virtual-rotation-rule' and sim.size > 1) else 0
    else:
        # Fetch may use an implicit zero, warm cache, external reader, or remote
        # extent. Physical I/O and Frame work are conditional without views.
        add('read','storage',0,sim.ownership.num_blocks,('CACHE_AND_EXTENT_STATE_UNKNOWN',))
        add('frame-resolve','main',0,sim.ownership.num_blocks,('BLOCK_FRAME_AND_LOG_UNKNOWN',))
        gate = physical[0]
        if sim.size > 1 and kind != 'operation-shipping':
            add('mpi-input','nic',0,sim.ownership.num_blocks,('INPUT_PLACEMENT_AND_ZERO_UNKNOWN',))
            reasons.append('INPUT_PLACEMENT_AND_ZERO_UNKNOWN')
        packed_send_bytes = 0
        if kind == 'mcx-packed':
            active = 1 << (b-sum(q < b for q in gate.controls))
            tile_count = min(active,sim.config.tile_bytes//16)
            fixed = {q-b:v for q,v in zip(gate.controls,gate.control_values) if q >= b}
            remote_blocks = (_matching_blocks(sim,fixed)
                             if gate.targets[0]-b >= sim.ownership.k else 0)
            tiles = remote_blocks * ((active+tile_count-1)//tile_count)
            packed_send_bytes = remote_blocks * active * 16
            add('packing','main',tiles)
            add('mpi-exchange','nic',tiles)
            add('unpacking','main',tiles)
        if kind == 'fused-local':
            calls = 0
            for op in physical:
                fixed = {q-b:v for q,v in zip(op.controls,op.control_values) if q >= b}
                calls += _matching_blocks(sim, fixed)
            kernel = calls * (1 << b)
            add('kernel','main',calls)
        elif kind == 'operation-shipping':
            # Worker follows extent storage rank rather than logical ownership.
            # Only P=1 determines this rank's kernel count without catalog scan.
            fixed = {q-b:v for q,v in zip(gate.controls,gate.control_values) if q >= b}
            calls = _matching_blocks(sim, fixed)
            kernel = calls * (1 << b) if sim.size == 1 else None
            add('kernel','main',calls if sim.size == 1 else 0,
                calls if sim.size == 1 else sim.ownership.num_blocks,
                () if sim.size == 1 else ('COMPUTE_PLACEMENT_UNKNOWN',))
        elif kind == 'mcx-packed':
            target = gate.targets[0]-b
            fixed = {q-b:v for q,v in zip(gate.controls,gate.control_values) if q >= b}
            fixed[target] = 0
            # Same-owner pairs call partial_mcx_pair; remote pairs only pack,
            # exchange and unpack, and have no separate arithmetic kernel.
            calls = _matching_blocks(sim,fixed) if target < sim.ownership.k else 0
            active = 1 << (b-sum(q < b for q in gate.controls))
            kernel = calls * 2 * active
            add('kernel','main',calls)
        else:
            bits = group_bits(gate,b)
            fixed = {q-b:v for q,v in zip(gate.controls,gate.control_values) if q >= b}
            if gate.kind == 'PAULI_ROT':
                # XOR pair canonical base has the highest changed bit clear.
                fixed[max(bits)-b] = 0
                partners = 2
            else:
                fixed.update({q-b:0 for q in bits}); partners = 1 << len(bits)
            calls = _matching_blocks(sim, fixed)
            kernel = calls * partners * (1 << b)
            add('kernel','main',calls)
        if sim.size > 1 and kind not in {'operation-shipping','mcx-packed'}:
            add('mpi-output','nic',0,sim.ownership.num_blocks,('OUTPUT_TRANSFER_COUNT_CONDITIONAL',))
        add('write','storage',0,sim.ownership.num_blocks,('DIRTY_WRITEBACK_DEFERRED_TO_PUBLICATION',))
        add('metadata','main',0,local_blocks)
        add('commit','main')
        payload = 0 if sim.size == 1 or kind == 'operation-shipping' else None
    if s1:
        payload = None
        kernel = None
        reasons.append('S1_RELOCATION_CONDITIONAL')
    lower = packed_send_bytes if not virtual and kind == 'mcx-packed' else 0
    upper = 0 if payload == 0 else 2 * sim.ownership.num_blocks * sim.store.payload_bytes
    # S1 relocation may precede the gate and add another full-state exchange.
    if s1:upper *= 2
    return ExecutorRoute(kind,tuple(stages),payload,kernel,tuple(reasons),payload_bytes_lower=lower,payload_bytes_upper=upper)
