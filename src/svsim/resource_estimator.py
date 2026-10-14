"""Allocation-free resource and measured-component estimates, model v1.

The eager reference bounds every gate by a complete block pass. It deliberately
does not infer fusion, overlap, metadata GC or end-to-end timing from kernels.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import math

from .cache import BufferLease
from .calibration_conditions import kernel_condition
from .journal import content_hash
from .ownership import OwnershipMap
from .profile_document import ProfileDocument, wire_encode
from .workload_spec import WorkloadSpec, integer


@dataclass(frozen=True)
class EstimateOptions:
    block_bits: int = 12
    ranks: int = 1
    nodes: int = 1
    threads: int = 1
    ram_budget_bytes_per_rank: int | None = None
    ram_budget_bytes_per_host: int | None = None
    disk_budget_bytes_per_host: int | None = None
    alignment: int = 4096
    segment_size_bytes: int = 1 << 30
    metadata_reserve_bytes: int = 128 * 1024
    library_reserve_bytes_per_rank: int = 64 * 1024**2
    os_reserve_bytes_per_host: int = 512 * 1024**2
    disk_extra_bytes_per_host: int = 16 * 1024**2
    drain_reserve_bytes_per_rank: int = 0
    backend: str = 'buffered'

    def __post_init__(self):
        if self.backend not in {'buffered','direct'}: raise ValueError('backend must be buffered or direct')
        for key, value in asdict(self).items():
            if key == 'backend': continue
            if value is not None: integer(value, key, 0)
            if value is not None and type(value) is not int: raise ValueError(f'{key} must be a Python integer')
        integer(self.segment_size_bytes,'segment_size_bytes',1)
        for key in ('block_bits','ranks','nodes','threads','alignment'):
            integer(getattr(self,key), key, 1, 1024 if key != 'alignment' else 1024**2)
        if self.ranks % self.nodes: raise ValueError('ranks must be evenly divisible by nodes')
        if self.alignment & (self.alignment - 1): raise ValueError('alignment must be a power of two')
        for key in ('ram_budget_bytes_per_rank','ram_budget_bytes_per_host','disk_budget_bytes_per_host'):
            if getattr(self,key) is not None: integer(getattr(self,key), key, 1)


def _minimum_known(*values):
    known = [v for v in values if type(v) is int and v >= 0]
    return min(known) if known else None


def _kernel(gate, workload, b):
    if gate.controls or any(q >= b for q in gate.targets): return None
    operation = {'H':'kernel-h','RX':'kernel-rx','PAULI_ROT':'kernel-pauli-rotation'}.get(gate.kind)
    if not operation: return None
    angle_class = None
    if gate.angle:
        # Foundation cells certify only this exact numeric generic angle. No interpolation.
        if gate.angle.op != 'const' or gate.angle.evaluate(workload.binding) != .37: return None
        angle_class = 'generic-0.37'
    return kernel_condition(operation,b,target=gate.targets,paulis=gate.paulis,angle_class=angle_class)


def _seconds(record, count, field):
    try: result = record['summary'][field] * count
    except OverflowError: return None
    return result if math.isfinite(result) else None


def estimate(workload: WorkloadSpec, profile: ProfileDocument, options: EstimateOptions, time_model=None,
             sync_seconds=None) -> dict:
    """Return a bounded report without constructing simulators, blocks or scratch.

    A fitted end-to-end time model supplies total time only for its exact
    configuration and condition class; it is never mixed with kernel cells.
    """
    ownership = OwnershipMap(workload.qubits, options.block_bits, options.ranks)
    b, block_count, local = options.block_bits, ownership.num_blocks, ownership.blocks_per_rank
    ranks_host = options.ranks // options.nodes
    payload = 16 * (1 << b)
    slot = (payload + options.alignment - 1) // options.alignment * options.alignment
    lease = BufferLease.allocation_cost(slot, options.alignment)
    state = 16 * (1 << workload.qubits)
    semantic_count = sum(entry.count for entry in workload.gates)
    lowered, unsupported = workload.lowered()
    lowering_incomplete = bool(unsupported)
    gate_count = sum(entry.count for entry in lowered)
    group = 1
    remote_count = 0
    for entry in lowered:
        gate = entry.gate
        upper = sum(q >= b for q in gate.targets)
        if gate.kind == 'PAULI_ROT':
            upper = int(any(q >= b and p in {'X','Y'} for q,p in zip(gate.targets,gate.paulis)))
        if gate.diagonal: upper = 0
        if upper:
            group = max(group, 1 << upper)
            if any(q >= workload.qubits - ownership.rank_bits for q in gate.targets): remote_count += entry.count
    if workload.algorithm['kind'] != 'circuit': unsupported.append('ALGORITHM_ENDPOINT_NOT_IMPLEMENTED:' + workload.algorithm['kind'])
    if workload.policies['gradient'] != 'none': unsupported.append('STATIC_GRADIENT_ADAPTER_NOT_IMPLEMENTED')
    if workload.policies['frame'] != 'materialize': unsupported.append('STATIC_FRAME_KEEP_ADAPTER_NOT_IMPLEMENTED')
    if workload.policies['swap'] != 'S0': unsupported.append('STATIC_LAYOUT_POLICY_ADAPTER_NOT_IMPLEMENTED')
    if semantic_count > 4096: unsupported.append('COMPRESSED_REPEAT_EXECUTOR_NOT_IMPLEMENTED')
    # The actual executor admits full group input/output plus staging buffers.
    workspace_buffers = 2 * group + 3
    workspace = workspace_buffers * lease
    io_mpi_pins = 2 * lease if options.ranks > 1 else lease
    # Initial rollback, current and new resident roots; no checkpoint/prefix retention in this scope.
    resident = 3 * local * lease if workload.policies['storage'] == 'ram' else 0
    explicit_metadata = workload.explicit_node_count * 4096
    # Current runtime stores occurrence IR; dry-run itself keeps only templates.
    runtime_metadata = options.metadata_reserve_bytes + explicit_metadata + semantic_count * 4096
    managed = resident + workspace + io_mpi_pins + runtime_metadata
    rss_reference = managed + options.library_reserve_bytes_per_rank
    host_ram = ranks_host * rss_reference + options.os_reserve_bytes_per_host
    # Full eager write upper bound, including initial populated state. Zero-state sparse
    # startup and virtual operations may use less; no savings are silently assumed.
    extents_per_rank = local * (1 + gate_count)
    amplitude_disk = extents_per_rank * slot if workload.policies['storage'] == 'file' else 0
    segment_slots = options.segment_size_bytes // slot
    if segment_slots == 0: unsupported.append('BLOCK_SLOT_EXCEEDS_SEGMENT_SIZE')
    tail_reserve = ((extents_per_rank + segment_slots - 1) // segment_slots *
                    (options.segment_size_bytes % slot)) if segment_slots and amplitude_disk else 0
    catalog_reference = extents_per_rank * 512 + gate_count * 4096 + block_count * 256
    disk_per_rank = amplitude_disk + tail_reserve + catalog_reference + options.drain_reserve_bytes_per_rank
    disk_host = ranks_host * disk_per_rank + options.disk_extra_bytes_per_host
    p = profile.data
    allocation = p['allocation']
    available = p['hardware'].get('ram_available_bytes')
    rank_budget = _minimum_known(options.ram_budget_bytes_per_rank, allocation.get('ram_budget_bytes_per_rank'))
    host_budget = _minimum_known(options.ram_budget_bytes_per_host, allocation.get('ram_budget_bytes_per_host'), available)
    if host_budget is not None:
        host_budget = _minimum_known(host_budget,p['hardware'].get('ram_total_bytes'))
    disk_budget = _minimum_known(options.disk_budget_bytes_per_host, allocation.get('disk_budget_bytes_per_host'))
    # File cache retains clean old versions until eviction; workspace alone is
    # not a peak bound. RAM-store reclamation prunes obsolete resident/cache views.
    cache_version_count = extents_per_rank if workload.policies['storage'] == 'file' else min(extents_per_rank,3*local)
    cache_upper = cache_version_count * lease
    cache_reference = cache_upper if rank_budget is None else min(cache_upper,max(0,rank_budget-managed))
    managed += cache_reference
    rss_reference = managed + options.library_reserve_bytes_per_rank
    host_ram = ranks_host * rss_reference + options.os_reserve_bytes_per_host
    reasons, unknown = [], []
    if lowering_incomplete: unknown.append('unsupported_lowering_resource_requirements')
    if workload.policies['gradient'] != 'none': unknown.append('gradient_workspace_and_retention_requirements')
    if workload.algorithm['kind'] != 'circuit': unknown.append('algorithm_endpoint_resources_not_in_body_summary')
    if rank_budget is None: unknown.append('ram_budget_bytes_per_rank')
    elif managed > rank_budget: reasons.append('RANK_RAM_BUDGET_EXCEEDED')
    if host_budget is None: unknown.append('ram_budget_bytes_per_host_or_observed_available_ram')
    elif host_ram > host_budget: reasons.append('HOST_RAM_BUDGET_EXCEEDED')
    if disk_budget is None: unknown.append('disk_budget_bytes_per_host_for_catalog_and_payloads')
    elif disk_host > disk_budget: reasons.append('HOST_DISK_BUDGET_EXCEEDED')
    # Multiple devices are not interchangeable: the foundation requires an explicit disk budget.
    if group > local and options.ranks == 1: unsupported.append('JOINT_GROUP_EXCEEDS_BLOCK_COUNT')
    if rank_budget is not None and workspace + runtime_metadata > rank_budget:
        unsupported.append('FULL_BLOCK_JOINT_ADMISSION_FAILED_NO_TILE_ROUTE')
    capacity = False if reasons else None if unknown else True
    calibrated_calls, component_rows, missing = 0, [], []
    profile_context = (allocation.get('ranks') == options.ranks and allocation.get('threads') == options.threads
                       and allocation.get('nodes') == options.nodes and options.ranks == 1)
    total_calls = gate_count * local
    seconds_known = 0.0
    overflow = False
    for entry in lowered:
        calls = entry.count * local
        condition = _kernel(entry.gate, workload, b)
        row = profile.match(condition) if condition is not None and profile_context else None
        if row is None:
            missing.append({'operation':entry.gate.kind,'calls':calls,'reason':'NO_EXACT_CALIBRATION' if profile_context else 'ALLOCATION_MISMATCH_OR_MULTI_RANK'})
            continue
        calibrated_calls += calls
        mean = _seconds(row,calls,'median_seconds')
        if mean is None: overflow = True
        else: seconds_known += mean
        component_rows.append({'condition':condition,'condition_key':row['condition_key'],'calls':calls,
                               'seconds':mean,'sample_min_seconds':_seconds(row,calls,'min_seconds'),
                               'sample_max_seconds':_seconds(row,calls,'max_seconds'),
                               'evidence':'measured','scope':row['scope']})
    if not math.isfinite(seconds_known): overflow = True
    missing_components = ['compile-and-bind','metadata-and-catalog','initialization','materialization-copy-and-hash','commit-and-cleanup']
    if workload.policies['storage'] == 'file': missing_components.extend(['storage-read-write','storage-flush'])
    if options.ranks > 1: missing_components.extend(['mpi-payload','mpi-control-and-reduction','rank-imbalance'])
    if workload.algorithm['kind'] != 'circuit': missing_components.append('algorithm-endpoint-and-error-model')
    if unsupported: missing_components.append('unsupported-work-not-costed')
    passes = gate_count * block_count
    time_report = {'lower_seconds':None,'expected_seconds':None,'conditional_upper_seconds':None,'phases':None,
                   'evidence':'derived','scope':'planning-only','model_id':None,
                   'reason':'end-to-end components are uncalibrated; standalone kernel sum is not total runtime'}
    coverage_status = 'partial' if calibrated_calls else 'unknown'
    if time_model is not None:
        from .time_model import workload_features
        features, _ = workload_features(workload, b, options.ranks)
        target = {'block_bits':b,'ranks':options.ranks,'nodes':options.nodes,'threads':options.threads,'storage':workload.policies['storage'],
                  'backend':options.backend,'mode':'distributed-sync','executor_policy':None,'frame':workload.policies['frame'],
                  'swap':workload.policies['swap'],'execution':workload.policies['execution'],
                  'initial_state':workload._data.get('initial_state','zero')}
        predicted = time_model.predict(features, target, profile.configuration_fingerprint, sync_seconds)
        if workload.algorithm['kind'] != 'circuit':
            predicted = {**predicted,'lower_seconds':None,'expected_seconds':None,'conditional_upper_seconds':None,
                         'phases':None,'evidence':'unknown','reason':'algorithm endpoint is outside the fitted circuit model'}
        time_report = {**predicted,'scope':'same-host' if predicted['evidence'] == 'derived' else 'planning-only'}
        if predicted['expected_seconds'] is not None:
            missing_components = [] if predicted['evidence'] == 'derived' and not unsupported else missing_components
            coverage_status = 'complete' if not missing_components else 'partial'
            if unsupported: time_report['reason'] += '; executor support is missing, so this is a planning reference only'
    report = {
        'schema_version':1,'kind':'execution-plan','model_version':'static-eager-reference-v1',
        'workload_id':workload.workload_id,'profile_id':profile.profile_id,
        'configuration_fingerprint':profile.configuration_fingerprint,'options':asdict(options),
        'capacity_feasible':capacity,'execution_supported':not unsupported,
        'calibration_coverage':{'status':coverage_status,
                                'kernel_call_fraction':None if lowering_incomplete else calibrated_calls / total_calls if total_calls else 1.0,
                                'kernel_payload_fraction':None if lowering_incomplete else calibrated_calls / total_calls if total_calls else 1.0,
                                'coverage_reason':'unsupported lowering has unknown invocation count' if lowering_incomplete else 'same block payload per invocation; excludes all uncalibrated lifecycle costs',
                                'calibrated_calls':calibrated_calls,'total_calls':total_calls,
                                'missing_conditions':missing,'missing_components':missing_components},
        'capacity_reasons':reasons,'capacity_unknown':unknown,'unsupported_capabilities':sorted(set(unsupported)),
        'resources':{
            'state_payload_bytes':state,'block_payload_bytes':payload,'block_slot_bytes':slot,'lease_allocation_bytes':lease,
            'block_count':block_count,'blocks_per_rank_count':local,'ranks_per_host':ranks_host,
            'joint_group_count':group,'workspace_buffer_count':workspace_buffers,
            'resident_rollback_current_output_bytes_per_rank':resident,'workspace_bytes_per_rank':workspace,
            'io_mpi_pin_bytes_per_rank':io_mpi_pins,'managed_peak_bytes_per_rank':managed,
            'cache_retention_reference_bytes_per_rank':cache_reference,
            'rss_reference_bytes_per_rank':rss_reference,'host_ram_required_bytes':host_ram,
            'ram_limit_bytes_per_rank':rank_budget,'ram_limit_bytes_per_host':host_budget,
            'append_only_amplitude_peak_bytes_per_rank':amplitude_disk,'catalog_reference_bytes_per_rank':catalog_reference,
            'segment_tail_reserve_bytes_per_rank':tail_reserve,
            'disk_required_bytes_per_host':disk_host,'disk_limit_bytes_per_host':disk_budget,
            'strict_capacity_guarantee':False,
            'assumptions':['eager full-state pass per lowered gate; no fusion or virtual savings',
                           'three resident roots for RAM; no durable checkpoints or retained parameter prefixes',
                           'catalog: assumed 512 bytes/extent, 4096 bytes/gate, 256 bytes/block plus explicit disk slack',
                           'catalog/journal growth, segment tails and SQLite reservation require P4-009; not a hard bound',
                           'RAM resident reclaim exists; file extents remain append-only',
                           'clean file cache may retain historical versions up to the managed rank budget',
                           'RSS reference sums per-rank shared pages conservatively; OS/library allowances are assumed'],
            'evidence':'derived'},
        'work':{'semantic_gate_count':semantic_count,'lowered_gate_count':gate_count,
                'native_gate_count':gate_count if workload.policies['execution']=='auto-native' else 0,
                'fallback_gate_count':gate_count if workload.policies['execution']=='forced-decomposition' else 0,
                'fused_gate_count':0,'block_pass_count':passes,'remote_gate_count':remote_count,
                'ram_kernel_payload_bytes':passes * payload,
                'storage_read_bytes':passes * slot if workload.policies['storage']=='file' else 0,
                'storage_write_bytes':(passes+block_count)*slot if workload.policies['storage']=='file' else 0,
                'mpi_send_bytes':None,'mpi_send_reason':'protocol route and peer class adapter not calibrated' if options.ranks>1 else 'no MPI in single rank'},
        'time':{**time_report,
                'reference_kernel_seconds':None if overflow or (not calibrated_calls and total_calls) else seconds_known,
                'components':component_rows,
                'interval_kind':'model fits to per-run sample median/min/max; not a confidence interval' if time_report['model_id']
                                else 'raw sample extrema, not confidence or end-to-end bounds'},
        'algorithm':workload.algorithm,'provenance':p['provenance'],
        'source_templates':workload.source_templates,
        'analysis':{'allocated_amplitudes':False,'enumerated_blocks':False,'expanded_repetitions':False,
                    'explicit_node_count':workload.explicit_node_count,'candidate_count':1,
                    'metadata_limit_bytes':64*1024**2,'output_limit_bytes':4*1024**2}}
    report = wire_encode(report)
    report['plan_id'] = content_hash(report)
    if len(json.dumps(report,ensure_ascii=False,allow_nan=False).encode()) > 4*1024**2:
        raise ValueError('execution plan exceeds output byte limit')
    return report
