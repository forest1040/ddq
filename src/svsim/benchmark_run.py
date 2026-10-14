"""Budgeted end-to-end workload runs on the actual distributed executor.

Every trial builds a fresh dataset in an explicit scratch directory, times the
serial phases setup/compile/execute/flush/cleanup as the MAX over ranks, and
validates the final state outside the timed region against the eager oracle.
Cases are interleaved round by round in a seeded shuffled order so variants
see the same drift. Nothing is submitted to a scheduler.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import math
import os
from pathlib import Path
import shutil
import statistics
import time
import uuid

import numpy as np

from .journal import content_hash
from .kernels import eager_reference
from .profile_document import ProfileDocument, wire_encode
from .resource_estimator import EstimateOptions, estimate
from .time_model import PHASES, workload_features
from .workload_spec import WorkloadSpec, gate_arguments

MAX_CASES = 256
MAX_EXECUTED_GATES = 4096


class CapacityError(ValueError):
    """The static preflight proves the requested run exceeds its budgets."""


@dataclass(frozen=True)
class RunOptions:
    block_bits: int = 4
    repetitions: int = 5
    warmup: int = 1
    ram_budget_bytes_per_rank: int = 64 * 1024**2
    disk_budget_bytes: int = 64 * 1024**2
    walltime_seconds: float = 60.0
    backend: str = 'buffered'
    seed: int = 0
    threads: int = 1
    oracle_max_qubits: int = 14
    executor: str = 'sync'
    nodes: int = 1

    def __post_init__(self):
        for name in ('block_bits', 'repetitions', 'warmup', 'ram_budget_bytes_per_rank', 'disk_budget_bytes',
                     'seed', 'threads', 'oracle_max_qubits'):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f'{name} must be a nonnegative integer')
        if not 1 <= self.block_bits <= 20 or not 1 <= self.repetitions <= 1000 or self.warmup > 1000:
            raise ValueError('invalid block size or trial counts')
        if self.ram_budget_bytes_per_rank < 1 or self.disk_budget_bytes < 1 or self.threads < 1:
            raise ValueError('budgets and threads must be positive')
        if not math.isfinite(self.walltime_seconds) or self.walltime_seconds <= 0:
            raise ValueError('walltime_seconds must be positive and finite')
        if self.backend not in {'buffered', 'direct'}: raise ValueError('backend must be buffered or direct')
        if self.seed > (1 << 64) - 1: raise ValueError('seed must fit in an unsigned 64-bit integer')
        if self.oracle_max_qubits > 20: raise ValueError('oracle gather is limited to 20 qubits')
        if self.executor not in {'sync', 'async'}: raise ValueError('executor must be sync or async')
        if self.executor == 'async' and self.threads != _async_policy().io_workers:
            raise ValueError('async runs label threads as the policy io_workers, as runtime profiles do')


def _async_policy():
    from .async_scheduler import AsyncPolicy
    return AsyncPolicy()


def executor_policy(options):
    if options.executor == 'sync': return None
    return content_hash(asdict(_async_policy()))


@dataclass(frozen=True)
class RunCase:
    workload: WorkloadSpec
    label: str
    variant: str = 'default'
    plan: dict | None = None


def _timestamp():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def stats(values):
    vals = sorted(values)
    index = (len(vals) - 1) * 0.95
    low, high = math.floor(index), math.ceil(index)
    return {'median_seconds': statistics.median(vals), 'mean_seconds': statistics.mean(vals),
            'min_seconds': vals[0], 'max_seconds': vals[-1],
            'p95_seconds': vals[low] + (vals[high] - vals[low]) * (index - low)}


def _supported(workload):
    reasons = []
    if workload.algorithm['kind'] != 'circuit': reasons.append('ALGORITHM_ENDPOINT_NOT_IMPLEMENTED')
    expected = {'frame': 'materialize', 'swap': 'S0', 'execution': 'auto-native', 'gradient': 'none'}
    reasons += [f'RUN_POLICY_NOT_SUPPORTED:{k}={workload.policies[k]}' for k, v in expected.items() if workload.policies[k] != v]
    if sum(entry.count for entry in workload.gates) > MAX_EXECUTED_GATES: reasons.append('EXPANDED_GATE_LIMIT_EXCEEDED')
    return reasons


def condition(case, options, ranks):
    w = case.workload
    return {'label': case.label, 'variant': case.variant, 'qubits': w.qubits, 'block_bits': options.block_bits,
            'ranks': ranks, 'nodes': options.nodes, 'threads': options.threads, 'storage': w.policies['storage'], 'backend': options.backend,
            'mode': f'distributed-{options.executor}', 'executor_policy': executor_policy(options),
            'frame': w.policies['frame'], 'swap': w.policies['swap'],
            'execution': w.policies['execution'], 'initial_state': w._data.get('initial_state', 'zero')}


def preflight(case, profile, options, ranks):
    """Reject unsupported or over-budget runs before creating any dataset."""
    reasons = _supported(case.workload)
    if reasons: raise CapacityError(f'{case.label}: unsupported run: {", ".join(reasons)}')
    estimate_options = EstimateOptions(block_bits=options.block_bits, ranks=ranks, nodes=options.nodes, threads=options.threads,
                                       ram_budget_bytes_per_rank=options.ram_budget_bytes_per_rank,
                                       ram_budget_bytes_per_host=None,
                                       disk_budget_bytes_per_host=options.disk_budget_bytes)
    plan = estimate(case.workload, profile, estimate_options)
    resources = plan['resources']
    if int(resources['managed_peak_bytes_per_rank']) > options.ram_budget_bytes_per_rank:
        raise CapacityError(f'{case.label}: managed peak exceeds the per-rank RAM budget')
    if int(resources['disk_required_bytes_per_host']) > options.disk_budget_bytes:
        raise CapacityError(f'{case.label}: disk reference exceeds the disk budget')
    if case.plan is not None:
        if case.plan['workload_id'] != case.workload.workload_id: raise ValueError(f'{case.label}: plan workload mismatch')
        planned = case.plan['options']
        if int(planned['block_bits']) != options.block_bits or int(planned['ranks']) != ranks:
            raise ValueError(f'{case.label}: plan block_bits/ranks differ from the run')
    return plan


def _initial(workload):
    if workload._data.get('initial_state', 'zero') == 'zero': return None
    rng = np.random.default_rng(int(workload._data['seed']))
    state = rng.normal(size=1 << workload.qubits) + 1j * rng.normal(size=1 << workload.qubits)
    return state / np.linalg.norm(state)


def _directory_bytes(path):
    payload = total = 0
    for item in Path(path).rglob('*'):
        if item.is_file():
            size = item.stat().st_size
            total += size
            if item.name.startswith('segment-') and item.suffix == '.bin': payload += size
    return payload, total


SYNC_PROBE_COUNT = 16


def sync_probe(directory, count=SYNC_PROBE_COUNT):
    """Median 4 KiB pwrite+fsync latency in seconds on the trial device, outside timing.

    Durable catalog commits dominate small-Block runs on devices whose fsync
    flushes a volatile cache; this samples the device state each trial sees.
    """
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / 'sync-probe.bin'
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        payload, samples = b'\0' * 4096, []
        for i in range(count):
            os.pwrite(fd, payload, (i % 8) * 4096)
            t = time.perf_counter()
            os.fsync(fd)
            samples.append(time.perf_counter() - t)
    finally:
        os.close(fd)
        path.unlink(missing_ok=True)
    return statistics.median(samples)


def _trial(case, options, directory, comm, eta_model=None, fingerprint=None, host_leader=None):
    """One collective trial; returns rank-MAX phase walls and validation outside timing."""
    from .distributed import DistributedConfig, DistributedSimulator
    workload = case.workload
    initial = _initial(workload)
    config = DistributedConfig(str(directory), options.block_bits,
                               ram_budget_bytes_per_rank=options.ram_budget_bytes_per_rank,
                               backend=options.backend, amplitude_store=workload.policies['storage'],
                               frame_policy='materialize', swap_policy='S0',
                               # Separate hosts never share one tree, even under the same path string.
                               storage_scope='node-local' if options.nodes > 1 else 'shared')
    walls, sim = {}, None
    rank = comm.Get_rank()
    leader = rank == 0 if host_leader is None else host_leader
    probe = sync_probe(directory / f'probe-rank-{rank}')
    comm.Barrier()
    cpu0 = time.process_time()
    try:
        t = time.perf_counter()
        if options.executor == 'async':
            from .async_scheduler import AsyncDistributedSimulator
            sim = AsyncDistributedSimulator(workload.qubits, config, comm=comm, initial_state=initial,
                                            policy=_async_policy())
        else: sim = DistributedSimulator(workload.qubits, config, comm=comm, initial_state=initial)
        walls['setup'] = time.perf_counter() - t
        t = time.perf_counter()
        for node in workload.expanded_operations(MAX_EXECUTED_GATES):
            kind, targets, kwargs = gate_arguments(node)
            sim.gate(kind, targets, **kwargs)
        binding = sim.bind(workload.binding.values)
        walls['compile'] = time.perf_counter() - t
        checks = []
        if eta_model is None:
            t = time.perf_counter()
            sim.materialize(binding=binding)
            walls['execute'] = time.perf_counter() - t
        else:
            walls['execute'], checks = _execute_with_eta(sim, binding, eta_model, comm, fingerprint)
        t = time.perf_counter()
        sim.cache.flush()
        if workload.policies['storage'] == 'file': sim.store.flush()
        walls['flush'] = time.perf_counter() - t
        peak = sim.budget.peak
        # State check after the timed phases: a device that switched sync state
        # mid-trial makes the trial unusable as a single-state calibration row.
        probe_after = sync_probe(directory / f'probe-after-rank-{rank}')
        # One dataset tree per host; each host's leader scans its own volume.
        scanned = _directory_bytes(directory) if leader else (0, 0)
        payload, total = comm.allreduce(scanned[0]), comm.allreduce(scanned[1])
        validation = {'oracle': 'not-run', 'max_abs_error': None, 'passed': None}
        if workload.qubits <= options.oracle_max_qubits:
            state = sim.state(binding=binding, allow_gather=True)
            if comm.Get_rank() == 0:
                expected = eager_reference(list(sim.sequencer.operations), workload.qubits, sim.assets,
                                           binding, initial_state=initial)
                error = float(np.max(np.abs(state - expected)))
                validation = {'oracle': 'eager-reference', 'max_abs_error': error, 'passed': error <= 1e-10}
            validation = comm.bcast(validation, root=0)
        t = time.perf_counter()
        sim.close()
        sim = None
        comm.Barrier()
        if leader: shutil.rmtree(directory, ignore_errors=True)
        walls['cleanup'] = time.perf_counter() - t
    finally:
        if sim is not None: sim.close()
        shutil.rmtree(directory, ignore_errors=True)
    walls = {phase: comm.allreduce(walls[phase], op=_max()) for phase in PHASES}
    walls['total'] = sum(walls[phase] for phase in PHASES)
    return {'wall_seconds': walls, 'eta_checks': checks, 'cpu_seconds': comm.allreduce(time.process_time() - cpu0),
            'managed_peak_bytes': comm.allreduce(peak, op=_max()),
            'disk_payload_bytes': payload, 'disk_total_bytes': total,
            'sync_probe_median_seconds': comm.allreduce(probe, op=_max()),
            'sync_probe_after_seconds': comm.allreduce(probe_after, op=_max()),
            'timestamp': _timestamp()}, validation


ETA_CHECK_FRACTIONS = (0.0, 0.5)


def _execute_with_eta(sim, binding, model, comm, fingerprint):
    """Execute in segments; before each, record the time-model ETA to the end (untimed).

    The observed value of a check is the rank-MAX wall of all later segments,
    i.e. the actual remaining gates-only execution time from that frontier.
    """
    from .runtime_time_model_eta import VIEW_NAME
    # Checks capture the whole remaining program at once; the runtime's own
    # in-run captures keep their default bounded limits.
    sim.runtime.configure_eta(max_bytes=64 * 1024, scan_items=MAX_EXECUTED_GATES, scan_seconds=1.0)
    sim.runtime.attach_time_model(model, configuration_fingerprint=fingerprint)
    last = len(sim.sequencer.operations) - 1
    marks = sorted({int(f * (last + 1)) for f in ETA_CHECK_FRACTIONS if last >= 0})
    predictions, segments = [], []
    for i, mark in enumerate(marks):
        end = marks[i + 1] - 1 if i + 1 < len(marks) else last
        sim.runtime.set_time_model_sync_latency(None)
        median_view = sim.runtime.update_eta(endpoint_frontier=last, collective=True)
        probe = comm.allreduce(sync_probe(sim.directory / f'eta-probe-{comm.Get_rank()}'), op=_max())
        sim.runtime.set_time_model_sync_latency(probe)
        result = sim.runtime.update_eta(endpoint_frontier=last, collective=True)
        view = result['views'][VIEW_NAME]
        predictions.append({'frontier': sim.frontier, 'remaining_gate_count': view['remaining_gate_count'],
                            'sync_probe_seconds': probe,
                            'median_sync_predicted_expected_seconds': median_view['rank_max_expected_seconds'][VIEW_NAME],
                            'predicted_lower_seconds': view['lower_seconds'],
                            'predicted_expected_seconds': result['rank_max_expected_seconds'][VIEW_NAME],
                            'predicted_upper_seconds': view['upper_seconds'],
                            'partial_expected_seconds': view['partial_expected_seconds'],
                            'evidence': view['evidence'], 'unknown_reasons': list(view['unknown_reasons']),
                            'extraction_seconds': result['extraction_seconds']})
        t = time.perf_counter()
        sim.materialize(frontier=end, binding=binding)
        segments.append(time.perf_counter() - t)
    for i, row in enumerate(predictions):
        row['observed_seconds'] = comm.allreduce(sum(segments[i:]), op=_max())
    return sum(segments), predictions


def _max():
    from mpi4py import MPI
    return MPI.MAX


def run_cases(cases, profile: ProfileDocument, scratch, options: RunOptions, comm, eta_model=None, hosts=None):
    """Run interleaved cases; returns one benchmark-run document per case.

    With ``eta_model`` (async executor only) every trial also records the
    runtime time-model ETA against the actual remaining execution time.
    """
    if eta_model is not None and options.executor != 'async':
        raise ValueError('runtime ETA checks need the async executor')
    if not 1 <= len(cases) <= MAX_CASES: raise ValueError(f'run needs 1..{MAX_CASES} cases')
    if len({case.label for case in cases}) != len(cases): raise ValueError('case labels must be unique')
    ranks = comm.Get_size()
    import socket
    hosts = tuple(hosts or comm.allgather(socket.gethostname()))
    host_leader = comm.Get_rank() == min(i for i, h in enumerate(hosts) if h == hosts[comm.Get_rank()])
    plans = [preflight(case, profile, options, ranks) for case in cases]
    root = Path(scratch).expanduser().resolve()
    job = root / f'svsim-run-{comm.bcast(uuid.uuid4().hex if comm.Get_rank() == 0 else None, root=0)}'
    rng = np.random.default_rng(options.seed)
    deadline = time.monotonic() + options.walltime_seconds
    trials = [[] for _ in cases]
    validations = [[] for _ in cases]
    omitted, failure = [], None
    try:
        for round_index in range(options.warmup + options.repetitions):
            order = [int(i) for i in rng.permutation(len(cases))]
            for position, index in enumerate(order):
                late = comm.bcast(time.monotonic() >= deadline if comm.Get_rank() == 0 else None, root=0)
                if late:
                    omitted.append({'round': round_index, 'case': cases[index].label, 'reason': 'walltime deadline before trial'})
                    continue
                directory = job / f'case-{index}-round-{round_index}'
                try:
                    trial, validation = _trial(cases[index], options, directory, comm, eta_model,
                                               profile.configuration_fingerprint, host_leader)
                except (ArithmeticError, OSError, MemoryError, ValueError, RuntimeError) as exc:
                    failure = {'case': cases[index].label, 'round': round_index, 'error': f'{type(exc).__name__}: {exc}'}
                    raise
                if trial['disk_total_bytes'] > options.disk_budget_bytes:
                    failure = {'case': cases[index].label, 'round': round_index, 'error': 'observed disk exceeded budget'}
                    raise CapacityError(failure['error'])
                if round_index >= options.warmup:
                    trial['order_index'] = round_index * len(cases) + position
                    trial['round'] = round_index - options.warmup
                    trials[index].append(trial)
                    validations[index].append(validation)
    except Exception:
        if failure is None: raise
    finally:
        shutil.rmtree(job, ignore_errors=True)
    return [_document(case, plan, profile, options, ranks, trials[i], validations[i], omitted, failure, eta_model, hosts)
            for i, (case, plan) in enumerate(zip(cases, plans))]


def _document(case, plan, profile, options, ranks, trials, validations, omitted, failure, eta_model=None, hosts=None):
    workload = case.workload
    features, _ = workload_features(workload, options.block_bits, ranks)
    lowered = sum(entry.count for entry in workload.lowered()[0])
    blocks = 1 << (workload.qubits - options.block_bits)
    errors = [v['max_abs_error'] for v in validations if v['max_abs_error'] is not None]
    if not validations: validation = {'oracle': 'not-run', 'max_abs_error': None, 'passed': None}
    elif all(v['oracle'] == 'not-run' for v in validations): validation = validations[0]
    else: validation = {'oracle': 'eager-reference', 'max_abs_error': max(errors) if errors else None,
                        'passed': all(v['passed'] is True for v in validations)}
    complete = failure is None and len(trials) == options.repetitions
    case_omitted = [row for row in omitted if row['case'] == case.label]
    document = {
        'schema_version': 1, 'kind': 'benchmark-run', 'run_type': 'workload-execution',
        'workload_id': workload.workload_id, 'plan_id': case.plan['plan_id'] if case.plan else None,
        'reference_plan_id': plan['plan_id'], 'profile_id': profile.profile_id,
        'configuration_fingerprint': profile.configuration_fingerprint,
        'case': condition(case, options, ranks),
        'work': {'lowered_gate_count': lowered, 'block_count': blocks, 'block_pass_count': lowered * blocks,
                 'features': [{'feature': k, 'count': v} for k, v in sorted(features.items())]},
        'phases': list(PHASES), 'trials': trials,
        'summary': {key: stats([t['wall_seconds'][key] for t in trials]) for key in (*PHASES, 'total')} if trials else {},
        'warmup_count': options.warmup, 'sample_count': len(trials), 'validation': validation,
        'status': 'completed' if complete else 'failed' if failure else 'partial',
        'omitted': case_omitted + ([failure] if failure and failure['case'] == case.label else []),
        'failure': failure, 'seed': options.seed, 'options': asdict(options),
        'eta_model_id': eta_model.model_id if eta_model is not None else None,
        'host_ids': sorted({content_hash({'host': h}) for h in hosts}) if hosts else None,
        'limits': {'executed_gate_limit_count': MAX_EXECUTED_GATES, 'max_case_count': MAX_CASES,
                   'deadline_scope': 'cooperative between trials; an in-flight trial is not preempted',
                   'ram_scope': 'managed budget per rank; process RSS is not limited'},
        'timing': {'phase_scope': 'rank MAX of local serial phase wall; total is the sum of phase MAX values',
                   'excluded': ['oracle gather and validation', 'disk usage scan', 'barriers between phases',
                                f'pre-trial sync probe ({SYNC_PROBE_COUNT} x 4 KiB pwrite+fsync, rank MAX of medians)',
                                'post-flush sync probe for device-state stability'],
                   'sync_probe_scope': 'device state context only; macOS fsync does not flush the device cache',
                   'eta_checks': 'time-model attach and ETA captures are outside the execute segments; '
                                 'observed is the rank-MAX wall of the remaining segments',
                   'order': 'seeded shuffled case order per round; warmup rounds discarded'},
        'provenance': profile.data['provenance']}
    encoded = wire_encode(document)
    encoded['run_id'] = content_hash(encoded)
    return encoded


CALIBRATION_CLASSES = {
    'empty': lambda n, b, top: [],
    'h-local': lambda n, b, top: [{'kind': 'H', 'targets': [0]}],
    'rx-local': lambda n, b, top: [{'kind': 'RX', 'targets': [0], 'angle': 0.37}],
    'pauli-local': lambda n, b, top: [{'kind': 'PAULI_ROT', 'targets': [0, 1, 2], 'paulis': ['Y', 'X', 'Z'], 'angle': 0.37}],
    # Highest intra-rank upper bit; absent when every upper bit is a rank bit.
    'h-joint': lambda n, b, top: [{'kind': 'H', 'targets': [top]}] if top >= b else None,
    # A rank bit forces an MPI exchange; absent for one rank.
    'h-remote': lambda n, b, top: [{'kind': 'H', 'targets': [n - 1]}] if top < n - 1 else None,
}


def calibration_suite(block_bits, qubits, counts, storage='ram', classes=tuple(CALIBRATION_CLASSES), ranks=1):
    """Single-class workloads so each route-class feature varies independently of the others."""
    if block_bits < 3: raise ValueError('calibration suite needs block_bits >= 3 for the Pauli cell')
    if len(qubits) * len(counts) * len(classes) > 256: raise ValueError('calibration suite exceeds 256 workloads')
    rank_bits = ranks.bit_length() - 1
    if ranks < 1 or ranks & (ranks - 1): raise ValueError('ranks must be a power of two')
    suite = {}
    for name in classes:
        for n in qubits:
            if n <= block_bits + rank_bits: raise ValueError('calibration qubits must exceed block and rank bits')
            body = CALIBRATION_CLASSES[name](n, block_bits, n - rank_bits - 1)
            if body is None: continue
            for count in (counts if name != 'empty' else (0,)):
                operations = [{'kind': 'REPEAT', 'count': count, 'body': body}] if body else []
                suite[f'cal-{name}-n{n}-g{count}'] = {'schema_version': 1, 'kind': 'workload', 'qubits': n,
                                                     'policies': {'storage': storage}, 'operations': operations}
    return suite
