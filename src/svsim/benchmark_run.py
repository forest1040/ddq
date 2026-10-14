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
            'ranks': ranks, 'nodes': 1, 'threads': options.threads, 'storage': w.policies['storage'], 'backend': options.backend,
            'mode': 'distributed-sync', 'frame': w.policies['frame'], 'swap': w.policies['swap'],
            'execution': w.policies['execution'], 'initial_state': w._data.get('initial_state', 'zero')}


def preflight(case, profile, options, ranks):
    """Reject unsupported or over-budget runs before creating any dataset."""
    reasons = _supported(case.workload)
    if reasons: raise CapacityError(f'{case.label}: unsupported run: {", ".join(reasons)}')
    estimate_options = EstimateOptions(block_bits=options.block_bits, ranks=ranks, nodes=1, threads=options.threads,
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


def _trial(case, options, directory, comm):
    """One collective trial; returns rank-MAX phase walls and validation outside timing."""
    from .distributed import DistributedConfig, DistributedSimulator
    workload = case.workload
    initial = _initial(workload)
    config = DistributedConfig(str(directory), options.block_bits,
                               ram_budget_bytes_per_rank=options.ram_budget_bytes_per_rank,
                               backend=options.backend, amplitude_store=workload.policies['storage'],
                               frame_policy='materialize', swap_policy='S0')
    walls, sim = {}, None
    probe = sync_probe(directory)
    comm.Barrier()
    cpu0 = time.process_time()
    try:
        t = time.perf_counter()
        sim = DistributedSimulator(workload.qubits, config, comm=comm, initial_state=initial)
        walls['setup'] = time.perf_counter() - t
        t = time.perf_counter()
        for node in workload.expanded_operations(MAX_EXECUTED_GATES):
            kind, targets, kwargs = gate_arguments(node)
            sim.gate(kind, targets, **kwargs)
        binding = sim.bind(workload.binding.values)
        walls['compile'] = time.perf_counter() - t
        t = time.perf_counter()
        sim.materialize(binding=binding)
        walls['execute'] = time.perf_counter() - t
        t = time.perf_counter()
        sim.cache.flush()
        if workload.policies['storage'] == 'file': sim.store.flush()
        walls['flush'] = time.perf_counter() - t
        peak = sim.budget.peak
        payload, total = _directory_bytes(directory)
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
        shutil.rmtree(directory, ignore_errors=True)
        walls['cleanup'] = time.perf_counter() - t
    finally:
        if sim is not None: sim.close()
        shutil.rmtree(directory, ignore_errors=True)
    walls = {phase: comm.allreduce(walls[phase], op=_max()) for phase in PHASES}
    walls['total'] = sum(walls[phase] for phase in PHASES)
    return {'wall_seconds': walls, 'cpu_seconds': comm.allreduce(time.process_time() - cpu0),
            'managed_peak_bytes': comm.allreduce(peak, op=_max()),
            'disk_payload_bytes': comm.allreduce(payload), 'disk_total_bytes': comm.allreduce(total),
            'sync_probe_median_seconds': comm.allreduce(probe, op=_max()),
            'timestamp': _timestamp()}, validation


def _max():
    from mpi4py import MPI
    return MPI.MAX


def run_cases(cases, profile: ProfileDocument, scratch, options: RunOptions, comm):
    """Run interleaved cases; returns one benchmark-run document per case."""
    if not 1 <= len(cases) <= MAX_CASES: raise ValueError(f'run needs 1..{MAX_CASES} cases')
    if len({case.label for case in cases}) != len(cases): raise ValueError('case labels must be unique')
    ranks = comm.Get_size()
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
                directory = job / f'case-{index}-round-{round_index}' / f'rank-{comm.Get_rank()}'
                try:
                    trial, validation = _trial(cases[index], options, directory, comm)
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
    return [_document(case, plan, profile, options, ranks, trials[i], validations[i], omitted, failure)
            for i, (case, plan) in enumerate(zip(cases, plans))]


def _document(case, plan, profile, options, ranks, trials, validations, omitted, failure):
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
        'limits': {'executed_gate_limit_count': MAX_EXECUTED_GATES, 'max_case_count': MAX_CASES,
                   'deadline_scope': 'cooperative between trials; an in-flight trial is not preempted',
                   'ram_scope': 'managed budget per rank; process RSS is not limited'},
        'timing': {'phase_scope': 'rank MAX of local serial phase wall; total is the sum of phase MAX values',
                   'excluded': ['oracle gather and validation', 'disk usage scan', 'barriers between phases',
                                f'pre-trial sync probe ({SYNC_PROBE_COUNT} x 4 KiB pwrite+fsync, rank MAX of medians)'],
                   'sync_probe_scope': 'device state context only; macOS fsync does not flush the device cache',
                   'order': 'seeded shuffled case order per round; warmup rounds discarded'},
        'provenance': profile.data['provenance']}
    encoded = wire_encode(document)
    encoded['run_id'] = content_hash(encoded)
    return encoded


CALIBRATION_CLASSES = {
    'empty': lambda n, b: [],
    'h-local': lambda n, b: [{'kind': 'H', 'targets': [0]}],
    'rx-local': lambda n, b: [{'kind': 'RX', 'targets': [0], 'angle': 0.37}],
    'pauli-local': lambda n, b: [{'kind': 'PAULI_ROT', 'targets': [0, 1, 2], 'paulis': ['Y', 'X', 'Z'], 'angle': 0.37}],
    'h-joint': lambda n, b: [{'kind': 'H', 'targets': [n - 1]}],
}


def calibration_suite(block_bits, qubits, counts, storage='ram', classes=tuple(CALIBRATION_CLASSES)):
    """Single-class workloads so each route-class feature varies independently of the others."""
    if block_bits < 3: raise ValueError('calibration suite needs block_bits >= 3 for the Pauli cell')
    if len(qubits) * len(counts) * len(classes) > 256: raise ValueError('calibration suite exceeds 256 workloads')
    suite = {}
    for name in classes:
        for n in qubits:
            if n <= block_bits: raise ValueError('calibration qubits must exceed block_bits for a joint class')
            for count in (counts if name != 'empty' else (0,)):
                body = CALIBRATION_CLASSES[name](n, block_bits)
                operations = [{'kind': 'REPEAT', 'count': count, 'body': body}] if body else []
                suite[f'cal-{name}-n{n}-g{count}'] = {'schema_version': 1, 'kind': 'workload', 'qubits': n,
                                                     'policies': {'storage': storage}, 'operations': operations}
    return suite
