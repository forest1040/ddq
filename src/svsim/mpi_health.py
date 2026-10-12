"""Passive, peer-and-packet matched MPI health observation."""
from dataclasses import replace,asdict
from .lifecycle import BenchmarkCondition,BenchmarkSample,StaticCircuitOptimizer
from .telemetry import ObservePerformanceGuard


def mpi_condition(sim, source, destination, count):
    """Match direction, packet size and the actual transport environment."""
    return BenchmarkCondition('mpi-state',block_bytes=count,io_depth=1,ranks=sim.size,
        details={'source':source,'peer':destination,'tile_bytes':sim.config.tile_bytes,
                 'backend':sim.config.backend,'host_source':sim.environment[source]['host'],
                 'host_destination':sim.environment[destination]['host'],
                 'mpi_library':sim.environment[source]['mpi_library'],
                 'mpi4py':sim.environment[source].get('mpi4py'),
                 'thread_level':sim.environment[source].get('thread_level'),
                 'concurrency':1,'scope':'whole-validated-transfer','warmup':1})


def attach_mpi_guard(sim, profile, window_size=8, consecutive_windows=3):
    """Attach source-only successful-transfer observation; never modify plans."""
    conditions = []
    if profile is not None and profile.matches(sim.store.payload_bytes,sim.config.tile_bytes,sim.config.backend):
        for record in profile.peer_samples:
            if record['source'] >= sim.size or record['destination'] >= sim.size: continue
            condition = mpi_condition(sim,record['source'],record['destination'],record['bytes'])
            # Reusing a saved calibration on a different MPI installation or
            # host pair supplies no baseline for that changed environment.
            if any(key in record and record[key] != condition.details[key]
                   for key in ('host_source','host_destination','mpi_library','mpi4py','thread_level')):
                continue
            conditions.append(BenchmarkSample(condition,latency_ms=record['seconds']*1000))
    hardware = replace(sim.local.profile,benchmarks=tuple(conditions),
                       profile_id=profile.profile_id if profile is not None else sim.local.profile.profile_id)
    # Separate observation plan keeps the simulator's existing immutable plan
    # and hardware profile untouched, including all storage baselines.
    plan = StaticCircuitOptimizer().create_plan(sim.circuit_hash,hardware,
        plan_id=sim.run_id+'-mpi-observe',expected_conditions=(s.condition for s in conditions))
    guard = ObservePerformanceGuard(hardware,plan,window_size=window_size,consecutive_windows=consecutive_windows)

    def observer(source,destination,count,overall_seconds):
        if source != sim.rank or source == destination: return
        sample = BenchmarkSample(mpi_condition(sim,source,destination,count),latency_ms=overall_seconds*1000)
        report = guard.observe(sample)
        if sim.trace:
            sim.trace.record(f'{source}->{destination}',performance_class='mpi-health',health_state=report.health_state,
                decision_reason=report.reason,metadata={'path':'MPI_HEALTH','rank':sim.rank,'peer':destination,
                    'bytes':count,'mode':'observe','plan_unchanged':True,'report':asdict(report)})

    sim.transport.observer = observer
    sim.mpi_guard = guard
    return guard
