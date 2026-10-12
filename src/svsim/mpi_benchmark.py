"""Collective bounded-buffer calibration for the actual MPI transport path."""
from hashlib import sha256
from dataclasses import asdict
from time import perf_counter
from .distributed_plan import MPIProfile
from .distributed_kernels import active_offsets,pack_active
from .journal import canonical


def calibrate_mpi(sim, repetitions=3):
    """Measure every directed rank pair; return worst-peer profile.

    Both directions use the runtime's tiled, validated transfer API. Reported
    latency includes its control collectives, so it must not be mistaken for
    raw NIC latency. Single-rank execution has no network calibration.
    """
    sim.transport.agree({'calibration_repetitions':repetitions})
    if not isinstance(repetitions,int) or repetitions < 1:
        raise ValueError('Calibration repetitions must be positive')
    if sim.size == 1: return None
    payload = sim.store.payload_bytes
    sim._admit(2)
    peers = ((source,destination) for source in range(sim.size) for destination in range(source+1,sim.size))
    sizes = sorted({16,*(max(16,payload >> k) for k in range(min(sim.block_bits,6)+1))})
    measurements = []
    for a,b in peers:
        with sim._buffers(1,a) as source, sim._buffers(1,b) as destination:
            for size in sizes:
                elapsed = {(a,b):0.0,(b,a):0.0}
                for repetition in range(repetitions+1):
                    for sender,receiver,sbuf,rbuf in ((a,b,source,destination),(b,a,destination,source)):
                        begin = perf_counter()
                        sim.transport.transfer(sbuf[0].view[:size] if sim.rank == sender else None,
                            rbuf[0].view[:size] if sim.rank == receiver else None,sender,receiver,
                            metadata={'path':'calibration','bytes':size})
                        duration = sim.transport.max_value(perf_counter()-begin)
                        if repetition: elapsed[sender,receiver] += duration
                for (sender,receiver),seconds in elapsed.items():
                    sample = {'source':sender,'destination':receiver,'bytes':size,'seconds':seconds/repetitions,
                              'host_source':sim.environment[sender]['host'],
                              'host_destination':sim.environment[receiver]['host'],
                              'mpi_library':sim.environment[sender]['mpi_library'],
                              'mpi4py':sim.environment[sender].get('mpi4py'),
                              'thread_level':sim.environment[sender].get('thread_level')}
                    measurements.append(sample)
                    if sim.trace: sim.trace.record(f'{sender}->{receiver}',performance_class='mpi-calibration-peer',metadata=sample)
    # Packing measurements use caller-owned block buffers, never index arrays.
    with sim._buffers(2,0) as buffers:
        seconds = 0.0
        if sim.rank == 0:
            src,dst = (b.array(payload) for b in buffers)
            try:
                for _ in range(repetitions):
                    begin = perf_counter()
                    pack_active(src,dst,active_offsets(sim.block_bits,(0,),(1,)))
                    seconds += perf_counter()-begin
            finally: del src,dst
        packing = sim.transport.max_value(seconds/repetitions)/(payload//2)
    latencies = [m['seconds'] for m in measurements if m['bytes'] == 16]
    # Effective bandwidth includes control overhead, deliberately conservative.
    bandwidth = min(payload/m['seconds'] for m in measurements if m['bytes'] == payload)
    hosts = {item['host'] for item in sim.environment}
    condition = {'block_bytes':payload,'tile_bytes':sim.config.tile_bytes,'backend':sim.config.backend,
                 'bandwidth_bytes_per_second':bandwidth,'latency_seconds':max(latencies),
                 'packing_seconds_per_byte':packing,'host_scope':'same-host' if len(hosts)==1 else 'multi-host',
                 'peer_samples':tuple(measurements)}
    profile = MPIProfile(sha256(canonical(condition).encode()).hexdigest(),**condition)
    sim.transport.agree(asdict(profile))
    if sim.trace: sim.trace.record(profile.profile_id,performance_class='mpi-calibration',metadata=asdict(profile))
    return profile
