"""Conservative communication choices; source gate order is never changed."""
from dataclasses import dataclass
from math import isfinite
from .ownership import OwnershipMap


@dataclass(frozen=True)
class MPIProfile:
    profile_id: str
    block_bytes: int
    tile_bytes: int
    backend: str
    bandwidth_bytes_per_second: float
    latency_seconds: float
    packing_seconds_per_byte: float
    host_scope: str = 'same-host'
    version: int = 1
    peer_samples: tuple[dict, ...] = ()

    def __post_init__(self):
        if not self.profile_id or self.block_bytes <= 0 or self.tile_bytes <= 0 or self.version < 1:
            raise ValueError('Invalid MPI profile identity/dimensions')
        if self.backend not in {'buffered','direct'} or self.host_scope not in {'same-host','multi-host'}:
            raise ValueError('Invalid MPI profile condition')
        for value in (self.bandwidth_bytes_per_second,self.latency_seconds,self.packing_seconds_per_byte):
            if not isfinite(value) or value < 0: raise ValueError('Invalid MPI profile measurement')
        if self.bandwidth_bytes_per_second == 0: raise ValueError('Bandwidth must be positive')
        object.__setattr__(self,'peer_samples',tuple(dict(sample) for sample in self.peer_samples))
        seen = set()
        for sample in self.peer_samples:
            source,destination,size,seconds = (sample[key] for key in ('source','destination','bytes','seconds'))
            if (not all(isinstance(v,int) and not isinstance(v,bool) for v in (source,destination,size))
                or source < 0 or destination < 0 or source == destination or not 0 < size <= self.block_bytes
                or not isfinite(seconds) or seconds <= 0):
                raise ValueError('Invalid MPI peer measurement')
            key = source,destination,size
            if key in seen: raise ValueError('Duplicate MPI peer measurement')
            seen.add(key)

    def matches(self, payload_bytes, tile_bytes, backend):
        return (self.block_bytes,self.tile_bytes,self.backend) == (payload_bytes,tile_bytes,backend)


def choose_mcx(profile, config, payload_bytes, active_bytes):
    if payload_bytes <= 0 or not 0 <= active_bytes <= payload_bytes:
        raise ValueError('Invalid MCX transfer sizes')
    policy = config.mcx_exchange
    if policy in {'packed','full'}: return policy
    if policy != 'auto': raise ValueError('Unknown MCX policy')
    if profile is None or not profile.matches(payload_bytes,config.tile_bytes,config.backend): return 'full'
    full = 2*payload_bytes/profile.bandwidth_bytes_per_second+2*profile.latency_seconds
    packed = 2*active_bytes/profile.bandwidth_bytes_per_second+2*profile.latency_seconds+2*active_bytes*profile.packing_seconds_per_byte
    return 'packed' if packed < full else 'full'


@dataclass(frozen=True)
class SwapDecision:
    bits: tuple[int,int] | None
    expected_saved_mpi_bytes: int = 0
    swap_mpi_bytes: int = 0
    swap_storage_bytes: int = 0
    frame_reserve_bytes: int = 0
    expected_net_seconds: float = 0.0
    reason: str = 'unknown-calibration-S0'
    profile_id: str | None = None


def choose_s1(num_qubits, block_bits, size, layout, remaining_gates, profile, storage_bandwidth=None):
    ownership = OwnershipMap(num_qubits,block_bits,size)
    payload = 16*(1 << block_bits)
    if len(layout.logical_to_physical) != num_qubits: raise ValueError('Layout width mismatch')
    if size == 1: return SwapDecision(None,reason='single-rank-S0')
    if (profile is None or profile.block_bytes != payload or storage_bandwidth is None
        or not isfinite(storage_bandwidth) or storage_bandwidth <= 0):
        return SwapDecision(None)
    # Explicit logical SWAPs bound the lookahead. No source gate is moved.
    gates = []
    for gate in remaining_gates:
        if gate.kind == 'SWAP': break
        gates.append(gate)
    rank_start = num_qubits-ownership.rank_bits
    inverse = {physical:logical for logical,physical in enumerate(layout.logical_to_physical)}
    useful = {'H','RX','RY'}

    def uses(logical):
        return sum(g.kind in useful and not g.controls and g.targets == (logical,) for g in gates)

    state_bytes = payload*ownership.num_blocks
    # Full-block reference routing and foreign extent fetch can exceed the
    # half-state ideal payload. Budget an upper bound for this runtime.
    swap_bytes = 2*state_bytes
    storage_bytes,frame_bytes = 2*state_bytes,state_bytes
    best = SwapDecision(None,reason='swap-cost-not-recovered-S0',profile_id=profile.profile_id)
    for global_bit in range(rank_start,num_qubits):
        for local_bit in range(rank_start):
            global_logical,local_logical = inverse[global_bit],inverse[local_bit]
            # Unsupported multi-wire interactions might become more expensive
            # after placement; reject rather than silently omitting that cost.
            if any(local_logical in g.qubits and not g.diagonal and
                   not (g.kind in useful and not g.controls and g.targets == (local_logical,)) for g in gates):
                continue
            count = uses(global_logical)-uses(local_logical)
            saved = max(0,count)*size*payload
            net = ((saved-swap_bytes)/profile.bandwidth_bytes_per_second
                   -(storage_bytes+frame_bytes)/storage_bandwidth
                   -2*size*profile.latency_seconds)
            if net > best.expected_net_seconds:
                best = SwapDecision((global_bit,local_bit),saved,swap_bytes,storage_bytes,frame_bytes,
                                    net,'measured-lower-bound-profitable-S1',profile.profile_id)
    return best
