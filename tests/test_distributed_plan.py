from types import SimpleNamespace
import pytest
from svsim.distributed_plan import MPIProfile,choose_mcx,choose_s1
from svsim.ir import GateNormalizer
from svsim.layout import QubitLayout


def profile(**kw):
    args = dict(profile_id='measured',block_bytes=128,tile_bytes=64,backend='buffered',
                bandwidth_bytes_per_second=1e9,latency_seconds=1e-6,packing_seconds_per_byte=1e-10)
    args.update(kw)
    return MPIProfile(**args)


def test_mcx_costs_unknown_conditions_and_overrides():
    config = SimpleNamespace(mcx_exchange='auto',tile_bytes=64,backend='buffered')
    assert choose_mcx(None,config,128,16) == 'full'
    assert choose_mcx(profile(),config,128,16) == 'packed'
    assert choose_mcx(profile(packing_seconds_per_byte=1),config,128,16) == 'full'
    assert choose_mcx(profile(tile_bytes=32),config,128,16) == 'full'
    config.mcx_exchange='packed'
    assert choose_mcx(None,config,128,128) == 'packed'


def test_s1_preserves_order_and_requires_recoverable_measured_cost():
    normalizer = GateNormalizer(6)
    gate = normalizer.gate('RX',(5,),angle=.2)
    layout = QubitLayout.identity(6)
    assert choose_s1(6,3,2,layout,[gate],None).bits is None
    assert choose_s1(6,3,2,layout,[gate],profile(),1e9).bits is None
    repeated = [gate]*100
    decision = choose_s1(6,3,2,layout,repeated,profile(),1e9)
    assert decision.bits is not None and decision.bits[0] == 5
    assert decision.expected_net_seconds > 0
    assert decision.expected_saved_mpi_bytes > decision.swap_mpi_bytes
    assert repeated == [gate]*100 and layout == QubitLayout.identity(6)
    assert choose_s1(6,3,2,layout,repeated,profile()).bits is None
    assert choose_s1(6,3,2,layout,repeated,profile(),1).bits is None
    assert choose_s1(6,3,2,layout,[normalizer.gate('SWAP',(0,5)),*repeated],profile(),1e9).bits is None


@pytest.mark.parametrize('kw',[{'bandwidth_bytes_per_second':0},{'latency_seconds':float('nan')},
                              {'packing_seconds_per_byte':-1},{'block_bytes':0}])
def test_profile_rejects_invalid_measurements(kw):
    with pytest.raises(ValueError): profile(**kw)


def test_passive_mpi_guard_matches_direction_and_packet_size():
    from svsim.mpi_health import attach_mpi_guard
    from svsim.lifecycle import HardwareProfile
    measured = profile(peer_samples=({'source':0,'destination':1,'bytes':128,'seconds':.001},))
    hardware = HardwareProfile('base',1,'now','host','os','release','python',1,'cpu',1024,1024)

    class Trace:
        def __init__(self): self.events=[]
        def record(self,*args,**kw): self.events.append(kw)

    trace = Trace()
    sim = SimpleNamespace(rank=0,size=2,local=SimpleNamespace(profile=hardware,plan='original'),
        config=SimpleNamespace(tile_bytes=64,backend='buffered'),store=SimpleNamespace(payload_bytes=128),
        transport=SimpleNamespace(observer=None),environment=[{'host':'host','mpi_library':'MPI'}]*2,
        circuit_hash='circuit',run_id='run',trace=trace)
    guard = attach_mpi_guard(sim,measured,window_size=1,consecutive_windows=2)
    sim.transport.observer(0,1,128,.01)
    sim.transport.observer(0,1,128,.01)
    assert trace.events[-1]['health_state'] == 'DEGRADED'
    sim.transport.observer(0,1,64,.01)
    assert trace.events[-1]['health_state'] == 'INSUFFICIENT_BASELINE'
    assert sim.local.plan == 'original' and sim.local.profile is hardware
    assert sim.mpi_guard is guard
