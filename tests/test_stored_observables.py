import numpy as np
import pytest
from svsim.stored_runtime import StoredSimulator
from svsim.storage import StorageConfig
from svsim.observables import PauliTerm, ObservableIR, StateHandle, BlockPauliInner
from svsim.kernels import eager_reference


@pytest.mark.parametrize('swap',[None,(0,7)])
def test_streaming_observables_without_gather(tmp_path,swap):
    config = StorageConfig(str(tmp_path/'run'),3,ram_budget_bytes=196608,alignment=64)
    with StoredSimulator(8,config) as sim:
        sim.gate('H',(0,)); sim.gate('H',(7,)); sim.gate('CX',(7,1)); sim.gate('RZ',(7,),angle=.7)
        sim.gate('T',(0,)); sim.gate('H',(7,))
        expected = eager_reference(sim.sequencer.operations,8,sim.assets)
        if swap: sim.physical_index_bit_swap(*swap)
        observables = [ObservableIR((PauliTerm.from_string(s,.3),PauliTerm(0,0,.2)))
                       for s in ['XYIIIIIY','ZIIIIIII','YIZIIIII','IXIIIIIY']]
        oracle = [BlockPauliInner().expectation(StateHandle.from_vector(expected,3),o) for o in observables]
        sim.state = lambda *a,**k: (_ for _ in ()).throw(AssertionError('gather forbidden'))
        np.testing.assert_allclose(sim.expectation(observables),oracle,atol=1e-14)
        assert sim.budget.peak <= config.ram_budget_bytes
        assert any(e['metadata'].get('path') == 'STREAMING_EXPECTATION' for e in sim.trace.iter_events())


def test_same_partner_mask_reads_each_pair_once(tmp_path):
    config = StorageConfig(str(tmp_path/'run'),2,alignment=64)
    with StoredSimulator(8,config) as sim:
        sim.gate('H',(7,)); sim.gate('T',(0,)); sim.materialize()
        observables = [ObservableIR((PauliTerm.from_string(s),)) for s in ['IIIIIIIX','ZIIIIIIX','YIIIIIIX']]
        sim.expectation(observables)
        event = list(sim.trace.iter_events())[-1]['metadata']
        assert event['mask_groups'] == 1
        assert event['partner_groups'] == sim.store.num_blocks//2
