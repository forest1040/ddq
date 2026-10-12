"""Large symbolic cuts must not enumerate the amplitude state."""
import pytest

MPI=pytest.importorskip('mpi4py.MPI')
from svsim import DistributedConfig,DistributedSimulator
from svsim.mpi_transport import CollectiveError


@pytest.mark.parametrize('num_qubits',(40,50))
def test_symbolic_cut_does_not_expand_blocks(tmp_path,num_qubits):
    config=DistributedConfig(str(tmp_path/str(num_qubits)),24,
                             ram_budget_bytes_per_rank=1024**2)
    with DistributedSimulator(num_qubits,config,comm=MPI.COMM_SELF) as sim:
        sim.gate('RZ',(num_qubits-1,),angle=.2)
        sim.gate('X',(num_qubits-1,))
        sim.gate('H',(0,))
        sim.materialize(binding=sim.bind())
        moved=1 << (num_qubits-1-sim.block_bits)
        view,_=sim.view(moved)
        assert view.version_id=='initial-one'
        assert view.frame_asset_id
        assert sim.store.db.execute('SELECT COUNT(*) FROM views').fetchone()[0]==0
        assert not list((sim.store.directory/'state').glob('segment-*.bin'))
        assert sim.budget.peak==config.metadata_reserve_bytes
        with pytest.raises(CollectiveError,match='Minimum task RAM'):
            sim.state(allow_gather=True)
