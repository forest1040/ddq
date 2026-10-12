"""Checkpoint failures preserve the authoritative committed restart cut."""
from dataclasses import replace,asdict
from pathlib import Path
import numpy as np
import pytest

MPI = pytest.importorskip('mpi4py.MPI')

from svsim.distributed import DistributedConfig,DistributedSimulator
from svsim.distributed_checkpoint import load_global
from svsim.mpi_transport import CollectiveError
import svsim.distributed_checkpoint as checkpoint


def config(path):
    return DistributedConfig(str(path),1,ram_budget_bytes_per_rank=4*1024**2,
                             metadata_reserve_bytes=256*1024)


@pytest.mark.parametrize('failure',['prepare','global'])
def test_failed_checkpoint_restores_prior_authoritative_cut(tmp_path,monkeypatch,failure):
    settings = config(tmp_path/'state')
    with DistributedSimulator(3,settings,comm=MPI.COMM_SELF) as sim:
        sim.gate('H',(0,))
        sim.materialize(binding=sim.bind())
        first = sim.commit_snapshot()
        expected = sim.state(allow_gather=True).copy()
        sim.gate('RX',(2,),angle=.37)
        sim.materialize(binding=sim.bind())
        original = checkpoint.atomic_json

        def fail(path,data):
            path = Path(path)
            if ((failure == 'prepare' and path.name.startswith('prepare-')) or
                (failure == 'global' and '/global/manifests/' in str(path))):
                raise OSError('injected checkpoint failure')
            original(path,data)

        with monkeypatch.context() as scope:
            scope.setattr(checkpoint,'atomic_json',fail)
            with pytest.raises(CollectiveError,match='injected'): sim.commit_snapshot()
        assert load_global(settings.directory)['snapshot_id'] == first['snapshot_id']
    with DistributedSimulator.resume(settings,comm=MPI.COMM_SELF) as restored:
        np.testing.assert_allclose(restored.state(allow_gather=True),expected,atol=1e-12)


@pytest.mark.parametrize('failure',['backup','lease'])
def test_fork_local_failure_is_voted_and_parent_remains_usable(tmp_path,monkeypatch,failure):
    settings = config(tmp_path/'parent')
    with DistributedSimulator(3,settings,comm=MPI.COMM_SELF) as sim:
        sim.gate('H',(0,))
        binding = sim.bind()
        sim.materialize(binding=binding)
        snapshot = sim.commit_snapshot()
        expected = sim.state(allow_gather=True).copy()
        with monkeypatch.context() as scope:
            if failure == 'backup':
                original = checkpoint.sqlite3.connect

                def fail_connect(database,*args,**kwargs):
                    if kwargs.get('uri') and 'mode=ro' in str(database):
                        raise OSError('injected metadata backup failure')
                    return original(database,*args,**kwargs)

                scope.setattr(checkpoint.sqlite3,'connect',fail_connect)
            else:
                original = checkpoint.atomic_json

                def fail_lease(path,data):
                    if '/leases/' in str(path): raise OSError('injected lease failure')
                    original(path,data)

                scope.setattr(checkpoint,'atomic_json',fail_lease)
            with pytest.raises(CollectiveError,match='injected'):
                sim.fork(snapshot,binding=binding,directory=tmp_path/'failed-suffix')
        np.testing.assert_allclose(sim.state(allow_gather=True),expected,atol=1e-12)
        assert load_global(settings.directory)['snapshot_id'] == snapshot['snapshot_id']


def test_global_manifest_rejects_duplicate_rank_prepares(tmp_path):
    settings = config(tmp_path/'state')
    with DistributedSimulator(3,settings,comm=MPI.COMM_SELF) as sim:
        sim.materialize(binding=sim.bind())
        snapshot = sim.commit_snapshot()
        damaged = dict(snapshot,prepares=snapshot['prepares']*2)
        path = Path(settings.directory)/'global/manifests'/f"{snapshot['snapshot_id']}.json"
        checkpoint.atomic_json(path,{'manifest':damaged,'checksum':checkpoint.content_hash(damaged)})
        with pytest.raises(OSError,match='one prepare per rank'):
            load_global(settings.directory,snapshot['snapshot_id'])


def test_phase2_snapshot_migration_preserves_source_and_adjoint_history(tmp_path):
    from svsim.stored_runtime import StoredSimulator
    from svsim.storage import StorageConfig
    from svsim.parameters import ParameterSymbol
    from svsim.observables import ObservableIR,PauliTerm
    source_config = StorageConfig(str(tmp_path/'stored'),1,ram_budget_bytes=4*1024**2,
                                  metadata_reserve_bytes=256*1024)
    theta = ParameterSymbol('migration-angle',parameter_id='migration-angle')
    with StoredSimulator(3,source_config) as source:
        source.gate('H',(0,)); source.gate('RY',(2,),angle=theta)
        source.materialize(binding=source.bind({theta.parameter_id:.31}))
        source.physical_index_bit_swap(0,2)
        source.gate('RZ',(0,),angle=theta)
        source.materialize(binding=source.bind({theta.parameter_id:.31}))
        snapshot = source.commit_snapshot()
        expected = source.state().copy()
    catalog = Path(source_config.directory)/'metadata/catalog.sqlite3'
    before = catalog.read_bytes()
    settings = config(tmp_path/'distributed')
    with DistributedSimulator(3,settings,comm=MPI.COMM_SELF) as target:
        migrated = checkpoint.migrate_stored_snapshot(target,source_config,snapshot)
        np.testing.assert_allclose(target.state(allow_gather=True),expected,atol=1e-12)
        assert target.frontier == snapshot.frontier and target.circuit_hash == snapshot.circuit_hash
        assert target.binding.values == snapshot.binding_values
        assert any(entry['kind']=='layout' for entry in target.history)
        observable = ObservableIR((PauliTerm.from_string('IIZ'),))
        gradient = target.gradient(observable,parameters=(theta,),binding=target.binding)
        assert gradient[theta.parameter_id] == pytest.approx(-np.sin(.31),abs=1e-10)
        assert migrated['root'] != snapshot.root
    assert catalog.read_bytes() == before


def test_phase1_catalog_migration_retains_legacy_relative_phase(tmp_path):
    import sqlite3
    from svsim.stored_runtime import StoredSimulator
    from svsim.storage import StorageConfig,StoredBlockView
    source_config = StorageConfig(str(tmp_path/'legacy'),1,ram_budget_bytes=4*1024**2,
                                  metadata_reserve_bytes=256*1024)
    with StoredSimulator(3,source_config) as source:
        word=[]
        for _ in range(3):
            word.extend((source.gate('H',(0,)),source.gate('S',(0,))))
        frame_id = source.store.intern_frame(word)
        source.store.phase2=None
        source.store.publish(source.root,[(0,StoredBlockView('initial-one',5,5,frame_id))])
        source.store.phase2=source.phase2
        source.frontier=5; source.binding=source.bind()
        snapshot = replace(source.commit_snapshot(),schema_version=1,semantics_version=1)
        source.store.db.execute('UPDATE snapshots SET data=? WHERE id=?',
                                (checkpoint.canonical(asdict(snapshot)),snapshot.snapshot_id))
        source.store.db.commit()
    catalog = Path(source_config.directory)/'metadata/catalog.sqlite3'
    db = sqlite3.connect(catalog)
    for table in ('root_edges','frames_v2','views_v2','root_log_bounds','catalog_checksums'):
        db.execute(f'DROP TABLE {table}')
    db.execute('PRAGMA user_version=1'); db.commit(); db.close()
    before = catalog.read_bytes()
    with DistributedSimulator(3,config(tmp_path/'migrated'),comm=MPI.COMM_SELF) as target:
        checkpoint.migrate_stored_snapshot(target,source_config,snapshot)
        expected = np.zeros(8,dtype=complex); expected[0]=np.exp(1j*np.pi/4)
        np.testing.assert_allclose(target.state(allow_gather=True),expected,atol=1e-12)
    assert catalog.read_bytes() == before


def test_failed_migration_global_publication_restores_empty_target(tmp_path,monkeypatch):
    from svsim.stored_runtime import StoredSimulator
    from svsim.storage import StorageConfig
    source_config=StorageConfig(str(tmp_path/'source'),1,ram_budget_bytes=4*1024**2,
                                metadata_reserve_bytes=256*1024)
    with StoredSimulator(3,source_config) as source:
        source.gate('H',(0,)); source.materialize(binding=source.bind())
        snapshot=source.commit_snapshot()
    with DistributedSimulator(3,config(tmp_path/'target'),comm=MPI.COMM_SELF) as target:
        original_root=target.root
        original=checkpoint.atomic_json

        def fail(path,data):
            if '/global/manifests/' in str(path): raise OSError('injected migration publication failure')
            original(path,data)

        with monkeypatch.context() as scope:
            scope.setattr(checkpoint,'atomic_json',fail)
            with pytest.raises(CollectiveError,match='publication'):
                checkpoint.migrate_stored_snapshot(target,source_config,snapshot)
        assert target.root == original_root and target.frontier == -1
        assert not target.sequencer.operations
        assert target.store.db.execute('SELECT count(*) FROM snapshots').fetchone()[0] == 0
        # A retry uses the same source cut without modifying its catalog.
        checkpoint.migrate_stored_snapshot(target,source_config,snapshot)
        assert target.frontier == snapshot.frontier
