"""Migration and long-lived catalog representation regressions."""
from dataclasses import asdict
import json
import numpy as np
import pytest
from svsim import StoredSimulator,StorageConfig,ExactPi,ParameterSymbol,CliffordTableau
from svsim.storage import StoredBlockView
from svsim.journal import canonical
from svsim.kernels import eager_reference


def cfg(path): return StorageConfig(str(path),2,ram_budget_bytes=1 << 20,metadata_reserve_bytes=128*1024,alignment=64)


def test_log_capacity_survives_compaction_snapshot_and_extension(tmp_path):
    config = cfg(tmp_path/'long')
    with StoredSimulator(4,config,frame_policy='keep') as sim:
        for i in range(sim.phase2.max_log): sim.gate('RZ' if i&1 else 'RX',(0,),angle=.17)
        sim.materialize(); sim.compact_metadata()
        sim.gate('RZ',(0,),angle=.23)
        sim.materialize()
        assert len(sim.block_view(0).base_log) <= sim.phase2.max_log
        snapshot = sim.commit_snapshot()
        expected = eager_reference(sim.sequencer.operations,4,sim.assets)
        np.testing.assert_allclose(sim.state(),expected,atol=1e-12)
    with StoredSimulator.resume(config,snapshot) as run:
        np.testing.assert_allclose(run.state(),expected,atol=1e-12)


def test_v1_word_frame_migration_keeps_extent_and_complex_phase(tmp_path):
    config = cfg(tmp_path/'legacy')
    vector = np.arange(16,dtype=complex)+2j
    with StoredSimulator(4,config,initial_state=vector) as sim:
        gate = sim.normalizer.gate('H',(0,))
        legacy = sim.store.intern_frame((gate,))
        root = sim.store.new_root()
        originals = []
        for h in range(4):
            view = sim.store.view(sim.root,h); originals.append(view.version_id)
            sim.store.publish(root,((h,StoredBlockView(view.version_id,frame_id=legacy,phase=(.3+.7j) if h&1 else 1)),))
        sim.root = root
        snapshot = sim.commit_snapshot()
        expected = sim.state()
        # v1 manifests lack the new fields and use the original sequence table.
        data = asdict(snapshot); data['schema_version'] = 1
        data.pop('binding_witnesses'); data.pop('semantics_version')
        sim.store.db.execute('UPDATE snapshots SET data=? WHERE id=?',(canonical(data),snapshot.snapshot_id)); sim.store.db.commit()
        sim.store.db.execute('DELETE FROM frames_v2'); sim.store.db.commit()
    with StoredSimulator.resume(config,snapshot.snapshot_id) as run:
        assert tuple(run.store.view(snapshot.root,h).version_id for h in range(4)) == tuple(originals)
        np.testing.assert_allclose(run.state(),expected,atol=1e-12)
        assert run.trace.write_bytes == 0
        assert run.store.db.execute('SELECT count(*) FROM frames_v2').fetchone()[0] > 0


def test_virtual_permutation_predicates_follow_source_labels_and_gc(tmp_path):
    config = cfg(tmp_path/'permutation')
    initial = np.arange(16,dtype=complex)+1j
    with StoredSimulator(4,config,initial_state=initial,frame_policy='keep') as sim:
        sim.gate('H',(0,),controls=(3,),control_values=(0,))
        sim.gate('PAULI_ROT',(0,3),paulis=('Y','Z'),angle=.3)
        sim.gate('X',(3,))
        sim.gate('S',(1,),controls=(3,),control_values=(1,))
        sim.gate('CX',(3,2))
        sim.gate('H',(3,))
        expected = eager_reference(sim.sequencer.operations,4,sim.assets,initial_state=initial)
        snapshot = sim.materialize().commit_snapshot()
        sim.collect_garbage()
        np.testing.assert_allclose(sim.state(),expected,atol=1e-12)
    with StoredSimulator.resume(config,snapshot) as run:
        np.testing.assert_allclose(run.state(),expected,atol=1e-12)


def test_explicit_base_log_flush_retains_frame_and_is_idempotent(tmp_path):
    with StoredSimulator(4,cfg(tmp_path/'flush'),frame_policy='keep') as sim:
        sim.gate('H',(0,)); sim.gate('T',(0,)); sim.gate('S',(1,)); sim.materialize()
        frame = sim.block_view(0).frame_asset_id
        expected = sim.state()
        sim.flush_base_logs()
        assert not sim.block_view(0).base_log
        assert sim.block_view(0).frame_asset_id == frame
        sim.flush_base_logs()
        np.testing.assert_allclose(sim.state(),expected,atol=1e-12)


def test_unsupported_catalog_does_not_add_tables(tmp_path):
    import sqlite3
    config = cfg(tmp_path/'future')
    with StoredSimulator(4,config): pass
    db = sqlite3.connect(tmp_path/'future/metadata/catalog.sqlite3')
    db.execute('DROP TABLE frames_v2'); db.execute('PRAGMA user_version=99'); db.commit(); db.close()
    with pytest.raises(OSError,match='schema'): StoredSimulator.resume(config,'nonexistent')
    db = sqlite3.connect(tmp_path/'future/metadata/catalog.sqlite3')
    assert db.execute("SELECT name FROM sqlite_master WHERE name='frames_v2'").fetchone() is None
    db.close()


def test_metadata_only_work_needs_no_amplitude_reservation(tmp_path):
    config = StorageConfig(str(tmp_path/'metadata'),24,ram_budget_bytes=192*1024,metadata_reserve_bytes=128*1024,alignment=64)
    with StoredSimulator(50,config,frame_policy='keep') as sim:
        sim.gate('H',(0,)); sim.gate('X',(49,)); sim.gate('T',(0,),controls=(49,)); sim.materialize()
        assert sim.budget.peak == config.metadata_reserve_bytes
        assert sim.store.db.execute('SELECT count(*) FROM views').fetchone()[0] == 0
        assert sim.block_view(1 << 25).version_id == 'initial-one'
        with pytest.raises(MemoryError): sim.state()


def test_catalog_corruption_prevents_snapshot_publication_and_resume(tmp_path):
    config = cfg(tmp_path/'corrupt')
    with StoredSimulator(4,config,frame_policy='keep') as sim:
        sim.gate('H',(0,)); sim.gate('T',(0,)); sim.materialize()
        snapshot = sim.commit_snapshot()
        edge = sim.store.db.execute('SELECT root,data FROM root_edges LIMIT 1').fetchone()
        data = json.loads(edge[1]); data['frontier'] += 1
        sim.store.db.execute('UPDATE root_edges SET data=? WHERE root=?',(canonical(data),edge[0])); sim.store.db.commit()
        with pytest.raises(OSError,match='checksum'): sim.commit_snapshot()
        assert sim.store.db.execute('SELECT count(*) FROM snapshots').fetchone()[0] == 1
    with pytest.raises(OSError,match='checksum'): StoredSimulator.resume(config,snapshot)


def test_forced_alignment_rejects_lower_control_without_publishing(tmp_path):
    with StoredSimulator(4,cfg(tmp_path/'illegal'),alignment_policy='input-a') as sim:
        sim.gate('H',(0,)); sim.gate('H',(3,),controls=(0,))
        root = sim.root
        with pytest.raises(ValueError,match='not legal'): sim.materialize()
        assert sim.root == root and sim.frontier == -1
