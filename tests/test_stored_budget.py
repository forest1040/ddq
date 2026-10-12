from dataclasses import replace
import numpy as np
import pytest
from svsim import StoredSimulator, StorageConfig, ObservableIR, PauliTerm, ParameterSymbol
from svsim.kernels import eager_reference
from svsim.benchmark import MockTransport
from svsim.telemetry import storage_condition, ObservePerformanceGuard
from svsim.lifecycle import HardwareProfiler, BenchmarkSample
from svsim.transport import open_storage


def test_state_larger_than_budget_executes_exports_and_streams(tmp_path):
    config = StorageConfig(str(tmp_path/'state'),6,ram_budget_bytes=98304,alignment=64,metadata_reserve_bytes=32768)
    with StoredSimulator(14,config) as sim:
        sim.gate('H',(0,)); sim.gate('H',(13,)); sim.gate('CX',(13,1)); sim.gate('RZ',(13,),angle=.7)
        sim.gate('T',(2,)); sim.gate('H',(13,)); sim.gate('MCX',(13,),controls=(0,1,12),control_values=(0,1,0))
        sim.gate('H',(0,))
        plan = sim.access_plan()
        expected = eager_reference(sim.sequencer.operations,14,sim.assets)
        sim.materialize()
        assert 16*2**14 > config.ram_budget_bytes
        assert sim.cache.evictions > 0 and sim.trace.write_bytes > 0 and sim.trace.read_bytes > 0
        assert sim.budget.peak <= config.ram_budget_bytes
        with pytest.raises(MemoryError): sim.state()
        output = sim.export_state(tmp_path/'logical.bin')
        np.testing.assert_allclose(np.fromfile(output,dtype='<c16'),expected,atol=2e-14)
        norm = sum(float(np.vdot(block,block).real) for _,block in sim.iter_blocks())
        assert norm == pytest.approx(1)
        assert sim.expectation(ObservableIR((PauliTerm(0,0),))) == pytest.approx(1)
        sim.commit_snapshot()
        # Plan is a conservative cold-cache bound for compute, excluding export/
        # observable rereads. It reports the complete state scan per source task.
        assert plan.read_bytes_upper >= 16*2**14
        assert sim.budget.peak <= config.ram_budget_bytes


def test_runtime_observe_uses_virtual_transport_and_never_replans(tmp_path):
    root = tmp_path/'state'
    root.mkdir()
    import os
    cond = storage_condition('write',128,backend='buffered',device_id=f'device-{os.stat(root).st_dev}',
                             alignment=64,pattern='sequential',flush='deferred')
    profile = HardwareProfiler([root]).discover()
    profile = replace(profile,benchmarks=(BenchmarkSample(cond,bandwidth_bytes_per_second=1000000),))
    def transport(*args,**kwargs):
        return MockTransport(open_storage(*args,**kwargs),bytes_per_second=1000000,delay_multipliers=(4,))
    config = StorageConfig(str(root),3,ram_budget_bytes=98304,alignment=64,metadata_reserve_bytes=32768)
    with StoredSimulator(8,config,profile=profile,transport_factory=transport) as sim:
        sim.gate('T',(0,)); sim.materialize()
        # Normal default guard remains observe-only. Short windows expedite this
        # deterministic integration test; no real sleeps are performed.
        sim.guard = ObservePerformanceGuard(profile,sim.plan,window_size=2,consecutive_windows=2)
        sim.store.guard = sim.guard
        before = sim.plan
        sim.cache.flush()
        events = list(sim.trace.iter_events())
        assert any(e.get('health_state') == 'DEGRADED' for e in events)
        assert sim.plan == before
        assert sim.guard.runtime_profile.version > 1


def test_failed_materialization_rolls_back_whole_logical_cut(tmp_path,monkeypatch):
    config = StorageConfig(str(tmp_path/'state'),3,alignment=64,ram_budget_bytes=196608)
    with StoredSimulator(8,config) as sim:
        sim.gate('H',(0,)); sim.gate('H',(7,)); sim.gate('T',(0,))
        root = sim.root
        original = sim._joint
        def failure(*args): raise OSError('injected joint failure')
        monkeypatch.setattr(sim,'_joint',failure)
        with pytest.raises(OSError,match='injected'): sim.materialize()
        assert sim.root == root and sim.frontier == -1
        monkeypatch.setattr(sim,'_joint',original)
        expected = eager_reference(sim.sequencer.operations,8,sim.assets)
        np.testing.assert_allclose(sim.state(),expected,atol=1e-14)


def test_gc_requires_quiescence_and_preserves_committed_snapshots(tmp_path):
    config = StorageConfig(str(tmp_path/'state'),3,alignment=64,segment_size_bytes=256)
    with StoredSimulator(8,config) as sim:
        sim.gate('H',(0,)); sim.gate('RY',(1,),angle=.3)
        sim.materialize()
        expected = sim.state()
        snap = sim.commit_snapshot()
        child = sim.fork(snap,binding=sim.bind())
        try:
            with pytest.raises(RuntimeError,match='single active'): sim.collect_garbage()
        finally: child.close()
        # An unreferenced orphan version occupies its own segment.
        from svsim.cache import BufferLease
        sim.cache.clear()
        orphan = BufferLease(sim.budget,config.slot_bytes,config.alignment)
        version = sim.store.new_version()
        sim.store.write_version(version,orphan.view)
        orphan.close()
        assert sim.collect_garbage() >= config.slot_bytes
        np.testing.assert_allclose(sim.state(),expected,atol=1e-14)
        with StoredSimulator.resume(config,snap) as restored:
            np.testing.assert_allclose(restored.state(),expected,atol=1e-14)


def test_uncommitted_journal_tail_repaired_before_next_checkpoint(tmp_path):
    config = StorageConfig(str(tmp_path/'state'),3,alignment=64)
    with StoredSimulator(8,config) as sim:
        sim.gate('H',(0,)); sim.gate('H',(7,))
        snap = sim.commit_snapshot()
    with open(tmp_path/'state/journal/commits.jsonl','ab') as stream: stream.write(b'{"unfinished":')
    with StoredSimulator.resume(config,snap) as resumed:
        resumed.materialize()
        expected = resumed.state()
        second = resumed.commit_snapshot()
    with StoredSimulator.resume(config,second) as restored:
        np.testing.assert_allclose(restored.state(),expected,atol=1e-14)


def test_failed_gate_registration_does_not_duplicate_semantic_ids(tmp_path,monkeypatch):
    config = StorageConfig(str(tmp_path/'state'),3,alignment=64)
    with StoredSimulator(8,config) as sim:
        original = sim.journal.append
        def append(kind,payload):
            if kind == 'shared': raise OSError('injected registration failure')
            return original(kind,payload)
        monkeypatch.setattr(sim.journal,'append',append)
        with pytest.raises(OSError): sim.gate('H',(0,))
        assert not sim.sequencer.operations
        monkeypatch.setattr(sim.journal,'append',original)
        assert sim.gate('H',(1,)).op_id == 0
        sim.materialize()
        snap = sim.commit_snapshot()
        expected = sim.state()
    with StoredSimulator.resume(config,snap) as resumed:
        np.testing.assert_allclose(resumed.state(),expected,atol=1e-14)
