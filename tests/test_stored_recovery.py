import sqlite3

import numpy as np
import pytest

from svsim.parameters import ParameterSymbol
from svsim.storage import StorageConfig
from svsim.stored_runtime import StoredSimulator
from svsim.transport import StorageFile


def config(path):
    return StorageConfig(str(path), block_bits=3, ram_budget_bytes=512 * 1024,
                         metadata_reserve_bytes=64 * 1024, alignment=64)


def test_snapshot_resume_preserves_pending_frame_and_upper_phase(tmp_path):
    cfg = config(tmp_path / "run")
    sim = StoredSimulator(8, cfg)
    sim.gate("H", (7,))
    sim.gate("H", (0,))
    sim.gate("RZ", (7,), angle=0.37)
    sim.materialize()
    expected = sim.state()
    assert any(sim.block_view(h).frame_id for h in range(sim.store.num_blocks))
    assert len({sim.block_view(h).phase for h in range(sim.store.num_blocks)}) > 1
    snapshot = sim.commit_snapshot()
    sim.close()

    resumed = StoredSimulator.resume(cfg, snapshot)
    try:
        np.testing.assert_allclose(resumed.state(), expected, rtol=0, atol=1e-13)
    finally:
        resumed.close()


def test_interleaved_binding_forks_preserve_prefix_extents(tmp_path):
    cfg = config(tmp_path / "run")
    alpha, theta = ParameterSymbol("alpha"), ParameterSymbol("theta")
    sim = StoredSimulator(8, cfg)
    sim.gate("RZ", (0,), angle=alpha)
    sim.gate("RY", (1,), angle=theta)
    base_binding = sim.bind({alpha: 0.2, theta: -0.3})
    prefix = sim.commit_prefix((theta,), binding=base_binding)
    prefix_state = sim.state()
    # Commit-prefix stops before the variable suffix; its root and extents stay immutable.
    prefix_versions = tuple(sim.store.view(prefix.root, h).version_id for h in range(sim.store.num_blocks))
    bind_a = sim.bind({alpha: 0.2, theta: 0.4})
    bind_b = sim.bind({alpha: 0.2, theta: -0.8})
    run_a = sim.fork(prefix, binding=bind_a)
    run_b = sim.fork(prefix, binding=bind_b)
    try:
        state_a = run_a.state()
        state_b = run_b.state()
        snap_a = run_a.commit_snapshot()
        snap_b = run_b.commit_snapshot()
        extent_owners = {}
        for owner, run, snap in (("a", run_a, snap_a), ("b", run_b, snap_b)):
            for h in range(run.store.num_blocks):
                version = run.store.view(snap.root, h).version_id
                ref = run.store.ref(version)
                if ref.offset is None: continue
                extent_owners.setdefault((ref.segment, ref.offset), set()).add((owner, version))
        for owners in extent_owners.values():
            versions = {version for _, version in owners}
            assert len(versions) == 1, "fork allocator reused an immutable extent"
        np.testing.assert_allclose(sim.state(), prefix_state, rtol=0, atol=1e-13)
        assert not np.allclose(state_a, state_b)
        assert tuple(sim.store.view(prefix.root, h).version_id for h in range(sim.store.num_blocks)) == prefix_versions
        assert run_a.binding.binding_id != run_b.binding.binding_id
        # A later fork must not reuse the first fork's immutable storage extents.
        reopened_a = StoredSimulator.resume(cfg, snap_a)
        try:
            np.testing.assert_allclose(reopened_a.state(), state_a, rtol=0, atol=1e-13)
        finally:
            reopened_a.close()
    finally:
        run_a.close(); run_b.close(); sim.close()


def test_fork_rejects_changed_fixed_parameter_and_foreign_snapshot(tmp_path):
    alpha, theta = ParameterSymbol("alpha"), ParameterSymbol("theta")
    cfg = config(tmp_path / "run")
    sim = StoredSimulator(8, cfg)
    sim.gate("RZ", (0,), angle=alpha)
    sim.gate("RY", (1,), angle=theta)
    binding = sim.bind({alpha: 0.2, theta: 0.1})
    prefix = sim.commit_prefix((theta,), binding=binding)
    with pytest.raises(ValueError, match="fixed-parameter"):
        sim.fork(prefix, binding=sim.bind({alpha: 0.9, theta: 0.3}))
    foreign_cfg = config(tmp_path / "other")
    foreign = StoredSimulator(8, foreign_cfg)
    foreign.gate("H", (0,))
    foreign_snapshot = foreign.commit_snapshot()
    with pytest.raises(ValueError, match="not committed"):
        sim.fork(foreign_snapshot, binding=binding)
    foreign.close(); sim.close()


def test_resume_rejects_corrupt_committed_journal_and_asset(tmp_path):
    cfg = config(tmp_path / "journal-run")
    sim = StoredSimulator(8, cfg)
    sim.gate("H", (0,))
    snapshot = sim.commit_snapshot()
    journal = cfg.directory + "/journal/gates.jsonl"
    sim.close()
    with open(journal, "r+b") as stream:
        first = stream.read(1)
        stream.seek(0); stream.write(b"X" if first != b"X" else b"Y")
    with pytest.raises(OSError, match="Corrupt journal"):
        StoredSimulator.resume(cfg, snapshot)

    asset_cfg = config(tmp_path / "asset-run")
    asset_sim = StoredSimulator(8, asset_cfg)
    asset_sim.gate("DENSE_UNITARY", (0,), matrix=np.eye(2, dtype=np.complex128))
    asset_snapshot = asset_sim.commit_snapshot()
    asset_id = next(iter(asset_sim.assets.assets))
    asset_path = tmp_path / "asset-run" / "assets" / f"{asset_id}.bin"
    asset_sim.close()
    asset_path.write_bytes(b"bad asset")
    with pytest.raises(OSError, match="asset|shape|checksum"):
        StoredSimulator.resume(asset_cfg, asset_snapshot)


def test_resume_detects_truncated_committed_extent(tmp_path):
    cfg = config(tmp_path / "run")
    sim = StoredSimulator(8, cfg)
    sim.gate("H", (7,))
    sim.materialize()
    snapshot = sim.commit_snapshot()
    refs = [sim.store.ref(sim.store.view(snapshot.root, h).version_id)
            for h in range(sim.store.num_blocks)]
    ref = next(ref for ref in refs if ref.segment is not None)
    segment = tmp_path / "run" / "state" / f"segment-{ref.segment:06d}.bin"
    sim.close()
    segment.write_bytes(segment.read_bytes()[:ref.offset + ref.storage_bytes - 1])
    resumed = StoredSimulator.resume(cfg, snapshot)
    try:
        with pytest.raises(OSError, match="Truncated|checksum"):
            resumed.state()
    finally:
        resumed.close()


@pytest.mark.parametrize("failure", ["write_from", "flush"])
def test_failed_snapshot_commit_leaves_previous_snapshot_resumable(tmp_path, failure):
    cfg = config(tmp_path / failure)
    faults = {"enabled": False}

    class FaultStorage(StorageFile):
        def write_from(self, offset, buffer):
            if faults["enabled"] and failure == "write_from": raise OSError("injected write failure")
            return super().write_from(offset, buffer)

        def flush(self):
            if faults["enabled"] and failure == "flush": raise OSError("injected flush failure")
            return super().flush()

    def factory(path, mode, **kwargs): return FaultStorage(path, mode, **kwargs)

    sim = StoredSimulator(8, cfg, transport_factory=factory)
    sim.gate("H", (0,))
    original = sim.commit_snapshot()
    expected = sim.state()
    sim.gate("RZ", (0,), angle=0.4)
    sim.materialize()
    faults["enabled"] = True
    with pytest.raises(OSError, match="injected"):
        sim.commit_snapshot()
    with sqlite3.connect(cfg.directory + "/metadata/catalog.sqlite3") as db:
        count = db.execute("SELECT count(*) FROM snapshots").fetchone()[0]
    assert count == 1
    faults["enabled"] = False
    sim.close()

    resumed = StoredSimulator.resume(cfg, original)
    try:
        np.testing.assert_allclose(resumed.state(), expected, rtol=0, atol=1e-13)
    finally:
        resumed.close()
