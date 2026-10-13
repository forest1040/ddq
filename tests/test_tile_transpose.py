import numpy as np
import pytest
from dataclasses import replace

from svsim.storage import StorageConfig
from svsim.stored_runtime import StoredSimulator
from svsim.tile_transpose import execute_tile_swap
from svsim.cache import BufferLease


def cfg(path, block_bits=3, ram=1 << 20, backend='buffered'):
    return StorageConfig(str(path), block_bits, ram_budget_bytes=ram,
                         metadata_reserve_bytes=64 * 1024, alignment=64, backend=backend)


@pytest.mark.parametrize('backend', ['buffered', 'direct'])
@pytest.mark.parametrize('pairs', [[(0, 1)], [(0, 4)], [(4, 5)], [(0, 4), (1, 5)]])
def test_tile_swap_matches_index_oracle(tmp_path, pairs, backend):
    initial = np.arange(1 << 7, dtype=np.complex128) + 1j
    with StoredSimulator(7, cfg(tmp_path/'sim', backend=backend), initial_state=initial) as sim:
        identity = sim.store._file(0).backend_identity
        assert identity == ('macos-f-nocache' if backend == 'direct' else 'buffered')
        execute_tile_swap(sim, pairs, tile_bytes=64)
        # The route changes physical coordinates and updates the layout, so
        # logical state remains identical to the input.
        np.testing.assert_array_equal(sim.state(), initial)


def test_tile_swap_rejects_deferred_frame_before_output(tmp_path):
    with StoredSimulator(6, cfg(tmp_path/'sim')) as sim:
        before = sim.root
        allocator = sim.store.db.execute("SELECT data FROM settings WHERE key='allocator'").fetchone()[0]
        view = sim.store.view(before, 0)
        sim.store.publish(before, ((0, replace(view, base_log=({'kind':'rotation'},))),))
        with pytest.raises(ValueError, match='UNSUPPORTED_FRAME_TILE_ROUTE'):
            execute_tile_swap(sim, [(0, 4)], tile_bytes=64)
        assert sim.root == before
        assert sim.store.db.execute("SELECT data FROM settings WHERE key='allocator'").fetchone()[0] == allocator


def test_tile_transpose_refuses_non_aligned_tile(tmp_path):
    with StoredSimulator(6, cfg(tmp_path/'sim')) as sim:
        with pytest.raises(ValueError, match='alignment'):
            execute_tile_swap(sim, [(0, 4)], tile_bytes=80)


def test_tile_route_short_source_read_preserves_old_cut(tmp_path, monkeypatch):
    with StoredSimulator(6, cfg(tmp_path/'sim'), initial_state=np.ones(64, dtype=complex)) as sim:
        sim.cache.flush()
        sim.cache.clear()
        old_root, old_layout = sim.root, sim.layout
        original = sim.store._file
        handle = original(0)

        class ShortReader:
            backend_identity = handle.backend_identity
            def read_into(self, offset, buffer): return 0
            def __getattr__(self, name): return getattr(handle, name)

        monkeypatch.setattr(sim.store, '_file', lambda segment: ShortReader())
        with pytest.raises(OSError, match='Short source checksum'):
            execute_tile_swap(sim, [(0, 4)], tile_bytes=64)
        assert sim.root == old_root
        assert sim.layout == old_layout


def test_tile_route_ram_admission_failure_preserves_old_cut(tmp_path):
    with StoredSimulator(6, cfg(tmp_path/'sim')) as sim:
        old_root, old_layout = sim.root, sim.layout
        sim.budget.limit = sim.budget.used
        with pytest.raises(MemoryError):
            execute_tile_swap(sim, [(0, 4)], tile_bytes=64)
        assert sim.root == old_root
        assert sim.layout == old_layout


@pytest.mark.parametrize('block_bits', [13, 14])
def test_tile_route_fits_when_full_block_does_not(tmp_path, block_bits):
    initial = np.ones(1 << block_bits, dtype=np.complex128)
    with StoredSimulator(block_bits, cfg(tmp_path/'sim', block_bits, ram=1 << 20),
                         initial_state=initial) as sim:
        sim.cache.flush()
        sim.cache.clear()
        metadata = sim.config.metadata_reserve_bytes
        small_tile_charge = (BufferLease.allocation_cost(4096, sim.config.alignment)
                             + BufferLease.allocation_cost(sim.config.alignment, sim.config.alignment))
        sim.budget.limit = metadata + small_tile_charge
        assert sim.store.slot_bytes > sim.budget.limit - metadata
        execute_tile_swap(sim, [(0, block_bits-1)], tile_bytes=4096)
        sim.budget.limit = 1 << 20
        np.testing.assert_array_equal(sim.state(), initial)


def test_async_distributed_p1_tile_route_state_expectation_and_inverse(tmp_path):
    MPI = pytest.importorskip('mpi4py.MPI')
    from svsim.async_scheduler import AsyncDistributedSimulator, AsyncPolicy
    from svsim.distributed import DistributedConfig
    from svsim.observables import ObservableIR, PauliTerm

    rng = np.random.default_rng(818)
    initial = rng.normal(size=16) + 1j*rng.normal(size=16)
    initial /= np.linalg.norm(initial)
    config = DistributedConfig(str(tmp_path/'async'), 2,
        ram_budget_bytes_per_rank=2*1024**2, metadata_reserve_bytes=128*1024,
        tile_bytes=64, alignment=64)
    policy = AsyncPolicy(io_engine='cooperative', prefetch_blocks=0, lookahead_gates=2)
    with AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF,
                                   initial_state=initial, policy=policy) as sim:
        before_layout = sim.layout.logical_to_physical
        expectation = sim.expectation(ObservableIR((PauliTerm(0, 1),)))
        sim.tile_index_bit_swap([(0, 3), (1, 2)], tile_bytes=64)
        assert sim.size == 1
        assert len(sim.history) == 2
        np.testing.assert_allclose(sim.state(allow_gather=True), initial, atol=1e-12, rtol=1e-12)
        assert sim.expectation(ObservableIR((PauliTerm(0, 1),))) == pytest.approx(expectation)
        sim.tile_index_bit_swap([(1, 2), (0, 3)], tile_bytes=64)
        assert sim.layout.logical_to_physical == before_layout
        assert len(sim.history) == 4
        np.testing.assert_allclose(sim.state(allow_gather=True), initial, atol=1e-12, rtol=1e-12)


def test_async_public_tile_route_rejects_p_gt_1_before_output(tmp_path):
    MPI = pytest.importorskip('mpi4py.MPI')
    from svsim.async_scheduler import AsyncDistributedSimulator, AsyncPolicy
    from svsim.distributed import DistributedConfig

    config = DistributedConfig(str(tmp_path/'async-reject'), 2,
        ram_budget_bytes_per_rank=2*1024**2, metadata_reserve_bytes=128*1024,
        tile_bytes=64, alignment=64)
    policy = AsyncPolicy(io_engine='cooperative', prefetch_blocks=0, lookahead_gates=2)
    with AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy) as sim:
        root, layout = sim.root, sim.layout
        allocator = sim.store.db.execute("SELECT data FROM settings WHERE key='allocator'").fetchone()[0]
        roots = sim.store.db.execute('SELECT count(*) FROM roots').fetchone()[0]
        actual_size = sim.size
        sim.size = 2  # Exercise the public capability guard without launching MPI ranks.
        try:
            with pytest.raises(ValueError, match='UNSUPPORTED_TILE_DISTRIBUTED'):
                sim.tile_index_bit_swap([(0, 3)], tile_bytes=64)
        finally:
            sim.size = actual_size
        assert sim.root == root
        assert sim.layout == layout
        assert sim.store.db.execute("SELECT data FROM settings WHERE key='allocator'").fetchone()[0] == allocator
        assert sim.store.db.execute('SELECT count(*) FROM roots').fetchone()[0] == roots
