import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path
import numpy as np
import pytest
from dataclasses import replace

from svsim.storage import StorageConfig
from svsim.stored_runtime import StoredSimulator
from svsim.tile_transpose import execute_tile_swap
from svsim.cache import BufferLease

_mpi_available = bool(shutil.which('mpiexec')) and importlib.util.find_spec('mpi4py') is not None
_mpi_test_timeout = int(os.environ.get('DDQ_MPI_TEST_TIMEOUT', '180'))
if not 1 <= _mpi_test_timeout <= 1800:
    raise ValueError('DDQ_MPI_TEST_TIMEOUT must be in 1..1800 seconds')


def _run_mpi_tile_worker(tmp_path, ranks, mode):
    env = dict(os.environ, PYTHONPATH='src')
    env.setdefault('OMPI_ALLOW_RUN_AS_ROOT', '1')
    env.setdefault('OMPI_ALLOW_RUN_AS_ROOT_CONFIRM', '1')
    subprocess.run([shutil.which('mpiexec'), '-n', str(ranks), sys.executable,
                    str(Path(__file__).with_name('mpi_tile_transpose_worker.py')), str(tmp_path), mode],
                   check=True, env=env, timeout=_mpi_test_timeout)


def cfg(path, block_bits=3, ram=1 << 20, backend='buffered', alignment=64):
    return StorageConfig(str(path), block_bits, ram_budget_bytes=ram,
                         metadata_reserve_bytes=64 * 1024, alignment=alignment, backend=backend)


@pytest.mark.parametrize('backend', ['buffered', 'direct'])
@pytest.mark.parametrize('pairs', [[(0, 1)], [(0, 4)], [(4, 5)], [(0, 4), (1, 5)]])
def test_tile_swap_matches_index_oracle(tmp_path, pairs, backend):
    initial = np.arange(1 << 7, dtype=np.complex128) + 1j
    alignment = 4096 if backend == 'direct' and sys.platform.startswith('linux') else 64
    with StoredSimulator(7, cfg(tmp_path/'sim', backend=backend, alignment=alignment), initial_state=initial) as sim:
        identity = sim.store._file(0).backend_identity
        expected = ('linux-o-direct' if sys.platform.startswith('linux') else 'macos-f-nocache') if backend == 'direct' else 'buffered'
        assert identity == expected
        execute_tile_swap(sim, pairs, tile_bytes=alignment)
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


@pytest.mark.skipif(not _mpi_available, reason='Local MPI runtime unavailable')
@pytest.mark.slow
@pytest.mark.parametrize('ranks,pairs', [
    (2, [(3, 2)]), (2, [(3, 0)]), (2, [(3, 2), (2, 0)]), (2, [(0, 1)]),
    (4, [(3, 2)]), (4, [(3, 0)]), (4, [(3, 2), (2, 0)]),
])
def test_async_distributed_p_gt_1_tile_route_matches_oracle(tmp_path, ranks, pairs):
    # Real MPI fragment communication: the route updates physical coordinates and
    # the layout, so logical state, expectation and the inverse all match.
    _run_mpi_tile_worker(tmp_path, ranks, repr(pairs))


@pytest.mark.skipif(not _mpi_available, reason='Local MPI runtime unavailable')
@pytest.mark.slow
def test_async_distributed_tile_route_fits_when_full_block_does_not(tmp_path):
    # The distributed route keeps one destination tile plus one source fragment
    # resident; it must complete when the full block buffer does not fit.
    _run_mpi_tile_worker(tmp_path, 2, 'bounded')
