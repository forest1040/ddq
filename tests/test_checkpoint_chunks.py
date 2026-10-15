"""Checkpoint payloads keep gates/history as write-once content-addressed chunks (format 2)."""
from pathlib import Path

import numpy as np
import pytest

MPI = pytest.importorskip('mpi4py.MPI')

import svsim.checkpoint_payload as payload
from svsim.distributed import DistributedConfig, DistributedSimulator
from svsim.distributed_checkpoint import load_global


def config(path):
    # node-local storage writes v4 checkpoints with streamed metadata payloads
    return DistributedConfig(str(path), 1, ram_budget_bytes_per_rank=16 * 1024**2,
                             metadata_reserve_bytes=2 * 1024**2, storage_scope='node-local')


def chunks(directory):
    return sorted((Path(directory) / 'rank-0000/manifests').glob('chunk-*.json'))


def append(sim, count, offset=0):
    for index in range(count): sim.gate('RX', ((index + offset) % 3,), angle=.001 * (index + offset))


def test_checkpoints_write_only_new_chunks_and_resume_exactly(tmp_path):
    settings = config(tmp_path / 'state')
    with DistributedSimulator(3, settings, comm=MPI.COMM_SELF) as sim:
        append(sim, 2 * payload.CHUNK_ENTRIES + 10)
        sim.materialize(binding=sim.bind())
        first = sim.commit_snapshot()
        assert first['payload_format'] == 2
        written = set(chunks(settings.directory))
        # gates: 2 full + tail, history: 2 full + tail
        assert len(written) == 6
        append(sim, 20, offset=10_000)
        sim.materialize(binding=sim.bind())
        second = sim.commit_snapshot()
        new = set(chunks(settings.directory)) - written
        assert len(new) == 2  # only the two changed tails; full chunks are reused, not rewritten
        expected = sim.state(allow_gather=True).copy()
        operations = [g for g in sim.sequencer.operations]
        loaded = load_global(settings.directory)
        assert loaded['snapshot_id'] == second['snapshot_id'] and len(loaded['gates']) == len(operations)
        assert len(loaded['history']) == len(sim.history)
    with DistributedSimulator.resume(settings, comm=MPI.COMM_SELF) as restored:
        np.testing.assert_allclose(restored.state(allow_gather=True), expected, atol=1e-12)
        assert [g.op_id for g in restored.sequencer.operations] == [g.op_id for g in operations]
        # A checkpoint after resume reuses the chunks it loaded and writes nothing for the unchanged prefix.
        before = set(chunks(settings.directory))
        restored.commit_snapshot()
        assert len(set(chunks(settings.directory)) - before) <= 2


def test_tampered_chunk_is_refused(tmp_path):
    settings = config(tmp_path / 'state')
    with DistributedSimulator(3, settings, comm=MPI.COMM_SELF) as sim:
        append(sim, payload.CHUNK_ENTRIES + 3)
        sim.materialize(binding=sim.bind())
        sim.commit_snapshot()
    target = chunks(settings.directory)[0]
    raw = bytearray(target.read_bytes())
    raw[-3] = ord('9') if raw[-3] != ord('9') else ord('8')
    target.write_bytes(bytes(raw))
    with pytest.raises(Exception):
        load_global(settings.directory)


def test_format1_checkpoints_still_load_and_resume(tmp_path, monkeypatch):
    settings = config(tmp_path / 'state')
    original = payload.distribute_payload

    def legacy(transport, data, directory, *, chunked, expand=True, chunk_cache=None, sources=None, **kwargs):
        return original(transport, data, directory, chunked=False, **kwargs)  # pre-chunk payload
    with DistributedSimulator(3, settings, comm=MPI.COMM_SELF) as sim:
        append(sim, 40)
        sim.materialize(binding=sim.bind())
        with monkeypatch.context() as scope:
            scope.setattr(payload, 'distribute_payload', legacy)
            sim.commit_snapshot()
        expected = sim.state(allow_gather=True).copy()
    assert not chunks(settings.directory)
    loaded = load_global(settings.directory)
    assert 'payload_format' not in loaded and len(loaded['gates']) == 40
    with DistributedSimulator.resume(settings, comm=MPI.COMM_SELF) as restored:
        np.testing.assert_allclose(restored.state(allow_gather=True), expected, atol=1e-12)
        assert restored.commit_snapshot()['payload_format'] == 2  # new checkpoints of a resumed run use chunks
    assert len(load_global(settings.directory)['gates']) == 40
