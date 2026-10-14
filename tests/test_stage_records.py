"""Frame resolve, packing and catalog/checkpoint commits are separate completion stages."""
import os
import shutil
import subprocess
import sys
import textwrap

import pytest

MPI = pytest.importorskip('mpi4py.MPI')

from svsim.async_scheduler import AsyncDistributedSimulator, AsyncPolicy
from svsim.distributed import DistributedConfig


def _sim(path, frame='keep'):
    policy = AsyncPolicy(io_engine='cooperative', io_workers=1, prefetch_blocks=0)
    config = DistributedConfig(str(path), 2, ram_budget_bytes_per_rank=8 * 1024**2, storage_scope='node-local',
                               frame_policy=frame)
    sim = AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy)
    sim.runtime.configure_measurements(max_bytes=64 * 1024)  # the default 8 KiB cap would drop later stages
    return sim


def _stages(sim):
    return [row for row in sim.runtime.measurements.snapshot()['observations']]


def test_catalog_commit_frame_resolve_and_checkpoint_stages_are_recorded_as_children(tmp_path):
    with _sim(tmp_path / 'data') as sim:
        sim.gate('H', (0,)); sim.gate('S', (1,)); sim.gate('RX', (0,), angle=.3)
        sim.materialize(binding=sim.bind())
        sim.state(allow_gather=True)
        sim.commit_snapshot()
        rows = _stages(sim)
        by_stage = {}
        for row in rows: by_stage.setdefault(row['stage'], []).append(row)
        assert {'catalog-commit', 'checkpoint-prepare', 'checkpoint-decision', 'checkpoint-ack'} <= set(by_stage)
        commits = by_stage['catalog-commit']
        assert all(r['parent_id'] and r['parent_id'].endswith(':children') for r in commits)  # inside a gate/epoch group
        assert all(r['validated'] and r['evidence'] == 'measured' and r['protocol_seconds'] >= 0 for r in commits)
        assert all(r['condition']['durability'] == 'synchronous-full' for r in commits)
        if 'frame-resolve' in by_stage:
            assert all(r['condition']['frame'] in {'clifford', 'phase-only'} for r in by_stage['frame-resolve'])
        groups = {r['event_id'] for r in rows if r['stage'] in {'executor-gate', 'executor-group', 'executor-epoch'}}
        consumed = set().union(*(sim.runtime.measurements.consumed_children(g + ':children') for g in groups))
        assert {r['event_id'] for r in commits} <= consumed  # a consumer never re-adds a stage to its group


def test_stage_records_follow_collection_switch_and_injection_scope(tmp_path):
    with _sim(tmp_path / 'data') as sim:
        sim.runtime.configure_measurements(max_bytes=64 * 1024, enabled=False)
        sim.gate('RX', (0,), angle=.3); sim.materialize(binding=sim.bind())
        assert sim.runtime.measurements.snapshot()['observations'] == []
        sim.runtime.configure_measurements(max_bytes=64 * 1024, enabled=True)
        sim._measurement_injected = True
        sim.gate('RX', (1,), angle=.3); sim.materialize(binding=sim.bind())
        rows = [r for r in _stages(sim) if r['stage'] == 'catalog-commit']
        assert rows and all(r['evidence'] == 'injected' for r in rows)
        assert not any(r['stage'] == 'catalog-commit' for r in
                       (o.to_dict() for o in sim.runtime.measurements.cost_observations()))


def test_epoch_boundary_spills_raw_lines_under_a_disk_lease_and_summarizes_ranks(tmp_path):
    import json
    with _sim(tmp_path / 'data', frame='materialize') as sim:
        for _ in range(6): sim.gate('RX', (0,), angle=.3)
        sim.materialize(binding=sim.bind())
        runtime = sim.runtime
        spill = runtime._raw_spill
        lines = spill['path'].read_bytes().splitlines()
        assert lines and all(json.loads(line)['event_id'] for line in lines)
        assert runtime.raw_spill_bytes == spill['path'].stat().st_size and not runtime.raw_spill_truncated
        budget = sim.disk_budget or sim.store.disk_budget
        assert budget.lease_reserved_bytes > 0  # the spill writes under its own append lease
        summary = runtime.epoch_summaries[-1]
        assert summary['ranks'] == 1 and 'executor-gate' in summary['stages']
        gate = summary['stages']['executor-gate']
        assert gate['rank_max_seconds'] >= gate['rank_mean_seconds'] > 0 and gate['imbalance'] == 1.0
        sim.gate('RX', (1,), angle=.3); sim.materialize(binding=sim.bind())
        stages = {json.loads(line)['stage'] for line in spill['path'].read_bytes().splitlines()}
        assert {'monitor-raw-flush', 'monitor-summary-exchange'} <= stages  # the monitor costs itself (next spill)
        runtime.RAW_SPILL_LIMIT_BYTES = runtime.raw_spill_bytes  # total limit reached: drop and count
        sim.gate('RX', (1,), angle=.3); sim.materialize(binding=sim.bind())
        assert runtime.raw_spill_truncated and runtime.measurements.dropped_raw > 0
        assert spill['path'].stat().st_size == runtime.raw_spill_bytes
