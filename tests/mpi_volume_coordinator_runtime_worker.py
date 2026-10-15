"""Volume coordinator through the normal runtime on one or more same-host ranks.

Modes: ``run`` (independent datasets, fork, parameter-shift, joint refusal), ``mismatch`` (ranks on one volume
choose different coordinators), ``unsupported`` (one rank refused by its filesystem probe), ``recover``
(offline checkpoint recovery republishes the settled claim).
"""
import argparse
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
import platform
from uuid import uuid4

import numpy as np
from mpi4py import MPI

from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig, DistributedSimulator
from svsim import ObservableIR, ParameterSymbol, PauliTerm
from svsim.disk_budget import DiskAdmissionError, DiskBudget
from svsim.kernels import eager_reference
from svsim.mpi_transport import CollectiveError
from svsim.volume_coordinator import VolumeCoordinator

parser = argparse.ArgumentParser()
parser.add_argument('scratch')
parser.add_argument('mode')
parser.add_argument('--backend', default='buffered')
parser.add_argument('--require-cross-host', action='store_true')
args = parser.parse_args()
comm = MPI.COMM_WORLD
rank, size = comm.Get_rank(), comm.Get_size()
hosts = comm.allgather(platform.node())
host_count = len(set(hosts))
if args.require_cross_host and host_count < 2:
    raise ValueError('Cross-host validation requires at least two actual hosts')
if not args.require_cross_host and host_count != 1:
    raise ValueError('Use --require-cross-host to validate distinct node-local coordinators')
host_leader = min(r for r, host in enumerate(hosts) if host == hosts[rank])
base = Path(args.scratch).resolve()
MIB = 1024 ** 2
QUOTA = 96 * MIB
coordinator_path = base / 'vol' / 'coordinator.sqlite3'
policy = AsyncPolicy(io_engine='cooperative', io_workers=1, prefetch_blocks=0, swap_policy='S2')


def config(name, coordinator=coordinator_path):
    return DistributedConfig(str(base / f'{name}-owner-{rank}'), 2, backend=args.backend,
                             ram_budget_bytes_per_rank=2 * MIB, tile_bytes=4096, frame_policy='materialize',
                             metadata_reserve_bytes=256 * 1024, storage_scope='node-local',
                             disk_quota_bytes=QUOTA, volume_coordinator=str(coordinator))


def refused(action, text):
    try:
        action()
    except (CollectiveError, DiskAdmissionError) as exc:
        assert text in str(exc), str(exc)
        return
    raise AssertionError(f'{text!r} was not refused')


if args.mode == 'mismatch':
    if not any(hosts.count(host) > 1 for host in hosts):
        raise ValueError('Mismatch validation needs at least two ranks on a host')
    refused(lambda: AsyncDistributedSimulator(4, config('m', base / 'vol' / f'coordinator-{rank}.sqlite3'),
                                              comm=comm, policy=policy), 'same volume coordinator')
    assert not list((base / 'vol').glob('coordinator-*.sqlite3'))

elif args.mode == 'unsupported':
    if rank == size - 1:
        VolumeCoordinator.__init__.__kwdefaults__['filesystem_probe'] = lambda path: 'nfs4'
    refused(lambda: AsyncDistributedSimulator(4, config('u'), comm=comm, policy=policy),
            'Unsupported filesystem')
    # Every rank refused together, before any dataset or ledger was created.
    assert not list(base.glob(f'u-owner-{rank}/.disk-admission/*.sqlite3'))

elif args.mode == 'run':
    theta = ParameterSymbol('vc-theta', 'vc-theta')
    with AsyncDistributedSimulator(4, config('a'), comm=comm, policy=policy) as a:
        assert a.disk_budget._coord is not None and str(a.disk_budget._coord.path) == str(coordinator_path.resolve())
        a.gate('H', (3,)); a.gate('RY', (0,), angle=theta)
        binding = a.bind({theta: .37})
        prefix = a.commit_prefix([theta], binding=binding)
        with AsyncDistributedSimulator(4, config('b'), comm=comm, policy=policy) as b:
            b.gate('X', (0,)); b.materialize(binding=b.bind())
            assert a.disk_budget._ledger_path != b.disk_budget._ledger_path  # independent ledgers, one coordinator
            coord = a.disk_budget._coord
            ledgers = coord.snapshot()['ledgers']
            assert {str(a.disk_budget._ledger_path), str(b.disk_budget._ledger_path)} <= set(ledgers)
            with a.fork(prefix, binding=binding) as child:
                # The fork inherits the binding before bootstrap; it never runs uncoordinated.
                assert child.config.volume_coordinator == a.config.volume_coordinator
                assert child.disk_budget._coord is not None
                assert child.disk_budget._coord.path == a.disk_budget._coord.path
                assert child.disk_budget._ledger_path == a.disk_budget._ledger_path
                child.materialize(binding=binding)
                state = child.state(allow_gather=True)
                error = None
                if rank == 0:
                    try:
                        np.testing.assert_allclose(state, eager_reference(a.sequencer.operations, 4, a.assets, binding),
                                                   atol=1e-10, rtol=1e-10)
                    except Exception as exc:
                        error = str(exc)
                child.transport.vote(error)
                observable = ObservableIR((PauliTerm(0, 1),))
                assert abs(child.expectation(observable) - np.cos(.37)) < 1e-10
                # Parameter-shift creates internal forks through the original runtime.
                a.materialize(binding=binding)
                shifted = a.gradient(observable, parameters=[theta], method='parameter-shift')
                assert abs(shifted[theta.parameter_id] + np.sin(.37)) < 1e-9
                child.drain(); a.drain(); b.drain(); comm.Barrier()
                # Joint refusal before mutation, through the normal API of two independent datasets.
                outcome = None
                if rank == host_leader:
                    before = b.disk_budget.db.execute('SELECT count(*) FROM reservations').fetchone()[0]
                    snap = coord.snapshot()
                    used = sum(1 for _ in snap['ledgers'])
                    token = a.disk_budget.reserve(30 * MIB, operation='coordinator-validation')
                    try:
                        b.disk_budget.reserve(60 * MIB, operation='coordinator-validation')
                    except DiskAdmissionError as exc:
                        outcome = str(exc)
                    assert outcome and 'Volume coordinator' in outcome, outcome
                    assert b.disk_budget.db.execute('SELECT count(*) FROM reservations').fetchone()[0] == before
                    other = b.disk_budget.reserve(5 * MIB, operation='coordinator-validation')
                    a.disk_budget.release(token); b.disk_budget.release(other)
                    assert used >= 2
                comm.Barrier()
                assert not child.disk_budget.strict_capacity_guarantee
                assert not child.disk_budget._coord.snapshot()['strict_capacity_guarantee']

elif args.mode == 'recover':
    from svsim.checkpoint_recovery import recover_checkpoint_reservations
    cfg = config('r')
    with DistributedSimulator(6, cfg, comm=comm) as sim:
        sim.gate('H', (0,)); sim.gate('RY', (5,), angle=.31); sim.materialize(binding=sim.bind())
        snapshot = sim.commit_snapshot(); expected = sim.state(allow_gather=True)
        ledger = sim.disk_budget._ledger_path
        cfg = replace(cfg, disk_admission_ledger=str(ledger))
    root = Path(cfg.directory).resolve()
    target = root / f'rank-{rank:04d}/manifests' / f'decision-{snapshot["snapshot_id"]}.json'
    raw = target.read_bytes(); target.unlink(); nonce = uuid4().hex
    temporary = target.with_name('.' + target.name + '.' + nonce + '.tmp')
    coord = VolumeCoordinator(coordinator_path, quota_bytes=QUOTA)
    budget = DiskBudget(ledger, [root], quota_bytes=QUOTA, shared_policy=True, volume_coordinator=coord)
    key = budget.reserve(len(raw), writing=True, drain=True, operation='atomic-json', owner=uuid4().hex, generation=nonce,
        checkpoint_link={'dataset_root': str(root), 'dataset_id': snapshot['dataset_id'],
                         'snapshot_id': snapshot['snapshot_id'], 'rank': rank, 'stage': 'decision-copy',
                         'target': str(target.resolve()), 'temporary': str(temporary.resolve()),
                         'digest': sha256(raw).hexdigest()})
    temporary.write_bytes(raw[:13]); budget.materialize(key, 13); budget.close()
    comm.Barrier()
    claim = coord.claim(ledger)['pending']
    pending_by_rank = comm.allgather(len(raw) - 13)
    assert claim == sum(value for r, value in enumerate(pending_by_rank) if hosts[r] == hosts[rank]), claim
    dry = recover_checkpoint_reservations(cfg, comm=comm)
    published = [r['volume_claim'] for r in dry['ranks'] if r['volume_claim']]
    assert len(published) == host_count and all(p['status'] == 'dry-run' for p in published)
    assert coord.claim(ledger)['pending'] == claim  # a dry run publishes nothing
    report = recover_checkpoint_reservations(cfg, comm=comm, apply=True)
    published = [r['volume_claim'] for r in report['ranks'] if r['volume_claim']]
    assert len(published) == host_count and all(p['status'] == 'republished' for p in published)
    local_published = [p for p in published if p['ledger'] == str(ledger)]
    # Host-local absolute paths may coincide on separate machines; every host
    # has the same local rank count in the registered cross-host placement.
    assert local_published and all(p['previous_claim_bytes'] == claim and p['published_pending_bytes'] == 0
                                   for p in local_published)
    assert coord.claim(ledger)['pending'] == 0 and coord.snapshot()['pending_bytes'] == 0
    comm.Barrier()
    again = recover_checkpoint_reservations(cfg, comm=comm, apply=True)  # idempotent
    published = [r['volume_claim'] for r in again['ranks'] if r['volume_claim']]
    assert len(published) == host_count and all(p['changed'] is False for p in published)
    assert coord.claim(ledger)['pending'] == 0
    coord.close()
    with DistributedSimulator.resume(cfg, snapshot, comm=comm) as restored:
        observed = restored.state(allow_gather=True)
        if rank == 0:
            np.testing.assert_allclose(observed, expected, atol=1e-12)
else:
    raise SystemExit('unknown mode')

comm.Barrier()
if rank == 0:
    print(f'volume coordinator runtime {args.mode} passed (hosts={host_count}, ranks={size})', flush=True)
