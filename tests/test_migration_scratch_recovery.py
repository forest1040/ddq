"""A metadata-only migration clone has source-bound, verified physical closure."""
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys

import numpy as np
import pytest

from svsim import DistributedSimulator,DistributedConfig
from svsim.disk_budget import DiskAdmissionError
from svsim.uncommitted_recovery import recover_uncommitted_reservations


def killed(tmp_path,stage):
    result=subprocess.run([sys.executable,'tests/worker_migration_recovery.py',str(tmp_path),stage],
        env={**os.environ,'PYTHONPATH':'src'},capture_output=True,timeout=90)
    assert result.returncode==-signal.SIGKILL,result.stderr.decode()
    return Path(result.stdout.decode().strip().splitlines()[-1])


@pytest.mark.parametrize('stage',['reserved','clone-open','partial-copy','clone-copy','block'])
def test_migration_clone_kill_reconciles_against_source_and_preserves_old_global_cut(tmp_path,stage):
    ledger=killed(tmp_path,stage)
    source={str(p.relative_to(tmp_path/'source')):p.read_bytes() for p in (tmp_path/'source').rglob('*') if p.is_file()}
    before={str(p.relative_to(tmp_path/'target')):p.read_bytes() for p in (tmp_path/'target').rglob('*')
            if p.is_file() and 'sqlite3' not in p.name}
    dry=recover_uncommitted_reservations(ledger)
    if stage=='partial-copy' and dry['refusals']:
        # No token is settled while SQLite still owns a hot clone journal.
        # Replay it through the ordinary admitted catalog opener under offline
        # exclusive leases, then obtain a fresh recovery plan.
        from svsim.uncommitted_recovery import offline_scope
        from svsim.catalog_admission import CatalogConnection
        clone=next((tmp_path/'target').rglob('.migration.*.tmp'))
        with offline_scope(ledger,writer=True) as scope:
            db=CatalogConnection(clone/'metadata/catalog.sqlite3',budget_getter=lambda:scope['budget'])
            assert db.execute('PRAGMA quick_check').fetchall()==[('ok',)]
            db.close()
        dry=recover_uncommitted_reservations(ledger)
    assert not dry['refusals'],dry['refusals']
    assert any(t['class']=='migration' for t in dry['tokens'])
    applied=recover_uncommitted_reservations(ledger,apply=True,confirm_plan=dry['plan_sha256'])
    assert applied['status']=='applied'
    assert recover_uncommitted_reservations(ledger)['status']=='nothing-to-do'
    assert {str(p.relative_to(tmp_path/'source')):p.read_bytes() for p in (tmp_path/'source').rglob('*') if p.is_file()}==source
    assert {str(p.relative_to(tmp_path/'target')):p.read_bytes() for p in (tmp_path/'target').rglob('*')
            if p.is_file() and 'sqlite3' not in p.name}==before
    from mpi4py import MPI
    config=DistributedConfig(str(tmp_path/'target'),1,ram_budget_bytes_per_rank=4*1024**2,
        metadata_reserve_bytes=256*1024,storage_scope='node-local')
    with DistributedSimulator.resume(config,comm=MPI.COMM_SELF) as target:
        expected=np.zeros(8,dtype='<c16');expected[0]=1
        np.testing.assert_allclose(target.state(allow_gather=True),expected)


def test_migration_clone_refuses_changed_source_extent_and_keeps_tokens(tmp_path):
    ledger=killed(tmp_path,'clone-copy')
    file=next((tmp_path/'source/state').glob('segment-*.bin'))
    with file.open('r+b') as stream:
        value=stream.read(1);stream.seek(0);stream.write(bytes([value[0]^1]))
    with pytest.raises(DiskAdmissionError,match='checksum'):
        recover_uncommitted_reservations(ledger)
    con=sqlite3.connect(ledger)
    try:assert con.execute("SELECT count(*) FROM reservations WHERE operation='migration-open' AND state='WRITING'").fetchone()[0]==1
    finally:con.close()


def test_migration_clone_refuses_live_source_owner_and_preserves_credit(tmp_path):
    from svsim.dataset_lease import DirectoryLease
    ledger=killed(tmp_path,'clone-copy')
    with DirectoryLease(tmp_path/'source'):
        with pytest.raises(DiskAdmissionError,match='live runtime'):
            recover_uncommitted_reservations(ledger)
    assert not recover_uncommitted_reservations(ledger)['refusals']


def test_migration_clone_refuses_metadata_not_attributable_to_source(tmp_path):
    ledger=killed(tmp_path,'clone-copy')
    clone=next((tmp_path/'target').rglob('.migration.*.tmp'))
    con=sqlite3.connect(clone/'metadata/catalog.sqlite3')
    try:
        con.execute('UPDATE versions SET offset=offset+1 WHERE segment IS NOT NULL');con.commit()
    finally:con.close()
    with pytest.raises(DiskAdmissionError,match='differs from its source'):
        recover_uncommitted_reservations(ledger)
