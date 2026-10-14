"""Precharged metadata permits narrow recovery without new admission."""
from hashlib import sha256
from pathlib import Path
import sqlite3
from uuid import uuid4

import pytest

from svsim.disk_budget import DiskAdmissionError,DiskBudget
from svsim.journal import content_hash

POOL=8*1024**2

def setup(root,pool=POOL):
    root.mkdir()
    ledger=root/'ledger.sqlite3'
    options=dict(quota_bytes=pool+65536,drain_reserve_bytes=4096,shared_policy=True,bootstrap_bytes=pool)
    budget=DiskBudget(ledger,[root],**options)
    sid=uuid4().hex;nonce=uuid4().hex
    target=root/'rank-0000/manifests'/f'prepare-{sid}.json'
    key=budget.reserve(512,writing=True,owner=uuid4().hex,generation=nonce,operation='atomic-json',
        checkpoint_link={'dataset_root':str(root),'dataset_id':uuid4().hex,'snapshot_id':sid,'rank':0,
            'stage':'prepare','target':str(target),'temporary':str(target.with_name('.'+target.name+'.'+nonce+'.tmp')),
            'digest':sha256(b'x'*512).hexdigest()})
    budget.close()
    return ledger,options,key

def recover(ledger,options):
    return DiskBudget(ledger,[ledger.parent],**options,recovery_only=True)

def settle(budget,key):
    item=budget.checkpoint_links(budget.roots[0])[0]
    proof={'version':1,'metadata_only':True,'reservation_id':key,'inventory':'verified-by-caller'}
    budget.reconcile_checkpoint(key,expected_reservation=item['reservation'],expected_link=item['link'],
        materialized_bytes=13,evidence='test:'+content_hash(proof),evidence_document=proof)

def test_existing_pool_recovers_over_quota_and_disallows_all_other_mutation(tmp_path):
    ledger,options,key=setup(tmp_path/'root')
    pressure=ledger.parent/'pressure.bin'
    pressure.write_bytes(b'x'*65536)
    with pytest.raises(DiskAdmissionError,match='quota'):
        DiskBudget(ledger,[ledger.parent],**options)
    before=sha256(ledger.read_bytes()).hexdigest()
    budget=recover(ledger,options)
    try:
        roots=budget.db.execute('SELECT * FROM roots').fetchall()
        policy=budget.db.execute('SELECT * FROM shared_policy').fetchall()
        assert sha256(ledger.read_bytes()).hexdigest()==before
        for action in (lambda:budget.reserve(0),lambda:budget.release(key),lambda:budget.materialize(key,13),
                       lambda:budget.mark_writing(key),lambda:budget.register_roots([ledger.parent]),
                       lambda:budget.reconcile(key,materialized_bytes=13,evidence='unverified')):
            with pytest.raises(DiskAdmissionError,match='Recovery-only'):action()
        with pytest.raises(DiskAdmissionError,match='proof receipt'):
            item=budget.checkpoint_links(ledger.parent)[0]
            budget.reconcile_checkpoint(key,expected_reservation=item['reservation'],expected_link=item['link'],
                materialized_bytes=13,evidence='missing-proof')
        settle(budget,key)
        assert budget.reserved_bytes==0 and budget.reservation(key)['materialized_bytes']==13
        assert budget.recovery_receipt(key)['reservation_id']==key
        assert roots==budget.db.execute('SELECT * FROM roots').fetchall()
        assert policy==budget.db.execute('SELECT * FROM shared_policy').fetchall()
        assert pressure.stat().st_size==65536
    finally:budget.close()

@pytest.mark.parametrize('fault',['missing-table','missing-pk','missing-fk','unbound','policy','version','trigger','named-trigger','root','side-file','symlink'])
def test_invalid_existing_metadata_is_refused_without_repair(tmp_path,fault):
    ledger,options,key=setup(tmp_path/'root')
    con=sqlite3.connect(ledger)
    try:
        if fault=='missing-table':con.execute('DROP TABLE checkpoint_recovery_receipts')
        elif fault in ('missing-pk','missing-fk'):
            table='checkpoint_reservations'
            sql=con.execute("SELECT sql FROM sqlite_master WHERE name=?",(table,)).fetchone()[0]
            rows=con.execute('SELECT * FROM checkpoint_reservations').fetchall()
            con.execute('DROP TABLE checkpoint_reservations')
            if fault=='missing-pk':sql=sql.replace('TEXT PRIMARY KEY','TEXT')
            else:sql=sql.replace(',\n                        FOREIGN KEY(id) REFERENCES reservations(id) ON DELETE CASCADE','')
            con.execute(sql)
            con.executemany('INSERT INTO checkpoint_reservations VALUES(?,?)',rows)
        elif fault=='unbound':con.execute('DELETE FROM shared_policy')
        elif fault=='policy':con.execute('UPDATE shared_policy SET quota_bytes=quota_bytes+1')
        elif fault=='version':con.execute('PRAGMA user_version=2')
        elif fault=='trigger':con.execute("CREATE TRIGGER rogue AFTER UPDATE ON reservations BEGIN DELETE FROM roots; END")
        elif fault=='named-trigger':
            sql=con.execute("SELECT sql FROM sqlite_master WHERE name='reservation_labels_update'").fetchone()[0]
            con.execute('DROP TRIGGER reservation_labels_update')
            con.execute(sql.replace('BEFORE UPDATE OF operation,owner,generation,evidence','AFTER UPDATE OF operation,owner,generation,evidence').replace('BEGIN SELECT RAISE', 'BEGIN DELETE FROM roots; SELECT RAISE'))
        elif fault=='root':con.execute('DELETE FROM roots')
        con.commit()
    finally:con.close()
    if fault=='side-file':Path(str(ledger)+'-journal').write_bytes(b'uncertain')
    original=ledger
    if fault=='symlink':
        ledger=ledger.parent/'alias.sqlite3';ledger.symlink_to(original)
    before=sha256(original.read_bytes()).hexdigest()
    with pytest.raises(DiskAdmissionError):recover(ledger,options)
    assert sha256(original.read_bytes()).hexdigest()==before

def test_missing_recovery_ledger_is_not_created(tmp_path):
    missing=tmp_path/'absent'/'ledger.sqlite3'
    with pytest.raises(FileNotFoundError):
        DiskBudget(missing,[tmp_path],shared_policy=True,recovery_only=True)
    assert not missing.parent.exists()


@pytest.mark.parametrize('fault',['physical-headroom','unregistered-root','pool-mismatch','guard-unavailable','guard-none'])
def test_recovery_open_cannot_acquire_new_credit_or_fallback(tmp_path,monkeypatch,fault):
    ledger,options,_=setup(tmp_path/'root')
    roots=[ledger.parent]
    if fault=='physical-headroom':monkeypatch.setattr(DiskBudget,'_observed_free',lambda self:0)
    elif fault=='unregistered-root':
        other=tmp_path/'other';other.mkdir();roots=[other]
    elif fault=='pool-mismatch':options={**options,'bootstrap_bytes':2*POOL}
    elif fault=='guard-none':monkeypatch.setattr('svsim.catalog_admission.get_sqlite_guard',lambda:None)
    else:
        def unavailable():raise OSError('injected unavailable guard')
        monkeypatch.setattr('svsim.catalog_admission.get_sqlite_guard',unavailable)
    before=sha256(ledger.read_bytes()).hexdigest()
    with pytest.raises(DiskAdmissionError):DiskBudget(ledger,roots,**options,recovery_only=True)
    assert sha256(ledger.read_bytes()).hexdigest()==before

def test_physical_headroom_is_rechecked_before_each_cas(tmp_path,monkeypatch):
    ledger,options,key=setup(tmp_path/'root')
    budget=recover(ledger,options)
    before=sha256(ledger.read_bytes()).hexdigest()
    try:
        monkeypatch.setattr(budget,'_free',lambda:budget._unallocated_metadata_pool()-1)
        with pytest.raises(DiskAdmissionError,match='physical headroom'):settle(budget,key)
        assert budget.reservation(key)['state']=='WRITING' and budget.recovery_receipt(key) is None
        assert sha256(ledger.read_bytes()).hexdigest()==before
    finally:budget.close()

@pytest.mark.parametrize('stage',['receipt','commit'])
def test_metadata_error_rolls_back_reservation_and_reopens_cleanly(tmp_path,stage):
    ledger,options,key=setup(tmp_path/'root')
    budget=recover(ledger,options)
    original=budget.db
    class Fault:
        def execute(self,sql,*args):
            if (stage=='receipt' and sql.startswith('INSERT INTO checkpoint_recovery_receipts')) or (stage=='commit' and sql=='COMMIT'):
                raise sqlite3.OperationalError('injected metadata I/O error')
            return original.execute(sql,*args)
        def __getattr__(self,name):return getattr(original,name)
    budget.db=Fault()
    try:
        with pytest.raises(sqlite3.OperationalError,match='metadata I/O'):settle(budget,key)
        assert budget.reservation(key)['state']=='WRITING' and budget.recovery_receipt(key) is None
    finally:budget.close()
    budget=recover(ledger,options)
    try:
        assert budget.db.execute('PRAGMA quick_check').fetchall()==[('ok',)]
        settle(budget,key)
        assert budget.reserved_bytes==0
    finally:budget.close()


def test_native_journal_cap_failure_does_not_settle_or_save_receipt(tmp_path):
    ledger,options,key=setup(tmp_path/'root')
    budget=recover(ledger,options)
    budget._ledger_side_cap=1  # inject native VFS exhaustion on the first journal write
    try:
        with pytest.raises(sqlite3.OperationalError,match='full'):settle(budget,key)
        assert budget.reservation(key)['state']=='WRITING' and budget.recovery_receipt(key) is None
    finally:budget.close()
    budget=recover(ledger,options)
    try:
        assert budget.db.execute('PRAGMA quick_check').fetchall()==[('ok',)]
        settle(budget,key)
    finally:budget.close()


def test_full_native_main_pool_rolls_back_an_oversized_receipt(tmp_path):
    ledger,options,key=setup(tmp_path/'root',pool=256*1024)
    writer=DiskBudget(ledger,[ledger.parent],**options)
    try:
        with writer._ledger_transaction():writer.db.execute('CREATE TABLE padding(data BLOB)')
        free_pages=writer.max_page_count-writer.db.execute('PRAGMA page_count').fetchone()[0]
        for amount in ((free_pages-2)*4096,8192,4096,2048,1024,512,256):
            try:
                with writer._ledger_transaction():writer.db.execute('INSERT INTO padding VALUES(zeroblob(?))',(amount,))
            except sqlite3.OperationalError as exc:assert 'full' in str(exc)
        assert writer.db.execute('PRAGMA page_count').fetchone()[0]==writer.max_page_count
        assert writer.db.execute('PRAGMA freelist_count').fetchone()[0]==0
    finally:writer.close()
    budget=recover(ledger,options)
    try:
        item=budget.checkpoint_links(ledger.parent)[0]
        proof={'version':1,'metadata_only':True,'padding':'x'*7000}
        with pytest.raises(sqlite3.OperationalError,match='full'):
            budget.reconcile_checkpoint(key,expected_reservation=item['reservation'],expected_link=item['link'],
                materialized_bytes=13,evidence='test:'+content_hash(proof),evidence_document=proof)
        assert budget.reservation(key)['state']=='WRITING' and budget.recovery_receipt(key) is None
    finally:budget.close()
    budget=recover(ledger,options)
    try:
        assert budget.db.execute('PRAGMA quick_check').fetchall()==[('ok',)]
        assert budget.reservation(key)['state']=='WRITING'
    finally:budget.close()
    budget=recover(ledger,options)
    try:
        assert budget.db.execute('PRAGMA quick_check').fetchall()==[('ok',)]
        settle(budget,key)
    finally:budget.close()
