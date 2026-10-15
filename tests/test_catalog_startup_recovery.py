"""Startup creation intents cover deaths before SQLite obtains a catalog inode."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from svsim.catalog_admission import CatalogConnection
from svsim.disk_budget import DiskBudget,DiskAdmissionError
from svsim.uncommitted_recovery import recover_uncommitted_reservations,UncommittedRecoveryRefused

ROOT=Path(__file__).resolve().parents[1]
WORKER=Path(__file__).with_name('worker_catalog_startup.py')
ENV={**os.environ,'PYTHONPATH':str(ROOT/'src'),'OMP_NUM_THREADS':'1'}


def killed(base,layout,point):
    out=subprocess.run([sys.executable,str(WORKER),str(base),layout,point],cwd=ROOT,env=ENV,
                       capture_output=True,text=True,timeout=120)
    assert out.returncode==17,out.stdout+'\n'+out.stderr
    assert (base/'fault.txt').read_text()==point
    return Path((base/'ledger.txt').read_text())


def artifacts(base):
    skip={'disk-ledger.sqlite3','disk-ledger.sqlite3-journal'}
    return {str(p.relative_to(base)):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (base/'store').rglob('*') if p.is_file() and p.name not in skip}


@pytest.mark.parametrize('layout',['private','shared'])
@pytest.mark.parametrize('point',['reserved','staged','bound','published','pre-open','opened','bound-open','ready'])
def test_startup_death_has_a_reviewable_single_settlement_without_artifact_deletion(tmp_path,layout,point):
    ledger=killed(tmp_path,layout,point)
    before=artifacts(tmp_path);ledger_before=hashlib.sha256(ledger.read_bytes()).hexdigest()
    report=recover_uncommitted_reservations(ledger)
    assert report['status']=='dry-run',report['refusals']
    assert len(report['tokens'])==1
    token=report['tokens'][0]
    assert token['class']==('catalog' if point in ('bound-open','ready') else 'catalog-startup')
    assert artifacts(tmp_path)==before and hashlib.sha256(ledger.read_bytes()).hexdigest()==ledger_before
    applied=recover_uncommitted_reservations(ledger,apply=True,confirm_plan=report['plan_sha256'])
    assert applied['artifacts_preserved'] and applied['pending_bytes_after']==0
    assert applied['observed_usage_after_bytes']==applied['observed_usage_bytes']
    assert artifacts(tmp_path)==before
    con=sqlite3.connect(ledger)
    try:
        receipt=json.loads(con.execute('SELECT proof FROM checkpoint_recovery_receipts WHERE id=?',
                                       (token['id'],)).fetchone()[0])
        if token['class']=='catalog-startup':assert receipt['attribution']==token['attribution']
    finally:con.close()
    assert recover_uncommitted_reservations(ledger)['status']=='nothing-to-do'
    with pytest.raises(UncommittedRecoveryRefused):
        recover_uncommitted_reservations(ledger,apply=True,confirm_plan=report['plan_sha256'])


@pytest.mark.parametrize('layout',['private','shared'])
def test_startup_hot_journal_requires_admitted_reopen_before_settlement(tmp_path,layout):
    ledger=killed(tmp_path,layout,'hot-journal')
    report=recover_uncommitted_reservations(ledger)
    assert report['status']=='refused' and any('journal' in r for r in report['refusals'])
    store=tmp_path/'store'
    budget=DiskBudget(ledger,[store],persist_private_policy=layout=='private',shared_policy=layout=='shared')
    try:
        db=CatalogConnection(store/'metadata/catalog.sqlite3',budget_getter=lambda:budget,require_native=True)
        with db:db.execute('CREATE TABLE probe(value)')
        db.close()
    finally:budget.close()
    report=recover_uncommitted_reservations(ledger)
    assert report['status']=='dry-run',report['refusals']
    before=artifacts(tmp_path)
    recover_uncommitted_reservations(ledger,apply=True,confirm_plan=report['plan_sha256'])
    assert artifacts(tmp_path)==before


@pytest.mark.parametrize('tamper',['replace','parent-replace','generation','legacy-unbound'])
def test_startup_identity_or_generation_loss_refuses_without_changing_tokens(tmp_path,tamper):
    ledger=killed(tmp_path,'shared','pre-open')
    con=sqlite3.connect(ledger)
    key,raw=con.execute('SELECT id,link FROM checkpoint_reservations').fetchone();link=json.loads(raw)
    path=Path(link['path'])
    if tamper=='replace':
        path.rename(path.with_suffix('.original'));path.write_bytes(b'')
    elif tamper=='parent-replace':
        parent=path.parent;parent.rename(parent.with_name('old-metadata'));parent.mkdir();path.write_bytes(b'')
    elif tamper=='generation':
        con.execute('UPDATE reservations SET generation=? WHERE id=?',('f'*32,key))
    else:
        con.execute('DELETE FROM checkpoint_reservations WHERE id=?',(key,))
        con.execute("UPDATE reservations SET operation='catalog-transaction',owner=?,generation=? WHERE id=?",
                    ('catalog-unbound-v3:'+hashlib.sha256(str(path).encode()).hexdigest(),'e'*64,key))
    con.commit();con.close()
    before=hashlib.sha256(ledger.read_bytes()).hexdigest(),artifacts(tmp_path)
    try:
        report=recover_uncommitted_reservations(ledger)
    except DiskAdmissionError:
        pass
    else:
        assert report['status']=='refused'
        with pytest.raises(UncommittedRecoveryRefused):
            recover_uncommitted_reservations(ledger,apply=True,confirm_plan=report['plan_sha256'])
    assert (hashlib.sha256(ledger.read_bytes()).hexdigest(),artifacts(tmp_path))==before


@pytest.mark.parametrize('birth',['unsupported','same-inode-new-birth'])
def test_startup_birth_uncertainty_never_releases_pending(tmp_path,monkeypatch,birth):
    ledger=killed(tmp_path,'shared','pre-open')
    import svsim.catalog_admission as catalog
    original=catalog.catalog_birth_identity
    target=tmp_path/'store/metadata/catalog.sqlite3';inode=target.stat().st_ino
    def identity(path,info):
        if Path(path)==target:return None if birth=='unsupported' else 'ns:reused-inode-generation'
        return original(path,info)
    monkeypatch.setattr(catalog,'catalog_birth_identity',identity)
    before=hashlib.sha256(ledger.read_bytes()).hexdigest()
    report=recover_uncommitted_reservations(ledger)
    assert report['status']=='refused'
    assert target.stat().st_ino==inode
    with pytest.raises(UncommittedRecoveryRefused):
        recover_uncommitted_reservations(ledger,apply=True,confirm_plan=report['plan_sha256'])
    assert hashlib.sha256(ledger.read_bytes()).hexdigest()==before


def test_birth_unsupported_refuses_creation_before_reservation_or_namespace_change(tmp_path,monkeypatch):
    import svsim.catalog_admission as catalog
    budget=DiskBudget(tmp_path/'ledger.sqlite3',[tmp_path])
    before=set(tmp_path.iterdir())
    monkeypatch.setattr(catalog,'catalog_birth_identity',lambda *args:None)
    try:
        with pytest.raises(DiskAdmissionError,match='birth identity'):
            CatalogConnection(tmp_path/'catalog.sqlite3',budget_getter=lambda:budget)
        assert budget.reserved_bytes==0 and set(tmp_path.iterdir())==before
    finally:budget.close()


def test_creation_binding_is_exact_cas_and_open_reuses_the_bound_lease(tmp_path,monkeypatch):
    import svsim.catalog_startup as startup
    budget=DiskBudget(tmp_path/'ledger.sqlite3',[tmp_path])
    original=budget.bind_catalog_startup;captured={}
    def bind(key,**kwargs):
        result=original(key,**kwargs)
        if kwargs['phase']=='bound':captured.update(key=key,arguments=kwargs)
        return result
    monkeypatch.setattr(budget,'bind_catalog_startup',bind)
    try:
        db=CatalogConnection(tmp_path/'catalog.sqlite3',budget_getter=lambda:budget)
        count=db.admission_status()['ledger_reservation_count']
        key=captured['key']
        assert budget.checkpoint_link(key) is None
        assert budget.reservation(key)['owner'].startswith('catalog-v3:')
        with pytest.raises(DiskAdmissionError,match='CAS mismatch'):
            original(key,**captured['arguments'])
        with db:db.execute('CREATE TABLE sample(value)')
        assert db.admission_status()['ledger_reservation_count']==count
        assert db.execute('PRAGMA synchronous').fetchone()==(3,)
        db.close()
    finally:budget.close()


def test_failed_exclusive_publication_preserves_the_intervening_database(tmp_path,monkeypatch):
    import svsim.catalog_startup as startup
    budget=DiskBudget(tmp_path/'ledger.sqlite3',[tmp_path])
    original=startup._exclusive_rename;target=tmp_path/'catalog.sqlite3'
    def publish(source,destination):
        target.write_bytes(b'other-owner')
        return original(source,destination)
    monkeypatch.setattr(startup,'_exclusive_rename',publish)
    try:
        with pytest.raises(FileExistsError):
            CatalogConnection(target,budget_getter=lambda:budget)
        assert target.read_bytes()==b'other-owner'
        assert budget.reserved_bytes>0
        assert len(list(tmp_path.glob('.*.startup')))==1
    finally:budget.close()


@pytest.mark.skipif(sys.platform!='linux',reason='Linux inode reuse regression')
def test_reused_linux_inode_is_distinguished_by_birth_identity(tmp_path):
    from svsim.catalog_startup import identity
    ledger=killed(tmp_path,'shared','pre-open')
    target=tmp_path/'store/metadata/catalog.sqlite3'
    original=identity(target)
    reused=False
    for _ in range(512):
        target.unlink();target.write_bytes(b'')
        current=identity(target)
        if current['inode']==original['inode']:
            reused=True;break
    if not reused:pytest.skip('filesystem did not reuse the catalog inode within the bounded probe')
    if current['birth_identity']==original['birth_identity']:
        pytest.skip('filesystem birth-time resolution cannot distinguish this replacement')
    before=hashlib.sha256(ledger.read_bytes()).hexdigest()
    report=recover_uncommitted_reservations(ledger)
    assert report['status']=='refused' and any('birth identity changed' in r for r in report['refusals'])
    assert hashlib.sha256(ledger.read_bytes()).hexdigest()==before


def test_unsettled_creation_blocks_retry_until_reviewed_offline_recovery(tmp_path):
    ledger=killed(tmp_path,'shared','reserved');store=tmp_path/'store'
    budget=DiskBudget(ledger,[store],shared_policy=True)
    try:
        pending=budget.reserved_bytes
        with pytest.raises(DiskAdmissionError,match='prior catalog startup'):
            CatalogConnection(store/'metadata/catalog.sqlite3',budget_getter=lambda:budget)
        assert budget.reserved_bytes==pending and not (store/'metadata/catalog.sqlite3').exists()
    finally:budget.close()
    report=recover_uncommitted_reservations(ledger)
    recover_uncommitted_reservations(ledger,apply=True,confirm_plan=report['plan_sha256'])
    budget=DiskBudget(ledger,[store],shared_policy=True)
    try:
        db=CatalogConnection(store/'metadata/catalog.sqlite3',budget_getter=lambda:budget)
        with db:db.execute('CREATE TABLE initialized(value)')
        assert db.execute('PRAGMA quick_check').fetchall()==[('ok',)]
        db.close()
    finally:budget.close()
