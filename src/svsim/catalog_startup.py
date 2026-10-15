"""Durable creation intent for a new catalog before SQLite can write its inode."""
import ctypes
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
from hashlib import sha256
from uuid import uuid4

from .disk_budget import DiskAdmissionError


def identity(path):
    from .catalog_admission import catalog_birth_identity
    path=Path(path);info=path.lstat();birth=catalog_birth_identity(path,info)
    if birth is None:raise DiskAdmissionError('Catalog startup requires a stable birth identity')
    return {'device':info.st_dev,'inode':info.st_ino,'birth_identity':birth}


def _parent(link):
    parent=Path(link['parent'])
    if not parent.is_absolute() or parent.resolve()!=parent or parent.is_symlink() or not parent.is_dir():
        raise DiskAdmissionError('Catalog startup parent is aliased or unavailable')
    if identity(parent)!=link['parent_identity']:
        raise DiskAdmissionError('Catalog startup parent generation changed')
    return parent


def make_intent(budget,path):
    path=Path(path)
    roots=[Path(p) for (p,) in budget.db.execute('SELECT path FROM roots')]
    covered=[root for root in roots if path.parent.is_relative_to(root)]
    if not covered:raise DiskAdmissionError('Catalog startup is outside registered roots')
    root=max(covered,key=lambda p:len(p.parts));generation=uuid4().hex
    return {'purpose':'catalog-startup','version':1,'dataset_root':str(root),'path':str(path),
            'parent':str(path.parent),'parent_identity':identity(path.parent),'generation':generation,
            'temporary':str(path.with_name('.'+path.name+'.'+generation+'.startup')),'phase':'planned'}


def normalize_intent(budget,link,amount):
    expected={'purpose','version','dataset_root','path','parent','parent_identity','generation','temporary','phase'}
    if set(link)!=expected or link['version']!=1 or link['phase']!='planned':
        raise ValueError('Invalid catalog startup intent')
    parent=_parent(link);path=Path(link['path']);generation=link['generation']
    if (not path.is_absolute() or path.parent!=parent or path.name in ('','..','.') or
            re.fullmatch('[0-9a-f]{32}',generation) is None or
            link['temporary']!=str(parent/('.'+path.name+'.'+generation+'.startup'))):
        raise ValueError('Invalid catalog startup path or generation')
    if path.exists() or path.is_symlink() or Path(link['temporary']).exists() or Path(link['temporary']).is_symlink():
        raise DiskAdmissionError('Catalog startup target or generation already exists')
    rows=budget.db.execute('SELECT c.link FROM checkpoint_reservations c JOIN reservations r ON r.id=c.id '
                           "WHERE r.state IN ('RESERVED','WRITING') AND r.operation='catalog-startup' LIMIT 4097").fetchall()
    if len(rows)>4096:raise DiskAdmissionError('Catalog startup intent limit exceeded')
    if any(json.loads(raw).get('path')==str(path) for (raw,) in rows):
        raise DiskAdmissionError('A prior catalog startup requires offline recovery before retry')
    root=Path(link['dataset_root'])
    roots=[Path(p) for (p,) in budget.db.execute('SELECT path FROM roots')]
    if root not in roots or not parent.is_relative_to(root) or parent.stat().st_dev!=root.stat().st_dev:
        raise DiskAdmissionError('Catalog startup root is unbound or on another volume')
    raw=json.dumps({**link,'expected_bytes':amount},sort_keys=True,separators=(',',':'))
    if len(raw.encode())>4096:raise ValueError('Catalog startup intent exceeds metadata limit')
    return raw


def owner_for(link):
    if link['phase']=='planned':return 'catalog-startup-v1:'+sha256(link['path'].encode()).hexdigest()
    info=link['database_identity']
    raw=json.dumps([link['path'],info['device'],info['inode'],info['birth_identity']],separators=(',',':'))
    return 'catalog-v3:'+sha256(raw.encode()).hexdigest()


def _sync_parent(parent):
    fd=os.open(parent,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


def _exclusive_rename(source,target):
    libc=ctypes.CDLL(None,use_errno=True)
    if sys.platform=='darwin':
        function=libc.renamex_np
        function.argtypes=[ctypes.c_char_p,ctypes.c_char_p,ctypes.c_uint]
        result=function(os.fsencode(source),os.fsencode(target),4)  # RENAME_EXCL, sys/stdio.h
    elif sys.platform=='linux':
        function=libc.renameat2
        function.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint]
        result=function(-100,os.fsencode(source),-100,os.fsencode(target),1)  # RENAME_NOREPLACE
    else:raise DiskAdmissionError('Atomic exclusive catalog publication is unavailable')
    if result:raise OSError(ctypes.get_errno(),os.strerror(ctypes.get_errno()),str(target))


def cas_bind(budget,key,*,expected_reservation,expected_link,phase):
    budget._require_writer()
    if phase not in ('bound','opened') or expected_link.get('purpose')!='catalog-startup':
        raise ValueError('Invalid catalog startup binding')
    before='planned' if phase=='bound' else 'bound'
    if expected_link.get('phase')!=before:raise DiskAdmissionError('Catalog startup binding is out of order')
    if budget.reservation(key)!=expected_reservation or budget.checkpoint_link(key)!=expected_link:
        raise DiskAdmissionError('Catalog startup reservation CAS mismatch')
    parent=_parent(expected_link)
    source=Path(expected_link['temporary'] if phase=='bound' else expected_link['path'])
    info=source.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1:
        raise DiskAdmissionError('Catalog startup inode is not an exclusive regular file')
    current_identity=identity(source)
    if current_identity['device']!=parent.stat().st_dev:
        raise DiskAdmissionError('Catalog startup inode changed volume')
    if phase=='bound' and info.st_size!=0:raise DiskAdmissionError('Unopened catalog startup file is not empty')
    if phase=='opened' and current_identity!=expected_link['database_identity']:
        raise DiskAdmissionError('Catalog startup inode changed before open binding')
    updated={**expected_link,'phase':phase,'database_identity':current_identity}
    raw=json.dumps(updated,sort_keys=True,separators=(',',':'))
    if len(raw.encode())>4096:raise ValueError('Catalog startup binding exceeds metadata limit')
    with budget._ledger_transaction():
        if budget.reservation(key)!=expected_reservation or budget.checkpoint_link(key)!=expected_link:
            raise DiskAdmissionError('Catalog startup reservation CAS mismatch')
        if expected_reservation['state']!='WRITING':raise DiskAdmissionError('Catalog startup reservation is terminal')
        operation='catalog-transaction' if phase=='opened' else 'catalog-startup'
        budget.db.execute('UPDATE checkpoint_reservations SET link=? WHERE id=?',(raw,key))
        budget.db.execute('UPDATE reservations SET owner=?,operation=? WHERE id=?',(owner_for(updated),operation,key))
        if phase=='opened':
            # The normal catalog owner now proves path/inode/birth. Retire the
            # creation intent atomically, preserving the existing unlinked lease API.
            budget.db.execute('DELETE FROM checkpoint_reservations WHERE id=?',(key,))
            budget.db.execute('UPDATE reservations SET generation=? WHERE id=?',
                              (sha256(os.urandom(16)).hexdigest(),key))
    return updated


def materialize(lease):
    budget,key=lease.budget,lease.key;link=budget.checkpoint_link(key)
    parent=_parent(link)
    fd=os.open(link['temporary'],os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    try:os.fsync(fd)
    finally:os.close(fd)
    _sync_parent(parent)
    link=budget.bind_catalog_startup(key,expected_reservation=budget.reservation(key),expected_link=link,phase='bound')
    _exclusive_rename(link['temporary'],link['path'])
    _sync_parent(parent)
    lease.owner=owner_for(link)


def finish_open(lease):
    budget,key=lease.budget,lease.key;link=budget.checkpoint_link(key)
    updated=budget.bind_catalog_startup(key,expected_reservation=budget.reservation(key),expected_link=link,phase='opened')
    lease.owner=owner_for(updated)
    lease.startup=False


def inventory_directories(con,roots):
    found=[]
    rows=con.execute('SELECT c.link FROM checkpoint_reservations c JOIN reservations r ON r.id=c.id '
                     "WHERE r.state IN ('RESERVED','WRITING') LIMIT 4097").fetchall()
    if len(rows)>4096:raise DiskAdmissionError('Startup intent inventory exceeds limit')
    for (raw,) in rows:
        link=json.loads(raw)
        if not isinstance(link,dict) or link.get('purpose')!='catalog-startup':continue
        parent=_parent(link)
        if not any(parent.is_relative_to(Path(root)) for root in roots):
            raise DiskAdmissionError('Catalog startup parent is outside registered roots')
        found.append(parent)
    return found


def has_private_intent(con,store):
    """Only complements the catalog-exists layout test; the recovery plan verifies the full intent."""
    for (raw,) in con.execute('SELECT link FROM checkpoint_reservations LIMIT 4097'):
        link=json.loads(raw)
        if (isinstance(link,dict) and link.get('purpose')=='catalog-startup' and
                link.get('path')==str(Path(store)/'metadata/catalog.sqlite3') and
                link.get('parent')==str(Path(store)/'metadata')):
            return True
    return False


def plan_recovery(token,link,scope):
    if (link.get('purpose')!='catalog-startup' or link.get('version')!=1 or
            link.get('phase') not in ('planned','bound','opened') or
            token['operation']!=('catalog-transaction' if link['phase']=='opened' else 'catalog-startup') or
            token['state']!='WRITING' or token['generation']!=link['generation'] or
            token['bytes']!=link.get('expected_bytes') or token['owner']!=owner_for(link)):
        raise DiskAdmissionError('Catalog startup intent or writer generation conflicts')
    parent=_parent(link);path=Path(link['path']);temporary=Path(link['temporary'])
    leased={Path(l.path) for l in scope['leases']} | {Path(scope['ledger_lease'].path)}
    if parent not in leased or not any(parent.is_relative_to(root) for root in scope['roots']):
        raise DiskAdmissionError('Catalog startup parent is not covered by an exclusive lease')
    if (path.parent!=parent or re.fullmatch('[0-9a-f]{32}',link['generation']) is None or
            temporary!=parent/('.'+path.name+'.'+link['generation']+'.startup')):
        raise DiskAdmissionError('Catalog startup paths conflict')
    artifacts={}
    for name,file in (('database',path),('temporary',temporary)):
        try:info=file.lstat()
        except FileNotFoundError:
            artifacts[name]={'path':str(file),'present':False};continue
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_dev!=parent.stat().st_dev:
            raise DiskAdmissionError('Catalog startup artifact is aliased or on another volume')
        file_identity=identity(file)
        if link['phase']=='planned':
            if name=='database' or info.st_size:raise DiskAdmissionError('Unbound catalog startup has unexpected bytes')
        elif file_identity!=link['database_identity']:
            raise DiskAdmissionError('Catalog startup database birth identity changed')
        elif name=='temporary' and info.st_size:
            raise DiskAdmissionError('Unopened catalog staging file has unexpected bytes')
        artifacts[name]={'path':str(file),'present':True,'bytes':info.st_size,**file_identity}
    if link['phase']!='planned' and sum(a['present'] for a in artifacts.values())!=1:
        raise DiskAdmissionError('Catalog startup bound inode is missing or duplicated')
    for suffix in ('-journal','-wal','-shm'):
        side=Path(str(path)+suffix)
        try:info=side.lstat()
        except FileNotFoundError:continue
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_size:
            raise DiskAdmissionError('Catalog startup hot side file requires admitted replay')
    if artifacts['database']['present']:
        con=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)
        try:
            if con.execute('PRAGMA quick_check').fetchall()!=[('ok',)]:
                raise DiskAdmissionError('Catalog startup database integrity check failed')
        finally:con.close()
    return {'class':'catalog-startup','attribution':{'intent':link,'artifacts':artifacts,'artifacts_preserved':True}}
