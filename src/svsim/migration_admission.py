"""Bounded migration scratch intent; a clone catalog never owns source extents."""
from contextlib import contextmanager
from dataclasses import asdict
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
from uuid import uuid4

from .catalog_admission import catalog_birth_identity,catalog_file_owner
from .disk_budget import DiskAdmissionError
from .journal import content_hash,Journal


def identity(path):
    path=Path(path);info=path.lstat();birth=catalog_birth_identity(path,info)
    if path.is_symlink() or path.resolve()!=path or birth is None:
        raise DiskAdmissionError('Migration identity is unavailable or aliased')
    return {'device':info.st_dev,'inode':info.st_ino,'birth':birth}


def read_snapshot(link):
    root=Path(link['source_root']);catalog=root/'metadata/catalog.sqlite3'
    if identity(root)!=link['source_identity'] or identity(catalog)!=link['source_catalog_identity']:
        raise DiskAdmissionError('Migration source identity changed')
    con=sqlite3.connect(catalog.as_uri()+'?mode=ro',uri=True)
    try:
        con.execute('PRAGMA query_only=ON')
        if con.execute('PRAGMA quick_check').fetchall()!=[('ok',)]:raise DiskAdmissionError('Migration source catalog is corrupt')
        row=con.execute('SELECT data FROM snapshots WHERE id=?',(link['source_snapshot'],)).fetchone()
        if row is None or len(row[0].encode())>1024**2:raise DiskAdmissionError('Migration source snapshot is unavailable')
        data=json.loads(row[0])
        if content_hash(data)!=link['source_digest']:raise DiskAdmissionError('Migration source snapshot changed')
        reader=Journal.__new__(Journal);reader.directory=root/'journal'
        for kind,cursor in data['journal_cursors'].items():
            for _ in reader.replay(kind,cursor):pass
        return data
    finally:con.close()


def normalize_intent(budget,link,amount):
    required={'purpose','dataset_root','dataset_identity','temporary','source_root','source_identity',
              'source_catalog_identity','source_snapshot','source_digest','generation'}
    if set(link)!=required or link['purpose']!='stored-migration' or amount!=1:
        raise ValueError('Invalid migration namespace intent')
    root=Path(link['dataset_root']);generation=link['generation']
    if re.fullmatch('[0-9a-f]{32}',generation) is None or link['temporary']!=f'.migration.{generation}.tmp':
        raise ValueError('Migration scratch generation is unbound')
    if identity(root)!=link['dataset_identity']:raise DiskAdmissionError('Migration target identity changed')
    roots=[Path(p) for p, in budget.db.execute('SELECT path FROM roots')]
    if not any(root.is_relative_to(p) for p in roots):raise DiskAdmissionError('Migration scratch is unmanaged')
    if (root/link['temporary']).exists():raise DiskAdmissionError('Migration scratch already exists')
    read_snapshot(link)
    raw=json.dumps(link,sort_keys=True,separators=(',',':'))
    if len(raw.encode())>4096:raise ValueError('Migration intent exceeds bound')
    return raw


@contextmanager
def scratch(sim,source,manifest):
    root=sim.store.directory.resolve();src=source.directory.resolve();generation=uuid4().hex
    link={'purpose':'stored-migration','dataset_root':str(root),'dataset_identity':identity(root),
          'temporary':f'.migration.{generation}.tmp','generation':generation,'source_root':str(src),
          'source_identity':identity(src),'source_catalog_identity':identity(src/'metadata/catalog.sqlite3'),
          'source_snapshot':manifest.snapshot_id,'source_digest':content_hash(asdict(manifest))}
    key=sim.store.disk_budget.reserve(1,operation='migration-open',owner=sim.run_id,generation=generation,
        writing=True,checkpoint_link=link)
    temporary=root/link['temporary']
    created=False
    try:
        temporary.mkdir();created=True;yield temporary
    finally:
        if created and temporary.exists():
            # An uncertain SQLite close must not erase the inode that attributes
            # its retained catalog/startup token. Leave the migration witness too.
            catalog=temporary/'metadata/catalog.sqlite3'
            owner=catalog_file_owner(catalog) if catalog.exists() else None
            for token in sim.store.disk_budget.live_reservations(linked=False):
                if token['operation']=='catalog-transaction' and token['owner']==owner:
                    raise DiskAdmissionError('Migration scratch retains unresolved catalog evidence')
            for token in sim.store.disk_budget.live_reservations(linked=True):
                other=sim.store.disk_budget.checkpoint_link(token['id'])
                if other.get('purpose')=='catalog-startup' and Path(other['path']).is_relative_to(temporary):
                    raise DiskAdmissionError('Migration scratch retains unresolved startup evidence')
            shutil.rmtree(temporary)
        fd=os.open(root,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
        sim.store.disk_budget.release(key)


def inventory(con,roots):
    result={}
    rows=con.execute('''SELECT r.id,r.generation,c.link,r.state FROM reservations r JOIN checkpoint_reservations c USING(id)
                       WHERE r.operation='migration-open' LIMIT 257''').fetchall()
    if len(rows)>256:raise DiskAdmissionError('Migration inventory limit exceeded')
    for key,generation,raw,state in rows:
        link=json.loads(raw)
        if link.get('purpose')!='stored-migration':continue
        root=Path(link['dataset_root']);temporary=root/link['temporary']
        if state not in ('RESERVED','WRITING') and not temporary.exists():continue
        if (not any(root.is_relative_to(p) for p in roots) or identity(root)!=link['dataset_identity']
                or link['generation']!=generation or link['temporary']!=f'.migration.{generation}.tmp'):
            raise DiskAdmissionError('Migration inventory is unbound')
        if str(temporary) in result:raise DiskAdmissionError('Concurrent migration scratch is ambiguous')
        result[str(temporary)]=link
    return result


def verify_clone_version(link,version,segment,offset,checksum):
    """Read one immutable source row and payload at a time; never pool the state."""
    root=Path(link['source_root'])
    con=sqlite3.connect((root/'metadata/catalog.sqlite3').as_uri()+'?mode=ro',uri=True)
    try:
        row=con.execute('SELECT segment,offset,checksum FROM versions WHERE id=?',(version,)).fetchone()
        if row!=(segment,offset,checksum):raise DiskAdmissionError('Migration clone extent differs from its source')
        config=json.loads(con.execute("SELECT data FROM settings WHERE key='config'").fetchone()[0])
        size=(1<<config['block_bits'])*16
    finally:con.close()
    path=root/'state'/f'segment-{segment:06d}.bin'
    if path.is_symlink() or path.stat().st_size<offset+size:raise DiskAdmissionError('Migration source extent is incomplete')
    digest=sha256();remaining=size
    with path.open('rb') as stream:
        stream.seek(offset)
        while remaining:
            chunk=stream.read(min(65536,remaining))
            if not chunk:raise DiskAdmissionError('Migration source extent is truncated')
            digest.update(chunk);remaining-=len(chunk)
    if digest.hexdigest()!=checksum:raise DiskAdmissionError('Migration source extent checksum mismatch')


def plan_recovery(token,link,scope):
    if (token['operation']!='migration-open' or token['generation']!=link['generation']
            or token['bytes']!=1 or identity(Path(link['dataset_root']))!=link['dataset_identity']):
        raise DiskAdmissionError('Migration reservation identity changed')
    leased={Path(l.path) for l in scope['leases']}
    if Path(link['source_root']) not in leased:raise DiskAdmissionError('Migration source is not exclusively leased')
    data=read_snapshot(link);temporary=Path(link['dataset_root'])/link['temporary']
    if str(temporary) not in scope['migration_clones']:raise DiskAdmissionError('Migration clone is unbound')
    return {'class':'migration','attribution':{'intent':link,'source_snapshot':data['snapshot_id'],
            'temporary_present':temporary.exists(),'artifacts_preserved':True}}
