"""Explicit offline settlement of checkpoint sidecar reservations.

MPI exchanges the authoritative metadata. Each rank reads its own files only.
This operation preserves artifacts and never repairs, deletes, or chooses an
older checkpoint. Extent and uncommitted-writer recovery remain separate.
"""
from hashlib import sha256
from itertools import islice
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from types import SimpleNamespace

from .dataset_lease import DirectoryLease
from .disk_budget import DiskAdmissionError, DiskBudget
from .journal import canonical, content_hash


def _checked_path(root, relative, *, missing=False):
    relative=Path(relative)
    if relative.is_absolute() or '..' in relative.parts:
        raise DiskAdmissionError('Recovery path escapes dataset')
    path=root
    for part in relative.parts:
        path=path/part
        try:info=path.lstat()
        except FileNotFoundError:
            if missing:return root/relative,None
            raise
        if stat.S_ISLNK(info.st_mode):raise DiskAdmissionError('Recovery refuses symlink artifacts')
        if info.st_dev!=root.stat().st_dev:raise DiskAdmissionError('Recovery artifact changed volume')
    if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1:
        raise DiskAdmissionError('Recovery requires an exclusively named regular file')
    return path,info


def _read_bytes(root, relative, limit, *, missing=False):
    path,info=_checked_path(root,relative,missing=missing)
    if info is None:return None
    if info.st_size>limit:raise DiskAdmissionError('Recovery artifact exceeds byte limit')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    with os.fdopen(fd,'rb') as stream:
        current=os.fstat(stream.fileno())
        if (current.st_dev,current.st_ino,current.st_size)!=(info.st_dev,info.st_ino,info.st_size):
            raise DiskAdmissionError('Recovery artifact changed during inspection')
        raw=stream.read(limit+1)
    if len(raw)!=info.st_size:raise DiskAdmissionError('Recovery artifact changed size')
    return raw


def _envelope(root,relative,limit):
    raw=_read_bytes(root,relative,limit)
    data=json.loads(raw)
    if data.get('checksum')!=content_hash(data.get('manifest')):
        raise DiskAdmissionError('Recovery checkpoint checksum mismatch')
    return data['manifest'],raw


def _catalog(root,rank,limit):
    relative=f'rank-{rank:04d}/metadata/catalog.sqlite3'
    path,info=_checked_path(root,relative)
    if info.st_size>limit:raise DiskAdmissionError('Recovery catalog exceeds inspection limit')
    for suffix in ('-journal','-wal','-shm'):
        _,side=_checked_path(root,relative+suffix,missing=True)
        if side is not None and side.st_size:
            raise DiskAdmissionError('Catalog side file needs admitted recovery before reservation settlement')
    con=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)
    try:
        con.execute('PRAGMA query_only=ON');con.execute('PRAGMA cache_size=-16')
        if con.execute('PRAGMA quick_check').fetchall()!=[('ok',)]:
            raise DiskAdmissionError('Recovery catalog integrity check failed')
        settings={key:json.loads(value) for key,value in con.execute("SELECT key,data FROM distributed_settings WHERE key IN ('dataset_id','store_id','registry','disk_admission_ledger')")}
        return con,settings,{'device':info.st_dev,'inode':info.st_ino}
    except BaseException:con.close();raise


def _journal_proof(root,rank,cursors,limit):
    if type(cursors) is not dict or set(cursors)-{'gates','local','shared','joint','commits','frame'}:
        raise DiskAdmissionError('Unsupported committed journal cursor')
    proofs={};remaining=limit
    for kind,cursor in sorted(cursors.items()):
        if type(cursor) is not int or cursor<0 or cursor>remaining:
            raise DiskAdmissionError('Committed journal exceeds inspection limit')
        remaining-=cursor
        path,info=_checked_path(root,f'rank-{rank:04d}/journal/{kind}.jsonl',missing=cursor==0)
        if info is None:
            proofs[kind]={'bytes':0,'sha256':sha256(b'').hexdigest()};continue
        if info.st_size<cursor:raise DiskAdmissionError('Truncated committed journal')
        digest=sha256();read=0
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
        with os.fdopen(fd,'rb') as stream:
            while read<cursor:
                raw=stream.readline(min(1024**2+1,cursor-read+1))
                if not raw or not raw.endswith(b'\n') or len(raw)>1024**2 or read+len(raw)>cursor:
                    raise DiskAdmissionError('Incomplete committed journal record')
                row=json.loads(raw);checksum=row.pop('checksum',None)
                if row.get('schema_version')!=1 or checksum!=content_hash(row):
                    raise DiskAdmissionError('Committed journal checksum mismatch')
                digest.update(raw);read+=len(raw)
        proofs[kind]={'bytes':read,'sha256':digest.hexdigest()}
    return proofs


def _validate_local_cut(root,rank,authority,config,max_metadata_bytes,max_journal_bytes):
    decision=authority['decision'];sid=decision['snapshot_id']
    prepared,raw=_envelope(root,f'rank-{rank:04d}/manifests/prepare-{sid}.json',max_metadata_bytes)
    expected=next(p for p in decision['prepares'] if p['rank']==rank)
    if (content_hash(prepared)!=expected['hash'] or prepared['schema_version']!=4 or
            prepared['snapshot_id']!=sid or prepared['rank']!=rank or
            prepared['root']!=decision['root'] or prepared['frontier']!=decision['frontier']):
        raise DiskAdmissionError('Recovery prepare conflicts with authoritative decision')
    con,settings,catalog_identity=_catalog(root,rank,max_journal_bytes)
    try:
        if settings['dataset_id']!=authority['dataset_id'] or settings['store_id']!=prepared['store_id']:
            raise DiskAdmissionError('Recovery dataset/owner identity mismatch')
        saved=con.execute('SELECT data FROM snapshots WHERE id=?',(sid,)).fetchone()
        if saved is None or canonical(json.loads(saved[0]))!=canonical(prepared):
            raise DiskAdmissionError('Recovery catalog snapshot conflicts with prepare')
        row=con.execute('SELECT sealed FROM roots WHERE id=?',(decision['root'],)).fetchone()
        if row is None or row[0]!=1:raise DiskAdmissionError('Recovery requires a sealed checkpoint root')
        # Reuse the normal semantic metadata validator without opening a
        # writable store, adding active runs, or changing catalog settings.
        def root_chain(start):
            seen=set()
            while start:
                if start in seen or len(seen)>=256:raise DiskAdmissionError('Invalid or excessive checkpoint root chain')
                seen.add(start)
                row=con.execute('SELECT parent FROM roots WHERE id=?',(start,)).fetchone()
                if row is None:raise DiskAdmissionError('Unknown checkpoint ancestor')
                yield start
                start=row[0]
        from .phase2_runtime import Phase2Catalog
        store=SimpleNamespace(db=con,root_chain=root_chain)
        Phase2Catalog(SimpleNamespace(store=store,config=config,block_bits=config.block_bits)).validate_root(decision['root'])
        if con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='resident_versions'").fetchone():
            if con.execute('SELECT 1 FROM resident_versions LIMIT 1').fetchone():
                raise DiskAdmissionError('Recovery refuses unflushed resident amplitudes')
    finally:con.close()
    return {'prepare_sha256':sha256(raw).hexdigest(),'catalog_identity':catalog_identity,
            'journal':_journal_proof(root,rank,prepared['journal_cursors'],max_journal_bytes)},raw


def _plan_item(root,rank,authority,cut_proof,prepare_raw,item,max_metadata_bytes):
    reservation,link=item['reservation'],item['link']
    if reservation['state'] not in ('RESERVED','WRITING'):raise DiskAdmissionError('Recovery reservation is already terminal')
    current=root.stat()
    if (link['dataset_root']!=str(root) or (link['dataset_device'],link['dataset_inode'])!=(current.st_dev,current.st_ino)
            or link['rank']!=rank or link['dataset_id']!=authority['dataset_id']
            or link['snapshot_id']!=authority['decision']['snapshot_id']):
        raise DiskAdmissionError('Recovery intent belongs to a different dataset generation or cut')
    if link.get('stage') in ('metadata-payload','metadata-chunk'):
        from .checkpoint_metadata_recovery import plan_recovery
        planned=plan_recovery(reservation,link,leased_directories=[root],max_bytes=max_metadata_bytes)
        proof={'version':1,'reservation_id':reservation['id'],'intent_sha256':content_hash(link),
               'local_cut':cut_proof,**planned,'artifacts_preserved':True}
        return {'item':item,'proof':proof,'materialized_bytes':reservation['materialized_bytes'],
                'terminal_state':'RECONCILED'}
    sid=link['snapshot_id'];stage=link['stage']
    targets={'prepare':f'rank-{rank:04d}/manifests/prepare-{sid}.json',
             'decision':f'global/manifests/{sid}.json','decision-copy':f'rank-{rank:04d}/manifests/decision-{sid}.json'}
    if stage not in targets or link['target']!=targets[stage] or (stage=='decision' and rank!=0):
        raise DiskAdmissionError('Recovery intent stage/path mismatch')
    if (reservation['operation']!='atomic-json' or re.fullmatch('[0-9a-f]{32}',reservation['owner'] or '') is None or
            re.fullmatch('[0-9a-f]{32}',reservation['generation'] or '') is None):
        raise DiskAdmissionError('Recovery refuses an unbound writer generation')
    target=root/link['target']
    expected_temp=str(target.relative_to(root).parent/('.'+target.name+'.'+reservation['generation']+'.tmp'))
    if link['temporary']!=expected_temp or link['expected_bytes']!=reservation['bytes']:
        raise DiskAdmissionError('Recovery temporary generation mismatch')
    decision=authority['decision']
    expected_raw=prepare_raw if stage=='prepare' else (canonical({'manifest':decision,'checksum':content_hash(decision)})+'\n').encode()
    if sha256(expected_raw).hexdigest()!=link['digest'] or len(expected_raw)!=reservation['bytes']:
        raise DiskAdmissionError('Recovery intended content conflicts with verified cut')
    inventory={};growth=0
    for kind,relative in (('target',link['target']),('temporary',link['temporary'])):
        raw=_read_bytes(root,relative,max_metadata_bytes,missing=True)
        if raw is not None:
            if len(raw)>reservation['bytes'] or (kind=='target' and raw!=expected_raw) or (len(raw)==reservation['bytes'] and raw!=expected_raw):
                raise DiskAdmissionError('Recovery artifact conflicts with reservation')
            if kind=='temporary' or not link['target_existed']:growth+=len(raw)
        inventory[kind]={'bytes':len(raw) if raw is not None else 0,'present':raw is not None,
                         'sha256':sha256(raw).hexdigest() if raw is not None else None}
    if growth>reservation['bytes']:raise DiskAdmissionError('Recovery inventory exceeds reserved generation')
    if stage in ('prepare','decision') and not inventory['target']['present']:
        raise DiskAdmissionError('Authoritative checkpoint artifact is missing')
    proof={'version':1,'reservation_id':reservation['id'],'intent_sha256':content_hash(link),
           'snapshot_id':sid,'decision_sha256':authority['decision_sha256'],
           'local_cut':cut_proof,'inventory':inventory,'observed_growth_bytes':growth,
           'metadata_only':True,'artifacts_preserved':True}
    return {'item':item,'proof':proof,'materialized_bytes':max(growth,reservation['materialized_bytes']),
            'terminal_state':'DURABLE' if inventory['target']['present'] else 'RECONCILED'}


def recover_checkpoint_reservations(config, *, comm=None, snapshot=None, apply=False,
                                   max_intents=128,max_metadata_bytes=16*1024**2,max_journal_bytes=64*1024**2):
    """Collectively inspect/apply sidecar settlement after all runtimes stop.

    Default is a dry run. A current v4 COMMIT, owner prepare, catalog semantic
    metadata, journals and generation inventory must validate on every rank
    before any reservation changes. This does not validate amplitude payloads
    or reclaim uncommitted, unlinked, catalog, or extent reservations. Payload
    and chunk intents of the verified current cut use their dedicated inventory
    and authoritative-content checks.
    """
    from .mpi_transport import MPITransport
    if comm is None:
        from mpi4py import MPI
        comm=MPI.COMM_WORLD
    transport=MPITransport(comm,tile_bytes=config.tile_bytes,metadata_limit=config.metadata_reserve_bytes//2)
    rank=transport.rank;leases={};budget=None;coordinator=None
    try:
        error=None
        try:
            if config.storage_scope!='node-local':raise ValueError('Recovery requires node-local configuration')
            if type(apply) is not bool:raise ValueError('apply must be boolean')
            if any(type(n) is not int or n<1 for n in (max_intents,max_metadata_bytes,max_journal_bytes)):
                raise ValueError('Recovery inspection limits must be positive integers')
            root=Path(config.directory).absolute()
            if root.is_symlink():raise DiskAdmissionError('Recovery dataset cannot be a symlink')
            root=root.resolve(strict=True)
            info=root.stat()
            if not stat.S_ISDIR(info.st_mode):raise ValueError('Recovery needs an existing dataset')
        except Exception as exc:error=str(exc)
        transport.vote(error)
        transport.agree({'apply':apply,'snapshot':snapshot,'max_intents':max_intents,
                         'max_metadata_bytes':max_metadata_bytes,'max_journal_bytes':max_journal_bytes,
                         'block_bits':config.block_bits,'storage_scope':config.storage_scope})
        environment=transport.handshake()
        records=transport.control.allgather({'host':environment[rank]['host'],'device':info.st_dev,'root':str(root),'rank':rank})
        error=None
        try:
            owners=[r['rank'] for r in records if r['host']==environment[rank]['host'] and r['root']==str(root)]
            if rank==min(owners):leases[str(root)]=DirectoryLease(root,exclusive=True)
            shard=root/f'rank-{rank:04d}'
            leases[str(shard)]=DirectoryLease(shard,exclusive=True)
        except Exception as exc:error=str(exc)
        transport.vote(error)
        error=None
        try:
            con,settings,_=_catalog(root,rank,max_journal_bytes)
            con.close()
            persisted=settings.get('disk_admission_ledger')
            ledger=config.disk_admission_ledger or persisted
            if ledger is None:raise DiskAdmissionError('Recovery needs a persisted or explicit ledger locator')
            ledger=Path(ledger)
            if not ledger.is_absolute() or ledger.is_symlink() or not ledger.is_file() or ledger.stat().st_dev!=info.st_dev:
                raise DiskAdmissionError('Recovery ledger locator is not an existing local ledger')
            ledger=ledger.resolve(strict=True)
            if persisted is not None and ledger!=Path(persisted).resolve(strict=True):
                raise DiskAdmissionError('Recovery ledger locator conflicts with the persisted locator')
        except Exception as exc:error=str(exc)
        transport.vote(error)
        locators=transport.control.allgather(str(ledger))
        error=None
        try:
            for record in records:
                if record['host']==environment[rank]['host'] and record['device']==info.st_dev:
                    if locators[record['rank']]!=str(ledger):
                        raise DiskAdmissionError('Recovery ranks on one volume require the same ledger')
            local_roots=sorted({r['root'] for r in records if r['host']==environment[rank]['host'] and r['device']==info.st_dev})
            binding=DiskBudget.read_volume_binding(ledger)
            if binding is not None:
                bound_ranks=[r['rank'] for r in records if r['host']==environment[rank]['host'] and r['device']==info.st_dev]
                declared=getattr(config,'volume_coordinator',None)
                if declared is not None and str(Path(declared).resolve())!=binding:
                    raise DiskAdmissionError('Recovery coordinator conflicts with the ledger binding')
                if rank==min(bound_ranks):
                    # One rank per ledger republishes the settled claim; it must hold the exclusive ledger-directory
                    # lease (no live writer) and a readable coordinator before any reservation changes.
                    from .volume_coordinator import VolumeCoordinator
                    key=str(ledger.parent)
                    if key not in leases:
                        try:leases[key]=DirectoryLease(ledger.parent,exclusive=True)
                        except Exception as exc:raise DiskAdmissionError(f'A live process holds the disk ledger: {exc}') from exc
                    coordinator=VolumeCoordinator.open_bound(binding)
            budget=DiskBudget(ledger,local_roots,quota_bytes=config.disk_quota_bytes,
                              min_free_bytes=config.min_free_bytes,drain_reserve_bytes=config.drain_reserve_bytes,
                              shared_policy=True,recovery_only=True)
        except Exception as exc:error=str(exc)
        transport.vote(error)
        error=None;authority=None
        if rank==0:
            try:
                # Bound every candidate before the existing newest-cut loader
                # parses it, including the separately streamed metadata payload.
                candidates=list(islice((root/'global/manifests').glob('*.json'),257))
                if len(candidates)>256:raise DiskAdmissionError('Recovery checkpoint candidate limit exceeded')
                if not candidates:raise DiskAdmissionError('Recovery needs an authoritative checkpoint')
                latest=max(candidates,key=lambda p:p.stat().st_mtime_ns)
                decision,_=_envelope(root,str(latest.relative_to(root)),max_metadata_bytes)
                if decision.get('schema_version')!=4:raise DiskAdmissionError('Recovery requires a v4 authoritative decision')
                ref=decision['metadata_payload']
                _read_bytes(root,f'rank-0000/manifests/{ref["name"]}',max_metadata_bytes)
                from .distributed_checkpoint import load_global
                from .checkpoint_payload import decision_record
                data=load_global(root,decision['snapshot_id'])
                if data['schema_version']!=4 or data['size']!=transport.size or data['block_bits']!=config.block_bits:
                    raise DiskAdmissionError('Recovery requires a matching v4 authoritative cut')
                if snapshot is not None:
                    requested=snapshot['snapshot_id'] if isinstance(snapshot,dict) else snapshot
                    if requested!=data['snapshot_id']:raise DiskAdmissionError('Recovery refuses an older requested cut')
                decision=decision_record(data)
                authority={'dataset_id':data['dataset_id'],'decision':decision,'decision_sha256':content_hash(decision)}
            except Exception as exc:error=str(exc)
        transport.vote(error)
        authority=transport.broadcast_json(authority)
        plans=[];error=None;settled=0
        try:
            cut_proof,prepare_raw=_validate_local_cut(root,rank,authority,config,max_metadata_bytes,max_journal_bytes)
            links=budget.checkpoint_links(root,limit=max_intents)
            targets=set()
            for item in links:
                if item['link'].get('purpose') in ('state-export','catalog-startup'):
                    continue  # Explicit export inventory is recovered by recover_uncommitted_reservations.
                if item['link']['rank']!=rank:continue
                if item['reservation']['state'] not in ('RESERVED','WRITING'):
                    settled+=1;continue
                if item['link']['target'] in targets:raise DiskAdmissionError('Concurrent intents for one target are ambiguous')
                targets.add(item['link']['target'])
                plans.append(_plan_item(root,rank,authority,cut_proof,prepare_raw,item,max_metadata_bytes))
            for lease in leases.values():lease.assert_current()
        except Exception as exc:error=str(exc)
        # Every owner validates before any owner applies. Applying receipts is
        # atomic per reservation; interruption may leave some owners settled.
        transport.vote(error)
        receipts=[];error=None
        try:
            for plan in plans:
                reservation=plan['item']['reservation'];link=plan['item']['link'];proof=plan['proof']
                if apply:
                    for lease in leases.values():lease.assert_current()
                    budget.reconcile_checkpoint(reservation['id'],expected_reservation=reservation,expected_link=link,
                        materialized_bytes=plan['materialized_bytes'],terminal_state=plan['terminal_state'],
                        evidence=f'checkpoint-recovery-v1:{link["snapshot_id"]}:{content_hash(proof)}',evidence_document=proof)
                receipts.append({'reservation_id':reservation['id'],'status':'settled' if apply else 'would-settle',
                                 'released_pending_bytes':reservation['bytes']-reservation['materialized_bytes'] if apply else 0,
                                 'would_release_pending_bytes':reservation['bytes']-reservation['materialized_bytes'],
                                 'terminal_state':plan['terminal_state'],'proof':proof})
        except Exception as exc:error=str(exc)
        transport.vote(error)
        # Every rank has finished applying (the vote above is collective) before the settled claim is published.
        claim=None;error=None
        try:
            if coordinator is not None:
                if apply:
                    claim=budget.republish_volume_claim(coordinator,leases[str(ledger.parent)])
                    claim['status']='republished'
                else:
                    published=coordinator.claim(ledger)
                    claim={'coordinator':str(coordinator.path),'ledger':str(ledger),'status':'dry-run',
                           'previous_claim_bytes':None if published is None else published['pending'],
                           'published_pending_bytes':budget.reserved_bytes}
        except Exception as exc:error=str(exc)
        transport.vote(error)
        reports=transport.control.allgather({'rank':rank,'receipts':receipts,'already_terminal':settled,'volume_claim':claim})
        return {'kind':'checkpoint-reservation-recovery','apply':apply,'snapshot_id':authority['decision']['snapshot_id'],
                'metadata_only':True,'artifacts_preserved':True,'ranks':reports,'strict_capacity_guarantee':False,
                'ledger_open_mode':'recovery-only','metadata_pool_bytes':budget.bootstrap_bytes}
    finally:
        if budget is not None:budget.close()
        if coordinator is not None:coordinator.close()
        for lease in leases.values():lease.close()
        transport.close()
