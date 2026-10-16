"""Durable rank prepare records and one authoritative global commit file."""
from dataclasses import replace
from hashlib import sha256
from contextlib import nullcontext
import json
import os
from pathlib import Path
import sqlite3
from uuid import uuid4
import numpy as np
from .journal import canonical,content_hash,gate_data,decode_gate
from .parameters import ParameterBinding,ExactPi
from .layout import QubitLayout
from .admitted_files import file_allocation,checked_sidecar_path


def atomic_json(path,data,*,disk_budget=None,checkpoint_context=None,owner=None,operation='atomic-json'):
    from .dataset_lease import DirectoryLease
    context=DirectoryLease(checkpoint_context['dataset_root']) if checkpoint_context is not None else nullcontext()
    with context:
        return _atomic_json(path,data,disk_budget=disk_budget,checkpoint_context=checkpoint_context,owner=owner,operation=operation)


def _atomic_json(path,data,*,disk_budget=None,checkpoint_context=None,owner=None,operation='atomic-json'):
    path=checked_sidecar_path(disk_budget,path)
    raw=(canonical(data)+'\n').encode()
    nonce=uuid4().hex
    temporary=path.with_name('.'+path.name+'.'+nonce+'.tmp')
    link=None
    if checkpoint_context is not None:
        link={**checkpoint_context,'target':str(path.absolute()),'temporary':str(temporary.absolute()),
              'digest':sha256(raw).hexdigest()}
    # Keep admission through the rename, directory durability barrier, and all
    # temporary-file cleanup. A rejected reservation therefore leaves no files.
    with file_allocation(disk_budget,len(raw),temporary,drain=True,operation=operation,
                         owner=owner,generation=nonce,checkpoint_link=link) as key:
        path.parent.mkdir(parents=True,exist_ok=True)
        created=False
        try:
            try:
                stream=open(temporary,'xb')
            except FileExistsError:
                # This token did not create the colliding artifact; leave that
                # file and its owner's recovery evidence untouched.
                if disk_budget is not None: disk_budget.release(key)
                raise
            created=True
            with stream as f:
                written=f.write(raw)
                if written != len(raw): raise OSError('Incomplete atomic JSON write')
                f.flush()
                if disk_budget is not None: disk_budget.materialize(key,len(raw))
                os.fsync(f.fileno())
            os.replace(temporary,path)
            fd=os.open(path.parent,os.O_RDONLY)
            try: os.fsync(fd)
            finally: os.close(fd)
        finally:
            if created: temporary.unlink(missing_ok=True)


def _disk_kwargs(sim,snapshot_id=None,stage=None):
    budget=getattr(sim,'disk_budget',None)
    if budget is None:return {}
    kwargs={'disk_budget':budget}
    if snapshot_id is None:kwargs['operation']='dataset-lease-json'
    if snapshot_id is not None:
        for lease in getattr(sim,'_dataset_leases',{}).values():lease.assert_current()
        kwargs.update(checkpoint_context={'dataset_root':str(sim.directory.absolute()),
            'dataset_id':sim.dataset_id,'snapshot_id':snapshot_id,'rank':sim.rank,'stage':stage},
            owner=sim.run_id)
    return kwargs


def commit_snapshot(sim,prefix_fingerprint=None):
    sim._open(); sim._checkpoint_decision_durable=False
    sim.transport.agree({'root':sim.root,'frontier':sim.frontier,'binding':sim.binding.binding_id if sim.binding else '',
        'layout':sim.layout.logical_to_physical,'placement':sim.placement_epoch})
    version=4 if sim.config.storage_scope == 'node-local' or hasattr(sim,'_checkpoint_state') else 3
    snapshot_id=sim.transport.broadcast_json(uuid4().hex if sim.rank == 0 else None)
    prepared=None; error=None
    from time import perf_counter
    record=getattr(sim,'_record_stage',None) or (lambda *a,**k: None)
    stage_started=perf_counter()
    try:
        sim.store._disk_drain=True
        if getattr(sim.store.db,'_batch',None) is not None: raise RuntimeError('Snapshot cannot start inside a catalog batch')
        sim.local._persist_assets(); sim.cache.flush()
        if hasattr(sim.store,'flush_to_file'): sim.store.flush_to_file()
        sim.local.journal.flush(); sim.store.flush(); sim.local.phase2.validate_root(sim.root); sim.store.seal(sim.root)
        prepared={'schema_version':version,'snapshot_id':snapshot_id,'rank':sim.rank,'root':sim.root,
                  'frontier':sim.frontier,'store_id':sim.store_id,'journal_cursors':sim.local.journal.cursors()}
        path=sim.directory/f'rank-{sim.rank:04d}'/'manifests'/f'prepare-{snapshot_id}.json'
        atomic_json(path,{'manifest':prepared,'checksum':content_hash(prepared)},**_disk_kwargs(sim,snapshot_id,'prepare'))
        with sim.store.db:
            sim.store.db.execute('INSERT OR REPLACE INTO snapshots VALUES(?,?)',(snapshot_id,canonical(prepared)))
    except Exception as exc: error=str(exc)
    finally: sim.store._disk_drain=False
    sim.transport.vote(error)
    record('checkpoint-prepare',{'schema_version':version,'store':sim.config.amplitude_store},
           seconds=perf_counter()-stage_started)
    stage_started=perf_counter()
    prepares=sim.transport.control.allgather({'rank':sim.rank,'hash':content_hash(prepared)})
    data={'schema_version':version,'semantics_version':version,'snapshot_id':snapshot_id,'dataset_id':sim.dataset_id,
          'num_qubits':sim.num_qubits,'block_bits':sim.block_bits,'size':sim.size,'root':sim.root,'frontier':sim.frontier,
          'layout':sim.layout.logical_to_physical,'layout_id':sim.layout.layout_id,'placement_epoch':sim.placement_epoch,
          'binding_values':sim.binding.values if sim.binding else (),
          'binding_witnesses':[(k,v.numerator,v.denominator) for k,v in (sim.binding.exact_witnesses if sim.binding else ())],
          'parameter_dependencies':sorted(frozenset().union(*(g.dependencies for g in sim.sequencer.operations[:sim.frontier+1]))),
          'circuit_hash':sim.circuit_hash,'gates':[gate_data(g) for g in sim.sequencer.operations],
          'assets':sorted(sim.assets.assets),'registry':sim.registry,'initial_state_hash':sim.initial_state_hash,
          'history':sim.history,'prefix_fingerprint':prefix_fingerprint,'prepares':prepares}
    if version == 4:
        data['storage_scope']=sim.config.storage_scope
        data['commit_decision']='COMMIT'
        data['phase4_runtime']=getattr(sim,'_checkpoint_state',lambda: {'execution':'synchronous-S0'})()
    sim.transport.agree(content_hash(data)); error=None
    from .checkpoint_payload import distribute_payload,decision_record
    if version == 4:
        # Gates and history go out as write-once content-addressed chunks; the payload holds
        # only their references, so a checkpoint writes the changed tail, not the whole lists.
        if not hasattr(sim,'_payload_chunk_cache'):sim._payload_chunk_cache={}
        payload=distribute_payload(sim.transport,data if sim.rank == 0 else None,sim.directory,chunked=True,
            expand=False,chunk_cache=sim._payload_chunk_cache,
            sources={'gates':id(sim.sequencer.operations),'history':id(sim.history)},
            disk_budget=getattr(sim,'disk_budget',None),budget=sim.budget,cache=sim.cache)
        # Every rank agreed on this data above: return it in the loaded (expanded) form without a read-back.
        data={**data,'payload_format':2,'metadata_payload':payload['metadata_payload']}
        decision=decision_record(payload)
    else: decision=data
    if version == 4: sim.transport.agree(decision)
    if sim.rank == 0:
        try: atomic_json(sim.directory/'global/manifests'/f'{snapshot_id}.json',{'manifest':decision,'checksum':content_hash(decision)},**_disk_kwargs(sim,snapshot_id,'decision'))
        except Exception as exc: error=str(exc)
    sim.transport.vote(error)
    sim._checkpoint_decision_durable=True
    record('checkpoint-decision',{'schema_version':version,'writer':'rank-0'},seconds=perf_counter()-stage_started)
    stage_started=perf_counter()
    if version == 4:
        # The coordinator file above is authoritative even if copying fails.
        try: decision=sim.transport.broadcast_json(decision if sim.rank == 0 else None)
        except Exception:
            sim.checkpoint_requires_recovery=snapshot_id
            raise
        error=None
        try:
            atomic_json(sim.directory/f'rank-{sim.rank:04d}'/'manifests'/f'decision-{snapshot_id}.json',
                {'manifest':decision,'checksum':content_hash(decision)},**_disk_kwargs(sim,snapshot_id,'decision-copy'))
        except Exception as exc: error='Committed decision copy failed; stop and recover: '+str(exc)
        try: sim.transport.vote(error)
        except Exception:
            sim.checkpoint_requires_recovery=snapshot_id
            raise
    if version == 4:
        record('checkpoint-ack',{'schema_version':version,'copy':'rank-local-decision'},seconds=perf_counter()-stage_started)
    sim.trace.record(snapshot_id,metadata={'path':'DISTRIBUTED_CHECKPOINT','root':sim.root,'frontier':sim.frontier,'rank':sim.rank})
    sim.committed_roots = getattr(sim,'committed_roots',set())|{sim.root}
    return data if version == 4 else sim.transport.broadcast_json(data if sim.rank == 0 else None)


def _read(path):
    envelope=json.loads(Path(path).read_text())
    if envelope.get('checksum') != content_hash(envelope.get('manifest')): raise OSError('Distributed manifest checksum mismatch')
    return envelope['manifest']


def load_global(directory,snapshot=None):
    directory=Path(directory)
    if snapshot is None:
        candidates=sorted((directory/'global/manifests').glob('*.json'),key=lambda p:p.stat().st_mtime_ns,reverse=True)
        if not candidates: raise FileNotFoundError('No committed global checkpoint')
    else:
        key=snapshot['snapshot_id'] if isinstance(snapshot,dict) else str(snapshot)
        candidates=[directory/'global/manifests'/f'{key}.json']
    failure=None
    v4_history=any((directory/'rank-0000/manifests').glob('payload-*.json'))
    for path in candidates:
        try:
            data=_read(path)
            if data.get('schema_version') == 4 and 'metadata_payload' in data:
                from .checkpoint_payload import read_payload
                data=read_payload(directory,data)
            if data.get('schema_version') not in {3,4} or data.get('semantics_version') != data.get('schema_version'): raise ValueError('Unsupported distributed checkpoint schema')
            if data['schema_version'] == 4 and data.get('commit_decision') != 'COMMIT': raise OSError('Missing authoritative commit decision')
            from .ownership import OwnershipMap
            OwnershipMap(data['num_qubits'],data['block_bits'],data['size'])
            if sorted(prepare['rank'] for prepare in data['prepares']) != list(range(data['size'])):
                raise OSError('Checkpoint requires exactly one prepare per rank')
            if isinstance(snapshot,dict) and canonical(data) != canonical(snapshot): raise ValueError('Snapshot fingerprint mismatch')
            for prepare in (data['prepares'] if data['schema_version'] == 3 else []):
                payload=_read(directory/f"rank-{prepare['rank']:04d}"/'manifests'/f"prepare-{data['snapshot_id']}.json")
                if (content_hash(payload) != prepare['hash'] or payload['root'] != data['root']
                    or payload['rank'] != prepare['rank'] or payload['snapshot_id'] != data['snapshot_id']
                    or payload['schema_version'] != 3 or payload['frontier'] != data['frontier']):
                    raise OSError('Rank prepare mismatch')
            return data
        except Exception as exc:
            failure=exc
            if snapshot is not None: raise
            if v4_history:
                raise OSError('Authoritative checkpoint is unreadable; refusing an older cut') from exc
    raise OSError('No valid committed distributed checkpoint') from failure


def load_owner_global(sim,snapshot):
    """Locate a named source cut on this coordinator's owner volumes only."""
    if sim.rank != 0: raise ValueError('Only the coordinator reads a global decision')
    if snapshot is None: return load_global(sim.directory,snapshot)
    from .storage_placement import resolve_owner_root
    roots=[sim.directory]
    for entry in sim.registry.values():
        root=resolve_owner_root(entry,0,host=sim.local.profile.hostname)
        if root not in roots: roots.append(root)
    failure=None
    for root in roots:
        try: return load_global(root,snapshot)
        except FileNotFoundError as exc: failure=exc
    raise FileNotFoundError('Checkpoint absent from coordinator owner volumes') from failure


def restore(sim,snapshot,*,validate=True):
    data=None; error=None
    try:
        if validate and sim.rank == 0:
            data=load_global(sim.directory,snapshot)
        elif not validate: data=snapshot
        if data is not None and (data['num_qubits'] != sim.num_qubits or data['block_bits'] != sim.block_bits or data['size'] != sim.size):
            raise ValueError('Checkpoint rank/dimensions mismatch; offline repartition required')
    except Exception as exc: error=str(exc)
    sim.transport.vote(error)
    if validate:
        version=sim.transport.broadcast_json(data['schema_version'] if sim.rank == 0 else None)
        if version == 4:
            from .checkpoint_payload import distribute_payload
            chunked=sim.transport.broadcast_json(data.get('payload_format')==2 if sim.rank == 0 else None)
            data=distribute_payload(sim.transport,data if sim.rank == 0 else None,sim.directory,chunked=chunked,
                disk_budget=getattr(sim,'disk_budget',None),budget=sim.budget,cache=sim.cache)
        else: data=sim.transport.broadcast_json(data if sim.rank == 0 else None)
    sim.transport.agree(content_hash(data)); error=None
    try:
        if validate:
            prepared=_read(sim.directory/f'rank-{sim.rank:04d}'/'manifests'/f"prepare-{data['snapshot_id']}.json")
            expected=next(p for p in data['prepares'] if p['rank']==sim.rank)
            if (content_hash(prepared) != expected['hash'] or prepared['rank'] != sim.rank or prepared['root'] != data['root']
                or prepared['frontier'] != data['frontier'] or prepared['schema_version'] != data['schema_version']
                or prepared['snapshot_id'] != data['snapshot_id'] or prepared['store_id'] != sim.store_id):
                raise OSError('Rank prepare mismatch')
            if data['schema_version'] == 4:
                from .storage_placement import resolve_owner_root
                if isinstance(data['registry'][sim.store_id],list):
                    root=resolve_owner_root(data['registry'][sim.store_id],sim.rank,host=sim.local.profile.hostname)
                    if root.resolve() != sim.directory.resolve(): raise OSError('Owner root changed; explicit migration required')
                copy=sim.directory/f'rank-{sim.rank:04d}'/'manifests'/f"decision-{data['snapshot_id']}.json"
                from .checkpoint_payload import decision_record
                decision=decision_record(data)
                if copy.exists() and content_hash(_read(copy)) != content_hash(decision): raise OSError('Decision copy conflicts with authoritative commit')
                # Repair a missing copy only after authority and prepare validate.
                if not copy.exists(): atomic_json(copy,{'manifest':decision,'checksum':content_hash(decision)},**_disk_kwargs(sim,data['snapshot_id'],'decision-copy'))
            for kind,cursor in prepared['journal_cursors'].items():
                for _ in sim.local.journal.replay(kind,cursor): pass
        sim.sequencer.operations[:] = [decode_gate(g) for g in data['gates']]
        sim.registry=dict(data['registry'])
        if sim.store_id not in sim.registry: sim.registry[sim.store_id]=sim._placement_entry()
        if sim.config.storage_scope == 'node-local': sim._upgrade_legacy_registry()
        for key in data['assets']:
            from .storage_placement import resolve_owner_root
            paths=[resolve_owner_root(p,sim.rank,host=sim.local.profile.hostname)/f'rank-{sim.rank:04d}/assets/{key}.bin' for p in sim.registry.values()]
            path=next((p for p in paths if p.is_file()),None)
            if path is None: raise OSError('Missing checkpoint matrix asset')
            raw=path.read_bytes(); n=int((len(raw)//16)**.5)
            if n*n*16 != len(raw): raise OSError('Invalid matrix asset')
            matrix=np.frombuffer(raw,dtype='<c16').reshape(n,n)
            if sim.assets.register(matrix,n.bit_length()-1) != key: raise OSError('Matrix asset checksum mismatch')
        if sim.circuit_hash != data['circuit_hash']: raise OSError('Checkpoint circuit hash mismatch')
        sim.binding=sim.local.binding=ParameterBinding(sim.circuit_hash,tuple(tuple(x) for x in data['binding_values']),
            tuple((k,ExactPi(n,d)) for k,n,d in data['binding_witnesses']))
        sim.root=sim.local.root=data['root']; sim.frontier=sim.local.frontier=data['frontier']
        sim.layout=sim.local.layout=QubitLayout(tuple(data['layout']),data['layout_id']); sim.placement_epoch=data['placement_epoch']
        sim.history=list(data['history']); sim.initial_state_hash=data['initial_state_hash']; sim.dataset_id=data['dataset_id']
        sim.local._readonly_circuit=True; sim.local.phase2.validate_root(sim.root); sim._save_settings()
        if hasattr(sim,'_restore_checkpoint_state'): sim._restore_checkpoint_state(data.get('phase4_runtime'))
    except Exception as exc: error=str(exc)
    sim.transport.vote(error)
    sim.committed_roots={sim.root}
    return sim


def fork(sim,snapshot,binding,directory=None):
    data=None; error=None
    try:
        data=load_owner_global(sim,snapshot) if sim.rank == 0 else None
    except Exception as exc: error=str(exc)
    sim.transport.vote(error)
    version=sim.transport.broadcast_json(data['schema_version'] if sim.rank == 0 else None)
    if version == 4:
        from .checkpoint_payload import distribute_payload
        chunked=sim.transport.broadcast_json(data.get('payload_format')==2 if sim.rank == 0 else None)
        data=distribute_payload(sim.transport,data if sim.rank == 0 else None,sim.directory,chunked=chunked,
            disk_budget=getattr(sim,'disk_budget',None),budget=sim.budget,cache=sim.cache)
    else: data=sim.transport.broadcast_json(data if sim.rank == 0 else None)
    error=None
    try:
        if binding.circuit_hash != sim.circuit_hash or data['circuit_hash'] != sim.circuit_hash: raise ValueError('Prefix source mismatch')
        fixed=dict(data['binding_values']); witnesses={k:ExactPi(n,d) for k,n,d in data['binding_witnesses']}
        for key in data['parameter_dependencies']:
            if binding.value(key) != fixed[key] or dict(binding.exact_witnesses).get(key) != witnesses.get(key):
                raise ValueError('Prefix fixed parameter/witness fingerprint mismatch')
        if tuple(data['layout']) != sim.layout.logical_to_physical or data['placement_epoch'] != sim.placement_epoch:
            raise ValueError('Prefix layout/placement mismatch')
    except Exception as exc: error=str(exc)
    sim.transport.vote(error); sim.transport.agree(binding.binding_id)
    if directory is None:
        run=sim.transport.broadcast_json(uuid4().hex if sim.rank == 0 else None)
        directory=str(sim.directory/'suffixes'/run)
    from .distributed import DistributedSimulator
    child_config=replace(sim.config,directory=str(directory))
    if sim.config.storage_scope=='node-local':
        child_config=replace(child_config,disk_admission_ledger=str(sim.disk_budget._ledger_path))
    child=type(sim)(sim.num_qubits,child_config,comm=sim.comm,
        **getattr(sim,'_fork_kwargs',lambda: {})())
    new_store=child.store_id; error=None
    try:
        source=sqlite3.connect((sim.store.directory/'metadata/catalog.sqlite3').resolve().as_uri()+'?mode=ro',uri=True)
        try: child.store.db.backup_from(source)
        finally: source.close()
        with child.store.db:
            child.store.db.execute('DELETE FROM active_runs')
            child.store.db.execute("UPDATE settings SET data='[0,0]' WHERE key='allocator'")
        child.store_id=new_store; child.stores={new_store:child.store}
    except Exception as exc: error=str(exc)
    # Every rank must finish local backup before any rank enters restore's
    # collectives. Otherwise a single backup failure can deadlock its peers.
    try: sim.transport.vote(error)
    except Exception: child.close(); raise
    try:
        restore(child,data,validate=False)
    except Exception:
        child.close(); raise
    error=None
    try:
        child.binding=child.local.binding=binding; child._save_settings()
        if child.config.storage_scope == 'node-local':
            child._attach_disk_budget()
        child.lease_path=sim.directory/'global/leases'/f'{child.run_id}-{sim.rank:04d}.json'
        atomic_json(child.lease_path,{'root':data['root'],'run_id':child.run_id,'size':sim.size,'store_ids':list(data['registry'])},**_disk_kwargs(child))
    except Exception as exc: error=str(exc)
    try: sim.transport.vote(error)
    except Exception: child.close(); raise
    child.trace.record('prefix-fork',metadata={'path':'DISTRIBUTED_PREFIX_FORK','amplitude_copy_bytes':0,'binding_id':binding.binding_id})
    return child


def migrate_stored_snapshot(sim,source_config,snapshot):
    """Stream a Phase 1/2 cut into a new P=1 distributed dataset.

    Source metadata and amplitudes are opened read-only. A temporary metadata
    backup permits legacy Frame adaptation without changing the source. The
    target's managed budget pays for both work buffers and reader metadata.
    Logical gate/layout history is retained for suffix execution and adjoints.
    """
    from tempfile import TemporaryDirectory
    from types import SimpleNamespace
    from .block_store import ReadOnlyBlockStore
    from .storage import FileBlockStore,StoredBlockView
    from .cache import BufferLease
    from .checkpoint import SnapshotManifest
    from .journal import Journal
    from .phase2_runtime import Phase2Catalog
    if sim.size != 1: raise ValueError('Stored checkpoint migration requires P=1; offline repartition required')
    if sim.frontier != -1 or sim.sequencer.operations or sim.history:
        raise ValueError('Migration requires an empty distributed target')
    if source_config.block_bits != sim.block_bits: raise ValueError('Migration block width mismatch')
    if Path(source_config.directory).resolve() == sim.directory.resolve():
        raise ValueError('Migration target must differ from source')
    source = ReadOnlyBlockStore(source_config,sim.num_qubits)
    metadata = min(source_config.metadata_reserve_bytes,sim.config.metadata_reserve_bytes)//2
    reserved=False
    root=None
    prior = sim.root,sim.frontier,sim.binding,sim.layout,sim.placement_epoch,sim.initial_state_hash
    cursors = sim.local.journal.cursors()
    original_assets = set(sim.assets.assets)
    try:
        sim.cache.ensure(metadata+sim.cache.buffer_cost+BufferLease.allocation_cost(source.slot_bytes,source_config.alignment))
        sim.budget.reserve(metadata); reserved=True
        key = snapshot.snapshot_id if isinstance(snapshot,SnapshotManifest) else str(snapshot)
        row = source.db.execute('SELECT data FROM snapshots WHERE id=?',(key,)).fetchone()
        if row is None: raise ValueError('Source checkpoint is not committed')
        manifest = SnapshotManifest.from_data(json.loads(row[0]))
        if isinstance(snapshot,SnapshotManifest) and snapshot != manifest: raise ValueError('Source checkpoint fingerprint mismatch')
        # Construct the replay-only journal reader without mkdir or append.
        journal = Journal.__new__(Journal)
        journal.directory = Path(source_config.directory)/'journal'
        for kind,cursor in manifest.journal_cursors.items():
            for _ in journal.replay(kind,cursor): pass
        for asset_id in manifest.assets:
            raw = (Path(source_config.directory)/'assets'/f'{asset_id}.bin').read_bytes()
            n = int((len(raw)//16)**.5)
            if n < 2 or n*n*16 != len(raw) or n&(n-1): raise OSError('Invalid source matrix asset')
            matrix = np.frombuffer(raw,dtype='<c16').reshape(n,n)
            if sim.assets.register(matrix,n.bit_length()-1) != asset_id: raise OSError('Source matrix asset checksum mismatch')
            del matrix,raw
        for record in journal.replay('gates',manifest.journal_cursors.get('gates',0)):
            gate = decode_gate(record['gate'])
            if gate.op_id != len(sim.sequencer.operations): raise OSError('Nonsequential source gate tape')
            sim.local.append_gate(replace(gate,op_id=-1))
        if sim.circuit_hash != manifest.circuit_hash: raise OSError('Source circuit hash mismatch')
        binding = ParameterBinding(manifest.circuit_hash,manifest.binding_values,
                                   tuple((k,ExactPi(n,d)) for k,n,d in manifest.binding_witnesses))
        layout = QubitLayout.identity(sim.num_qubits); history=[]; next_gate=0

        def record_gates(frontier):
            nonlocal next_gate
            while next_gate <= frontier:
                gate = sim.sequencer.operations[next_gate]
                physical = replace(gate,targets=tuple(layout.logical_to_physical[q] for q in gate.targets),
                                   controls=tuple(layout.logical_to_physical[q] for q in gate.controls))
                history.append({'kind':'gate','source':next_gate,'physical':gate_data(physical),'layout':layout.logical_to_physical})
                next_gate += 1

        for record in journal.replay('commits',manifest.journal_cursors.get('commits',0)):
            if record.get('kind') == 'layout':
                record_gates(record['frontier'])
                history.append({'kind':'layout','bits':record['bits'],'frontier':record['frontier'],
                                'layout_before':layout.logical_to_physical,'placement_before':sum(e['kind']=='layout' for e in history)})
                layout = layout.swapped_physical(*record['bits'])
        record_gates(manifest.frontier)
        if layout.logical_to_physical != manifest.layout: raise OSError('Source layout history mismatch')
        if len(canonical(history).encode())*4 > sim.config.metadata_reserve_bytes:
            raise MemoryError('Migration history exceeds metadata reserve')
        from .migration_admission import scratch
        with scratch(sim,source,manifest) as temporary:
            root = sim._new_root()
            copied = replace(source_config,directory=temporary,metadata_reserve_bytes=metadata)
            (Path(temporary)/'metadata').mkdir()
            from .catalog_admission import CatalogConnection
            target_db = CatalogConnection(Path(temporary)/'metadata/catalog.sqlite3',
                budget_getter=lambda:sim.store.disk_budget)
            try: target_db.backup_from(source.db)
            finally: target_db.close()
            catalog = FileBlockStore(copied,sim.num_qubits,create=False,disk_budget=sim.store.disk_budget)
            try:
                proxy = SimpleNamespace(store=catalog,config=copied,block_bits=sim.block_bits,
                                        assets=sim.assets,binding=binding,trace=sim.trace)
                phase2 = Phase2Catalog(proxy); catalog.phase2=phase2
                phase2.validate_root(manifest.root)
                work = BufferLease(sim.budget,source.slot_bytes,source_config.alignment)
                try:
                    for h in range(source.num_blocks):
                        view = catalog.view(manifest.root,h)
                        source.read_version(view.version_id,work.view)
                        array = work.array(source.payload_bytes)
                        try: phase2.resolve(array,view,h)
                        finally: del array
                        with sim.cache.outputs(1) as outputs:
                            padding = outputs[0].array()
                            padding.fill(0); del padding
                            outputs[0].view[:source.payload_bytes] = work.view[:source.payload_bytes]
                            version = sim.cache.adopt(outputs[0])
                            sim._publish(root,h,StoredBlockView(version,manifest.frontier,manifest.frontier))
                        sim.cache.flush()
                        if hasattr(sim.store,'flush_to_file'): sim.store.flush_to_file()
                        sim.cache.clear()
                finally: work.close()
            finally: catalog.close()
        sim.root=sim.local.root=root; sim.frontier=sim.local.frontier=manifest.frontier
        sim.binding=sim.local.binding=binding
        sim.layout=sim.local.layout=QubitLayout(manifest.layout,manifest.layout_id)
        sim.placement_epoch=sum(entry['kind']=='layout' for entry in history)
        sim.history=history; sim.initial_state_hash=manifest.initial_state_hash
        return commit_snapshot(sim,manifest.prefix_fingerprint)
    except Exception:
        # A failure in telemetry/control after publication cannot undo the
        # already authoritative global commit or truncate its semantic tape.
        committed=False
        if root is not None:
            try: committed=load_global(sim.directory)['root'] == root
            except Exception: pass
        if committed: raise
        if root is not None:
            # Failed rank prepares are not commits. Remove their local rows
            # before rolling back the journal's protected source-gate cursor.
            with sim.store.db:
                sim.store.db.execute("DELETE FROM snapshots WHERE json_extract(data,'$.root')=?",(root,))
        sim.root,sim.frontier,sim.binding,sim.layout,sim.placement_epoch,sim.initial_state_hash=prior
        sim.local.root,sim.local.frontier,sim.local.binding,sim.local.layout=sim.root,sim.frontier,sim.binding,sim.layout
        sim.sequencer.operations.clear(); sim.history.clear(); sim.local.journal.rollback(cursors)
        for key in set(sim.assets.assets)-original_assets: sim.assets.assets.pop(key)
        raise
    finally:
        source.close()
        if reserved: sim.budget.release(metadata)
