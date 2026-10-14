"""Synchronous MPI block runtime with replicated rules and sharded overrides.

Every public execution operation is collective. Amplitudes use typed buffers;
metadata alone is replicated. No rank stores the full state unless explicitly
requested through the budget-checked debug gather.
"""
from contextlib import contextmanager,ExitStack
from dataclasses import dataclass,replace,asdict
from hashlib import sha256
import json
import os
from pathlib import Path
import sqlite3
from time import perf_counter
from uuid import uuid4
import numpy as np
from .storage import StorageConfig,StoredBlockView
from .stored_runtime import StoredSimulator
from .cache import BufferLease
from .ownership import OwnershipMap
from .mpi_transport import MPITransport,CollectiveError
from .journal import canonical,content_hash,gate_data,decode_gate
from .parameters import ParameterBinding,ExactPi
from .layout import QubitLayout
from .phase import PhaseValue
from .permutation import PermutationEdge
from .streaming_kernels import apply_local,apply_group
from .phase2_runtime import pauli_of
from .pauli_rotation import apply_pauli_pair,apply_pauli_rotation
from .distributed_kernels import active_offsets,partial_mcx_pair


@dataclass(frozen=True)
class DistributedConfig:
    directory: str
    block_bits: int
    ram_budget_bytes_per_rank: int = 64*1024**2
    metadata_reserve_bytes: int = 128*1024
    tile_bytes: int = 256*1024
    backend: str = 'buffered'
    alignment: int = 4096
    swap_policy: str = 'S0'
    mcx_exchange: str = 'auto'
    amplitude_store: str = 'file'
    storage_budget_bytes_per_rank: int | None = None
    frame_policy: str = 'keep'
    storage_scope: str = 'shared'
    disk_quota_bytes: int | None = None
    min_free_bytes: int = 0
    drain_reserve_bytes: int = 0
    disk_admission_ledger: str | None = None

    def __post_init__(self):
        StorageConfig(self.directory,self.block_bits,self.ram_budget_bytes_per_rank,self.backend,self.alignment,
                      metadata_reserve_bytes=self.metadata_reserve_bytes)
        if self.storage_scope not in {'shared','node-local'}: raise ValueError('Invalid storage scope')
        if self.disk_admission_ledger is not None:
            if (self.storage_scope!='node-local' or not isinstance(self.disk_admission_ledger,str) or
                    not self.disk_admission_ledger or len(self.disk_admission_ledger.encode())>4096 or
                    not Path(self.disk_admission_ledger).is_absolute()):
                raise ValueError('disk_admission_ledger requires an absolute node-local path')
        if self.disk_quota_bytes is not None and self.disk_quota_bytes <= 0: raise ValueError('Invalid disk quota')
        if self.min_free_bytes < 0 or self.drain_reserve_bytes < 0: raise ValueError('Invalid disk reserve')
        if self.tile_bytes < 16 or self.tile_bytes%16: raise ValueError('tile_bytes must be a positive multiple of 16')
        if self.swap_policy not in {'S0','S1'} or self.mcx_exchange not in {'auto','packed','full'}: raise ValueError('Unknown distributed policy')
        if self.amplitude_store not in {'file','ram'}: raise ValueError('Unknown amplitude store')
        if self.frame_policy not in {'keep','materialize'}: raise ValueError('Unknown distributed Frame policy')
        if self.storage_budget_bytes_per_rank is not None and self.storage_budget_bytes_per_rank <= 0: raise ValueError('Invalid storage budget')


class DistributedSimulator:
    transport_class = MPITransport
    def __init__(self,num_qubits,config,*,comm=None,initial_state=None,initial_blocks=None,network_profile=None,storage_bandwidth=None,_create=True,_snapshot=None):
        self._dataset_leases={}
        try:
            self._initialize(num_qubits,config,comm=comm,initial_state=initial_state,
                             initial_blocks=initial_blocks,network_profile=network_profile,
                             storage_bandwidth=storage_bandwidth,_create=_create,_snapshot=_snapshot)
        except BaseException:
            # Preserve the constructor error while closing all partial owners.
            quiescent=True
            for resource in (getattr(self,'local',None),getattr(self,'disk_budget',None),getattr(self,'transport',None)):
                if resource is not None:
                    try:resource.close()
                    except BaseException:quiescent=False
            if quiescent:self._close_dataset_leases()
            raise

    def _initialize(self,num_qubits,config,*,comm=None,initial_state=None,initial_blocks=None,network_profile=None,storage_bandwidth=None,_create=True,_snapshot=None):
        if comm is None:
            from mpi4py import MPI
            comm = MPI.COMM_WORLD
        self.config,self.num_qubits = config,num_qubits
        self.comm = comm
        self.network_profile,self.storage_bandwidth = network_profile,storage_bandwidth
        self.transport = self.transport_class(comm,tile_bytes=config.tile_bytes,metadata_limit=config.metadata_reserve_bytes//2)
        self.rank,self.size = self.transport.rank,self.transport.size
        self.local = None; self.closed = False; self.stores = {}; self.history = []
        self.disk_budget = None
        error = None
        try:
            self.ownership = OwnershipMap(num_qubits,config.block_bits,self.size)
            common_config=asdict(config)
            if config.storage_scope == 'node-local':
                common_config.pop('directory')
                common_config.pop('disk_admission_ledger')
            self.transport.agree({'num_qubits':num_qubits,'config':common_config,'schema':3})
            self.environment = self.transport.handshake()
            self.block_bits = config.block_bits
            self.directory = Path(config.directory)
            self.directory.mkdir(parents=True,exist_ok=True)
            self._lease_dataset_roots([self.directory])
            if config.storage_scope=='node-local':self._bootstrap_disk_budget()
            storage = StorageConfig(str(self.directory/f'rank-{self.rank:04d}'),config.block_bits,
                config.ram_budget_bytes_per_rank,config.backend,config.alignment,metadata_reserve_bytes=config.metadata_reserve_bytes)
            self.local = StoredSimulator(num_qubits,storage,_create=_create,frame_policy=config.frame_policy,
                                         disk_budget=self.disk_budget)
            self.store,self.cache,self.budget = self.local.store,self.local.cache,self.local.budget
            self.trace = self.local.trace; self.transport.trace = self.trace
            self.store.db.executescript('''
                CREATE TABLE IF NOT EXISTS distributed_roots(root TEXT PRIMARY KEY,basis INTEGER,store_id TEXT);
                CREATE TABLE IF NOT EXISTS extent_owners(root TEXT,h INTEGER,store_id TEXT,rank INTEGER,PRIMARY KEY(root,h));
                CREATE TABLE IF NOT EXISTS distributed_settings(key TEXT PRIMARY KEY,data TEXT);
            ''')
            self.store.db.commit()
        except Exception as exc: error = str(exc)
        try: self.transport.vote(error)
        except Exception:
            if self.local: self.local.close()
            if self.disk_budget:self.disk_budget.close()
            self.transport.close(); raise
        capacities=self.transport.control.allgather({'host':self.local.profile.hostname,
            'available':self.local.profile.ram_available_bytes,'budget':config.ram_budget_bytes_per_rank})
        error=None
        for host in {c['host'] for c in capacities}:
            ranks=[c for c in capacities if c['host']==host]
            known=[c['available'] for c in ranks if c['available'] is not None]
            if known:
                available=min(known); headroom=max(256*1024**2,int(available*.05))
                if sum(c['budget'] for c in ranks)+headroom > available:
                    error=f'Host RAM admission failed for {host}: rank budgets plus MPI/OS headroom exceed available RAM'
        try: self.transport.vote(error)
        except Exception:
            self.local.close(); self.transport.close(); raise
        self.run_id = self.transport.broadcast_json(uuid4().hex if self.rank == 0 else None)
        self.committed_roots=set()
        if _create:
            self.dataset_id = self.transport.broadcast_json(uuid4().hex if self.rank == 0 else None)
            self.store_id = self.transport.broadcast_json(uuid4().hex if self.rank == 0 else None)
            self.registry = {self.store_id:self._placement_entry()}
            self._save_settings()
        else:
            settings = {k:json.loads(v) for k,v in self.store.db.execute('SELECT key,data FROM distributed_settings')}
            self.dataset_id,self.store_id,self.registry = settings['dataset_id'],settings['store_id'],settings['registry']
        self.stores[self.store_id] = self.store
        error=None
        try:
            if config.storage_scope == 'node-local':
                self._upgrade_legacy_registry()
                self._attach_disk_budget()
        except Exception as exc: error=str(exc)
        try: self.transport.vote(error)
        except Exception:
            if self.disk_budget is not None: self.disk_budget.close()
            self.local.close(); self.transport.close(); raise
        self.layout = QubitLayout.identity(num_qubits); self.frontier = -1; self.binding = None
        self.placement_epoch = 0
        self.root = self._new_root(basis=1)
        if config.amplitude_store == 'ram':
            from .block_store import MemoryBlockStore
            self.cache.discard_unwritten(); self.store.close()
            self.store = MemoryBlockStore(storage,num_qubits,self.budget,create=False,trace=self.trace,
                                          disk_budget=self.disk_budget)
            self.local.store = self.store; self.local.phase2.store = self.store; self.store.phase2 = self.local.phase2
            self.cache.store = self.store; self.stores[self.store_id] = self.store
            if self.disk_budget is not None: self.store.disk_budget = self.disk_budget
        if _snapshot is not None: self._restore(_snapshot)
        elif initial_state is not None or initial_blocks is not None: self._import(initial_state,initial_blocks)
        self.initial_state_hash = getattr(self,'initial_state_hash',content_hash({'initial-one':num_qubits}))
        self.set_network_profile(network_profile)

    def _placement_entry(self):
        if self.config.storage_scope == 'shared': return str(self.directory)
        from .storage_placement import StoragePlacement
        entry=None; error=None
        try: entry=StoragePlacement.local(self.store_id,self.rank,self.local.profile.hostname,self.directory)
        except Exception as exc: error=str(exc)
        self.transport.vote(error)
        entries=self.transport.control.allgather(entry.to_data())
        self.transport.agree(content_hash(entries))
        return entries

    def _upgrade_legacy_registry(self):
        """Upgrade a v3 cut only where its existing owner volumes are local.

        Copying a shared cluster dataset to different volumes remains an
        explicit offline operation, never an implicit shared-disk reader.
        """
        if all(isinstance(value,list) for value in self.registry.values()): return
        from .storage_placement import StoragePlacement
        self.transport.agree(sorted(self.registry))
        entries=[]; error=None
        try:
            for store_id,value in sorted(self.registry.items()):
                if isinstance(value,str):
                    if store_id == self.store_id and Path(value).resolve() != self.directory.resolve():
                        raise OSError('v3 owner root moved; explicit offline migration required')
                    entries.append(StoragePlacement.local(store_id,self.rank,self.local.profile.hostname,value).to_data())
        except Exception as exc: error=str(exc)
        self.transport.vote(error)
        gathered=self.transport.control.allgather(entries)
        for store_id,value in list(self.registry.items()):
            if isinstance(value,str): self.registry[store_id]=[p for group in gathered for p in group if p['store_id']==store_id]
        self.transport.agree(content_hash(self.registry))
        self._save_settings()

    def _bootstrap_disk_budget(self):
        """Admit catalogs and metadata before StoredSimulator's first SQL write."""
        from .disk_budget import DiskBudget
        record=None;error=None
        try:
            self.directory.mkdir(parents=True,exist_ok=True)
            from .storage_placement import StoragePlacement
            StoragePlacement.local('bootstrap',self.rank,self.environment[self.rank]['host'],self.directory)
            record={'host':self.environment[self.rank]['host'],'volume':os.stat(self.directory).st_dev,
                    'root':str(self.directory.resolve()),
                    'ledger':str(Path(self.config.disk_admission_ledger).resolve())
                        if self.config.disk_admission_ledger is not None else None}
        except Exception as exc:error=str(exc)
        self.transport.vote(error)
        gathered=self.transport.control.allgather(record)
        local=[r for r in gathered if r['host']==record['host'] and r['volume']==record['volume']]
        roots=sorted({r['root'] for r in local})
        key=content_hash({'host':record['host'],'volume':record['volume'],'roots':roots})
        error=None
        choices={r['ledger'] for r in local}
        if len(choices)!=1:error='Same-host/volume ranks must select the same disk admission ledger'
        self.transport.vote(error)
        selected=next(iter(choices))
        ledger=Path(selected) if selected is not None else Path(roots[0])/'.disk-admission'/f'{key}.sqlite3'
        error=None
        try:
            self.disk_budget=DiskBudget(ledger,roots,quota_bytes=self.config.disk_quota_bytes,
                min_free_bytes=self.config.min_free_bytes,drain_reserve_bytes=self.config.drain_reserve_bytes,
                shared_policy=True)
        except Exception as exc:error=str(exc)
        self.transport.vote(error)

    def _attach_disk_budget(self):
        from .disk_budget import DiskBudget
        from .storage_placement import StoragePlacement
        entries=self.registry[self.store_id]
        own=StoragePlacement.local(self.store_id,self.rank,self.local.profile.hostname,self.directory)
        roots=sorted({p['root'] for value in self.registry.values() if isinstance(value,list) for p in value
            if p['host']==own.host and p['volume_id']==own.volume_id})
        self._lease_dataset_roots(roots)
        key=content_hash({'dataset':self.dataset_id,'host':own.host,'volume':own.volume_id})
        if self.disk_budget is None:
            ledger=Path(own.root)/'.disk-admission'/f'{key}.sqlite3'
            self.disk_budget=DiskBudget(ledger,roots,quota_bytes=self.config.disk_quota_bytes,
                min_free_bytes=self.config.min_free_bytes,drain_reserve_bytes=self.config.drain_reserve_bytes,
                shared_policy=True)
        self.disk_budget.register_roots(roots)
        self.store.disk_budget=self.disk_budget

    def _lease_dataset_roots(self,roots):
        from .dataset_lease import DirectoryLease
        for root in sorted(roots,key=str):
            key=str(Path(root).resolve())
            if key not in self._dataset_leases:
                self._dataset_leases[key]=DirectoryLease(root)
            self._dataset_leases[key].assert_current()

    def _close_dataset_leases(self):
        for lease in self._dataset_leases.values():lease.close()
        self._dataset_leases.clear()

    def set_network_profile(self,profile):
        """Collectively select a calibration and attach passive observation."""
        self.transport.agree(asdict(profile) if profile is not None else None)
        if profile is not None:
            scope='same-host' if len({r['host'] for r in self.environment})==1 else 'multi-host'
            compatible=profile.matches(self.store.payload_bytes,self.config.tile_bytes,self.config.backend) and profile.host_scope==scope
            for sample in profile.peer_samples:
                a,b=sample['source'],sample['destination']
                if a >= self.size or b >= self.size:
                    compatible=False; continue
                expected={'host_source':self.environment[a]['host'],'host_destination':self.environment[b]['host'],
                          'mpi_library':self.environment[a]['mpi_library'],'mpi4py':self.environment[a]['mpi4py'],
                          'thread_level':self.environment[a]['thread_level']}
                if any(k in sample and sample[k]!=v for k,v in expected.items()): compatible=False
            if not compatible:
                self.trace.record(profile.profile_id,metadata={'path':'MPI_PROFILE_REJECTED','reason':'condition-mismatch'})
                profile=None
        self.network_profile=profile
        from .mpi_health import attach_mpi_guard
        return attach_mpi_guard(self,profile)

    def calibrate_network(self,*,repetitions=3):
        from .mpi_benchmark import calibrate_mpi
        # Calibration must not compare itself against a previous baseline.
        self.transport.observer=None
        try: profile=calibrate_mpi(self,repetitions)
        except Exception:
            self.set_network_profile(self.network_profile)
            raise
        self.set_network_profile(profile)
        return profile

    def _save_settings(self):
        with self.store.db:
            settings={'dataset_id':self.dataset_id,'store_id':self.store_id,'registry':self.registry}
            if self.disk_budget is not None:settings['disk_admission_ledger']=str(self.disk_budget._ledger_path)
            for k,v in settings.items():
                self.store.db.execute('INSERT OR REPLACE INTO distributed_settings VALUES(?,?)',(k,canonical(v)))

    @property
    def sequencer(self): return self.local.sequencer
    @property
    def assets(self): return self.local.assets
    @property
    def circuit_hash(self): return self.local.circuit_hash

    def _open(self):
        if self.closed: raise ValueError('Distributed simulator is closed')
        for lease in self._dataset_leases.values():lease.assert_current()
        if getattr(self,'checkpoint_requires_recovery',None):
            raise RuntimeError('Authoritative checkpoint committed; decision delivery failed. Resume before executing.')

    @contextmanager
    def _lease(self,size):
        with BufferLease(self.budget,size,self.config.alignment) as lease: yield lease

    def _new_root(self,parent=None,*,basis=0):
        key = self.transport.broadcast_json(uuid4().hex if self.rank == 0 else None)
        error = None
        try:
            with self.store.db:
                self.store.db.execute('INSERT INTO roots(id,parent) VALUES(?,?)',(key,parent))
                self.store.db.execute('INSERT INTO distributed_roots VALUES(?,?,?)',(key,basis,self.store_id))
                self.store.db.execute('INSERT INTO root_log_bounds VALUES(?,?)',(key,self.store.log_bound(parent)))
        except Exception as exc: error = str(exc)
        self.transport.vote(error)
        return key

    def _admit(self,buffers,*,extra=0,output_blocks=0):
        error = None
        try:
            self.cache.ensure(buffers*self.cache.buffer_cost+extra)
            if self.config.storage_budget_bytes_per_rank is not None:
                used = sum(p.stat().st_size for p in (self.store.directory/'state').glob('segment-*.bin'))
                pending = sum(e.dirty for e in self.cache.entries.values())*self.store.slot_bytes
                if used+pending+output_blocks*self.store.slot_bytes > self.config.storage_budget_bytes_per_rank:
                    raise OSError('Distributed storage capacity admission failed')
        except Exception as exc: error = str(exc)
        self.transport.vote(error)

    @contextmanager
    def _buffers(self,count,owner):
        stack = ExitStack(); buffers = []
        error = None
        try:
            if self.rank == owner: buffers = stack.enter_context(self.cache.outputs(count))
        except Exception as exc: error = str(exc)
        try:
            self.transport.vote(error)
            yield buffers
        finally: stack.close()

    def _import(self,initial_state,initial_blocks):
        error = None; digest = sha256()
        try:
            if initial_state is not None:
                vector = np.asarray(initial_state)
                if vector.shape != (1 << self.num_qubits,) or not np.isfinite(vector).all(): raise ValueError('Invalid initial state')
                size = 1 << self.block_bits
                initial_blocks = (vector[h*size:(h+1)*size] for h in self.ownership.local_blocks(self.rank))
            iterator = iter(initial_blocks)
            for h in self.ownership.local_blocks(self.rank):
                block = np.asarray(next(iterator))
                with self.cache.outputs(1) as buffers:
                    array = buffers[0].array(self.store.payload_bytes)
                    try:
                        if block.shape != array.shape or not np.isfinite(block).all(): raise ValueError('Invalid local initial Block')
                        array[:] = block; digest.update(array.data)
                        v = self.cache.adopt(buffers[0]); self._publish(self.root,h,StoredBlockView(v))
                    finally: del array
            if next(iterator,None) is not None: raise ValueError('Too many initial Blocks')
        except Exception as exc: error = str(exc)
        self.transport.vote(error)
        hashes = self.transport.control.allgather(digest.hexdigest())
        self.initial_state_hash = content_hash({'shards':hashes,'n':self.num_qubits,'b':self.block_bits})

    def gate(self,kind,targets,**kwargs):
        self._open(); gate = None; error = None
        before=len(self.sequencer.operations); cursors=self.local.journal.cursors(); old_assets=set(self.assets.assets)
        try: gate = self.local.normalizer.gate(kind,targets,**kwargs)
        except Exception as exc: error = str(exc)
        self.transport.vote(error)
        data = gate_data(gate); self.transport.agree(data)
        if gate.matrix_asset_ref:
            asset = self.assets.get(gate.matrix_asset_ref)
            self.transport.agree({'asset':asset.asset_id if hasattr(asset,'asset_id') else gate.matrix_asset_ref})
        op = None; error = None
        try: op = self.local.append_gate(gate)
        except Exception as exc: error = str(exc)
        try: self.transport.vote(error)
        except Exception:
            del self.sequencer.operations[before:]
            self.local.journal.rollback(cursors)
            for key in set(self.assets.assets)-old_assets: self.assets.assets.pop(key)
            raise
        if self.binding: self.binding = ParameterBinding(self.circuit_hash,self.binding.values,self.binding.exact_witnesses)
        return op

    def bind(self,values=()): return self.local.bind(values)

    def _set_binding(self,binding,last):
        error = None
        try:
            self.local.frontier,self.local.binding = self.frontier,self.binding
            self.local._validate_binding(binding,last)
        except Exception as exc: error = str(exc)
        self.transport.vote(error); self.transport.agree({'binding':binding.binding_id,'frontier':last,'layout':self.layout.logical_to_physical})
        self.binding = self.local.binding = binding

    def _publish(self,root,h,view,*,store_id=None,storage_rank=None):
        self.store.publish(root,((h,view),))
        with self.store.db:
            self.store.db.execute('INSERT OR REPLACE INTO extent_owners VALUES(?,?,?,?)',
                (root,h,store_id or self.store_id,self.rank if storage_rank is None else storage_rank))

    def _locate(self,root,h):
        pending = []; current = root
        for depth in range(257):
            row = self.store.db.execute('SELECT version,base,frontier,frame,re,im FROM views WHERE root=? AND h=?',(current,h)).fetchone()
            if row:
                extra = self.store.db.execute('SELECT data FROM views_v2 WHERE root=? AND h=?',(current,h)).fetchone()
                owner = self.store.db.execute('SELECT store_id,rank FROM extent_owners WHERE root=? AND h=?',(current,h)).fetchone()
                data = {'version':row[0],'base':row[1],'frontier':row[2],'frame_id':row[3],'phase':[row[4],row[5]],
                    'extra':self.local.phase2.checked_data(f'view:{current}:{h}',extra[0]) if extra else None,
                    'store_id':owner[0] if owner else self.store_id,'storage_rank':owner[1] if owner else self.rank}
                if data['extra'] and data['extra']['frame']:
                    data['tableau'] = self.local.phase2.asset(data['extra']['frame']).tableau.canonical_bytes().hex()
                return {'view':data,'pending':pending}
            rule = self.local.phase2.rule(current)
            parent = self.store.db.execute('SELECT parent FROM roots WHERE id=?',(current,)).fetchone()
            if parent is None: raise OSError('Missing distributed root')
            if rule:
                if rule['kind'] == 'permutation':
                    p = rule['permutation']; h = PermutationEdge(p['kind'],tuple(p['targets']),tuple(p['controls']),tuple(p['control_values'])).inverse(h)
                pending.append((current,h))
            if parent[0] is None:
                base = self.store.db.execute('SELECT basis,store_id FROM distributed_roots WHERE root=?',(current,)).fetchone()
                return {'view':{'version':'initial-one' if base and base[0] and h == 0 else 'zero','base':-1,'frontier':-1,
                    'frame_id':0,'phase':[1,0],'extra':None,'store_id':base[1] if base else self.store_id,'storage_rank':self.ownership.shard_owner(h)},'pending':pending}
            current = parent[0]
            if self.ownership.shard_owner(h) != self.rank: return {'next_root':current,'next_h':h,'pending':pending}
        raise MemoryError('Distributed root depth exceeded; compact_metadata() required')

    def view(self,h,*,root=None):
        self._open(); source_h = h; current = root or self.root; pending = []
        for _ in range(257):
            owner = self.ownership.shard_owner(source_h); data = None; error = None
            if self.rank == owner:
                try: data = self._locate(current,source_h)
                except Exception as exc: error = str(exc)
            self.transport.vote(error)
            data = self.transport.broadcast_json(data,root=owner); pending.extend(data['pending'])
            if 'view' in data: break
            current,source_h = data['next_root'],data['next_h']
        else: raise MemoryError('Distributed metadata routing depth exceeded')
        d = data['view']; view = StoredBlockView(d['version'],d['base'],d['frontier'],d['frame_id'],complex(*d['phase']))
        if d['extra']:
            e = d['extra']; phase = PhaseValue.from_data(e['phase'])
            if e['frame']:
                from .clifford import CliffordTableau
                from .frame import FrameAsset
                asset = FrameAsset.from_tableau(CliffordTableau.from_bytes(bytes.fromhex(d['tableau'])))
                if asset.content_id != e['frame']: raise OSError('Shipped Frame checksum mismatch')
                self.local.phase2.intern(asset)
            view = replace(view,frame_asset_id=e['frame'],phase_value=phase,phase=phase.evaluate(),base_log=tuple(e['log']),base_coordinate_id=e['coordinate'])
        view = self.local.phase2.adapt(view)
        for r,label in reversed(pending):
            rule = self.local.phase2.rule(r)
            for op in rule.get('ops',()): view = self.local.phase2.apply_rule(view,op,label)
            view = replace(view,logical_frontier=rule['frontier'])
        return view,{'store_id':d['store_id'],'storage_rank':d['storage_rank'],'source_h':source_h}

    def _reader(self,store_id):
        if store_id not in self.stores:
            from .block_store import ReadOnlyBlockStore
            from .storage_placement import resolve_owner_root
            root=resolve_owner_root(self.registry[store_id],self.rank,host=self.local.profile.hostname)
            storage = replace(self.local.config,directory=str(root/f'rank-{self.rank:04d}'))
            self.stores[store_id] = ReadOnlyBlockStore(storage,self.num_qubits)
        return self.stores[store_id]

    @contextmanager
    def fetch(self,h,destination,*,root=None,stream='forward'):
        view,where = self.view(h,root=root)
        source = where['storage_rank']
        self._admit(3)
        with ExitStack() as stack:
            out = work = None; error = None
            try:
                if self.rank == destination: out = stack.enter_context(self.cache.outputs(1))[0]
                if self.rank == source:
                    work = out if source == destination else stack.enter_context(self.cache.outputs(1))[0]
                    reader = self._reader(where['store_id'])
                    if reader is self.store:
                        with self.cache.pin(view.version_id) as old: work.view[:] = old.view
                    else: reader.read_version(view.version_id,work.view)
                    array = work.array(self.store.payload_bytes)
                    try: self.local.phase2.resolve(array,view,h)
                    finally: del array
            except Exception as exc: error = str(exc)
            self.transport.vote(error)
            if source != destination:
                if view.version_id == 'zero':
                    if out is not None:
                        array=out.array(self.store.payload_bytes)
                        try: array.fill(0)
                        finally: del array
                else:
                    self.transport.transfer(work.view[:self.store.payload_bytes] if work else None,
                        out.view[:self.store.payload_bytes] if out else None,source,destination,
                        metadata={'run_id':self.run_id,'binding_id':self.binding.binding_id if self.binding else '',
                            'stream':stream,'root':root or self.root,'h':h,'version':view.version_id,'store':where['store_id'],
                            'layout':self.layout.layout_id,'placement':self.placement_epoch,'representation':'logical-identity'})
            yield out

    def _catalog_batch(self):
        """One durable catalog transaction per gate stage; recovery uses snapshots only."""
        from contextlib import nullcontext
        batch=getattr(self.store.db,'batch',None)
        return batch() if batch is not None else nullcontext()

    def _rule(self,rule):
        new = self._new_root(self.root); error = None
        try:
            with self.store.db:
                self.store.db.execute('INSERT INTO root_edges VALUES(?,?)',(new,canonical(rule)))
                self.store.db.execute('INSERT INTO catalog_checksums VALUES(?,?)',('edge:'+new,content_hash(rule)))
                bound = self.store.log_bound(self.root)+sum(op['kind']=='rotation' for op in rule.get('ops',()))
                self.store.db.execute('UPDATE root_log_bounds SET rotations=? WHERE root=?',(bound,new))
        except Exception as exc: error = str(exc)
        self.transport.vote(error); self._commit_stage(new,rule['frontier'])

    def _commit_stage(self,new,frontier):
        self.transport.agree({'root':new,'frontier':frontier,'binding':self.binding.binding_id if self.binding else '',
                              'layout':self.layout.logical_to_physical,'placement':self.placement_epoch})
        old=self.root; error=None
        try:
            self.store.seal(old)
            self.local.journal.append('commits',{'kind':'distributed-stage','root':new,'frontier':frontier,'run_id':self.run_id})
            self.trace.record(new,metadata={'path':'DISTRIBUTED_STAGE','frontier':frontier,'rank':self.rank,'placement':self.placement_epoch})
            self.local.root=new
        except Exception as exc: error=str(exc)
        try: self.transport.vote(error)
        except Exception:
            self.local._root=old
            raise
        self.root=new; self.frontier=self.local.frontier=frontier
        if hasattr(self.store,'reclaim_versions'):
            self._memory_reclaim({new,getattr(self,'_rollback_root',new)}|self.committed_roots|getattr(self,'_workspace_roots',set()))

    def _memory_reclaim(self,roots):
        live=set()
        for root in sorted(roots):
            for h in range(self.ownership.num_blocks):
                view,where=self.view(h,root=root)
                if where['store_id'] == self.store_id and where['storage_rank'] == self.rank: live.add(view.version_id)
        error=None
        try:
            for version in list(self.cache.entries):
                if version not in live:
                    entry=self.cache.entries[version]; entry.dirty=False
                    self.cache._evict(version); self.store.delete_unwritten(version)
            self.store.reclaim_versions(live)
        except Exception as exc: error=str(exc)
        self.transport.vote(error)

    def materialize(self,frontier=None,binding=None):
        self._open(); last = len(self.sequencer.operations)-1 if frontier is None else frontier
        if last < self.frontier or last >= len(self.sequencer.operations): raise ValueError('Invalid frontier')
        prior = self.root,self.frontier,self.binding,self.layout,self.placement_epoch,len(self.history)
        self._rollback_root=self.root
        self._set_binding(binding or self.binding or self.bind(),last)
        try:
            for original in self.sequencer.operations[self.frontier+1:last+1]:
                if self.config.swap_policy == 'S1':
                    from .distributed_plan import choose_s1
                    profile=self.network_profile
                    if profile is not None and not profile.matches(self.store.payload_bytes,self.config.tile_bytes,self.config.backend): profile=None
                    decision=choose_s1(self.num_qubits,self.block_bits,self.size,self.layout,
                        self.sequencer.operations[original.op_id:last+1],profile,self.storage_bandwidth)
                    self.transport.agree(asdict(decision))
                    self.trace.record(str(original.op_id),performance_class='swap-plan',metadata={'path':'S1_DECISION',**asdict(decision)})
                    if decision.bits: self.physical_index_bit_swap(*decision.bits,_pending=True)
                gate = replace(original,targets=tuple(self.layout.logical_to_physical[q] for q in original.targets),
                    controls=tuple(self.layout.logical_to_physical[q] for q in original.controls))
                with self._catalog_batch(): self._execute(gate,original.op_id)
                self.history.append({'kind':'gate','source':original.op_id,'physical':gate_data(gate),'layout':self.layout.logical_to_physical})
        except Exception:
            self.root,self.frontier,self.binding,self.layout,self.placement_epoch,length = prior
            self.local.root,self.local.frontier,self.local.binding,self.local.layout = self.root,self.frontier,self.binding,self.layout
            del self.history[length:]
            raise
        finally:
            self._rollback_root=self.root
        return self

    def _execute(self,gate,end,*,physical_swap=False):
        b = self.block_bits
        if not physical_swap and gate.kind in {'MCX','SWAP'} and all(q >= b for q in gate.qubits):
            self._rule({'kind':'permutation','permutation':asdict(PermutationEdge.from_gate(gate,b)),'frontier':end})
            return
        from .planner import group_bits
        bits = group_bits(gate,b)
        if self.config.frame_policy == 'keep' and not bits and not physical_swap:
            rule = self.local.phase2.make_rule(gate,(gate.op_id,),self.binding)
            if rule is not None:
                if rule['kind']=='rotation' and self.store.log_bound(self.root) >= self.local.phase2.max_log: self.compact_metadata(flush_logs=True)
                self._rule({'kind':'local','ops':[rule],'frontier':end})
                return
        if not bits and not physical_swap: return self._operation_ship(gate,end)
        if gate.kind == 'MCX' and bits:
            from .distributed_plan import choose_mcx
            active=16*(1 << (b-sum(q < b for q in gate.controls)))
            choice=choose_mcx(self.network_profile,self.config,self.store.payload_bytes,active)
            self.trace.record(str(gate.op_id),metadata={'path':'MCX_EXCHANGE_DECISION','choice':choice,'active_bytes':active,'block_bytes':self.store.payload_bytes})
            if choice == 'packed': return self._mcx_packed(gate,end)
        p = pauli_of(gate) if gate.kind == 'PAULI_ROT' else None
        count = 2 if p and bits else 1 << len(bits)
        self._admit(2*count+3,output_blocks=self.ownership.blocks_per_rank)
        parent = self.root; new = self._new_root(parent)
        mask = (p.x_mask >> b) if p and bits else sum(1 << (q-b) for q in bits)
        for base in range(self.ownership.num_blocks):
            if p and bits:
                if base > (base ^ mask): continue
                partners = [base,base^mask]
            else:
                if base&mask or not self.local._upper_ok(gate,base): continue
                partners = [base|sum(((j >> i)&1) << (q-b) for i,q in enumerate(bits)) for j in range(count)]
            coordinator = self.ownership.shard_owner(partners[0])
            with self._buffers(2*count,coordinator) as buffers:
                error = None
                arrays = [buf.array(self.store.payload_bytes) for buf in buffers]
                try:
                    for i,h in enumerate(partners):
                        with self.fetch(h,coordinator,root=parent) as fetched:
                            if self.rank == coordinator: buffers[i].view[:] = fetched.view
                    if self.rank == coordinator:
                        try:
                            if p and bits: apply_pauli_pair(*arrays[:2],*arrays[2:],p,gate.angle.evaluate(self.binding),*partners,b)
                            elif p: apply_pauli_rotation(arrays[0],p,gate.angle.evaluate(self.binding),block_id=base,block_bits=b); arrays[1][:] = arrays[0]
                            elif bits: apply_group(arrays[:count],arrays[count:],self.local._local(gate),b,bits,self.assets,self.binding)
                            else: arrays[1][:] = arrays[0]; apply_local(arrays[1],self.local._local(gate),self.assets,self.binding,block_id=base,block_bits=b)
                        except Exception as exc: error = str(exc)
                    self.transport.vote(error)
                    for i,h in enumerate(partners):
                        destination = self.ownership.shard_owner(h)
                        with self._buffers(1,destination) as outputs:
                            if coordinator == destination:
                                if self.rank == destination: outputs[0].view[:] = buffers[count+i].view
                            else: self.transport.transfer(buffers[count+i].view[:self.store.payload_bytes] if self.rank == coordinator else None,
                                outputs[0].view[:self.store.payload_bytes] if outputs else None,coordinator,destination,
                                metadata={'root':new,'h':h,'source_ops':[gate.op_id],'stream':'forward','representation':'logical-identity'})
                            error = None
                            if outputs:
                                try:
                                    v = self.cache.adopt(outputs[0]); self._publish(new,h,StoredBlockView(v,end,end))
                                except Exception as exc: error = str(exc)
                            self.transport.vote(error)
                finally: arrays.clear()
        self.local.phase2.progress(new,end); self._commit_stage(new,end)

    def _operation_ship(self,gate,end):
        self._admit(4,output_blocks=self.ownership.blocks_per_rank)
        parent=self.root; new=self._new_root(parent)
        for h in range(self.ownership.num_blocks):
            if not self.local._upper_ok(gate,h): continue
            view,where=self.view(h,root=parent)
            shard=self.ownership.shard_owner(h); worker=where['storage_rank']
            if view.version_id == 'zero': worker=shard
            version=None; error=None
            with self.fetch(h,worker,root=parent) as buf:
                if buf:
                    arr=buf.array(self.store.payload_bytes)
                    try:
                        if gate.kind == 'PAULI_ROT': apply_pauli_rotation(arr,pauli_of(gate),gate.angle.evaluate(self.binding),block_id=h,block_bits=self.block_bits)
                        else: apply_local(arr,self.local._local(gate),self.assets,self.binding,block_id=h,block_bits=self.block_bits)
                        version=self.cache.adopt(buf)
                    except Exception as exc: error=str(exc)
                    finally: del arr
                self.transport.vote(error)
            version=self.transport.broadcast_json(version,root=worker)
            error=None
            if self.rank == shard:
                try: self._publish(new,h,StoredBlockView(version,end,end),storage_rank=worker)
                except Exception as exc: error=str(exc)
            self.transport.vote(error)
            self.trace.record(str(gate.op_id),performance_class='shipping',metadata={'path':'OPERATION_SHIPPING','h':h,
                'metadata_rank':shard,'compute_rank':worker,'input_version':view.version_id,'output_version':version,
                'binding_id':self.binding.binding_id,'source_ops':[gate.op_id]})
        self.local.phase2.progress(new,end); self._commit_stage(new,end)

    def _mcx_packed(self,gate,end):
        b = self.block_bits; delta = 1 << (gate.targets[0]-b)
        controls = tuple(q for q in gate.controls if q < b)
        values = tuple(v for q,v in zip(gate.controls,gate.control_values) if q < b)
        active = 1 << (b-len(controls)); tile_count = min(active,self.config.tile_bytes//16)
        extra = 2*BufferLease.allocation_cost(tile_count*16,self.config.alignment)
        self._admit(6,extra=extra,output_blocks=self.ownership.blocks_per_rank)
        parent = self.root; new = self._new_root(parent)
        for a in range(self.ownership.num_blocks):
            if a&delta or not self.local._upper_ok(gate,a): continue
            pair = [a,a|delta]; owners = [self.ownership.shard_owner(h) for h in pair]
            with ExitStack() as stack:
                old = [stack.enter_context(self.fetch(h,r,root=parent)) for h,r in zip(pair,owners)]
                outputs = [stack.enter_context(self._buffers(1,r)) for r in owners]
                error = None; arrays = []
                try:
                    if owners[0] == owners[1]:
                        if self.rank == owners[0]:
                            arrays = [buf.array(self.store.payload_bytes) for buf in [*old,outputs[0][0],outputs[1][0]]]
                            partial_mcx_pair(*arrays,controls,values)
                    else:
                        send = recv = None
                        allocation_error=None
                        if self.rank in owners:
                            try:
                                self.cache.ensure(extra)
                                index = owners.index(self.rank); outputs[index][0].view[:] = old[index].view
                                send = stack.enter_context(BufferLease(self.budget,tile_count*16,self.config.alignment))
                                recv = stack.enter_context(BufferLease(self.budget,tile_count*16,self.config.alignment))
                            except Exception as exc: allocation_error=str(exc)
                        self.transport.vote(allocation_error)
                        sendarr = send.array() if send else None; recvarr = recv.array() if recv else None
                        source_offsets = iter(active_offsets(b,controls,values)); destination_offsets = iter(active_offsets(b,controls,values))
                        for start in range(0,active,tile_count):
                            n = min(tile_count,active-start)
                            if self.rank in owners:
                                idx = owners.index(self.rank); arr = old[idx].array(self.store.payload_bytes)
                                try:
                                    for j in range(n): sendarr[j] = arr[next(source_offsets)]
                                finally: del arr
                            for sender,receiver in ((owners[0],owners[1]),(owners[1],owners[0])):
                                self.transport.transfer(sendarr[:n] if self.rank == sender else None,recvarr[:n] if self.rank == receiver else None,
                                    sender,receiver,metadata={'root':parent,'pair':pair,'offset_tile':start,'predicate':[controls,values],'path':'MCX_PACKED'})
                            if self.rank in owners:
                                idx = owners.index(self.rank); arr = outputs[idx][0].array(self.store.payload_bytes)
                                try:
                                    for j in range(n): arr[next(destination_offsets)] = recvarr[j]
                                finally: del arr
                        del sendarr,recvarr
                    for h,out in zip(pair,outputs):
                        if out:
                            v = self.cache.adopt(out[0]); self._publish(new,h,StoredBlockView(v,end,end))
                except Exception as exc: error = str(exc)
                finally: arrays.clear()
                self.transport.vote(error)
        self.local.phase2.progress(new,end); self._commit_stage(new,end)

    def physical_index_bit_swap(self,a,b,*,_pending=False):
        if not _pending: self.materialize(binding=self.binding)
        self.transport.agree({'physical_swap':[a,b]})
        if not all(isinstance(q,int) and 0 <= q < self.num_qubits for q in (a,b)):
            raise ValueError('Physical SWAP bit out of range')
        prior=(self.root,self.frontier,self.layout,self.placement_epoch,len(self.history))
        rollback=getattr(self,'_rollback_root',self.root)
        self._rollback_root=self.root if not _pending else rollback
        try: return self._physical_swap(a,b)
        except Exception:
            self.root,self.frontier,self.layout,self.placement_epoch,length=prior
            self.local.root,self.local.frontier,self.local.layout=self.root,self.frontier,self.layout
            del self.history[length:]
            raise
        finally: self._rollback_root=rollback if _pending else self.root

    def _physical_swap(self,a,b):
        if a == b: return self
        before = self.layout
        if a >= self.block_bits and b >= self.block_bits:
            parent = self.root; new = self._new_root()
            self._admit(4,output_blocks=self.ownership.blocks_per_rank)
            for h in range(self.ownership.num_blocks):
                aa,bb = a-self.block_bits,b-self.block_bits; d=((h >> aa)^(h >> bb))&1
                source = h^(d << aa)^(d << bb); owner=self.ownership.shard_owner(h)
                with self.fetch(source,owner,root=parent) as buf:
                    error=None
                    if buf:
                        try: self._publish(new,h,StoredBlockView(self.cache.adopt(buf),self.frontier,self.frontier))
                        except Exception as exc: error=str(exc)
                    self.transport.vote(error)
            self._commit_stage(new,self.frontier)
        else:
            from .ir import GateOp
            self._execute(GateOp('SWAP',(a,b),op_id=self.frontier),self.frontier,physical_swap=True)
        self.layout = self.local.layout = before.swapped_physical(a,b); self.placement_epoch += 1
        self.history.append({'kind':'layout','bits':[a,b],'layout_before':before.logical_to_physical,'frontier':self.frontier})
        self.transport.agree({'layout':self.layout.logical_to_physical,'placement':self.placement_epoch})
        return self

    def compact_metadata(self,*,flush_logs=False):
        parent = self.root; new = self._new_root()
        for h in range(self.ownership.num_blocks):
            view,where = self.view(h,root=parent); owner = self.ownership.shard_owner(h)
            error=None
            if flush_logs and view.base_log:
                with self.fetch(h,owner,root=parent) as buf:
                    if buf:
                        try: self._publish(new,h,StoredBlockView(self.cache.adopt(buf),self.frontier,self.frontier))
                        except Exception as exc: error=str(exc)
            elif self.rank == owner:
                try: self._publish(new,h,view,store_id=where['store_id'],storage_rank=where['storage_rank'])
                except Exception as exc: error=str(exc)
            self.transport.vote(error)
        if not flush_logs:
            self.store.db.execute('UPDATE root_log_bounds SET rotations=? WHERE root=?',(self.store.log_bound(parent),new)); self.store.db.commit()
        self._commit_stage(new,self.frontier); return self

    def expectation(self,observables,*,binding=None):
        from .distributed_observables import expectation
        self.materialize(binding=binding); return expectation(self,observables)

    def gradient(self,observable,*,parameters=None,binding=None,method='adjoint'):
        from .distributed_adjoint import gradient
        if method != 'adjoint': raise ValueError('Only unitary adjoint supported')
        self.materialize(binding=binding); return gradient(self,observable,parameters)

    def state(self,*,binding=None,allow_gather=False):
        if not allow_gather: raise ValueError('Full-state debug gather requires allow_gather=True')
        self.materialize(binding=binding); needed=(1 << self.num_qubits)*16
        self._admit(3,extra=needed if self.rank == 0 else 0)
        out = None
        if self.rank == 0: self.budget.reserve(needed); out=np.empty(1 << self.num_qubits,dtype='<c16')
        try:
            for h in range(self.ownership.num_blocks):
                with self.fetch(h,0) as buf:
                    if buf:
                        arr=buf.array(self.store.payload_bytes)
                        try:
                            for i,v in enumerate(arr):
                                physical=(h << self.block_bits)|i
                                logical=sum(((physical >> p)&1) << q for q,p in enumerate(self.layout.logical_to_physical)); out[logical]=v
                        finally: del arr
            return out
        finally:
            if self.rank == 0: self.budget.release(needed)

    def export_state(self,path,*,binding=None):
        self.materialize(binding=binding); error=None; f=None
        try:
            target=Path(path); target.mkdir(parents=True,exist_ok=True)
            f=open(target/f'rank-{self.rank:04d}.bin','wb')
        except Exception as exc: error=str(exc)
        self.transport.vote(error)
        try:
            for h in range(self.ownership.num_blocks):
                owner=self.ownership.shard_owner(h)
                with self.fetch(h,owner) as buf:
                    error=None
                    if buf:
                        try: f.write(buf.view[:self.store.payload_bytes])
                        except Exception as exc: error=str(exc)
                    self.transport.vote(error)
        finally:
            if f: f.close()
        error=None
        if self.rank == 0:
            try:
                from .distributed_checkpoint import atomic_json
                atomic_json(Path(path)/'manifest.json',{'n':self.num_qubits,'b':self.block_bits,'ranks':self.size,
                    'order':'physical-shard','layout':self.layout.logical_to_physical,'root':self.root})
            except Exception as exc: error=str(exc)
        self.transport.vote(error); return Path(path)

    def commit_snapshot(self,*,prefix_fingerprint=None):
        from .distributed_checkpoint import commit_snapshot
        return commit_snapshot(self,prefix_fingerprint)

    def commit_prefix(self,variable_parameters,*,binding=None):
        ids={p.parameter_id if hasattr(p,'parameter_id') else str(p) for p in variable_parameters}
        first=next((g.op_id for g in self.sequencer.operations if g.dependencies&ids),len(self.sequencer.operations))
        if self.frontier >= first: raise ValueError('Variable-dependent source already executed')
        self.materialize(first-1,binding=binding)
        deps=frozenset().union(*(g.dependencies for g in self.sequencer.operations[:first]))
        fixed=[(k,self.binding.value(k),[(v.numerator,v.denominator) for key,v in self.binding.exact_witnesses if key == k]) for k in sorted(deps)]
        fingerprint=content_hash({'prefix':[gate_data(g) for g in self.sequencer.operations[:first]],'initial':self.initial_state_hash,
            'fixed':fixed,'layout':self.layout.logical_to_physical,'placement':self.placement_epoch,'frontier':self.frontier,'schema':3})
        return self.commit_snapshot(prefix_fingerprint=fingerprint)

    def _restore(self,snapshot):
        from .distributed_checkpoint import restore
        restore(self,snapshot)

    @classmethod
    def resume(cls,config,snapshot=None,*,comm=None):
        from .distributed_checkpoint import load_global
        if comm is None:
            from mpi4py import MPI
            comm=MPI.COMM_WORLD
        transport=MPITransport(comm,tile_bytes=config.tile_bytes,metadata_limit=config.metadata_reserve_bytes//2)
        data=None; error=None
        try:
            if transport.rank == 0:
                try: data=load_global(config.directory,snapshot)
                except Exception as exc: error=str(exc)
            transport.vote(error)
            header=transport.broadcast_json({'num_qubits':data['num_qubits'],'snapshot_id':data['snapshot_id']} if transport.rank == 0 else None)
        finally: transport.close()
        return cls(header['num_qubits'],config,comm=comm,_create=False,_snapshot=header['snapshot_id'])

    def fork(self,snapshot,*,binding,directory=None):
        from .distributed_checkpoint import fork
        return fork(self,snapshot,binding,directory)

    def migrate_stored_snapshot(self,source_config,snapshot):
        """Import an immutable Phase 1/2 checkpoint into an empty P=1 run."""
        from .distributed_checkpoint import migrate_stored_snapshot
        return migrate_stored_snapshot(self,source_config,snapshot)

    def collect_garbage(self):
        self._open(); self.transport.agree({'gc_root':self.root})
        error=None
        if any((self.directory/'global/leases').glob('*.json')): error='External prefix leases prohibit garbage collection'
        if any(e.pins for e in self.cache.entries.values()): error='Pinned cache prohibits garbage collection'
        self.transport.vote(error)
        roots={self.root}|self.committed_roots
        error=None
        try:
            for path in (self.directory/'global/manifests').glob('*.json'):
                from .distributed_checkpoint import _read
                roots.add(_read(path)['root'])
        except Exception as exc: error=str(exc)
        self.transport.vote(error)
        roots=set().union(*(set(r) for r in self.transport.control.allgather(sorted(roots))))
        self.transport.agree(sorted(roots))
        if hasattr(self.store,'reclaim_versions'): self._memory_reclaim(roots)
        self.cache.flush(); self.cache.clear()
        db=self.store.db
        db.execute('CREATE TEMP TABLE IF NOT EXISTS distributed_live_versions(id TEXT PRIMARY KEY)')
        db.execute('DELETE FROM distributed_live_versions')
        # Conservative ancestor closure preserves inherited immutable views.
        # Remote extent references are routed to the physical owner, not shard.
        ancestors=set()
        for root in roots: ancestors.update(self.store.root_chain(root))
        for catalog_rank in range(self.size):
            for root in sorted(ancestors):
                cursor=db.execute('SELECT v.version,o.store_id,o.rank FROM views v LEFT JOIN extent_owners o ON o.root=v.root AND o.h=v.h WHERE v.root=?',(root,)) if self.rank == catalog_rank else None
                while True:
                    row=cursor.fetchone() if cursor else None
                    row=self.transport.broadcast_json(list(row) if row else None,root=catalog_rank)
                    if row is None: break
                    version,store,rank=row
                    if (store or self.store_id) == self.store_id and (catalog_rank if rank is None else rank) == self.rank:
                        db.execute('INSERT OR IGNORE INTO distributed_live_versions VALUES(?)',(version,))
        with db:
            db.execute('DELETE FROM versions WHERE id NOT IN (SELECT id FROM distributed_live_versions)')
        removed=0
        for path in (self.store.directory/'state').glob('segment-*.bin'):
            segment=int(path.stem.split('-')[1])
            if db.execute('SELECT 1 FROM versions WHERE segment=? LIMIT 1',(segment,)).fetchone(): continue
            handle=self.store.files.pop(segment,None)
            if handle: handle.close()
            removed+=path.stat().st_size; path.unlink()
        self.trace.record('gc',metadata={'path':'DISTRIBUTED_GC','reclaimed_bytes':removed,'whole_segments_only':True})
        self.transport.vote(); return removed

    def close(self):
        if not self.closed:
            for key,reader in self.stores.items():
                if reader is not self.store: reader.close()
            self.local.close(); self.transport.close()
            if getattr(self,'disk_budget',None): self.disk_budget.close()
            if getattr(self,'lease_path',None): self.lease_path.unlink(missing_ok=True)
            self.closed=True
            self._close_dataset_leases()

    def __enter__(self): return self
    def __exit__(self,*args): self.close()
