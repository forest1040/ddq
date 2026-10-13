"""Phase-aware catalog rules and bounded, lazy block-local representation.

Rules capture the bound semantic operation at its source root. Traversing a
permutation translates labels before old predicates are evaluated. No rule
expands a block-label space into an in-memory membership list.
"""
from dataclasses import replace
from fractions import Fraction
from collections import OrderedDict
import json
import cmath
import math
import sys
from dataclasses import fields,is_dataclass
from time import perf_counter
from .journal import canonical, content_hash, gate_data, decode_gate
from .ir import GateOp
from .parameters import exact_pi_coefficient
from .phase import PhaseValue
from .permutation import PermutationEdge
from .pauli_rotation import SignedPauli, apply_pauli_rotation
from .storage import StoredBlockView
from .streaming_kernels import apply_local


def inverse_word(word):
    result = []
    for g in reversed(word):
        result.append(replace(g,kind={'S':'SDG','SDG':'S'}.get(g.kind,g.kind)))
    return tuple(result)


def pauli_of(gate):
    letters = gate.paulis if gate.kind == 'PAULI_ROT' else ({'RX':'X','RY':'Y','RZ':'Z','T':'Z','TDG':'Z'}[gate.kind],)
    return SignedPauli(sum(1 << q for q,p in zip(gate.targets,letters) if p in {'X','Y'}),
                       sum(1 << q for q,p in zip(gate.targets,letters) if p in {'Z','Y'}))


def quarter_word(pauli,k):
    """Chronological exact word and scalar for exp(-ik*pi*P/4)."""
    if pauli.sign_bit: k = -k
    if k % 4 == 0: return (),PhaseValue(pi_numerator=-k,pi_denominator=4)
    support = pauli.x_mask | pauli.z_mask
    if not support: return (),PhaseValue(pi_numerator=-k,pi_denominator=4)
    pivot = (support & -support).bit_length()-1
    basis = []
    for q in range(support.bit_length()):
        if not (support >> q)&1: continue
        if (pauli.x_mask >> q)&1:
            if (pauli.z_mask >> q)&1: basis.append(GateOp('SDG',(q,),clifford=True,diagonal=True))
            basis.append(GateOp('H',(q,),clifford=True))
    for q in range(support.bit_length()):
        if q != pivot and (support >> q)&1:
            basis.append(GateOp('MCX',(pivot,),controls=(q,),control_values=(1,),clifford=True,permutation=True))
    middle = [GateOp('S',(pivot,),clifford=True,diagonal=True)]*(k % 4)
    return tuple(basis)+tuple(middle)+inverse_word(basis),PhaseValue(pi_numerator=-k,pi_denominator=4)


class Phase2Catalog:
    def __init__(self,sim):
        self.sim, self.store = sim,sim.store
        from .frame import FrameAsset
        self.identity = FrameAsset.identity(sim.block_bits)
        identity_bytes = self.asset_cost(self.identity)
        if identity_bytes > sim.config.metadata_reserve_bytes//4:
            raise MemoryError('Identity tableau exceeds reserved metadata space')
        self.memo = OrderedDict()
        self.memo_bytes = 0
        self.cache_limit = max(0,(sim.config.metadata_reserve_bytes//8-identity_bytes)//2)
        self.aux = OrderedDict()
        self.aux_bytes = 0
        self.max_log = min(64,max(1,sim.config.metadata_reserve_bytes//4096))
        self.max_rule_depth = min(256,max(8,sim.config.metadata_reserve_bytes//1024))

    def asset(self,key=''):
        from .frame import FrameAsset
        from .clifford import CliffordTableau
        if not key: return self.identity
        if key in self.memo:
            value,cost = self.memo.pop(key); self.memo[key] = value,cost
            return value
        row = self.store.db.execute('SELECT data FROM frames_v2 WHERE id=?',(key,)).fetchone()
        if row is None: raise OSError('Missing phase-aware Frame asset')
        data = json.loads(row[0])
        if __import__('hashlib').sha256(bytes.fromhex(data['tableau'])).hexdigest() != key: raise OSError('Frame asset checksum mismatch')
        t = data['tableau']
        # Public foundation serialization is supplied by its implementation.
        tableau = CliffordTableau.from_bytes(bytes.fromhex(t))
        asset = FrameAsset.from_tableau(tableau)
        self._memo(key,asset,self.asset_cost(asset))
        return asset

    @staticmethod
    def asset_cost(asset):
        seen = set()
        def cost(value):
            if id(value) in seen: return 0
            seen.add(id(value)); total = sys.getsizeof(value)
            if is_dataclass(value): total += sum(cost(getattr(value,f.name)) for f in fields(value))
            elif isinstance(value,(tuple,list)): total += sum(cost(v) for v in value)
            if hasattr(value,'__dict__'): total += sys.getsizeof(value.__dict__)
            return total
        return cost(asset)

    def _memo(self,key,asset,cost):
        if cost > self.sim.config.metadata_reserve_bytes//2:
            raise MemoryError('Clifford synthesis metadata exceeds reserved working space')
        if cost > self.cache_limit: return
        if key in self.memo: self.memo_bytes -= self.memo.pop(key)[1]
        while self.memo and self.memo_bytes+cost > self.cache_limit:
            _,(_,old) = self.memo.popitem(last=False); self.memo_bytes -= old
        self.memo[key] = asset,cost; self.memo_bytes += cost

    def intern(self,asset):
        data = {'tableau':asset.tableau.canonical_bytes().hex()}
        key = asset.content_id
        encoded=canonical(data)
        existing=self.store.db.execute('SELECT data FROM frames_v2 WHERE id=?',(key,)).fetchone()
        if existing is not None:
            if existing[0]!=encoded: raise OSError('Existing Clifford asset checksum mismatch')
        else:
            self.store.db.execute('INSERT OR IGNORE INTO frames_v2 VALUES(?,?)',(key,encoded))
            self.store.db.commit()
        self._memo(key,asset,self.asset_cost(asset))
        return key

    def cached_transform(self,key,function):
        if key in self.aux:
            value,cost = self.aux.pop(key); self.aux[key] = value,cost
            return value
        value = function()
        cost = sys.getsizeof(key)+sys.getsizeof(value)+512
        if cost <= self.cache_limit:
            while self.aux and self.aux_bytes+cost > self.cache_limit:
                _,(_,old) = self.aux.popitem(last=False); self.aux_bytes -= old
            self.aux[key] = value,cost; self.aux_bytes += cost
        return value

    def adapt(self,view):
        if view.frame_asset_id: return view
        from .frame import frame_from_word
        asset,delta = frame_from_word(self.sim.block_bits,self.store.frame(view.frame_id)) if view.frame_id else (self.identity,0)
        phase = (view.phase_value or PhaseValue(legacy=view.phase)).times(PhaseValue(z8=delta))
        return replace(view,frame_id=0,frame_asset_id=self.intern(asset),phase_value=phase,
                       phase=phase.evaluate(),base_coordinate_id=view.version_id)

    def publish_extra(self,root,h,view):
        if not view.frame_asset_id and view.phase_value is None and not view.base_log: return
        phase = view.phase_value or PhaseValue(legacy=view.phase)
        data = {'frame':view.frame_asset_id,'phase':phase.data(),'log':view.base_log,'coordinate':view.base_coordinate_id}
        self.store.db.execute('INSERT OR REPLACE INTO views_v2 VALUES(?,?,?)',(root,h,canonical(data)))
        self.store.db.execute('INSERT OR REPLACE INTO catalog_checksums VALUES(?,?)',(f'view:{root}:{h}',content_hash(data)))

    def checked_data(self,key,text):
        if len(text.encode())*4 > self.sim.config.metadata_reserve_bytes//2:
            raise MemoryError('Catalog record exceeds reserved metadata working space')
        data = json.loads(text)
        row = self.store.db.execute('SELECT checksum FROM catalog_checksums WHERE key=?',(key,)).fetchone()
        if row is None or content_hash(data) != row[0]: raise OSError('Catalog metadata checksum mismatch')
        return data

    def rule(self,root,text=None):
        if text is None:
            row = self.store.db.execute('SELECT data FROM root_edges WHERE root=?',(root,)).fetchone()
            if row is None: return None
            text = row[0]
        return self.checked_data('edge:'+root,text)

    def progress(self,root,frontier):
        rule = {'kind':'local','ops':[],'frontier':frontier}
        with self.store.db:
            self.store.db.execute('INSERT INTO root_edges VALUES(?,?)',(root,canonical(rule)))
            self.store.db.execute('INSERT INTO catalog_checksums VALUES(?,?)',('edge:'+root,content_hash(rule)))

    def validate_root(self,root):
        for r in self.store.root_chain(root):
            self.rule(r)
            for h,text in self.store.db.execute('SELECT h,data FROM views_v2 WHERE root=?',(r,)):
                data = self.checked_data(f'view:{r}:{h}',text)
                if data['frame']:
                    row = self.store.db.execute('SELECT data FROM frames_v2 WHERE id=?',(data['frame'],)).fetchone()
                    if row is None: raise OSError('Missing phase-aware Frame asset')
                    stored = json.loads(row[0])
                    from .clifford import CliffordTableau
                    tableau = CliffordTableau.from_bytes(bytes.fromhex(stored['tableau']))
                    if __import__('hashlib').sha256(tableau.canonical_bytes()).hexdigest() != data['frame']:
                        raise OSError('Frame asset checksum mismatch')
                phase = PhaseValue.from_data(data['phase'])
                for record in data['log']:
                    if record['coordinate'] != data['coordinate'] or not math.isfinite(record['angle']):
                        raise OSError('Invalid persisted base log coordinate/angle')

    def view(self,root,h):
        pending = []
        view = None
        for depth,r in enumerate(self.store.root_chain(root)):
            if depth > self.max_rule_depth: raise MemoryError('Catalog rule depth exceeded; run compact_metadata()')
            row = self.store.db.execute('SELECT version,base,frontier,frame,re,im FROM views WHERE root=? AND h=?',(r,h)).fetchone()
            if row is not None:
                view = StoredBlockView(*row[:4],complex(*row[4:]))
                extra = self.store.db.execute('SELECT data FROM views_v2 WHERE root=? AND h=?',(r,h)).fetchone()
                if extra:
                    e = self.checked_data(f'view:{r}:{h}',extra[0]); phase = PhaseValue.from_data(e['phase'])
                    view = replace(view,frame_asset_id=e['frame'],phase_value=phase,phase=phase.evaluate(),
                                   base_log=tuple(e['log']),base_coordinate_id=e['coordinate'])
                break
            edge = self.store.db.execute('SELECT data FROM root_edges WHERE root=?',(r,)).fetchone()
            if edge:
                rule = self.rule(r,edge[0])
                if rule['kind'] == 'permutation':
                    p = rule['permutation']; h = PermutationEdge(p['kind'],tuple(p['targets']),tuple(p['controls']),tuple(p['control_values'])).inverse(h)
                    pending.append((r,h))
                else: pending.append((r,h))
        if view is None: view = StoredBlockView('initial-one' if h == 0 else 'zero')
        view = self.adapt(view)
        for rule_root,label in reversed(pending):
            rule = self.rule(rule_root)
            if rule['kind'] == 'permutation':
                view = replace(view,logical_frontier=rule['frontier'])
            else:
                for op in rule['ops']: view = self.apply_rule(view,op,label)
                view = replace(view,logical_frontier=rule['frontier'])
        return view

    def apply_rule(self,view,op,h):
        if not all(((h >> q)&1) == v for q,v in op['upper_controls']): return view
        phase = view.phase_value
        if op['kind'] == 'upper-scalar':
            g = decode_gate(op['gate'])
            from .kernels import gate_matrix
            index = sum(((h >> (q-self.sim.block_bits))&1) << j for j,q in enumerate(g.targets))
            # Rule is bound at creation; diagonal entries are captured below.
            multiplier = complex(*op['diagonal'][index])
            if op.get('exact_coefficient') is not None:
                c = Fraction(*op['exact_coefficient'])
                p = c/2 if index else -c/2
                phase = phase.times(PhaseValue(pi_numerator=p.numerator,pi_denominator=p.denominator))
            else: phase = phase.times(PhaseValue(legacy=multiplier))
            return replace(view,phase_value=phase,phase=phase.evaluate())
        if op.get('target_predicate') and not all(((h >> q)&1) == v for q,v in op['target_predicate']): return view
        if op['kind'] == 'clifford':
            negative = bool((op.get('upper_z_mask',0)&h).bit_count()&1)
            word = op['word_negative'] if negative else op['word']
            scalar_phase = op['phase_negative'] if negative else op['phase']
            def transform():
                start = perf_counter()
                asset = self.asset(view.frame_asset_id); delta_total = 0
                for data in word:
                    asset,delta = asset.left_update(decode_gate(data)); delta_total += delta
                key = self.intern(asset)
                self.sim.trace.record(str(op['sources']),performance_class='frame-update',transfer_ms=(perf_counter()-start)*1000,
                    metadata={'path':'TABLEAU_UPDATE','frame_before':view.frame_asset_id,'frame_after':key,'phase_delta_z8':delta_total%8,
                        'source_ops':op['sources'],'block_id':h})
                return key,delta_total%8
            key,delta = self.cached_transform(('clifford',view.frame_asset_id,content_hash(word)),transform)
            phase = phase.times(PhaseValue(z8=delta))
            phase = phase.times(PhaseValue.from_data(scalar_phase))
            return replace(view,frame_asset_id=key,phase_value=phase,phase=phase.evaluate())
        p = SignedPauli(*op['pauli'])
        upper_sign = ((p.z_mask >> self.sim.block_bits)&h).bit_count()&1
        local_mask = (1 << self.sim.block_bits)-1
        local = SignedPauli(p.x_mask&local_mask,p.z_mask&local_mask,p.sign_bit ^ upper_sign)
        if not local.x_mask and not local.z_mask:
            value = cmath.exp(-.5j*op['angle']*(-1 if local.sign_bit else 1))
            phase = phase.times(PhaseValue(legacy=value))
            return replace(view,phase_value=phase,phase=phase.evaluate())
        def conjugate():
            start = perf_counter()
            result = self.asset(view.frame_asset_id).tableau.conjugate_inverse(local)
            self.sim.trace.record(str(op['sources']),performance_class='pauli-conjugation',transfer_ms=(perf_counter()-start)*1000,
                metadata={'path':'PAULI_CONJUGATION','frame_id':view.frame_asset_id,'source_ops':op['sources'],
                    'x_mask':result.x_mask,'z_mask':result.z_mask,'sign_bit':result.sign_bit,
                    'support_weight':(result.x_mask|result.z_mask).bit_count(),'coordinate':view.base_coordinate_id,'binding_id':op['binding_id']})
            return result
        base = self.cached_transform(('pauli',view.frame_asset_id,local),conjugate)
        angle = -op['angle'] if base.sign_bit else op['angle']
        record = {'x':base.x_mask,'z':base.z_mask,'angle':angle,'binding_id':op['binding_id'],
                  'coordinate':view.base_coordinate_id,'source_ops':op['sources'],'expr_ref':content_hash(op['expr'])}
        log = list(view.base_log)
        if log and (log[-1]['x'],log[-1]['z'],log[-1]['coordinate']) == (record['x'],record['z'],record['coordinate']):
            prior = log.pop(); record['angle'] += prior['angle']; record['source_ops'] = prior['source_ops']+record['source_ops']
        log.append(record)
        if len(log) > self.max_log: raise MemoryError('Base-side log limit exceeded; flush_base_logs() before extending')
        phase = phase.times(PhaseValue.from_data(op['phase']))
        return replace(view,base_log=tuple(log),phase_value=phase,phase=phase.evaluate())

    def make_rule(self,gate,sources,binding=None):
        binding = binding or self.sim.binding
        b = self.sim.block_bits
        local = self.sim._local(gate)
        predicate = [(q-b,v) for q,v in zip(gate.controls,gate.control_values) if q >= b]
        common = {'upper_controls':predicate,'sources':list(sources),'binding_id':binding.binding_id}
        if gate.kind in {'S','SDG','Z','T','TDG','RZ','CZ'} and all(q >= b for q in gate.targets) and not local.controls:
            from .kernels import gate_matrix
            mat = gate_matrix(gate,self.sim.assets,binding)
            c = exact_pi_coefficient(gate.angle,binding) if gate.kind == 'RZ' else None
            return dict(common,kind='upper-scalar',gate=gate_data(gate),diagonal=[[float(v.real),float(v.imag)] for v in mat.diagonal()],
                        exact_coefficient=[c.numerator,c.denominator] if c is not None else None)
        if gate.kind == 'CZ' and any(q >= b for q in gate.targets):
            lower = next(q for q in gate.targets if q < b); upper = next(q for q in gate.targets if q >= b)
            return dict(common,kind='clifford',word=[gate_data(GateOp('Z',(lower,),clifford=True,diagonal=True))],
                        phase=PhaseValue().data(),target_predicate=[(upper-b,1)])
        if self.sim._frame_clifford(local):
            return dict(common,kind='clifford',word=[gate_data(local)],phase=PhaseValue().data())
        if gate.kind not in {'RX','RY','RZ','T','TDG','PAULI_ROT'} or local.controls: return None
        p = pauli_of(gate)
        if p.x_mask >> b: return None
        from .journal import expr_data
        from .parameters import expression, ExactPi
        expr = gate.angle if gate.angle is not None else expression(ExactPi(1 if gate.kind == 'T' else -1,4))
        coefficient = exact_pi_coefficient(expr,binding)
        if coefficient is not None and (2*coefficient).denominator == 1:
            low_mask = (1 << b)-1
            lower = SignedPauli(p.x_mask&low_mask,p.z_mask&low_mask)
            word,phase = quarter_word(lower,int(2*coefficient))
            inverse,opposite = quarter_word(lower,-int(2*coefficient))
            return dict(common,kind='clifford',word=[gate_data(g) for g in word],phase=phase.data(),
                        upper_z_mask=p.z_mask >> b,word_negative=[gate_data(g) for g in inverse],phase_negative=opposite.data(),
                        specialization={'witness_hash':binding.witness_hash,'coefficient':[coefficient.numerator,coefficient.denominator],'format':2})
        scalar = PhaseValue(pi_numerator=1 if gate.kind == 'T' else -1,pi_denominator=8) if gate.kind in {'T','TDG'} else PhaseValue()
        return dict(common,kind='rotation',pauli=[p.x_mask,p.z_mask,p.sign_bit],angle=expr.evaluate(binding),
                    expr=expr_data(expr),phase=scalar.data())

    def add_rule(self,rule):
        if len(canonical(rule).encode())*4 > self.sim.config.metadata_reserve_bytes//2:
            raise MemoryError('Rule metadata exceeds reserved budget')
        parent = self.sim.root
        if sum(1 for _ in self.store.root_chain(parent)) >= self.max_rule_depth:
            raise MemoryError('Catalog rule depth exceeded; run compact_metadata()')
        root = self.store.new_root(parent)
        self.store.db.execute('INSERT INTO root_edges VALUES(?,?)',(root,canonical(rule)))
        self.store.db.execute('INSERT INTO catalog_checksums VALUES(?,?)',('edge:'+root,content_hash(rule)))
        rotations = self.store.log_bound(parent)+sum(op['kind']=='rotation' for op in rule.get('ops',()))
        self.store.db.execute('UPDATE root_log_bounds SET rotations=? WHERE root=?',(rotations,root))
        self.store.db.commit()
        self.store.seal(parent)
        self.sim.root = root
        self.sim.frontier = rule['frontier']
        self.sim.journal.append('commits',{'root':root,'frontier':rule['frontier'],'rule':rule,'semantics_version':2})
        self.sim.trace.record(str(rule['frontier']),performance_class='metadata',metadata={'path':'VIRTUAL_PERMUTATION' if rule['kind']=='permutation' else 'FRAME_BASE_LOG_RULE',
            'source_ops':rule.get('sources',[]),'binding_id':self.sim.binding.binding_id,'layout_id':self.sim.layout.layout_id,'root':root,
            'managed_frame_cache_bytes':self.memo_bytes,'rule_bytes':len(canonical(rule).encode())})

    def resolve_base(self,array,view,h):
        for record in view.base_log:
            apply_pauli_rotation(array,SignedPauli(record['x'],record['z']),record['angle'],block_id=h,block_bits=self.sim.block_bits)

    def resolve(self,array,view,h):
        view = self.adapt(view)
        self.resolve_base(array,view,h)
        asset = self.asset(view.frame_asset_id)
        for gate in asset.word: apply_local(array,gate,self.sim.assets,self.sim.binding,block_id=h,block_bits=self.sim.block_bits)
        array *= view.phase_value.evaluate()*cmath.exp(-1j*math.pi*asset.correction/4)
