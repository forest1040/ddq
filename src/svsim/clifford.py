"""Immutable bit-packed Clifford operators and exact polynomial phase anchors.

The auxiliary anchor is an affine-quadratic stabilizer path sum, never a dense
state. Its Z4 Gaussian sums retain the eighth-root scalar lost by a tableau.
"""
from dataclasses import dataclass
from .ir import GateOp
from .pauli_rotation import SignedPauli


def _p(x, z, r=0):
    return SignedPauli(x, z, r & 1)


def _phase_product(a, b):
    ax, az, s = a
    bx, bz, t = b
    return ax ^ bx, az ^ bz, (s + t + 2 * (az & bx).bit_count()) % 4


def _generator(p, gate):
    x, z, r = p.x_mask, p.z_mask, p.sign_bit
    k = gate.kind
    q = gate.targets[0]
    m = 1 << q
    xb, zb = bool(x & m), bool(z & m)
    if k == 'H':
        r ^= xb and zb
        if xb != zb: x ^= m; z ^= m
    elif k in ('S', 'SDG'):
        r ^= xb and (zb if k == 'S' else not zb)
        if xb: z ^= m
    elif k == 'Z': r ^= xb
    elif k in ('X', 'MCX'):
        if gate.controls:
            if len(gate.controls) != 1: raise ValueError('Non-Clifford MCX')
            c = gate.controls[0]
            if gate.control_values and not gate.control_values[0]:
                p = _generator(p, GateOp('X', (c,)))
            else: p = _p(x, z, r)
            x, z, r = p.x_mask, p.z_mask, p.sign_bit
            xc, zc, xt, zt = (x >> c) & 1, (z >> c) & 1, (x >> q) & 1, (z >> q) & 1
            r ^= xc & zt & (xt ^ zc ^ 1)
            if xc: x ^= m
            if zt: z ^= 1 << c
            p = _p(x, z, r)
            return _generator(p, GateOp('X', (c,))) if gate.control_values and not gate.control_values[0] else p
        r ^= zb
    elif k == 'CZ':
        c, t = gate.targets
        for g in (GateOp('H', (t,)), GateOp('MCX', (t,), (c,), (1,)), GateOp('H', (t,))): p = _generator(p, g)
        return p
    elif k == 'SWAP':
        a, b = gate.targets
        if ((x >> a) ^ (x >> b)) & 1: x ^= (1 << a) | (1 << b)
        if ((z >> a) ^ (z >> b)) & 1: z ^= (1 << a) | (1 << b)
    else: raise ValueError(f'Not a Clifford generator: {k}')
    return _p(x, z, r)


@dataclass(frozen=True)
class CliffordTableau:
    block_bits: int
    rows: tuple[SignedPauli, ...]
    format_version: int = 1

    def __post_init__(self):
        b = self.block_bits
        object.__setattr__(self, 'rows', tuple(self.rows))
        if self.format_version != 1: raise ValueError('Unsupported tableau format')
        if b < 0 or len(self.rows) != 2*b: raise ValueError('Invalid tableau dimensions')
        for row in self.rows:
            if row.x_mask < 0 or row.z_mask < 0 or (row.x_mask | row.z_mask) >> b or row.sign_bit not in (0,1): raise ValueError('Invalid Pauli row')
        for i, a in enumerate(self.rows):
            for j, c in enumerate(self.rows[:i]):
                anti = ((a.x_mask & c.z_mask).bit_count() + (a.z_mask & c.x_mask).bit_count()) & 1
                if anti != int(i == j+b): raise ValueError('Tableau is not symplectic')

    @property
    def b(self): return self.block_bits

    @classmethod
    def identity(cls, b):
        return cls(b, tuple(_p(1 << q, 0) for q in range(b)) + tuple(_p(0, 1 << q) for q in range(b)))

    def conjugate_forward(self, p):
        if (p.x_mask | p.z_mask) >> self.b: raise ValueError('Pauli outside tableau')
        value = (0, 0, ((p.x_mask & p.z_mask).bit_count() + 2*p.sign_bit) % 4)
        for mask, offset in ((p.x_mask, 0), (p.z_mask, self.b)):
            for q in range(self.b):
                if mask >> q & 1:
                    a = self.rows[offset+q]
                    value = _phase_product(value, (a.x_mask, a.z_mask, ((a.x_mask & a.z_mask).bit_count()+2*a.sign_bit)%4))
        x, z, s = value
        delta = (s - (x & z).bit_count()) % 4
        if delta & 1: raise ValueError('Non-Hermitian image')
        return _p(x, z, delta//2)

    def inverse(self):
        b = self.b
        # Packed GF(2) row reduction tracks preimage generators.
        basis = {}
        for i, p in enumerate(self.rows):
            v, pre = p.x_mask | (p.z_mask << b), 1 << i
            while v:
                k = v.bit_length()-1
                if k in basis: v ^= basis[k][0]; pre ^= basis[k][1]
                else: basis[k] = (v, pre); break
        result = []
        for i in range(2*b):
            v, pre = 1 << i, 0
            while v:
                k = v.bit_length()-1
                v ^= basis[k][0]; pre ^= basis[k][1]
            a = _p(pre & ((1 << b)-1), pre >> b)
            image = self.conjugate_forward(a)
            result.append(_p(a.x_mask, a.z_mask, image.sign_bit))
        return CliffordTableau(b, tuple(result))

    def conjugate_inverse(self, p): return self.inverse().conjugate_forward(p)

    def then(self, gate):
        if any(q < 0 or q >= self.b for q in gate.qubits): raise ValueError('Generator outside tableau')
        if gate.controls and gate.kind != 'MCX': raise ValueError('Controlled non-Pauli gate is not a generator')
        return CliffordTableau(self.b, tuple(_generator(p, gate) for p in self.rows))

    def compose(self, other):
        """Return self * other (other acts first)."""
        if self.b != other.b: raise ValueError('Tableau dimensions differ')
        return CliffordTableau(self.b, tuple(self.conjugate_forward(p) for p in other.rows))

    def canonical_bytes(self):
        width = max(1, (self.b+7)//8)
        return self.format_version.to_bytes(2,'little') + self.b.to_bytes(4,'little') + b''.join(p.x_mask.to_bytes(width,'little') + p.z_mask.to_bytes(width,'little') + bytes([p.sign_bit]) for p in self.rows)

    @classmethod
    def from_bytes(cls, data):
        """Validate a persisted tableau without trusting its symplectic rows."""
        if len(data) < 6: raise ValueError('Truncated tableau')
        version = int.from_bytes(data[:2], 'little')
        if version != 1: raise ValueError('Unsupported tableau format')
        b = int.from_bytes(data[2:6], 'little')
        width = max(1, (b+7)//8)
        if len(data) != 6 + 2*b*(2*width+1): raise ValueError('Invalid tableau byte length')
        rows = []
        for offset in range(6, len(data), 2*width+1):
            rows.append(SignedPauli(int.from_bytes(data[offset:offset+width], 'little'),
                                    int.from_bytes(data[offset+width:offset+2*width], 'little'),
                                    data[offset+2*width]))
        return cls(b, tuple(rows), version)

    def synthesis(self):
        """Deterministic symplectic elimination, O(b²) named generators."""
        t, eliminate = self, []
        def emit(k, targets, controls=()):
            nonlocal t
            g = GateOp('MCX' if k == 'X' else k, targets, controls, tuple(1 for _ in controls), clifford=True,
                       diagonal=k in ('S','SDG','Z'), permutation=k in ('MCX','SWAP','X'))
            eliminate.append(g); t = t.then(g)
        for i in range(self.b):
            a = t.rows[i]
            q = ((a.x_mask | a.z_mask) & ~((1 << i)-1)).bit_length()-1
            if q != i: emit('SWAP', (i,q))
            a = t.rows[i]
            if not (a.x_mask >> i & 1): emit('H', (i,))
            elif a.z_mask >> i & 1: emit('SDG', (i,))
            for j in range(i+1,self.b):
                a = t.rows[i]
                if not ((a.x_mask | a.z_mask) >> j & 1): continue
                if not (a.x_mask >> j & 1): emit('H',(j,))
                elif a.z_mask >> j & 1: emit('SDG',(j,))
                emit('MCX',(j,),(i,))
            if t.rows[i].sign_bit: emit('Z',(i,))
            a = t.rows[self.b+i]
            if a.x_mask >> i & 1:
                emit('H',(i,)); emit('S',(i,)); emit('H',(i,))
            for j in range(i+1,self.b):
                a = t.rows[self.b+i]
                if not ((a.x_mask | a.z_mask) >> j & 1): continue
                if not (a.z_mask >> j & 1): emit('H',(j,))
                elif a.x_mask >> j & 1: emit('S',(j,)); emit('H',(j,))
                emit('MCX',(i,),(j,))
            if t.rows[self.b+i].sign_bit: emit('X',(i,))
        if t != CliffordTableau.identity(self.b): raise ArithmeticError('Clifford elimination failed')
        return tuple(GateOp('SDG' if g.kind=='S' else 'S' if g.kind=='SDG' else g.kind,
                            g.targets,g.controls,g.control_values,clifford=True,
                            diagonal=g.diagonal,permutation=g.permutation) for g in reversed(eliminate))


class _Quadratic:
    def __init__(self, n=0): self.n=n; self.c=0; self.a=[0]*n; self.edges=set()
    def variable(self): self.a.append(0); self.n+=1; return (1 << (self.n-1),0)
    def bit(self, bit, coefficient):
        mask, c = bit; coefficient %=4
        self.c = (self.c+coefficient*c)%4
        indices = [i for i in range(self.n) if mask >> i & 1]
        for i in indices: self.a[i]=(self.a[i]+coefficient*(1-2*c))%4
        if coefficient&1:
            for pos,i in enumerate(indices):
                for j in indices[pos+1:]:
                    pair=(i,j)
                    if pair in self.edges: self.edges.remove(pair)
                    else: self.edges.add(pair)
    def product(self, u, v):
        # Add 2*u*v. Only the GF(2) expansion matters.
        um,uc=u; vm,vc=v
        self.c=(self.c+2*uc*vc)%4
        for i in range(self.n):
            if ((um >> i & 1)*vc ^ (vm >> i & 1)*uc ^ ((um & vm) >> i & 1)):
                self.a[i]=(self.a[i]+2)%4
        for i in range(self.n):
            for j in range(i+1,self.n):
                if ((um >> i & 1)*(vm >> j & 1)) ^ ((um >> j & 1)*(vm >> i & 1)):
                    pair=(i,j)
                    if pair in self.edges: self.edges.remove(pair)
                    else: self.edges.add(pair)
    def substitute(self, index, value):
        olda, olde, oldc = self.a, self.edges, self.c
        oldn=self.n; self.n-=1; self.a=[0]*self.n; self.edges=set(); self.c=oldc
        def shrink(mask): return (mask & ((1 << index)-1)) | ((mask >> (index+1)) << index)
        images=[(1 << (i-(i>index)),0) if i!=index else (shrink(value[0]),value[1]) for i in range(oldn)]
        for i,coef in enumerate(olda):
            if coef: self.bit(images[i],coef)
        for i,j in olde: self.product(images[i],images[j])
        return shrink
    def gauss_phase(self):
        phase=2*self.c
        self.c=0
        while self.n:
            odd=next((i for i,a in enumerate(self.a) if a&1),None)
            if odd is not None:
                a=self.a[odd]; neighbors=sum(1 << (j if i==odd else i) for i,j in self.edges if i==odd or j==odd)
                self.a[odd]=0
                self.edges={e for e in self.edges if odd not in e}
                shrink=self.substitute(odd,(0,0))
                phase += 1 if a==1 else -1
                self.bit((shrink(neighbors),0),3 if a==1 else 1)
            else:
                i=0; neighbors=sum(1 << (k if j==i else j) for j,k in self.edges if j==i or k==i)
                if not neighbors:
                    if self.a[i]==2: raise ArithmeticError('Zero anchor amplitude')
                    self.substitute(i,(0,0))
                else:
                    j=(neighbors & -neighbors).bit_length()-1
                    constant=self.a[i]//2
                    self.a[i]=0; self.edges={e for e in self.edges if i not in e}
                    shrink=self.substitute(i,(0,0)); neighbors=shrink(neighbors); j-=j>i
                    self.substitute(j,(neighbors ^ (1 << j),constant))
            phase += 2*self.c; self.c=0
        return phase%8


def _minimum_support(tableau):
    # Diagonal members of the stabilizer group supply affine support equations.
    basis={}; equations=[]
    for a in tableau.rows[tableau.b:]:
        value=(a.x_mask,a.z_mask,((a.x_mask&a.z_mask).bit_count()+2*a.sign_bit)%4)
        while value[0]:
            k=value[0].bit_length()-1
            if k not in basis: basis[k]=value; break
            value=_phase_product(value,basis[k])
        if not value[0]: equations.append((value[1],value[2]//2))
    # Greedy high-bit zero with consistency gives minimum integer support.
    def consistent(rows):
        pivots={}
        for m,c in rows:
            while m:
                p=m.bit_length()-1
                if p in pivots: m^=pivots[p][0]; c^=pivots[p][1]
                else: pivots[p]=(m,c); break
            if not m and c: return False
        return True
    output=0
    for q in reversed(range(tableau.b)):
        if consistent(equations+[(1 << q,0)]): equations.append((1 << q,0))
        else: equations.append((1 << q,1)); output |= 1 << q
    return output


def anchor_phase(b, word, tableau=None):
    """Exact Z8 phase of the first nonzero amplitude of word|0>."""
    if tableau is None:
        tableau=CliffordTableau.identity(b)
        for gate in word: tableau=tableau.then(gate)
    target=_minimum_support(tableau)
    poly=_Quadratic(); bits=[(0,0)]*b
    def run(g):
        k=g.kind; q=g.targets[0]
        if k=='H':
            old=bits[q]; bits[q]=poly.variable(); poly.product(old,bits[q])
        elif k in ('S','SDG','Z'): poly.bit(bits[q],{'S':1,'SDG':3,'Z':2}[k])
        elif k in ('X','MCX'):
            if not g.controls: bits[q]=(bits[q][0],bits[q][1]^1)
            else:
                c=g.controls[0]; a=bits[q]; u=bits[c]
                bits[q]=(a[0]^u[0],a[1]^u[1]^(not g.control_values[0] if g.control_values else 0))
        elif k=='CZ': poly.product(bits[g.targets[0]],bits[g.targets[1]])
        elif k=='SWAP': a,c=g.targets; bits[a],bits[c]=bits[c],bits[a]
        else: raise ValueError('Unsupported anchor gate')
    for g in word: run(g)
    constraints=[(m,c ^ ((target >> q)&1)) for q,(m,c) in enumerate(bits)]
    while constraints:
        m,c=constraints.pop()
        if not m:
            if c: raise ArithmeticError('Anchor outside support')
            continue
        i=m.bit_length()-1; value=(m ^ (1 << i),c)
        shrink=poly.substitute(i,value)
        updated=[]
        for mask,constant in constraints:
            if mask >> i & 1: mask ^= 1 << i; mask ^=value[0]; constant ^=value[1]
            updated.append((shrink(mask),constant))
        constraints=updated
    return poly.gauss_phase()
