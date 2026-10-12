"""Numerical reference for arbitrary-control MCX with blocked state vectors.

Not an MPI or out-of-core production implementation.
Run: python mcx_block_reference_test.py  (requires numpy)
"""

import itertools
import numpy as np


def dense_mcx(vector, controls, values, target):
    n = (len(vector) - 1).bit_length()
    out = vector.copy()
    cm = sum(1 << c for c in controls)
    cv = sum(v << c for c, v in zip(controls, values))
    tm = 1 << target
    for index in range(1 << n):
        if not (index & tm) and (index & cm) == cv:
            partner = index | tm
            out[index], out[partner] = out[partner], out[index]
    return out


def blocked_mcx(vector, controls, values, target, b):
    n = (len(vector)-1).bit_length()
    blocks = {h: vector[h*(1<<b):(h+1)*(1<<b)].copy() for h in range(1 << (n-b))}
    c_local = [(c, v) for c, v in zip(controls, values) if c < b]
    c_upper = [(c-b, v) for c, v in zip(controls, values) if c >= b]
    def upper_ok(h):
        return all(((h >> c)&1) == v for c,v in c_upper)
    if target < b:
        for h in blocks:
            if upper_ok(h):
                blocks[h] = dense_mcx(blocks[h], [x for x,v in c_local], [v for x,v in c_local], target)
    else:
        delta = 1 << (target-b)
        lower_mask = sum(1 << c for c, v in c_local)
        lower_value = sum(v << c for c,v in c_local)
        selected = np.array([l for l in range(1 << b) if (l & lower_mask)==lower_value], dtype=int)
        for h0 in range(1 << (n-b)):
            if h0 & delta:  # target bit must be zero
                continue
            if not upper_ok(h0):
                continue
            h1 = h0 | delta
            a = blocks[h0][selected].copy()
            blocks[h0][selected] = blocks[h1][selected]
            blocks[h1][selected] = a
    return np.concatenate([blocks[h] for h in range(1 << (n-b))])

rng = np.random.default_rng(20261012)
tests = 0
for n in range(2, 10):
    state = rng.normal(size=1<<n) + 1j*rng.normal(size=1<<n)
    state = state / np.linalg.norm(state)
    for b in range(1, n):
        for target in range(n):
            pool = [q for q in range(n) if q != target]
            for k in range(min(6,n-1)+1):
                chosen = tuple(int(x) for x in rng.choice(pool,size=k,replace=False))
                values = tuple(int(x) for x in rng.integers(0,2,size=k))
                expect = dense_mcx(state,chosen,values,target)
                actual = blocked_mcx(state,chosen,values,target,b)
                if not np.array_equal(expect, actual):
                    raise AssertionError((n,b,target,chosen,values,np.max(np.abs(expect-actual))))
                assert np.array_equal(state,dense_mcx(expect,chosen,values,target))
                tests += 1
print('PASS',tests,'random blocked MCX comparisons against dense reference')
