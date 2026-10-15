#!/usr/bin/env python3
"""Aggregate p4_002 result JSON files (or their .rows.jsonl) into markdown tables.

usage: analyze.py file.json [file2.json ...]   (a missing .json falls back to <file>.rows.jsonl)
"""
import json, statistics, sys, os, collections

def load(path):
    if os.path.exists(path):
        d = json.load(open(path)); return d['rows'], d
    rows = [json.loads(l) for l in open(path + '.rows.jsonl')]
    return rows, {}

def pct(v, p):
    v = sorted(v); k = (len(v) - 1) * p / 100; f = int(k); c = min(f + 1, len(v) - 1)
    return v[f] + (v[c] - v[f]) * (k - f)

def summ(rows):
    ans = [r for r in rows if r.get('relative_error') is not None]
    e = [abs(r['relative_error']) for r in ans]
    cov = [r['covered'] for r in ans if r.get('covered') is not None]
    return (len(rows), len(ans), len(ans) / len(rows) if rows else 0,
            statistics.median(e) if e else None, pct(e, 95) if e else None, max(e) if e else None,
            sum(cov) / len(cov) if cov else None)

def f(x, pc=True):
    return '-' if x is None else (f'{x*100:.1f}%' if pc else f'{x:.3f}')

def table(rows, key):
    print(f'| {key} | checks | answered | answered % | median | p95 | worst | coverage |\n|---|---|---|---|---|---|---|---|')
    keys = sorted({str(key(r)) if callable(key) else str(r.get(key)) for r in rows})
    for k in keys:
        sub = [r for r in rows if (str(key(r)) if callable(key) else str(r.get(key))) == k]
        n, a, fr, m, p, w, c = summ(sub)
        print(f'| {k} | {n} | {a} | {f(fr)} | {f(m)} | {f(p)} | {f(w)} | {f(c)} |')

def causes(rows):
    """Why unanswered: (a) shape unknown, (b) class-level decision reasons from per-class diagnostics."""
    un = [r for r in rows if r.get('status') == 'ok' and r.get('eta') is None]
    shape = sum(1 for r in un if 'FUTURE_EXECUTOR_SHAPE_UNKNOWN' in r.get('unknown_reasons', []))
    t = collections.Counter(); rowsby = collections.Counter(); pooled = exact = 0
    for r in rows:
        for d in r.get('class_diag') or []:
            exact += d.get('exact', 0); pooled += d.get('pooled', 0)
    for r in un:
        rs = set()
        for d in r.get('class_diag') or []:
            for k in d.get('missing_detail', {}):
                rs.add(k.rsplit(':', 1)[-1])
        for x in rs: rowsby[x] += 1
    print(f'unanswered rows {len(un)}; with FUTURE_EXECUTOR_SHAPE_UNKNOWN {shape}')
    print('unanswered rows by a missing-class decision reason (a row may have several):', dict(rowsby))
    print(f'task classes priced across all checks (rank-summed): exact {exact}, pooled-angle {pooled}')

if __name__ == '__main__':
    for path in sys.argv[1:]:
        rows, d = load(path)
        print(f'\n### {path}\n')
        print('env:', d.get('env')); print('overall:', summ(rows))
        for k in ('workload', 'policy', 'store'): print(); table(rows, k)
        print(); causes(rows)
        for w, o in (d.get('monitor_overhead') or {}).items():
            print(f"overhead {w}: median diff {o['median_wall_difference_seconds']:.4f}s, ratio {o['ratio_of_medians']:.4f}, pairs {[round(p['difference_seconds'],3) for p in o['measured_pairs']]}")
