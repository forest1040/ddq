"""Reproduce the frozen v3 evidence audit and reductions from the repository root."""
import hashlib
import itertools
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[4]
BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'examples'))
from p4_002_review_results import summarize_heldout, summarize_overhead

reports = {}
audit = {'source_revision': '89e8ad2', 'cases': {}, 'all_source_files_match_checkout': True}
expected = set(itertools.product(['rx-ry-low', 'rx-ry-var', 'mixed'], ['S0', 'S1', 'S2'], ['file', 'ram'], [.4, .6, .8]))
image = 'sha256:3bee5df0c2960ab030ea1e9cb5528638929ab3cb6e45ee2ee073102f1d8cce14'
for backend, ranks in itertools.product(['buffered', 'direct'], [1, 2]):
    key = f'{backend}-p{ranks}'
    for stage in ['heldout', 'overhead']:
        case = f'{stage}-{key}'
        attempt = BASE / case / 'attempt-01'
        status = json.loads((attempt / 'status.json').read_text())
        manifest = json.loads((attempt / 'case-manifest.json').read_text())
        assert status['status'] == 'complete' and status['return_code'] == 0
        assert status['container_cleanup']['verified_absent'] is True
        assert manifest['source_revision'] == '89e8ad2' and manifest['image'] == image
        assert manifest['backend'] == backend and manifest['ranks'] == ranks
        hashes = attempt / 'source-hashes.txt'
        assert hashlib.sha256(hashes.read_bytes()).hexdigest() == manifest['source_hash_manifest_sha256']
        checked = 0
        for line in hashes.read_text().splitlines():
            digest, relative = line.split(maxsplit=1)
            assert hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() == digest, relative
            checked += 1
        audit['cases'][case] = {'status': 'complete', 'source_files_verified': checked,
                               'cleanup_verified_absent': True}
    h = BASE / f'heldout-{key}/attempt-01/output/heldout-heldout-{key}.json'
    o = BASE / f'overhead-{key}/attempt-01/output/overhead-overhead-{key}.json'
    hd, od = json.loads(h.read_text()), json.loads(o.read_text())
    rows = hd['rows']
    assert [json.loads(line) for line in Path(str(h) + '.rows.jsonl').read_text().splitlines() if line.strip()] == rows
    progress = [json.loads(line) for line in Path(str(o) + '.rows.jsonl').read_text().splitlines() if line.strip()]
    paired = [pair for result in od['results'].values() for pair in result['warmup_pairs'] + result['measured_pairs']]
    assert len(progress) == len(paired) == 18
    pair_key = lambda pair: (pair['workload'], pair['pair_index'])
    assert sorted(progress, key=pair_key) == sorted(paired, key=pair_key)
    assert len(rows) == 54
    assert {(r['workload'], r['policy'], r['store'], r['frac']) for r in rows} == expected
    assert all(r['status'] == 'ok' and r['trial'] == 0 and r['backend'] == backend and r['ranks'] == ranks for r in rows)
    assert od['status'] == 'complete' and od['warmup_pairs_per_workload'] == 2 and od['measured_pairs_per_workload'] == 7
    assert set(od['results']) == {'rx-ry-low', 'mixed'}
    for result in od['results'].values():
        warm, measured = result['warmup_pairs'], result['measured_pairs']
        assert len(warm) == 2 and len(measured) == 7
        assert {p['pair_index'] for p in warm} == {0, 1}
        assert {p['pair_index'] for p in measured} == set(range(2, 9))
        for p in warm + measured:
            assert p['status'] == 'ok' and not p['errors']
            assert p['warmup'] is (p['pair_index'] < 2)
            for side in ['eta_on', 'eta_off']:
                evidence = p[side]['storage_io_by_rank']
                assert len(evidence) == ranks
                for e in evidence:
                    assert e['backend_observation_complete'] is True
                    assert e['backend_identities'] == (['buffered'] if backend == 'buffered' else ['linux-o-direct'])
                    assert e['outstanding_requests_at_observation'] == 0
                    assert e['trace_event_bytes_match_counters'] is True
    reports[key] = {'heldout': summarize_heldout(hd),
                    'short_fraction_08': summarize_heldout({'rows': [r for r in rows if r['frac'] == .8]}),
                    'overhead': summarize_overhead(o, expected_pairs=7)}
    assert all(v['campaign_status'] == 'complete' for v in reports[key]['overhead']['by_workload'].values())
(BASE / 'analysis-summary.json').write_text(json.dumps(reports, indent=2, sort_keys=True) + '\n')
(BASE / 'integrity-audit.json').write_text(json.dumps(audit, indent=2, sort_keys=True) + '\n')
print('PASS: 8 complete cases, 284 matching source files per case, 216 unique heldouts, 16 warmup + 56 measured pairs, all observed backend identities and cleanup verified.')
