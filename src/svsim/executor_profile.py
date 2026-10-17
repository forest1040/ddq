"""Pre-calibrated executor-class profile: held-out certified per-call costs for classes a run has not executed.

A calibration run executes the circuit (or its repeated layer) under each
layout of interest; every executor class and physical bit swap that passes the
rolling certification (burn-in, sample count, drift, held-out coverage; see
``runtime_observed_costs``) is exported with its envelope. Rows are keyed by the
class (op ids removed; layout, policy, engine and angles kept) and the physical
context only (n, block_bits, backend, engine, ranks, threads): a Binding, policy
epoch or plan version does not change the physical cost of a call. A document
binds the configuration fingerprint of the host/build that measured it and is
identified by the sha256 of its canonical content.
"""
from __future__ import annotations

from statistics import median
from dataclasses import asdict

from .journal import canonical, content_hash
from .runtime_cost import CostSample
from .runtime_observed_costs import BURN_IN, CERTIFIED, ENDPOINT_STAGES, EXECUTOR_STAGES, _assess, _class

KIND = 'executor-class-profile'
SWAP_STAGE = 'physical-bit-swap'
PHYSICAL_KEYS = ('n', 'block_bits', 'backend', 'engine', 'ranks', 'threads')
MAX_ROWS = 4096


def production_health_context(sim):
    """Execution settings required by the opt-in health observer, beyond class keys.

    Directory names are not cost identities; storage volume is already bound by
    the configuration fingerprint. Control-only policy settings may differ
    between calibration and observation without changing executor work.
    """
    config = asdict(sim.config)
    for key in ('directory', 'disk_admission_ledger', 'volume_coordinator'):
        value = config.pop(key)
        if key != 'directory':
            config[key + '_enabled'] = value is not None
    policy = asdict(sim.policy)
    for key in ('mode', 'cooldown_seconds', 'minimum_gain_fraction', 'candidate_ttl_seconds'):
        policy.pop(key)
    return {'schema_version': 1, 'config': config, 'policy': policy}


def physical_context(context):
    return {key: context.get(key) for key in PHYSICAL_KEYS}


def volume_root(path):
    """Mount root of the filesystem holding ``path`` (same device as its parents up to there)."""
    import os
    from pathlib import Path
    current = Path(path).resolve()
    device = os.stat(current).st_dev
    while current.parent != current and os.stat(current.parent).st_dev == device:
        current = current.parent
    return str(current)


def runtime_fingerprint(sim):
    """Configuration fingerprint of the host/build/allocation and the storage volume of a running simulator.

    The probe is taken on the volume's mount root, not the run directory, so a
    profile calibrated in another directory of the same volume matches while a
    different device, build or allocation does not.
    """
    from .profile_document import probe_profile
    return probe_profile({'scope': 'local', 'ranks': sim.size, 'nodes': len({row['host'] for row in sim.environment}),
                          'threads': sim.policy.io_workers}, (volume_root(sim.directory),)).configuration_fingerprint


def swap_condition(sim, a, b):
    """Stage condition of a physical index-bit swap (the path depends on whether both bits address Blocks)."""
    a, b = sorted((a, b))
    return {'operation': SWAP_STAGE, 'bits': [a, b], 'n': sim.num_qubits, 'block_bits': sim.block_bits,
            'path': 'block-permutation' if a >= sim.block_bits else 'swap-kernel',
            'backend': sim.config.backend, 'engine': sim.io_engine.identity, 'ranks': sim.size}


def export_profile(sim, *, fingerprint=None):
    """Certified executor classes and swaps of ``sim``'s collector as a profile document (rank-local)."""
    runtime = sim.runtime
    groups = {}
    for row in runtime.measurements.cost_rows():
        if row.stage in EXECUTOR_STAGES or row.stage in ENDPOINT_STAGES or row.stage == SWAP_STAGE:
            # Executor and endpoint classes repeat with warm-up; a swap sequence has none.
            groups.setdefault((row.stage != SWAP_STAGE, _class(row.condition), canonical(physical_context(row.context))),
                              []).append(row)
    rows = []
    for (executor, key, _), members in sorted(groups.items(), key=lambda item: item[0][1:]):
        reason, values, lower, upper, coverage = _assess(members, runtime.policy_epoch, runtime.plan_version,
                                                         BURN_IN if executor else 0)
        if reason != CERTIFIED: continue
        held = len(values) - len(values) // 2
        rows.append({'class_key': key, 'context': physical_context(members[-1].context),
                     'lower_seconds': lower, 'expected_seconds': median(values), 'upper_seconds': upper,
                     'held_out_count': held, 'held_out_coverage': coverage, 'samples': len(values),
                     'validation': 'held-out', 'evidence': 'measured', 'units': 'seconds/call'})
    if len(rows) > MAX_ROWS: raise ValueError('Executor profile exceeds row limit')
    return document(rows, fingerprint or runtime_fingerprint(sim),
                    health_context=production_health_context(sim))


def document(rows, fingerprint, *, health_context=None):
    body = {'schema_version': 1, 'kind': KIND, 'configuration_fingerprint': fingerprint, 'rows': rows}
    if health_context is not None:
        body['health_context'] = health_context
    return {**body, 'profile_id': content_hash(body)}


def merge(documents):
    """One document from several calibration runs of the same host/build; later rows win per class."""
    fingerprints = {doc['configuration_fingerprint'] for doc in documents}
    if len(fingerprints) != 1: raise ValueError('Executor profiles from different configurations')
    rows = {}
    contexts = {canonical(doc.get('health_context')) for doc in documents}
    if len(contexts) != 1:
        raise ValueError('Executor profiles have different health execution settings')
    for doc in documents:
        validate(doc)
        for row in doc['rows']: rows[(row['class_key'], canonical(row['context']))] = row
    return document(list(rows.values()), fingerprints.pop(),
                    health_context=documents[0].get('health_context'))


def validate(doc):
    if not isinstance(doc, dict) or doc.get('kind') != KIND or doc.get('schema_version') != 1:
        raise ValueError('Not an executor-class profile')
    body = {k: doc[k] for k in ('schema_version', 'kind', 'configuration_fingerprint', 'rows')}
    if 'health_context' in doc:
        body['health_context'] = doc['health_context']
    if doc.get('profile_id') != content_hash(body): raise ValueError('Executor profile id does not match its content')
    if not isinstance(doc['rows'], list) or len(doc['rows']) > MAX_ROWS: raise ValueError('Invalid executor profile rows')
    for row in doc['rows']:
        values = [row.get(k) for k in ('lower_seconds', 'expected_seconds', 'upper_seconds')]
        if (row.get('evidence') != 'measured' or row.get('validation') != 'held-out' or row.get('units') != 'seconds/call'
                or any(isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0 for v in values)
                or not values[0] <= values[1] <= values[2]):
            raise ValueError('Executor profile row is not a measured held-out envelope')
    return doc


class ExecutorProfile:
    """Exact class + physical-context lookup into one validated document."""

    def __init__(self, doc):
        self.document = validate(doc)
        self.profile_id = doc['profile_id']
        self.fingerprint = doc['configuration_fingerprint']
        self._rows = {(row['class_key'], canonical(row['context'])): row for row in doc['rows']}

    def cost_sample(self, context, condition):
        if not isinstance(context, dict) or not isinstance(condition, dict): return None
        row = self._rows.get((_class(condition), canonical(physical_context(context))))
        if row is None: return None
        return CostSample('executor-profile:' + condition.get('operation', ''), row['expected_seconds'],
                          row['lower_seconds'], row['upper_seconds'], measured=True)
