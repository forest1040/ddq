"""Explicit offline reconcile of unlinked reservations and temporary-artifact GC.

Scope: a node-local, shared-policy disk ledger whose writers are all stopped.
Owner death is proved with kernel flock leases (the ledger inode and every
registered dataset directory), never with a PID or a timestamp. The functions
are dry runs by default. ``apply`` additionally needs the ``plan_sha256`` of a
dry run, so only the reviewed plan can be applied, and any unknown state makes
the whole run refuse. No artifact is deleted or rewritten by the reservation
reconcile; surviving bytes stay charged by the observed-usage scan, and only
the owner-dead, never-written pending credit is settled.
"""
from contextlib import contextmanager
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sqlite3
import stat

from .catalog_admission import CatalogConnection, catalog_birth_identity, catalog_file_owner
from .checkpoint_recovery import _checked_path
from .dataset_lease import DatasetBusyError, DirectoryLease
from .disk_budget import DiskAdmissionError, DiskBudget
from .journal import canonical, content_hash

MAX_STORES = 256
MAX_TOKENS = 256
MAX_ENTRIES = 8192
APPEND_OPERATIONS = ('trace-append', 'journal-append', 'measurement-append')
EXTENT_OPERATIONS = ('amplitude-extent', 'tile-amplitude-extent')
SIDECAR_OPERATIONS = ('matrix-asset', 'snapshot-export', 'sidecar-write', 'dataset-lease-json')
NAMESPACE_OPERATIONS = ('trace-open','journal-open','migration-open','store-open')
KNOWN_OPERATIONS = APPEND_OPERATIONS + EXTENT_OPERATIONS + SIDECAR_OPERATIONS + NAMESPACE_OPERATIONS + ('catalog-transaction',)
TEMPORARY_NAME = re.compile(r'^\.?.+\.[0-9a-f]{32}\.tmp$')
_JOURNAL_KINDS = ('gates', 'local', 'shared', 'joint', 'commits', 'frame')


class UncommittedRecoveryRefused(DiskAdmissionError):
    """The inspected state is unknown or unsafe; nothing was changed."""

    def __init__(self, message, report=None):
        super().__init__(message)
        self.report = report


def _digest(document):
    return sha256(canonical(document).encode()).hexdigest()


def _private_layout(ledger, roots, persisted=None, startup=False):
    """Store-private ledger: ``<store>/metadata/disk-ledger.sqlite3`` with no bound shared policy and one root.

    A ledger created with ``persist_private_policy`` carries a ``private_policy`` row and recovery verifies and
    uses it. An older ledger without a saved policy is refused, even when the
    caller supplies defaults: its historical policy cannot be inferred.
    """
    store = ledger.parent.parent
    if (ledger.name != 'disk-ledger.sqlite3' or ledger.parent.name != 'metadata' or ledger.parent.is_symlink()
            or not (store / 'metadata' / 'catalog.sqlite3').is_file() and not startup):
        raise DiskAdmissionError('Recovery requires a bound shared-policy ledger or a store-private ledger')
    if [Path(p) for p in roots] != [store]:
        raise DiskAdmissionError('A store-private ledger must register exactly its own store')
    if persisted is None:
        raise DiskAdmissionError('Store-private policy persisted none; explicit migration is required')
    return dict(persisted, private=True), roots


def _persisted(ledger):
    info = ledger.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise DiskAdmissionError('Recovery requires an existing exclusively named ledger')
    for suffix in ('-journal', '-wal', '-shm'):  # refuse before any SQLite open can touch a side file
        try:
            side = Path(str(ledger) + suffix).lstat()
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(side.st_mode) or side.st_nlink != 1 or side.st_size:
            raise DiskAdmissionError('Recovery ledger side file requires separate recovery')
    con = sqlite3.connect(ledger.as_uri() + '?mode=ro', uri=True)
    try:
        con.execute('PRAGMA query_only=ON')
        policy = con.execute('''SELECT quota_bytes,min_free_bytes,drain_reserve_bytes,bootstrap_bytes,max_records
                                FROM shared_policy WHERE singleton=1''').fetchone()
        roots = [p for (p,) in con.execute('SELECT path FROM roots LIMIT 257')]
        private = None
        from .catalog_startup import has_private_intent
        startup = has_private_intent(con,ledger.parent.parent) if policy is None else False
        if policy is None and con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='private_policy'").fetchone():
            private = con.execute('''SELECT quota_bytes,min_free_bytes,drain_reserve_bytes,bootstrap_bytes,max_records
                                    FROM private_policy WHERE singleton=1''').fetchone()
    except sqlite3.Error as exc:
        raise DiskAdmissionError(f'Recovery cannot read the persisted ledger policy: {exc}') from exc
    finally:
        con.close()
    if policy is None:
        keys = ('quota_bytes', 'min_free_bytes', 'drain_reserve_bytes', 'bootstrap_bytes', 'max_records')
        return _private_layout(ledger, roots, dict(zip(keys, private)) if private else None,startup=startup)
    if not roots or len(roots) > 256:
        raise DiskAdmissionError('Recovery needs 1..256 registered roots')
    return dict(zip(('quota_bytes', 'min_free_bytes', 'drain_reserve_bytes', 'bootstrap_bytes', 'max_records'), policy)), roots


def _discover_stores(root):
    """Dataset directories (own catalog) at depth <= 2, and the directories to lease."""
    stores, leased = [], {root}
    for depth1 in sorted(os.scandir(root), key=lambda e: e.name):
        if not depth1.is_dir(follow_symlinks=False):
            continue
        first = Path(depth1.path)
        if (first / 'metadata' / 'catalog.sqlite3').exists():
            stores.append(first); leased.add(first)
        for depth2 in sorted(os.scandir(first), key=lambda e: e.name):
            if depth2.is_dir(follow_symlinks=False) and (Path(depth2.path) / 'metadata' / 'catalog.sqlite3').exists():
                stores.append(Path(depth2.path)); leased.update((first, Path(depth2.path)))
    if (root / 'metadata' / 'catalog.sqlite3').exists():
        stores.append(root)
    return stores, leased


@contextmanager
def offline_scope(ledger, *, policy=None, writer=False):
    """Prove all owners dead (exclusive ledger + dataset leases) and open a budget.

    The default budget is recovery-only (no new admission credit). ``writer``
    opens a normal admitting budget that adopts the exclusive ledger lease, for
    maintenance that must write a catalog under the same admission rules.
    """
    ledger = Path(ledger)
    if not ledger.is_absolute() or ledger.is_symlink():
        raise DiskAdmissionError('Recovery needs an absolute, non-symlink ledger path')
    ledger = ledger.resolve(strict=True)
    leases, budget, ledger_lease, coordinator = [], None, None, None
    try:
        try:
            ledger_lease = DirectoryLease(ledger.parent, exclusive=True)
        except DatasetBusyError as exc:
            raise UncommittedRecoveryRefused('A live process holds the disk ledger; owner death is not proven') from exc
        persisted, roots = _persisted(ledger)
        private = persisted.pop('private', False)
        if policy is not None and {k: policy.get(k, persisted[k]) for k in persisted} != persisted:
            raise DiskAdmissionError('Recovery policy conflicts with the persisted shared policy')
        roots = [Path(p) for p in roots]
        if any(not p.is_absolute() or p.is_symlink() or not p.is_dir() or p.resolve() != p for p in roots):
            raise DiskAdmissionError('Registered roots changed or are unavailable')
        stores, to_lease = [], set()
        for root in roots:
            found, directories = _discover_stores(root)
            stores.extend(found); to_lease.update(directories)
        con = sqlite3.connect(ledger.as_uri() + '?mode=ro', uri=True)
        try:
            from .migration_admission import inventory
            migration_clones=inventory(con,roots)
            to_lease.update(Path(link['source_root']) for link in migration_clones.values())
            from .catalog_startup import inventory_directories
            to_lease.update(inventory_directories(con,roots))
            try:
                from .export_admission import export_inventory_roots
            except ImportError:
                if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='export_roots'").fetchone():
                    raise DiskAdmissionError('Export inventory support is unavailable')
                export_roots = []
            else:
                export_roots = export_inventory_roots(con)
        finally:
            con.close()
        for exported in export_roots:
            path = Path(exported['path'])
            if path.stat().st_dev != ledger.stat().st_dev:
                raise DiskAdmissionError('Export inventory changed volume')
            to_lease.add(path)
        if len(stores) > MAX_STORES:
            raise DiskAdmissionError('Recovery store limit exceeded')
        to_lease.discard(Path(ledger_lease.path))  # one flock per inode: the ledger directory lease already covers it
        for directory in sorted(to_lease, key=str):
            try:
                leases.append(DirectoryLease(directory, exclusive=True))
            except DatasetBusyError as exc:
                raise UncommittedRecoveryRefused(f'A live runtime holds dataset lease {directory}; owner death is not proven') from exc
        binding = DiskBudget.read_volume_binding(ledger)
        if binding is not None:
            # A bound ledger needs its coordinator: opened before any mutation (a missing or foreign one refuses).
            from .volume_coordinator import VolumeCoordinator
            coordinator = VolumeCoordinator.open_bound(binding)
        if private:
            if writer:
                budget = DiskBudget(ledger, roots, ledger_lease=ledger_lease, persist_private_policy=True,
                                    volume_coordinator=coordinator, **persisted)
            else:
                budget = DiskBudget(ledger, roots, recovery_only=True, store_private=True, **persisted)
        elif writer:
            budget = DiskBudget(ledger, roots, shared_policy=True, ledger_lease=ledger_lease,
                                volume_coordinator=coordinator, **persisted)
        else:
            budget = DiskBudget(ledger, roots, shared_policy=True, recovery_only=True, **persisted)
        yield {'ledger': ledger, 'ledger_lease': ledger_lease, 'leases': leases, 'budget': budget, 'roots': roots,
               'stores': sorted(set(stores), key=str), 'policy': persisted, 'private_ledger': private,
               'coordinator': coordinator, 'export_roots': export_roots, 'migration_clones':migration_clones}
    finally:
        if budget is not None:
            budget.close()
        if coordinator is not None:
            coordinator.close()
        for lease in leases:
            lease.close()
        if ledger_lease is not None:
            ledger_lease.close()


def volume_claim_report(scope, *, apply):
    """Republish (apply) or preview (dry run) a bound ledger's true pending in the volume coordinator.

    Runs under the scope's exclusive ledger lease, so nothing can publish concurrently; it is idempotent and
    returns the receipt that the recovery report embeds (``None`` for an unbound ledger).
    """
    coordinator = scope['coordinator']
    if coordinator is None:
        return None
    budget = scope['budget']
    if apply:
        _assert_leases(scope)
        receipt = budget.republish_volume_claim(coordinator, scope['ledger_lease'])
        receipt['status'] = 'republished'
        return receipt
    claim = coordinator.claim(scope['ledger'])
    return {'coordinator': str(coordinator.path), 'ledger': str(scope['ledger']), 'status': 'dry-run',
            'previous_claim_bytes': None if claim is None else claim['pending'],
            'published_pending_bytes': budget.reserved_bytes}


def _assert_leases(scope):
    scope['ledger_lease'].assert_current()
    for lease in scope['leases']:
        lease.assert_current()


def _scan_journal(path, limit):
    size = path.stat().st_size
    if size > limit:
        raise DiskAdmissionError('Journal exceeds inspection limit')
    valid = 0; bad_seen = False
    with open(path, 'rb') as stream:
        while True:
            raw = stream.readline(1024 ** 2 + 2)
            if not raw:
                break
            try:
                if not raw.endswith(b'\n'):
                    raise ValueError('partial')
                row = json.loads(raw); checksum = row.pop('checksum')
                if row.get('schema_version') != 1 or checksum != content_hash(row):
                    raise ValueError('checksum')
                if bad_seen:
                    return {'blocker': 'journal-record-after-corruption'}
                valid = stream.tell()
            except (ValueError, KeyError, TypeError):
                bad_seen = True
    return {'bytes': size, 'valid_prefix_bytes': valid, 'uncommitted_tail_bytes': size - valid}


def _walk_files(store, budget_entries):
    """Regular files and symlink/odd entries under one store, bounded."""
    files, odd = [], []
    for current, directories, names in os.walk(store, followlinks=False):
        for name in names + directories:
            budget_entries[0] += 1
            if budget_entries[0] > MAX_ENTRIES:
                raise DiskAdmissionError('Recovery inspection entry limit exceeded')
            path = Path(current) / name
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                odd.append(str(path))
            elif stat.S_ISREG(info.st_mode):
                files.append((path, info))
    return files, odd


def _inspect_store(store, *, max_journal_bytes, entries, migration=None):
    out = {'path': str(store), 'blockers': []}
    block = out['blockers']
    if migration is not None:
        from .migration_admission import read_snapshot
        read_snapshot(migration)
        out['migration_source_digest']=migration['source_digest']
    relative = 'metadata/catalog.sqlite3'
    path, info = _checked_path(store, relative)
    artifacts = {'database': {'path': str(path), 'device': info.st_dev, 'inode': info.st_ino,
                              'birth_identity': catalog_birth_identity(path, info),
                              'bytes': info.st_size}, 'side_files': []}
    for suffix in ('-journal', '-wal', '-shm'):
        side_path, side = _checked_path(store, relative + suffix, missing=True)
        artifacts['side_files'].append({'path': str(side_path), 'present': side is not None,
                                      **({'device': side.st_dev, 'inode': side.st_ino, 'bytes': side.st_size}
                                         if side is not None else {})})
        if side is not None and side.st_size:
            block.append(f'catalog{suffix}-present: admitted reopen must replay it first')
    if not block:
        con = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
        try:
            con.execute('PRAGMA query_only=ON')
            if con.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
                block.append('catalog-integrity-check-failed')
            else:
                tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                out['published_versions'] = out['orphan_versions'] = 0
                segments = {p.name: p.stat().st_size for p in (store / 'state').glob('segment-*.bin')} if (store / 'state').is_dir() else {}
                if 'versions' in tables:
                    for version,segment,offset,checksum in con.execute('SELECT id,segment,offset,checksum FROM versions'):
                        if segment is None:
                            out['orphan_versions'] += 1; continue
                        out['published_versions'] += 1
                        if migration is not None:
                            from .migration_admission import verify_clone_version
                            if out['published_versions']>MAX_ENTRIES:raise DiskAdmissionError('Migration clone version limit exceeded')
                            verify_clone_version(migration,version,segment,offset,checksum)
                            continue
                        size = segments.get(f'segment-{segment:06d}.bin')
                        if size is None or offset >= size:
                            block.append('published-extent-beyond-segment-file')
                            break
                out['segment_bytes'] = sum(segments.values())
                if 'resident_versions' in tables and con.execute('SELECT 1 FROM resident_versions LIMIT 1').fetchone():
                    block.append('unflushed-resident-amplitudes')
                out['catalog_roots'] = con.execute('SELECT count(*) FROM roots').fetchone()[0] if 'roots' in tables else 0
                out['stale_active_runs'] = con.execute('SELECT count(*) FROM active_runs').fetchone()[0] if 'active_runs' in tables else 0
        except sqlite3.Error as exc:
            block.append(f'catalog-unreadable: {exc}')
        finally:
            con.close()
    journals = {}
    jdir = store / 'journal'
    if jdir.is_dir():
        for kind in _JOURNAL_KINDS:
            file = jdir / f'{kind}.jsonl'
            if file.exists():
                _checked_path(store, f'journal/{kind}.jsonl')
                scan = _scan_journal(file, max_journal_bytes)
                if 'blocker' in scan:
                    block.append(f'{kind}-{scan["blocker"]}')
                else:
                    journals[kind] = scan
    out['journals'] = journals
    files, odd = _walk_files(store, entries)
    if odd:
        block.append('symlink-artifact-present')
    out['trace_bytes'] = sum(i.st_size for p, i in files if p.parent.name == 'traces')
    out['temporaries'] = sorted(({'path': str(p.relative_to(store)), 'bytes': i.st_size, 'dev': i.st_dev, 'ino': i.st_ino}
                                 for p, i in files if p.name.endswith('.tmp')), key=lambda t: t['path'])
    out['catalog_identity'] = {'device': info.st_dev, 'inode': info.st_ino}
    out['catalog_attribution'] = artifacts
    out['catalog_owner'] = catalog_file_owner(path, info)
    return out


def _catalog_owner_digests(stores):
    return {store['catalog_owner']: store['catalog_attribution'] for store in stores
            if store['catalog_owner'].startswith('catalog-v3:')}


def _assert_catalog_files(stores):
    """Recheck the complete catalog namespace before any token is settled."""
    for store in stores:
        attribution = store['catalog_attribution']
        for expected in [attribution['database'], *attribution['side_files']]:
            path = Path(expected['path'])
            _, info = _checked_path(Path(store['path']), path.relative_to(store['path']),
                                    missing='present' in expected)
            if info is None:
                matches = expected.get('present') is False
            else:
                matches = (expected.get('present', True) and
                           (info.st_dev, info.st_ino, info.st_size) ==
                           (expected['device'], expected['inode'], expected['bytes']))
                if 'birth_identity' in expected:
                    matches = matches and catalog_birth_identity(path, info) == expected['birth_identity']
            if not matches:
                raise UncommittedRecoveryRefused('Catalog file attribution changed during recovery')


def _classify(token, owners):
    operation = token['operation']
    if operation not in KNOWN_OPERATIONS:
        return None, f'unknown-operation:{operation}'
    if token['state'] not in ('RESERVED', 'WRITING'):
        return None, 'terminal'
    if operation == 'catalog-transaction':
        if token['owner'] not in owners:
            return None, 'catalog-owner-not-attributable-to-a-registered-file-identity'
        if re.fullmatch('[0-9a-f]{64}', token['generation'] or '') is None:
            return None, 'catalog-generation-unbound'
        return 'catalog', None
    if operation in NAMESPACE_OPERATIONS:
        return 'namespace', None
    if operation in APPEND_OPERATIONS:
        return 'append', None
    if operation in EXTENT_OPERATIONS:
        return 'extent', None
    return 'sidecar', None


def _committed_snapshots(scope, links):
    """Snapshot ids with a regular global decision file under a leased directory (checkpoint recovery owns those cuts)."""
    found = set()
    directories = set(scope['roots']) | set(scope['stores']) | {Path(l['dataset_root']) for l in links if l}
    for base in directories:
        directory = base / 'global/manifests'
        if directory.is_dir() and not directory.is_symlink():
            found.update(p.stem for p in directory.glob('*.json') if p.is_file() and not p.is_symlink())
    return found


def _classify_uncommitted_checkpoint(token, link, scope):
    """A live checkpoint-sidecar token whose snapshot never reached a global decision (owner proven dead)."""
    if link is None:
        return None, 'checkpoint-intent-unreadable'
    if token['operation'] != 'atomic-json' or token['state'] not in ('RESERVED', 'WRITING'):
        return None, f"unknown-operation:{token['operation']}"
    if link.get('stage') not in ('prepare', 'decision'):
        # a decision-copy token exists only after the global decision committed; without one the state is inconsistent
        return None, f"checkpoint-stage-without-committed-decision:{link.get('stage')}"
    root = Path(str(link.get('dataset_root', '')))
    real_root = Path(os.path.realpath(root))
    leased = {Path(os.path.realpath(l.path)) for l in scope['leases']} | {Path(os.path.realpath(scope['ledger_lease'].path))}
    if real_root not in leased:
        return None, 'checkpoint-dataset-not-under-an-exclusive-lease'
    try:
        info = root.stat()
    except OSError:
        return None, 'checkpoint-dataset-unavailable'
    if (link.get('dataset_device'), link.get('dataset_inode')) != (info.st_dev, info.st_ino):
        return None, 'checkpoint-dataset-identity-mismatch'
    sid, stage, rank = link.get('snapshot_id'), link['stage'], link.get('rank')
    if (not isinstance(rank, int) or isinstance(rank, bool) or rank < 0 or (stage == 'decision' and rank != 0)
            or re.fullmatch('[0-9a-f]{32}', str(sid)) is None):
        return None, 'checkpoint-intent-malformed'
    target = f'global/manifests/{sid}.json' if stage == 'decision' else f'rank-{rank:04d}/manifests/prepare-{sid}.json'
    generation = token['generation'] or ''
    if re.fullmatch('[0-9a-f]{32}', generation) is None or re.fullmatch('[0-9a-f]{32}', token['owner'] or '') is None:
        return None, 'checkpoint-writer-generation-unbound'
    parent, name = target.rsplit('/', 1)
    if link.get('target') != target or link.get('temporary') != f'{parent}/.{name}.{generation}.tmp' \
            or link.get('expected_bytes') != token['bytes']:
        return None, 'checkpoint-intent-path-mismatch'
    if Path(os.path.realpath(real_root / parent)) != real_root / parent:
        return None, 'checkpoint-directory-aliased'
    for key in ('target', 'temporary'):
        path = real_root / link[key]
        if path.exists() or path.is_symlink():
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > token['bytes']:
                return None, f'checkpoint-{key}-not-an-exclusive-regular-file'
    return 'checkpoint-uncommitted', None


def recover_uncommitted_reservations(ledger, *, apply=False, confirm_plan=None, policy=None,
                                     max_tokens=MAX_TOKENS, max_journal_bytes=64 * 1024 ** 2):
    """Dry-run or apply settlement of live reservations that have no checkpoint intent.

    Tokens with a checkpoint intent are settled only when their snapshot never reached a global decision (prepare/decision
    stage, class ``checkpoint-uncommitted``); committed checkpoint sidecars are
    deferred to checkpoint recovery. Dedicated metadata and export intents are
    inspected here independently of whether their snapshot committed.
    Every unlinked token must be of a known operation; catalog tokens must be
    attributable to a registered store; every store must pass inspection (no
    non-empty catalog side file, quick_check ok, published extents inside
    their segment, no resident amplitudes, no interior journal corruption, no
    symlink). Otherwise the run refuses and changes nothing.
    """
    if type(apply) is not bool:
        raise ValueError('apply must be boolean')
    if apply and not (isinstance(confirm_plan, str) and re.fullmatch('[0-9a-f]{64}', confirm_plan)):
        raise ValueError('apply needs the plan_sha256 of a reviewed dry run')
    if type(max_tokens) is not int or max_tokens < 1:
        raise ValueError('max_tokens must be a positive integer')
    with offline_scope(ledger, policy=policy) as scope:
        budget = scope['budget']
        entries = [0]
        stores = [_inspect_store(s, max_journal_bytes=max_journal_bytes, entries=entries,
                                 migration=scope['migration_clones'].get(str(s))) for s in scope['stores']]
        owners = _catalog_owner_digests(stores)
        live = budget.live_reservations(linked=False, limit=max_tokens)
        linked = budget.live_reservations(linked=True, limit=max_tokens)
        blockers = [f"{s['path']}: {b}" for s in stores for b in s['blockers']]
        tokens, refused = [], list(blockers)
        usage_before = budget._observed_usage()
        link_of = {token['id']: budget.checkpoint_link(token['id']) for token in linked}
        committed_snapshots = _committed_snapshots(scope, link_of.values())
        deferred, uncommitted_linked = [], []
        for token in linked:
            link = link_of[token['id']]
            dedicated = link is not None and (link.get('purpose') in ('state-export','catalog-startup','stored-migration') or
                                              link.get('stage') in ('metadata-payload','metadata-chunk'))
            if not dedicated and link is not None and link.get('snapshot_id') in committed_snapshots:
                deferred.append(token)       # a committed cut: checkpoint recovery owns it
            else:
                uncommitted_linked.append((token, link))
        linked = deferred
        for token, link in uncommitted_linked:
            attribution = None
            if link is not None and (link.get('purpose') in ('state-export','catalog-startup','stored-migration') or
                                     link.get('stage') in ('metadata-payload','metadata-chunk')):
                try:
                    if link.get('purpose') == 'stored-migration':
                        from .migration_admission import plan_recovery
                        dedicated_plan = plan_recovery(token,link,scope)
                    elif link.get('purpose') == 'catalog-startup':
                        from .catalog_startup import plan_recovery
                        dedicated_plan = plan_recovery(token,link,scope)
                    elif link.get('purpose') == 'state-export':
                        from .export_admission import plan_recovery
                        dedicated_plan = plan_recovery(token,link,scope)
                    else:
                        from .checkpoint_metadata_recovery import plan_recovery
                        dedicated_plan = plan_recovery(token,link,leased_directories=
                            [l.path for l in scope['leases']] + [scope['ledger_lease'].path])
                    klass, reason = dedicated_plan['class'], None
                    attribution = dedicated_plan['attribution']
                except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
                    klass, reason = None, str(exc)
            else:
                klass, reason = _classify_uncommitted_checkpoint(token, link, scope)
            entry = {'id': token['id'], 'operation': token['operation'], 'class': klass, 'state': token['state'],
                     'bytes': token['bytes'], 'materialized_bytes': token['materialized_bytes'],
                     'release_pending_bytes': token['bytes'] - token['materialized_bytes'],
                     'disposition': 'settle' if klass and not blockers else 'refuse', 'reason': reason}
            if attribution is not None:
                entry['attribution'] = attribution
            elif klass:
                entry['checkpoint'] = {k: link[k] for k in ('snapshot_id', 'stage', 'rank', 'dataset_id')}
            if reason:
                refused.append(f"{token['id']}: {reason}")
            tokens.append((entry, token, link))
        for token in live:
            klass, reason = _classify(token, owners)
            entry = {'id': token['id'], 'operation': token['operation'], 'class': klass, 'state': token['state'],
                     'bytes': token['bytes'], 'materialized_bytes': token['materialized_bytes'],
                     'release_pending_bytes': token['bytes'] - token['materialized_bytes'],
                     'disposition': 'settle' if klass and not blockers else 'refuse', 'reason': reason}
            if klass == 'catalog':
                entry['catalog_attribution'] = owners[token['owner']]
            if reason:
                refused.append(f"{token['id']}: {reason}")
            tokens.append((entry, token, None))
        plan = {'version': 1, 'ledger': str(scope['ledger']), 'stores': stores,
                'tokens': [e for e, _, _ in tokens], 'linked_live_ids': [t['id'] for t in linked],
                'observed_usage_bytes': usage_before}
        plan_sha = _digest(plan)
        report = {'kind': 'uncommitted-reservation-recovery', 'apply': apply, 'plan_sha256': plan_sha,
                  'owner_death_proof': {'ledger_lease': 'exclusive-flock-on-ledger-directory', 'dataset_leases': sorted(l.path for l in scope['leases']) + [scope['ledger_lease'].path],
                                        'pid_or_timestamp_used': False},
                  'ledger_layout': 'store-private' if scope['private_ledger'] else 'shared-policy',
                  'stores': stores, 'tokens': [e for e, _, _ in tokens], 'linked_live_tokens_deferred': len(linked),
                  'pending_bytes_before': budget.reserved_bytes, 'observed_usage_bytes': usage_before,
                  'would_release_pending_bytes': sum(e['release_pending_bytes'] for e, _, _ in tokens if not refused),
                  'refusals': refused, 'artifacts_preserved': True, 'strict_capacity_guarantee': False,
                  'status': 'refused' if refused else ('nothing-to-do' if not tokens else 'dry-run')}
        report['volume_claim'] = volume_claim_report(scope, apply=False)
        if refused:
            if apply:
                raise UncommittedRecoveryRefused('Refusing: ' + '; '.join(refused[:5]), report)
            return report
        if not apply:
            return report
        if confirm_plan != plan_sha:
            raise UncommittedRecoveryRefused('Plan changed since the dry run; refusing to apply', report)
        _assert_catalog_files(stores)
        for entry, token, link in tokens:
            if entry['class'] == 'catalog-startup':
                from .catalog_startup import plan_recovery
                if plan_recovery(token,link,scope)['attribution'] != entry['attribution']:
                    raise UncommittedRecoveryRefused('Catalog startup inventory changed after inspection',report)
            elif entry['class'] == 'export':
                from .export_admission import plan_recovery, recheck_recovery
                recheck_recovery(token,link,scope)
                if plan_recovery(token,link,scope)['attribution'] != entry['attribution']:
                    raise UncommittedRecoveryRefused('Export inventory changed after inspection',report)
            elif entry['class'] == 'checkpoint-metadata':
                from .checkpoint_metadata_recovery import plan_recovery
                current = plan_recovery(token,link,leased_directories=
                    [l.path for l in scope['leases']] + [scope['ledger_lease'].path])
                if current['attribution'] != entry['attribution']:
                    raise UncommittedRecoveryRefused('Checkpoint metadata changed after inspection',report)
        released = 0
        for entry, token, link in tokens:
            _assert_leases(scope)
            proof = {'version': 1, 'kind': 'uncommitted-recovery', 'reservation_id': token['id'],
                     'operation': token['operation'], 'class': entry['class'], 'plan_sha256': plan_sha,
                     'ledger_lease': 'exclusive-flock-on-ledger-directory', 'dataset_lease_count': len(scope['leases']),
                     'store_inventory_sha256': _digest(stores), 'observed_usage_bytes': usage_before,
                     'released_pending_bytes': entry['release_pending_bytes'], 'artifacts_preserved': True}
            if entry['class'] == 'catalog':
                proof['catalog_attribution'] = entry['catalog_attribution']
            if 'attribution' in entry:
                proof['attribution'] = entry['attribution']
            if link is None:
                budget.reconcile_unlinked(token['id'], expected_reservation=token,
                                          materialized_bytes=token['materialized_bytes'],
                                          evidence='uncommitted-recovery-v1:' + content_hash(proof),
                                          evidence_document=proof)
            else:
                proof['checkpoint_intent_sha256'] = _digest(link)
                budget.reconcile_checkpoint(token['id'], expected_reservation=token, expected_link=link,
                                            materialized_bytes=token['materialized_bytes'],
                                            evidence='uncommitted-recovery-v1:' + content_hash(proof),
                                            evidence_document=proof)
            released += entry['release_pending_bytes']
        usage_after = budget._observed_usage()
        report.update(status='applied', released_pending_bytes=released, observed_usage_after_bytes=usage_after,
                      artifacts_preserved=usage_after == usage_before, pending_bytes_after=budget.reserved_bytes)
        report['volume_claim'] = volume_claim_report(scope, apply=True)
        if usage_after != usage_before:
            raise UncommittedRecoveryRefused('Observed artifact bytes changed during recovery', report)
        return report


# ---------------------------------------------------------------------------
# Temporary artifact and dead-branch GC
# ---------------------------------------------------------------------------

_ROOT_ID = re.compile(r'[0-9a-f]{32}')


def _manifest_root_mentions(store, roots):
    """Roots named anywhere in the store's manifests (and the global decisions beside it)."""
    mentioned = set()
    for directory in (store / 'manifests', store.parent / 'global' / 'manifests', store / 'global' / 'manifests'):
        if not directory.is_dir() or directory.is_symlink():
            continue
        for file in sorted(directory.glob('*.json'))[:MAX_ENTRIES]:
            info = file.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_size > 16 * 1024 ** 2:
                raise DiskAdmissionError(f'Manifest {file} is not a bounded regular file')
            mentioned.update(set(_ROOT_ID.findall(file.read_text(errors='replace'))) & roots)
    return mentioned


def _plan_store_branches(store):
    path = store / 'metadata/catalog.sqlite3'
    con = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
    try:
        con.execute('PRAGMA query_only=ON')
        parents = dict(con.execute('SELECT id,parent FROM roots'))
        live = set()

        def keep(root):
            while root and root not in live:
                if root not in parents:
                    raise DiskAdmissionError('A committed reference names an unknown catalog root')
                live.add(root)
                root = parents[root]
        snapshots = 0
        for (data,) in con.execute('SELECT data FROM snapshots'):
            snapshots += 1
            keep(json.loads(data)['root'])
        for root in _manifest_root_mentions(store, set(parents)):
            keep(root)
        dead = sorted(set(parents) - live)
        views = {}
        for root, version in con.execute('SELECT root,version FROM views'):
            views.setdefault(root, set()).add(version)
        # Same liveness rule as the quiescent StoredSimulator.collect_garbage.
        live_versions = {v for r in live for v in views.get(r, ())}
        versions = dict(con.execute('SELECT id,segment FROM versions'))
        removable = [v for v in versions if v not in live_versions]
        kept_segments = {seg for v, seg in versions.items() if v in live_versions and seg is not None}
        sizes = {p.name: p.stat().st_size for p in (store / 'state').glob('segment-*.bin')} if (store / 'state').is_dir() else {}
        removable_segments = sorted(name for name in sizes if int(name[8:14]) not in kept_segments)
        return {'store': str(store), 'snapshots': snapshots, 'live_roots': len(live), 'dead_roots': dead,
                'removable_versions': len(removable), 'removable_segments': [[n, sizes[n]] for n in removable_segments],
                'stale_active_runs': sorted(r for (r,) in con.execute('SELECT id FROM active_runs'))}
    finally:
        con.close()


def _plan_temporaries(scope, stores_report):
    links = scope['budget'].db.execute('SELECT link FROM checkpoint_reservations LIMIT 4097').fetchall()
    if len(links) > 4096:
        raise DiskAdmissionError('Too many checkpoint intents to cross-check temporaries')
    referenced = set()
    for (link,) in links:
        temporary = json.loads(link).get('temporary')
        if isinstance(temporary, str):
            referenced.add(os.path.basename(temporary))
    items = []
    for store in stores_report:
        for tmp in store['temporaries']:
            name = os.path.basename(tmp['path'])
            reason = None
            if TEMPORARY_NAME.fullmatch(name) is None:
                reason = 'name-is-not-a-writer-generation-temporary'
            elif name in referenced:
                reason = 'referenced-by-checkpoint-intent'
            items.append({'store': store['path'], 'path': tmp['path'], 'bytes': tmp['bytes'], 'dev': tmp['dev'],
                          'ino': tmp['ino'], 'disposition': 'keep' if reason else 'delete', 'reason': reason})
    return items


def _fsync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _inspect_all(scope, max_journal_bytes):
    entries = [0]
    stores = [_inspect_store(s, max_journal_bytes=max_journal_bytes, entries=entries,
                                 migration=scope['migration_clones'].get(str(s))) for s in scope['stores']]
    return stores, [f"{s['path']}: {b}" for s in stores for b in s['blockers']]


def _proof(scope):
    return {'ledger_lease': 'exclusive-flock-on-ledger-directory',
            'dataset_leases': sorted(l.path for l in scope['leases']) + [scope['ledger_lease'].path],
            'pid_or_timestamp_used': False}


def _check_apply_args(apply, confirm_plan):
    if type(apply) is not bool:
        raise ValueError('apply must be boolean')
    if apply and not (isinstance(confirm_plan, str) and re.fullmatch('[0-9a-f]{64}', confirm_plan)):
        raise ValueError('apply needs the plan_sha256 of a reviewed dry run')


def collect_temporary_artifacts(ledger, *, apply=False, confirm_plan=None, policy=None,
                                max_journal_bytes=64 * 1024 ** 2):
    """Dry-run or delete writer-generation temporaries that no checkpoint intent names.

    Needs the same owner-death proof as reservation recovery and works without
    admission credit (it only shrinks the namespace), so it can run on a full
    volume. Only ``<name>.<32 hex>.tmp`` regular, singly-linked files are
    candidates; each is re-checked (device/inode/size) immediately before
    ``unlink``. Reservation tokens are not changed.
    """
    _check_apply_args(apply, confirm_plan)
    with offline_scope(ledger, policy=policy) as scope:
        stores, blockers = _inspect_all(scope, max_journal_bytes)
        items = _plan_temporaries(scope, stores)
        plan_sha = _digest({'version': 1, 'kind': 'temporaries', 'items': items, 'blockers': blockers})
        deletions = [i for i in items if i['disposition'] == 'delete']
        report = {'kind': 'temporary-artifact-gc', 'apply': apply, 'plan_sha256': plan_sha, 'items': items,
                  'refusals': blockers, 'would_reclaim_bytes': sum(i['bytes'] for i in deletions),
                  'owner_death_proof': _proof(scope), 'strict_capacity_guarantee': False,
                  'status': 'refused' if blockers else ('nothing-to-do' if not deletions else 'dry-run')}
        if blockers:
            if apply:
                raise UncommittedRecoveryRefused('Refusing: ' + '; '.join(blockers[:5]), report)
            return report
        if not apply:
            return report
        if confirm_plan != plan_sha:
            raise UncommittedRecoveryRefused('Plan changed since the dry run; refusing to apply', report)
        reclaimed = 0
        for item in deletions:
            _assert_leases(scope)
            path = Path(item['store']) / item['path']
            info = path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or (info.st_dev, info.st_ino, info.st_size) != (item['dev'], item['ino'], item['bytes'])):
                raise UncommittedRecoveryRefused(f'Temporary {path} changed after review', report)
            path.unlink()
            _fsync_directory(path.parent)
            reclaimed += item['bytes']
        report.update(status='applied', reclaimed_bytes=reclaimed)
        return report


_BRANCH_SQL = (
    'DELETE FROM views WHERE root NOT IN (SELECT root FROM live_roots)',
    'DELETE FROM views_v2 WHERE root NOT IN (SELECT root FROM live_roots)',
    'DELETE FROM root_edges WHERE root NOT IN (SELECT root FROM live_roots)',
    'DELETE FROM root_log_bounds WHERE root NOT IN (SELECT root FROM live_roots)',
    'DELETE FROM catalog_checksums WHERE substr(key,6,32) NOT IN (SELECT root FROM live_roots)',
    'DELETE FROM roots WHERE id NOT IN (SELECT root FROM live_roots)',
    'DELETE FROM versions WHERE id NOT IN (SELECT version FROM views)',
)


def collect_dead_branches(ledger, *, apply=False, confirm_plan=None, policy=None, allow_no_snapshot=False,
                          max_journal_bytes=64 * 1024 ** 2):
    """Dry-run or reclaim catalog roots, versions and whole segments no committed cut references.

    Live roots are the ancestor chains of every ``snapshots`` row and of every
    root named in a manifest. Stale ``active_runs`` rows are removed because
    all runtimes are proved dead. The catalog is edited through an admitting
    ``CatalogConnection`` under the adopted exclusive lease, so a volume with
    no admission headroom refuses (free space first with
    ``collect_temporary_artifacts``). A store without any snapshot refuses
    unless ``allow_no_snapshot``.
    """
    _check_apply_args(apply, confirm_plan)
    if type(allow_no_snapshot) is not bool:
        raise ValueError('allow_no_snapshot must be boolean')

    def build(scope):
        stores, blockers = _inspect_all(scope, max_journal_bytes)
        plans = [] if blockers else [_plan_store_branches(Path(s['path'])) for s in stores]
        refusals = list(blockers)
        for plan in plans:
            if plan['dead_roots'] and plan['live_roots'] == 0 and not allow_no_snapshot:
                refusals.append(f"{plan['store']}: no committed snapshot; every root would be reclaimed")
        digest = _digest({'version': 1, 'kind': 'branches', 'stores': plans, 'blockers': blockers,
                          'allow_no_snapshot': allow_no_snapshot})
        return plans, refusals, digest

    def has_work(plan):
        return bool(plan['dead_roots'] or plan['removable_versions'] or plan['removable_segments'] or plan['stale_active_runs'])

    with offline_scope(ledger, policy=policy) as scope:
        plans, refusals, plan_sha = build(scope)
        report = {'kind': 'dead-branch-gc', 'apply': apply, 'plan_sha256': plan_sha, 'stores': plans,
                  'refusals': refusals, 'strict_capacity_guarantee': False,
                  'would_reclaim_segment_bytes': sum(n for p in plans for _, n in p['removable_segments']),
                  'owner_death_proof': _proof(scope),
                  'status': 'refused' if refusals else ('dry-run' if any(map(has_work, plans)) else 'nothing-to-do')}
        if refusals:
            if apply:
                raise UncommittedRecoveryRefused('Refusing: ' + '; '.join(refusals[:5]), report)
            return report
        if not apply:
            return report
        if confirm_plan != plan_sha:
            raise UncommittedRecoveryRefused('Plan changed since the dry run; refusing to apply', report)
    # Re-enter with an admitting budget; proof and plan are recomputed under it.
    with offline_scope(ledger, policy=policy, writer=True) as scope:
        plans, refusals, again = build(scope)
        if refusals or again != plan_sha:
            raise UncommittedRecoveryRefused('Plan changed while acquiring the maintenance budget', report)
        reclaimed = 0
        for plan in plans:
            if not has_work(plan):
                continue
            _assert_leases(scope)
            store = Path(plan['store'])
            db = CatalogConnection(store / 'metadata/catalog.sqlite3', budget_getter=lambda: scope['budget'],
                                   require_native=True)
            try:
                with db:
                    db.execute('CREATE TEMP TABLE IF NOT EXISTS live_roots(root TEXT PRIMARY KEY)')
                    db.execute('DELETE FROM live_roots')
                    dead = set(plan['dead_roots'])
                    for (root,) in db.execute('SELECT id FROM roots').fetchall():
                        if root not in dead:
                            db.execute('INSERT OR IGNORE INTO live_roots VALUES(?)', (root,))
                    for sql in _BRANCH_SQL:
                        db.execute(sql)
                    db.execute('DELETE FROM active_runs')
                survivors = {seg for (seg,) in db.execute('SELECT DISTINCT segment FROM versions WHERE segment IS NOT NULL').fetchall()}
                if db.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
                    raise UncommittedRecoveryRefused('Catalog integrity check failed after branch GC', report)
            finally:
                db.close()
            for name, size in plan['removable_segments']:
                if int(name[8:14]) in survivors:
                    raise UncommittedRecoveryRefused(f'Segment {name} gained a reference during GC', report)
                path = store / 'state' / name
                info = path.lstat()
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size != size:
                    raise UncommittedRecoveryRefused(f'Segment {name} changed after review', report)
                path.unlink()
                reclaimed += size
            if plan['removable_segments']:
                _fsync_directory(store / 'state')
        report.update(status='applied', reclaimed_segment_bytes=reclaimed)
        return report
