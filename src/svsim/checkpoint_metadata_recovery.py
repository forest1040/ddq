"""Inventory and cut validation for write-once checkpoint payload/chunk intents."""
from hashlib import sha256
import json
from pathlib import Path
import re

from .catalog_admission import catalog_birth_identity
from .disk_budget import DiskAdmissionError
from .journal import content_hash

METADATA_STAGES = ('metadata-payload', 'metadata-chunk')
METADATA_OPERATIONS = tuple('checkpoint-' + stage for stage in METADATA_STAGES)


def plan_recovery(token, link, *, leased_directories, max_bytes=64 * 1024**2):
    from .checkpoint_recovery import _checked_path, _envelope, _read_bytes
    if not isinstance(link, dict) or link.get('stage') not in METADATA_STAGES:
        raise DiskAdmissionError('Unknown checkpoint metadata intent')
    stage = link['stage']
    if (token['operation'] != 'checkpoint-' + stage or token['state'] not in ('RESERVED', 'WRITING') or
            re.fullmatch('[0-9a-f]{32}', token['owner'] or '') is None or
            re.fullmatch('[0-9a-f]{32}', token['generation'] or '') is None):
        raise DiskAdmissionError('Checkpoint metadata writer generation is unbound')
    root = Path(link.get('dataset_root', ''))
    if not root.is_absolute() or root.resolve() != root or root not in {Path(p) for p in leased_directories}:
        raise DiskAdmissionError('Checkpoint metadata dataset is not under an exclusive lease')
    current = root.lstat()
    birth = catalog_birth_identity(root, current)
    if (birth is None or birth != link.get('dataset_birth_identity') or
            (current.st_dev, current.st_ino) != (link.get('dataset_device'), link.get('dataset_inode'))):
        raise DiskAdmissionError('Checkpoint metadata dataset generation changed')
    sid, dataset_id, rank = link.get('snapshot_id'), link.get('dataset_id'), link.get('rank')
    digest = link.get('digest', '')
    if (any(re.fullmatch('[0-9a-f]{32}', str(v)) is None for v in (sid, dataset_id)) or
            re.fullmatch('[0-9a-f]{64}', digest) is None or type(rank) is not int or not 0 <= rank <= 1024 or
            link.get('expected_bytes') != token['bytes'] or link.get('target_existed') is not False):
        raise DiskAdmissionError('Checkpoint metadata intent is malformed')
    filename = f'payload-{sid}.json' if stage == 'metadata-payload' else f'chunk-{digest}.json'
    target = f'rank-{rank:04d}/manifests/{filename}'
    temporary = f'rank-{rank:04d}/manifests/.{filename}.{token["generation"]}.tmp'
    if link.get('target') != target or link.get('temporary') != temporary:
        raise DiskAdmissionError('Checkpoint metadata target or temporary generation conflicts')
    inventory, growth = {}, 0
    for kind, relative in (('target', target), ('temporary', temporary)):
        path, info = _checked_path(root, relative, missing=True)
        raw = _read_bytes(root, relative, max_bytes, missing=True)
        if raw is not None:
            complete = len(raw) == token['bytes']
            if (len(raw) > token['bytes'] or kind == 'target' and not complete or
                    (kind == 'target' or complete) and sha256(raw).hexdigest() != digest):
                raise DiskAdmissionError('Checkpoint metadata artifact conflicts with intended content')
            growth += len(raw)
        inventory[kind] = {'path': str(path), 'present': info is not None,
                           **({'device': info.st_dev, 'inode': info.st_ino, 'bytes': info.st_size,
                               'birth_identity': catalog_birth_identity(path, info),
                               'sha256': sha256(raw).hexdigest()} if info is not None else {})}
    if growth > token['bytes']:
        raise DiskAdmissionError('Checkpoint metadata inventory exceeds its reserved generation')
    decision_relative = f'global/manifests/{sid}.json'
    decision_raw = _read_bytes(root, decision_relative, max_bytes, missing=True)
    authority = {'status': 'uncommitted', 'snapshot_id': sid}
    if decision_raw is not None:
        decision, _ = _envelope(root, decision_relative, max_bytes)
        if decision.get('snapshot_id') != sid or decision.get('commit_decision') != 'COMMIT':
            raise DiskAdmissionError('Checkpoint metadata decision conflicts with its intent')
        ref = decision.get('metadata_payload', {})
        if ref.get('name') != f'payload-{sid}.json':
            raise DiskAdmissionError('Checkpoint metadata decision has an invalid payload path')
        payload_raw = _read_bytes(root, f'rank-0000/manifests/{ref["name"]}', max_bytes)
        if len(payload_raw) != ref.get('bytes') or sha256(payload_raw).hexdigest() != ref.get('sha256'):
            raise DiskAdmissionError('Checkpoint metadata authoritative payload is incomplete')
        payload = json.loads(payload_raw)
        from .checkpoint_payload import decision_record
        if payload.get('dataset_id') != dataset_id or decision_record({**payload, 'metadata_payload': ref}) != decision:
            raise DiskAdmissionError('Checkpoint metadata payload belongs to a different cut')
        if stage == 'metadata-payload':
            expected = ref
        else:
            refs = [r for key in ('gates_chunks', 'history_chunks') for r in payload.get(key, [])]
            matches = [r for r in refs if r.get('sha256') == digest]
            if not matches:
                raise DiskAdmissionError('Checkpoint metadata chunk is not referenced by its cut')
            expected = matches[0]
            raw = _read_bytes(root, f'rank-0000/manifests/{filename}', max_bytes)
            if len(raw) != expected.get('bytes') or sha256(raw).hexdigest() != digest:
                raise DiskAdmissionError('Checkpoint metadata authoritative chunk is incomplete')
        if expected.get('bytes') != token['bytes'] or expected.get('sha256') != digest:
            raise DiskAdmissionError('Checkpoint metadata reservation conflicts with committed cut')
        authority = {'status': 'committed', 'snapshot_id': sid, 'decision_sha256': content_hash(decision)}
    return {'class': 'checkpoint-metadata', 'attribution': {
        'dataset_root': str(root), 'dataset_id': dataset_id, 'snapshot_id': sid, 'stage': stage, 'rank': rank,
        'dataset_birth_identity': birth, 'inventory': inventory, 'authority': authority,
        'observed_growth_bytes': growth, 'artifacts_preserved': True}}
