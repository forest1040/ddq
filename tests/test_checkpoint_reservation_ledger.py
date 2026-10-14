import os
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from svsim.disk_budget import DiskAdmissionError, DiskBudget

SID = 'a' * 32
DID = 'b' * 32
DIGEST = 'c' * 64


def make_link(root, stage='prepare', rank=0):
    if stage == 'prepare':
        rel = Path(f'rank-{rank:04d}/manifests/prepare-{SID}.json')
    elif stage == 'decision':
        rel = Path(f'global/manifests/{SID}.json')
    else:
        rel = Path(f'rank-{rank:04d}/manifests/decision-{SID}.json')
    target = root / rel
    temp = target.with_name(f'.{target.name}.{"d" * 32}.tmp')
    return {'dataset_root': str(root), 'dataset_id': DID, 'snapshot_id': SID, 'rank': rank,
            'stage': stage, 'target': str(target), 'temporary': str(temp), 'digest': DIGEST}


def make_budget(root):
    return DiskBudget(root / 'ledger.sqlite', [root], usage=lambda: 0, free=lambda: 10**9)


def test_checkpoint_intent_is_canonical_cas_receipt_and_terminal(tmp_path):
    b = make_budget(tmp_path)
    key = b.reserve(17, operation='checkpoint', checkpoint_link=make_link(tmp_path))
    item = b.checkpoint_links(tmp_path)[0]
    assert item['reservation']['id'] == key
    assert item['link']['target'] == f'rank-0000/manifests/prepare-{SID}.json'
    assert item['link']['expected_bytes'] == 17 and item['link']['target_existed'] is False
    b.reconcile_checkpoint(key, expected_reservation=item['reservation'], expected_link=item['link'],
                           materialized_bytes=17, evidence='authoritative-cut:verified')
    terminal = b.checkpoint_links(tmp_path)[0]
    assert terminal['reservation']['state'] == 'RECONCILED'
    with pytest.raises(DiskAdmissionError):
        b.reconcile_checkpoint(key, expected_reservation=item['reservation'], expected_link=item['link'],
                               materialized_bytes=17, evidence='repeat')
    with pytest.raises(DiskAdmissionError):
        b.reconcile_checkpoint(key, expected_reservation=terminal['reservation'], expected_link=item['link'],
                               materialized_bytes=17, evidence='stale')
    b.close()


@pytest.mark.parametrize('mutate', [
    lambda x: x.update(dataset_id='Z' * 32),
    lambda x: x.update(snapshot_id='bad'),
    lambda x: x.update(rank=True),
    lambda x: x.update(rank=1025),
    lambda x: x.update(stage='other'),
    lambda x: x.update(digest='d' * 63),
    lambda x: x.update(target='/outside.json'),
    lambda x: x.update(temporary=str(Path(x['temporary']).with_name('.wrong.tmp'))),
    lambda x: x.update(target=str(Path(x['dataset_root']) / 'rank-0000' / 'manifests' / '..' / 'manifests' / f'prepare-{SID}.json')),
    lambda x: x.update(temporary=str(Path(x['dataset_root']) / 'rank-0000' / 'manifests' / '..' / 'manifests' / f'.prepare-{SID}.json.{"d" * 32}.tmp')),
    lambda x: x.update(stage='decision', rank=1),
])
def test_invalid_checkpoint_links_leave_no_reservation(tmp_path, mutate):
    b = make_budget(tmp_path)
    link = make_link(tmp_path)
    mutate(link)
    with pytest.raises((ValueError, DiskAdmissionError)):
        b.reserve(1, checkpoint_link=link)
    assert b.reserved_bytes == 0 and b.checkpoint_links(tmp_path) == []
    b.close()


def test_checkpoint_link_atomic_insert_and_path_symlink_rejection(tmp_path):
    b = make_budget(tmp_path)
    link = make_link(tmp_path)
    b.db.execute("CREATE TRIGGER reject_checkpoint BEFORE INSERT ON checkpoint_reservations BEGIN SELECT RAISE(ABORT,'injected'); END")
    with pytest.raises(Exception):
        b.reserve(1, checkpoint_link=link)
    assert b.reserved_bytes == 0 and b.checkpoint_links(tmp_path) == []
    b.db.execute('DROP TRIGGER reject_checkpoint')
    target = tmp_path / 'target-dir'
    target.mkdir()
    (tmp_path / 'rank-0000').symlink_to(target)
    with pytest.raises(ValueError, match='symlink'):
        b.reserve(1, checkpoint_link=link)
    assert b.reserved_bytes == 0
    b.close()


def test_checkpoint_root_must_be_registered(tmp_path):
    root = tmp_path / 'registered'
    root.mkdir()
    external = tmp_path / 'external'
    external.mkdir()
    b = DiskBudget(tmp_path / 'ledger.sqlite', [root], usage=lambda: 0, free=lambda: 10**9)
    with pytest.raises(ValueError, match='registered root'):
        b.reserve(1, checkpoint_link=make_link(external))
    b.close()


@pytest.mark.parametrize('which', ['target', 'temporary'])
def test_existing_checkpoint_paths_must_be_regular_files(tmp_path, which):
    b = make_budget(tmp_path)
    link = make_link(tmp_path)
    path = Path(link[which])
    path.mkdir(parents=True)
    with pytest.raises(ValueError, match='regular file'):
        b.reserve(1, checkpoint_link=link)
    assert b.reserved_bytes == 0 and b.checkpoint_links(tmp_path) == []
    b.close()


def test_checkpoint_links_refuses_over_limit_without_truncation(tmp_path):
    b = make_budget(tmp_path)
    first = b.reserve(1, checkpoint_link=make_link(tmp_path))
    link = make_link(tmp_path)
    link['snapshot_id'] = 'e' * 32
    link['target'] = str(tmp_path / 'rank-0000' / 'manifests' / f'prepare-{link["snapshot_id"]}.json')
    link['temporary'] = str(tmp_path / 'rank-0000' / 'manifests' / f'.prepare-{link["snapshot_id"]}.json.{"d" * 32}.tmp')
    second = b.reserve(1, checkpoint_link=link)
    with pytest.raises(DiskAdmissionError, match='exceeds limit'):
        b.checkpoint_links(tmp_path, limit=1)
    assert {entry['reservation']['id'] for entry in b.checkpoint_links(tmp_path, limit=2)} == {first, second}
    b.close()


def _proof_evidence(document):
    payload = json.dumps(document, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    return 'verified-recovery:' + hashlib.sha256(payload).hexdigest()


def test_recovery_receipt_is_atomic_and_removed_with_reservation(tmp_path):
    b = make_budget(tmp_path)
    key = b.reserve(5, checkpoint_link=make_link(tmp_path))
    item = b.checkpoint_links(tmp_path)[0]
    proof = {'cut': 'committed', 'inventory': ['manifest', 'payload']}
    evidence = _proof_evidence(proof)
    b.db.execute("CREATE TRIGGER reject_receipt BEFORE INSERT ON checkpoint_recovery_receipts BEGIN SELECT RAISE(ABORT,'injected'); END")
    with pytest.raises(Exception):
        b.reconcile_checkpoint(key, expected_reservation=item['reservation'], expected_link=item['link'],
                               materialized_bytes=5, evidence=evidence, evidence_document=proof)
    assert b.reservation(key)['state'] == 'RESERVED'
    assert b.recovery_receipt(key) is None
    b.db.execute('DROP TRIGGER reject_receipt')
    b.reconcile_checkpoint(key, expected_reservation=item['reservation'], expected_link=item['link'],
                           materialized_bytes=5, evidence=evidence, evidence_document=proof)
    assert b.recovery_receipt(key) == proof
    with pytest.raises(DiskAdmissionError):
        b.reconcile_checkpoint(key, expected_reservation=item['reservation'], expected_link=item['link'],
                               materialized_bytes=5, evidence=evidence, evidence_document=proof)
    b.release(key)
    assert b.recovery_receipt(key) is None
    assert b.checkpoint_links(tmp_path) == []
    b.close()


@pytest.mark.parametrize('document,evidence', [
    ({'state': 'valid'}, 'wrong-digest'),
    ({'text': 'x' * 8200}, 'evidence'),
])
def test_invalid_recovery_document_does_not_mutate_reservation(tmp_path, document, evidence):
    b = make_budget(tmp_path)
    key = b.reserve(5, checkpoint_link=make_link(tmp_path))
    item = b.checkpoint_links(tmp_path)[0]
    with pytest.raises(ValueError):
        b.reconcile_checkpoint(key, expected_reservation=item['reservation'], expected_link=item['link'],
                               materialized_bytes=5, evidence=evidence, evidence_document=document)
    assert b.reservation(key) == item['reservation']
    assert b.recovery_receipt(key) is None
    b.close()


def test_checkpoint_reservation_rejects_preexisting_temporary(tmp_path):
    b = make_budget(tmp_path)
    link = make_link(tmp_path)
    temporary = Path(link['temporary'])
    temporary.parent.mkdir(parents=True)
    temporary.write_bytes(b'foreign')
    with pytest.raises(ValueError, match='already exists'):
        b.reserve(1, checkpoint_link=link)
    assert b.reserved_bytes == 0 and b.checkpoint_links(tmp_path) == []
    b.close()


def test_checkpoint_paths_accept_root_parent_alias_but_store_canonical_paths(tmp_path):
    root = tmp_path / 'dataset'
    root.mkdir()
    alias_parent = tmp_path / 'parent-alias'
    alias_parent.symlink_to(tmp_path, target_is_directory=True)
    alias_root = alias_parent / 'dataset'
    b = make_budget(tmp_path)
    link = make_link(alias_root)
    key = b.reserve(4, checkpoint_link=link)
    saved = b.checkpoint_links(root)[0]
    assert saved['reservation']['id'] == key
    assert saved['link']['dataset_root'] == str(root.resolve())
    assert saved['link']['target'] == f'rank-0000/manifests/prepare-{SID}.json'
    b.close()


def test_checkpoint_paths_accept_root_external_dotdot_components(tmp_path):
    base = tmp_path / 'base'
    root = base / 'dataset'
    (base / 'other').mkdir(parents=True)
    root.mkdir()
    lexical_root = base / 'other' / '..' / 'dataset'
    b = DiskBudget(base / 'ledger.sqlite', [base], usage=lambda: 0, free=lambda: 10**9)
    key = b.reserve(2, checkpoint_link=make_link(lexical_root))
    saved = b.checkpoint_links(root)[0]
    assert saved['reservation']['id'] == key
    assert saved['link']['dataset_root'] == str(root.resolve())
    b.close()


def test_checkpoint_target_rejects_abspath_alias_to_different_directory(tmp_path):
    base = tmp_path / 'base'
    link_target = base / 'link-target'
    (link_target / 'inner').mkdir(parents=True)
    actual_root = link_target / 'dataset'
    actual_root.mkdir()
    decoy_root = base / 'dataset'
    decoy_root.mkdir()
    (base / 'alias').symlink_to(link_target / 'inner', target_is_directory=True)
    lexical_root = base / 'alias' / '..' / 'dataset'
    b = DiskBudget(base / 'ledger.sqlite', [base], usage=lambda: 0, free=lambda: 10**9)
    valid = b.reserve(2, checkpoint_link=make_link(lexical_root))
    assert b.checkpoint_links(actual_root)[0]['link']['dataset_root'] == str(actual_root.resolve())
    b.release(valid)
    link = make_link(lexical_root)
    link['target'] = str(decoy_root / 'rank-0000' / 'manifests' / f'prepare-{SID}.json')
    with pytest.raises(ValueError, match='outside dataset_root|does not resolve'):
        b.reserve(2, checkpoint_link=link)
    assert b.reserved_bytes == 0 and b.checkpoint_links(actual_root) == []
    b.close()


def test_optional_link_preserves_existing_reserve_api(tmp_path):
    b = make_budget(tmp_path)
    key = b.reserve(3)
    assert b.checkpoint_links(tmp_path) == []
    b.release(key)
    b.close()


def test_subprocess_exit_keeps_checkpoint_intent_for_reopened_cas(tmp_path):
    script = '''
import sys
from pathlib import Path
from svsim.disk_budget import DiskBudget
r=Path(sys.argv[1]); b=DiskBudget(r/'ledger.sqlite',[r],usage=lambda:0,free=lambda:10**9)
s='a'*32
link={'dataset_root':str(r),'dataset_id':'b'*32,'snapshot_id':s,'rank':0,'stage':'prepare',
'target':str(r/f'rank-0000/manifests/prepare-{s}.json'),
'temporary':str(r/'rank-0000'/'manifests'/('.prepare-'+s+'.json.'+'d'*32+'.tmp')),'digest':'c'*64}
b.reserve(9,checkpoint_link=link)
'''
    subprocess.run([sys.executable, '-c', script, str(tmp_path)], check=True,
                   env={**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1] / 'src')})
    b = make_budget(tmp_path)
    item = b.checkpoint_links(tmp_path)[0]
    b.reconcile_checkpoint(item['reservation']['id'], expected_reservation=item['reservation'],
                           expected_link=item['link'], materialized_bytes=9, evidence='reopened:verified')
    assert b.checkpoint_links(tmp_path)[0]['reservation']['state'] == 'RECONCILED'
    b.close()


def test_checkpoint_link_records_directory_identity_across_path_replacement(tmp_path):
    base = tmp_path / 'base'
    root = base / 'dataset'
    root.mkdir(parents=True)
    ledger = base / 'ledger.sqlite'
    b = DiskBudget(ledger, [root], usage=lambda: 0, free=lambda: 10**9)
    key = b.reserve(1, checkpoint_link=make_link(root))
    old_link = b.checkpoint_links(root)[0]['link']
    old_info = root.stat()
    assert old_link['dataset_device'] == old_info.st_dev
    assert old_link['dataset_inode'] == old_info.st_ino
    b.close()

    root.rename(base / 'retired-dataset')
    root.mkdir()
    b = DiskBudget(ledger, [root], usage=lambda: 0, free=lambda: 10**9)
    current = root.stat()
    assert (current.st_dev, current.st_ino) != (old_link['dataset_device'], old_link['dataset_inode'])
    # Same pathname and reservation context still carry the old directory generation.
    saved = b.checkpoint_links(root)[0]
    assert saved['reservation']['id'] == key
    assert (saved['link']['dataset_device'], saved['link']['dataset_inode']) == (
        old_link['dataset_device'], old_link['dataset_inode'])
    b.close()
