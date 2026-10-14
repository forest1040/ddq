import os
import stat
from pathlib import Path

import pytest

from svsim import distributed_checkpoint as checkpoint
from svsim.disk_budget import DiskAdmissionError, DiskBudget


class RejectingBudget:
    def reserve(self, amount, **kwargs):
        raise DiskAdmissionError('quota')


def _make_budget(tmp_path):
    return DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path], quota_bytes=100_000,
                      min_free_bytes=0, usage=lambda: 0, free=lambda: 1_000_000)


def test_reservation_covers_file_fsync_replace_directory_fsync_and_cleanup(tmp_path, monkeypatch):
    target = tmp_path / 'manifest.json'
    target.write_text('old\n')
    budget = _make_budget(tmp_path)
    original_fsync = os.fsync
    original_replace = os.replace
    original_unlink = Path.unlink
    events = []

    def live_row():
        rows=budget.db.execute("SELECT bytes,materialized,state FROM reservations WHERE operation='atomic-json'").fetchall()
        assert len(rows)==1
        amount,materialized,state=rows[0]
        assert amount>0 and materialized==len(b'{"ok":true}\n') and state=='WRITING'
        assert budget.reserved_bytes==0

    def checked_fsync(fd):
        live_row()
        mode = os.fstat(fd).st_mode
        events.append('directory-fsync' if stat.S_ISDIR(mode) else 'file-fsync')
        return original_fsync(fd)

    def checked_replace(src, dst):
        live_row()
        events.append('replace')
        return original_replace(src, dst)

    def checked_unlink(path, *args, **kwargs):
        if path.name.endswith('.tmp'):
            live_row()
            events.append('cleanup')
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(checkpoint.os, 'fsync', checked_fsync)
    monkeypatch.setattr(checkpoint.os, 'replace', checked_replace)
    monkeypatch.setattr(Path, 'unlink', checked_unlink)
    try:
        checkpoint.atomic_json(target, {'ok': True}, disk_budget=budget)
        assert target.read_text() == '{"ok":true}\n'
        assert budget.reserved_bytes == 0
        assert events == ['file-fsync', 'replace', 'directory-fsync', 'cleanup']
        assert list(tmp_path.glob('.manifest.json.*')) == []
    finally:
        budget.close()


def test_rejected_admission_leaves_destination_and_directory_unchanged(tmp_path):
    target = tmp_path / 'absent' / 'manifest.json'
    with pytest.raises(DiskAdmissionError):
        checkpoint.atomic_json(target, {'ok': True}, disk_budget=RejectingBudget())
    assert not target.parent.exists()


def test_temp_name_collision_preserves_other_file_and_releases_own_token(tmp_path,monkeypatch):
    target=tmp_path/'manifest.json'
    target.write_text('old contents\n')
    temporary=tmp_path/'.manifest.json.fixed.tmp'
    temporary.write_bytes(b'other writer data')
    budget=_make_budget(tmp_path)
    monkeypatch.setattr(checkpoint,'uuid4',lambda:type('Token',(),{'hex':'fixed'})())
    try:
        with pytest.raises(FileExistsError):
            checkpoint.atomic_json(target,{'ok':True},disk_budget=budget)
        assert target.read_text()=='old contents\n'
        assert temporary.read_bytes()==b'other writer data'
        assert budget.reserved_bytes==0
        assert budget.db.execute("SELECT count(*) FROM reservations WHERE operation='atomic-json'").fetchone()[0]==0
    finally:
        budget.close()


@pytest.mark.parametrize(('failure', 'expected'), [
    ('write', 'old contents\n'),
    ('rename', 'old contents\n'),
    ('directory-fsync', '{"ok":true}\n'),
])
def test_failures_keep_pre_or_post_rename_state_and_clean_temporary_files(tmp_path, monkeypatch, failure, expected):
    target = tmp_path / 'manifest.json'
    target.write_text('old contents\n')
    budget = _make_budget(tmp_path)
    original_fsync = os.fsync
    original_replace = os.replace

    if failure == 'write':
        original_open = open

        class ShortWriter:
            def __init__(self, stream): self.stream = stream
            def __enter__(self): return self
            def __exit__(self, *args): return self.stream.__exit__(*args)
            def write(self, raw): return max(0, len(raw) - 1)
            def flush(self): return self.stream.flush()
            def fileno(self): return self.stream.fileno()

        def short_open(path, mode='r', *args, **kwargs):
            stream = original_open(path, mode, *args, **kwargs)
            return ShortWriter(stream) if PathLike(path) and mode == 'xb' else stream

        monkeypatch.setattr('builtins.open', short_open)
    elif failure == 'rename':
        def failed_replace(src, dst):
            if os.fspath(dst) == os.fspath(target): raise OSError('rename fault')
            return original_replace(src, dst)
        monkeypatch.setattr(checkpoint.os, 'replace', failed_replace)
    else:
        def failed_directory_fsync(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode): raise OSError('directory fsync fault')
            return original_fsync(fd)
        monkeypatch.setattr(checkpoint.os, 'fsync', failed_directory_fsync)

    try:
        with pytest.raises(OSError):
            checkpoint.atomic_json(target, {'ok': True}, disk_budget=budget)
        # Before rename, the old cut remains authoritative. Once rename has
        # happened, a failed directory barrier leaves the new file in place;
        # its durability is uncertain and the caller must stop/recover.
        assert target.read_text() == expected
        assert budget.reserved_bytes == 0
        assert list(tmp_path.glob('.manifest.json.*')) == []
    finally:
        budget.close()


def test_failed_rename_and_failed_cleanup_keep_temp_and_live_token(tmp_path,monkeypatch):
    target=tmp_path/'manifest.json'
    target.write_text('old contents\n')
    budget=_make_budget(tmp_path)
    original_replace=os.replace
    original_unlink=Path.unlink

    def failed_replace(src,dst):
        if os.fspath(dst)==os.fspath(target):raise OSError('rename fault')
        return original_replace(src,dst)

    def failed_cleanup(path,*args,**kwargs):
        if path.name.endswith('.tmp') and path.exists():raise OSError('cleanup fault')
        return original_unlink(path,*args,**kwargs)

    monkeypatch.setattr(checkpoint.os,'replace',failed_replace)
    monkeypatch.setattr(Path,'unlink',failed_cleanup)
    try:
        with pytest.raises(OSError,match='cleanup fault'):
            checkpoint.atomic_json(target,{'ok':True},disk_budget=budget)
        assert target.read_text()=='old contents\n'
        temps=list(tmp_path.glob('.manifest.json.*.tmp'))
        assert len(temps)==1 and temps[0].read_bytes()==b'{"ok":true}\n'
        rows=budget.db.execute("SELECT bytes,materialized,state FROM reservations WHERE operation='atomic-json'").fetchall()
        assert len(rows)==1 and rows[0][0]>0 and rows[0][1]==rows[0][0] and rows[0][2]=='WRITING'
        assert budget.reserved_bytes==0
    finally:
        budget.close()


def PathLike(value):
    return isinstance(value, (str, bytes, os.PathLike))
