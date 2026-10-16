"""State exports keep old outputs intact and account streamed generations."""
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys

import numpy as np
import pytest

from svsim import StoredSimulator, StorageConfig
from svsim.disk_budget import DiskAdmissionError
from svsim.export_admission import export_inventory_roots
from svsim import state_export


def config(path):
    return StorageConfig(str(path),2,alignment=64,ram_budget_bytes=98304,metadata_reserve_bytes=32768)


def test_stored_export_registers_existing_output_and_streams_atomic_replacement(tmp_path):
    output=tmp_path/'out'/'logical.bin';output.parent.mkdir();output.write_bytes(b'old')
    with StoredSimulator(8,config(tmp_path/'store')) as sim:
        sim.gate('H',(0,));sim.gate('RY',(7,),angle=.3);sim.materialize()
        expected=sim.state();budget=sim.store.disk_budget
        initial=budget._observed_usage()
        assert sim.export_state(output)==output
        np.testing.assert_allclose(np.fromfile(output,dtype='<c16'),expected)
        assert budget._observed_usage()>=initial+output.stat().st_size
        roots=export_inventory_roots(budget.db)
        assert roots==[{'path':str(output.parent),'device':output.parent.stat().st_dev,'inode':output.parent.stat().st_ino,'birth':roots[0]['birth']}]
        assert not budget.live_reservations(linked=True)
        assert not list(output.parent.glob('*.tmp'))
        sim.export_state(output,order='physical')
        assert output.stat().st_size==(1<<8)*16


def test_stored_export_quota_refuses_before_materialization_or_output_mutation(tmp_path,monkeypatch):
    output=tmp_path/'out.bin';output.write_bytes(b'old-output')
    with StoredSimulator(4,config(tmp_path/'store')) as sim:
        sim.materialize();budget=sim.store.disk_budget;budget.register_export_root(tmp_path)
        budget.quota_bytes=budget._observed_usage()+budget.bootstrap_bytes+budget.reserved_bytes+255
        monkeypatch.setattr(sim,'materialize',lambda **kw:pytest.fail('materialized before export admission'))
        with pytest.raises(DiskAdmissionError,match='quota'):sim.export_state(output)
        assert output.read_bytes()==b'old-output'
        assert not list(tmp_path.glob('*.tmp'))
        assert not budget.live_reservations(linked=True)


@pytest.mark.parametrize('fault',['short-write','write-error','rename-error'])
def test_stored_export_failed_write_keeps_old_output_and_cleans_reserved_temporary(tmp_path,monkeypatch,fault):
    output=tmp_path/'out.bin';output.write_bytes(b'old-output')
    with StoredSimulator(4,config(tmp_path/'store')) as sim:
        sim.gate('H',(0,));sim.materialize();budget=sim.store.disk_budget
        if fault=='rename-error':
            monkeypatch.setattr(state_export.os,'replace',lambda *a:(_ for _ in ()).throw(OSError('rename-error')))
        else:
            def fail(stream,data):
                if fault=='short-write':stream.write(data[:-1])
                raise OSError(fault)
            monkeypatch.setattr(state_export,'write_exact',fail)
        with pytest.raises(OSError,match=fault):sim.export_state(output)
        assert output.read_bytes()==b'old-output'
        assert not list(tmp_path.glob('*.tmp'))
        assert not budget.live_reservations(linked=True)


def test_export_detects_a_short_stream_write():
    class Short:
        def write(self,data):return len(data)-1
    with pytest.raises(OSError,match='Incomplete'):state_export.write_exact(Short(),b'1234')


def test_stored_export_failed_directory_fsync_keeps_intent_for_explicit_recovery(tmp_path,monkeypatch):
    output=tmp_path/'out.bin';output.write_bytes(b'old-output')
    with StoredSimulator(4,config(tmp_path/'store')) as sim:
        sim.gate('H',(0,));sim.materialize()
        monkeypatch.setattr(state_export,'fsync_directory',lambda p:(_ for _ in ()).throw(OSError('directory-fsync')))
        with pytest.raises(OSError,match='directory-fsync'):sim.export_state(output)
        tokens=sim.store.disk_budget.live_reservations(linked=True)
        assert len(tokens)==1
        assert sim.store.disk_budget.checkpoint_link(tokens[0]['id'])['purpose']=='state-export'
        assert output.stat().st_size==256
        assert tokens[0]['materialized_bytes']==256


def test_stored_export_rejects_a_different_volume_before_mutation(tmp_path,monkeypatch):
    from svsim.export_admission import prepare_export
    output=tmp_path/'out.bin';output.write_bytes(b'old')
    with StoredSimulator(4,config(tmp_path/'store')) as sim:
        budget=sim.store.disk_budget
        # Model a managed source volume distinct from the real destination.
        class OtherVolume:
            def stat(self):return type('Info',(),{'st_dev':tmp_path.stat().st_dev+1})()
        monkeypatch.setattr(budget,'roots',(OtherVolume(),))
        with pytest.raises(DiskAdmissionError,match='volume'):prepare_export(budget,output)
        assert output.read_bytes()==b'old'
        assert not export_inventory_roots(budget.db)
        monkeypatch.undo()


@pytest.mark.parametrize('stage',['write','rename'])
def test_killed_stored_export_retains_named_intent_and_whole_old_or_new_output(tmp_path,stage):
    output=tmp_path/'out.bin';output.write_bytes(b'old')
    script=r'''
import os,signal,sys
from pathlib import Path
from svsim import StoredSimulator,StorageConfig
from svsim import state_export
root=Path(sys.argv[1]);stage=sys.argv[2]
cfg=StorageConfig(str(root/'store'),2,alignment=64,ram_budget_bytes=98304,metadata_reserve_bytes=32768)
with StoredSimulator(4,cfg) as sim:
 sim.gate('H',(0,));sim.materialize();sim.commit_snapshot()
 if stage=='write':
  original=state_export.write_exact
  def kill(stream,data):
   original(stream,data);os.kill(os.getpid(),signal.SIGKILL)
  state_export.write_exact=kill
 else:
  original=state_export.os.replace
  def kill(a,b):
   original(a,b);os.kill(os.getpid(),signal.SIGKILL)
  state_export.os.replace=kill
 sim.export_state(root/'out.bin')
'''
    proc=subprocess.run([sys.executable,'-c',script,str(tmp_path),stage],env={**os.environ,'PYTHONPATH':'src'},
                        capture_output=True,timeout=60)
    assert proc.returncode==-signal.SIGKILL,proc.stderr.decode()
    ledger=tmp_path/'store'/'metadata'/'disk-ledger.sqlite3'
    con=sqlite3.connect(ledger)
    try:
        rows=con.execute("SELECT r.operation,r.state,c.link FROM reservations r JOIN checkpoint_reservations c USING(id) WHERE r.operation='state-export'").fetchall()
        assert len(rows)==1 and rows[0][:2]==('state-export','WRITING')
        intent=json.loads(rows[0][2]);assert intent['purpose']=='state-export'
        assert intent['expected_bytes']==256 and intent['export_root']==str(tmp_path)
    finally:con.close()
    if stage=='write':assert output.read_bytes()==b'old'
    else:
        expected=np.zeros(16,dtype='<c16');expected[:2]=1/np.sqrt(2)
        np.testing.assert_allclose(np.fromfile(output,dtype='<c16'),expected)
    from svsim.uncommitted_recovery import recover_uncommitted_reservations
    before=output.read_bytes()
    dry=recover_uncommitted_reservations(ledger.resolve())
    assert not dry['refusals'] and any(t['class']=='export' for t in dry['tokens'])
    report=recover_uncommitted_reservations(ledger.resolve(),apply=True,confirm_plan=dry['plan_sha256'])
    assert report['status']=='applied' and output.read_bytes()==before
    again=recover_uncommitted_reservations(ledger.resolve())
    assert again['status']=='nothing-to-do'


def distributed(path,comm=None):
    from mpi4py import MPI
    from svsim import DistributedSimulator,DistributedConfig
    return DistributedSimulator(4,DistributedConfig(str(path),2,ram_budget_bytes_per_rank=8*1024**2,
        metadata_reserve_bytes=128*1024,tile_bytes=4096,frame_policy='materialize',storage_scope='node-local'),
        comm=MPI.COMM_SELF if comm is None else comm)


def test_distributed_export_publishes_shards_and_manifest_as_one_generation(tmp_path):
    with distributed(tmp_path/'store') as sim:
        sim.gate('H',(3,));sim.gate('RY',(0,),angle=.3)
        output=tmp_path/'export'
        sim.export_state(output)
        expected=sim.state(allow_gather=True)
        np.testing.assert_allclose(np.fromfile(output/'rank-0000.bin',dtype='<c16'),expected)
        old=json.loads((output/'manifest.json').read_text())
        sim.gate('X',(2,));sim.export_state(output)
        np.testing.assert_allclose(np.fromfile(output/'rank-0000.bin',dtype='<c16'),sim.state(allow_gather=True))
        assert json.loads((output/'manifest.json').read_text())['root']!=old['root']
        assert not list(tmp_path.glob('*.tmp'))
        assert not sim.disk_budget.live_reservations(linked=True)
        assert sim.budget.peak<=sim.config.ram_budget_bytes_per_rank


@pytest.mark.parametrize('fault',['write','exchange'])
def test_distributed_export_failure_preserves_old_directory(tmp_path,monkeypatch,fault):
    with distributed(tmp_path/'store') as sim:
        output=tmp_path/'export';sim.export_state(output)
        before={p.name:p.read_bytes() for p in output.iterdir()}
        sim.gate('H',(0,));sim.materialize()
        def fail(*args):raise OSError('injected-export-failure')
        monkeypatch.setattr(state_export,'write_exact' if fault=='write' else 'exchange_directories',fail)
        with pytest.raises(Exception,match='injected-export-failure'):sim.export_state(output)
        assert {p.name:p.read_bytes() for p in output.iterdir()}==before
        assert not sim.disk_budget.live_reservations(linked=True)
        assert not list(tmp_path.glob('*.tmp'))


def test_distributed_export_admits_manifest_and_shard_before_directory_creation(tmp_path,monkeypatch):
    with distributed(tmp_path/'store') as sim:
        budget=sim.disk_budget;output=tmp_path/'export'
        original=budget.reserve;admitted=[]
        def reserve(amount,**kw):
            if kw.get('operation')=='state-export-manifest':raise DiskAdmissionError('manifest quota')
            key=original(amount,**kw)
            if kw.get('operation','').startswith('state-export'):admitted.append(key)
            return key
        monkeypatch.setattr(budget,'reserve',reserve)
        with pytest.raises(Exception,match='manifest quota'):sim.export_state(output)
        assert admitted and not output.exists() and not list(tmp_path.glob('*.tmp'))
        assert not budget.live_reservations(linked=True)



def test_distributed_export_refuses_unrelated_directory_contents(tmp_path):
    output=tmp_path/'out';output.mkdir();(output/'user.txt').write_text('keep')
    with distributed(tmp_path/'store') as sim:
        with pytest.raises(Exception,match='exclusive state export'):sim.export_state(output)
        assert (output/'user.txt').read_text()=='keep'
        assert not sim.disk_budget.live_reservations(linked=True)


def test_export_keeps_reservation_through_directory_fsync(tmp_path,monkeypatch):
    with StoredSimulator(4,config(tmp_path/'store')) as sim:
        sim.materialize();budget=sim.store.disk_budget;observed=[]
        original=state_export.fsync_directory
        def sync(path):
            tokens=budget.live_reservations(linked=True)
            observed.append(tokens)
            assert len(tokens)==1 and tokens[0]['operation']=='state-export'
            original(path)
        monkeypatch.setattr(state_export,'fsync_directory',sync)
        sim.export_state(tmp_path/'out.bin')
        assert observed and not budget.live_reservations(linked=True)


@pytest.mark.skipif(__import__('shutil').which('mpiexec') is None,reason='MPI launcher unavailable')
def test_mpi_export_atomic_generation_and_partial_rank_admission_refusal(tmp_path):
    import shutil
    script=r'''
import sys
from pathlib import Path
import numpy as np
from mpi4py import MPI
from svsim import DistributedSimulator,DistributedConfig
from svsim.disk_budget import DiskAdmissionError
root=Path(sys.argv[1]);comm=MPI.COMM_WORLD
cfg=DistributedConfig(str(root/'store'),2,ram_budget_bytes_per_rank=8*1024**2,
 metadata_reserve_bytes=128*1024,tile_bytes=4096,frame_policy='materialize',storage_scope='node-local')
with DistributedSimulator(4,cfg,comm=comm) as sim:
 # Delay a real peer's ledger release: a collective export must not return
 # while that peer still owns live export tokens, including on refusal.
 import time
 original_release=sim.disk_budget.release
 def delayed_release(key):
  if sim.rank==0 and sim.disk_budget.reservation(key)['operation'].startswith('state-export'):
   time.sleep(.05)
  return original_release(key)
 sim.disk_budget.release=delayed_release
 sim.gate('H',(3,));sim.gate('RY',(0,),angle=.3)
 output=root/'out';sim.export_state(output)
 expected=sim.state(allow_gather=True)
 if sim.rank==0:
  actual=np.concatenate([np.fromfile(output/f'rank-{r:04d}.bin',dtype='<c16') for r in range(sim.size)])
  np.testing.assert_allclose(actual,expected)
 before={p.name:p.read_bytes() for p in output.iterdir()}
 original=sim.disk_budget.reserve
 def reserve(amount,**kw):
  if sim.rank==1 and kw.get('operation')=='state-export-shard':raise DiskAdmissionError('rank-one-quota')
  return original(amount,**kw)
 sim.disk_budget.reserve=reserve
 try:sim.export_state(output)
 except Exception as exc:assert 'rank-one-quota' in str(exc)
 else:raise AssertionError('rank-local quota was not collectively refused')
 sim.disk_budget.reserve=original
 assert {p.name:p.read_bytes() for p in output.iterdir()}==before
 assert not sim.disk_budget.live_reservations(linked=True)
 sim.gate('X',(2,));sim.export_state(output)
 expected=sim.state(allow_gather=True)
 if sim.rank==0:
  actual=np.concatenate([np.fromfile(output/f'rank-{r:04d}.bin',dtype='<c16') for r in range(sim.size)])
  np.testing.assert_allclose(actual,expected)
 assert not list(root.glob('*.tmp'))
'''
    result=subprocess.run([shutil.which('mpiexec'),'-n','2',sys.executable,'-c',script,str(tmp_path)],
        env={**os.environ,'PYTHONPATH':'src','OMP_NUM_THREADS':'1'},capture_output=True,timeout=120)
    assert result.returncode==0,result.stdout.decode()+result.stderr.decode()


@pytest.mark.parametrize('stage',['shard','manifest','exchange'])
def test_killed_distributed_export_recovers_purpose_without_deleting_old_or_new_generation(tmp_path,stage):
    script=r'''
import os,signal,sys
from pathlib import Path
from mpi4py import MPI
from svsim import DistributedSimulator,DistributedConfig
from svsim import state_export
root=Path(sys.argv[1]);stage=sys.argv[2]
cfg=DistributedConfig(str(root/'store'),2,ram_budget_bytes_per_rank=8*1024**2,
 metadata_reserve_bytes=128*1024,tile_bytes=4096,frame_policy='materialize',storage_scope='node-local')
with DistributedSimulator(4,cfg,comm=MPI.COMM_SELF) as sim:
 sim.gate('H',(0,));sim.materialize();sim.commit_snapshot();sim.export_state(root/'out')
 (root/'before.bin').write_bytes((root/'out/rank-0000.bin').read_bytes())
 sim.gate('X',(2,));sim.materialize();sim.commit_snapshot()
 print(sim.disk_budget._ledger_path,flush=True)
 if stage=='exchange':
  original=state_export.exchange_directories
  def kill(a,b):
   original(a,b);os.kill(os.getpid(),signal.SIGKILL)
  state_export.exchange_directories=kill
 else:
  original=state_export.write_exact
  def kill(stream,data):
   original(stream,data)
   if (stage=='shard' and Path(stream.name).name.startswith('rank-')) or (stage=='manifest' and Path(stream.name).name=='manifest.json'):
    os.kill(os.getpid(),signal.SIGKILL)
  state_export.write_exact=kill
 sim.export_state(root/'out')
'''
    result=subprocess.run([sys.executable,'-c',script,str(tmp_path),stage],env={**os.environ,'PYTHONPATH':'src'},
                          capture_output=True,timeout=90)
    assert result.returncode==-signal.SIGKILL,result.stderr.decode()
    ledger=Path(result.stdout.decode().strip().splitlines()[-1])
    output=tmp_path/'out'
    before={p.name:p.read_bytes() for p in output.iterdir()}
    if stage!='exchange':assert before['rank-0000.bin']==(tmp_path/'before.bin').read_bytes()
    else:
        expected=np.zeros(16,dtype='<c16');expected[4:6]=1/np.sqrt(2)
        np.testing.assert_allclose(np.fromfile(output/'rank-0000.bin',dtype='<c16'),expected)
    from svsim.uncommitted_recovery import recover_uncommitted_reservations
    dry=recover_uncommitted_reservations(ledger)
    assert not dry['refusals'],dry['refusals']
    exports=[t for t in dry['tokens'] if t['class']=='export']
    assert {t['operation'] for t in exports}=={'state-export-shard','state-export-manifest'}
    report=recover_uncommitted_reservations(ledger,apply=True,confirm_plan=dry['plan_sha256'])
    assert report['status']=='applied'
    assert {p.name:p.read_bytes() for p in output.iterdir()}==before
    assert recover_uncommitted_reservations(ledger)['status']=='nothing-to-do'


def test_export_inventory_rejects_birth_identity_change_even_if_device_inode_match(tmp_path,monkeypatch):
    import svsim.export_admission as admission
    with StoredSimulator(4,config(tmp_path/'store')) as sim:
        budget=sim.store.disk_budget;budget.register_export_root(tmp_path)
        old=admission.catalog_birth_identity
        monkeypatch.setattr(admission,'catalog_birth_identity',lambda p,s:'changed-birth' if Path(p)==tmp_path else old(p,s))
        with pytest.raises(DiskAdmissionError,match='identity changed'):export_inventory_roots(budget.db)
        monkeypatch.undo()


def test_file_fsync_failure_preserves_old_output_and_releases_after_durable_cleanup(tmp_path,monkeypatch):
    with StoredSimulator(4,config(tmp_path/'store')) as sim:
        sim.materialize();output=tmp_path/'out.bin';output.write_bytes(b'old')
        original=state_export.os.fsync
        def sync(fd):
            import stat
            if stat.S_ISREG(os.fstat(fd).st_mode):raise OSError('file-fsync-failed')
            original(fd)
        monkeypatch.setattr(state_export.os,'fsync',sync)
        with pytest.raises(OSError,match='file-fsync-failed'):sim.export_state(output)
        assert output.read_bytes()==b'old'
        assert not sim.store.disk_budget.live_reservations(linked=True)
        monkeypatch.undo()


def test_failed_export_cleanup_keeps_temporary_and_recovery_intent(tmp_path,monkeypatch):
    with StoredSimulator(4,config(tmp_path/'store')) as sim:
        sim.materialize();output=tmp_path/'out.bin';output.write_bytes(b'old')
        original=Path.unlink
        def unlink(path,*args,**kwargs):
            if path.name.endswith('.tmp'):raise OSError('cleanup-failed')
            return original(path,*args,**kwargs)
        def write(stream,data):
            stream.write(data[:1]);raise OSError('write-failed')
        monkeypatch.setattr(state_export,'write_exact',write);monkeypatch.setattr(Path,'unlink',unlink)
        with pytest.raises(OSError,match='cleanup-failed'):sim.export_state(output)
        assert output.read_bytes()==b'old'
        tokens=sim.store.disk_budget.live_reservations(linked=True)
        assert len(tokens)==1
        intent=sim.store.disk_budget.checkpoint_link(tokens[0]['id'])
        assert (Path(intent['export_root'])/intent['temporary']).exists()
        monkeypatch.undo()
