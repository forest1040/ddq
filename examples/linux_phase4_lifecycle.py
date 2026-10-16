"""Bounded Linux lifecycle delta suite; --plan never imports or executes tests.

Run through run_phase4_linux_lifecycle.sh after performance measurement finishes.
Source provenance covers src/tests/examples only. This is functional acceptance,
not a performance campaign or an io_uring capability claim.
"""
import argparse
from dataclasses import asdict,dataclass
from hashlib import sha256
import json
import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET


@dataclass(frozen=True)
class Stage:
    name: str
    backend: str
    seconds: int
    nodes: tuple[str,...]


CONTRACT=(
    'tests/test_catalog_file_identity.py',
    'tests/test_catalog_startup_recovery.py',
    'tests/test_bootstrap_namespace.py',
    'tests/test_runtime_budget_inheritance.py',
    'tests/test_checkpoint_recovery.py::test_standalone_shard_reader_prevents_recovery',
    'tests/test_checkpoint_recovery.py::test_standalone_shard_refuses_unproven_shared_ledger',
    'tests/test_checkpoint_recovery.py::test_missing_private_ledger_cannot_be_recreated_as_a_shard',
    'tests/test_checkpoint_recovery.py::test_standalone_shard_refuses_explicit_policy_conflict',
    'tests/test_checkpoint_recovery.py::test_checkpoint_writer_and_recovery_accept_root_external_symlink_parent_traversal',
    'tests/test_checkpoint_recovery.py::test_checkpoint_settlement_can_spend_precharged_pool_at_pending_quota_boundary',
    'tests/test_writer_namespace_admission.py',
    'tests/test_admitted_files.py::test_admitted_sidecars_cannot_escape_or_alias_the_charged_inventory',
    'tests/test_admitted_files.py::test_sidecar_parent_spelling_is_canonicalized_without_losing_inventory',
    'tests/test_trace_admission.py::test_real_process_exit_without_close_leaves_settleable_lease_tokens',
    'tests/test_migration_scratch_recovery.py',
)
PHYSICAL=(
    'tests/test_private_ledger_recovery.py',
    'tests/test_private_volume_coordination.py::test_independent_private_stores_share_one_volume_quota',
    'tests/test_private_volume_coordination.py::test_private_offline_recovery_republishes_bound_claim_and_resumes_oracle',
    'tests/test_private_volume_coordination.py::test_injected_coordinator_cannot_admit_a_store_on_another_real_volume',
    'tests/test_uncommitted_gc.py',
    'tests/test_segment_stage_kills.py',
    'tests/test_segment_power_loss.py',
    'tests/test_disk_quota_sweep.py::test_quota_boundary_is_sharp_and_refusal_precedes_any_mutation[extent]',
    'tests/test_disk_quota_sweep.py::test_mid_write_extent_failure_keeps_the_old_cut_and_releases_the_token',
)
STAGES=(Stage('linux-contracts','buffered',240,CONTRACT),
    Stage('native-vfs-fault-points','buffered',120,('tests/test_vfs_fault_points.py',)),
    *(Stage('physical-'+backend,backend,240,PHYSICAL) for backend in ('buffered','direct')),
    *(Stage('real-enospc-'+backend,backend,120,('tests/test_disk_real_enospc.py',)) for backend in ('buffered','direct')))
# A bounded inode-reuse search may not find an actual reused inode. All other
# skips (including VFS unavailability, missing tmpfs and ENOSPC opt-out) fail acceptance.
OPTIONAL_SKIP='test_reused_linux_inode_is_distinguished_by_birth_identity'
OPTIONAL_SKIP_REASON='filesystem did not reuse the catalog inode within the bounded probe'


def source_inventory(root):
    entries=[]
    for directory in ('src','tests','examples'):
        for path in sorted((root/directory).rglob('*')):
            if '__pycache__' in path.parts or path.suffix in ('.pyc','.pyo'):continue
            if path.is_symlink():raise RuntimeError('Source inventory contains a symlink: '+str(path))
            if path.is_file():entries.append((str(path.relative_to(root)),sha256(path.read_bytes()).hexdigest()))
    raw=json.dumps(entries,separators=(',',':')).encode()
    return {'sha256':sha256(raw).hexdigest(),'files':entries}


def direct_and_birth_probe(directory):
    """Require an actual O_DIRECT FD and aligned round trip; no fallback."""
    import fcntl
    import mmap
    from svsim.catalog_admission import catalog_birth_identity,get_sqlite_guard
    from svsim.transport import open_storage
    path=directory/'direct-probe.bin';data=mmap.mmap(-1,4096);data[:]=bytes(range(256))*16
    try:
        with open_storage(path,'w+b',backend='direct',alignment=4096) as handle:
            assert handle.backend_identity=='linux-o-direct'
            assert fcntl.fcntl(handle.fd,fcntl.F_GETFL)&os.O_DIRECT
            assert handle.write_from(0,data)==4096
            handle.flush();data[:]=bytes(4096)
            assert handle.read_into(0,data)==4096 and bytes(data)==bytes(range(256))*16
            birth=catalog_birth_identity(path,path.stat())
            if birth is None:raise RuntimeError('Linux filesystem exposes no usable catalog birth identity')
            return {'backend':handle.backend_identity,'o_direct_flag':True,'alignment':4096,
                    'birth_identity':birth,'vfs_capability':get_sqlite_guard().capability()}
    finally:data.close();path.unlink(missing_ok=True)


def read_junit(path):
    root=ET.parse(path).getroot();cases=list(root.iter('testcase'))
    if not cases:raise RuntimeError('No test cases collected')
    skipped=[{'name':case.get('name'),'reason':case.find('skipped').get('message','')}
             for case in cases if case.find('skipped') is not None]
    return {'cases':len(cases),'failed':sum(case.find('failure') is not None or case.find('error') is not None for case in cases),
            'skipped':skipped,'unexpected_skips':[case for case in skipped
                if case['name']!=OPTIONAL_SKIP or case['reason']!=OPTIONAL_SKIP_REASON]}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan',action='store_true')
    parser.add_argument('--output',type=Path,default=Path('/results'))
    parser.add_argument('--source-revision',default='unspecified')
    parser.add_argument('--deadline-seconds',type=int,default=1200)
    parser.add_argument('--stages',help='Comma-separated known stages; executed in canonical order')
    parser.add_argument('--stage-timeout-seconds',type=int,help='Override each selected stage limit (1..1200)')
    args=parser.parse_args(argv)
    if not 1<=args.deadline_seconds<=1200:parser.error('Deadline must be 1..1200 seconds')
    if args.stage_timeout_seconds is not None and not 1<=args.stage_timeout_seconds<=1200:
        parser.error('Stage timeout must be 1..1200 seconds')
    planned=[stage.name for stage in STAGES]
    requested=planned if args.stages is None else args.stages.split(',')
    if len(requested)!=len(set(requested)):parser.error('Duplicate stage names are not allowed')
    if any(name not in planned for name in requested):parser.error('Unknown or empty stage name')
    selected=tuple(stage for stage in STAGES if stage.name in requested)
    omitted=[name for name in planned if name not in requested]
    if args.plan:
        print(json.dumps({'stages':[{**asdict(stage),'seconds':args.stage_timeout_seconds or stage.seconds} for stage in selected],
            'planned_stages':planned,'selected_stages':[stage.name for stage in selected],
            'intentionally_omitted_stages':omitted,'not_run':[stage.name for stage in selected],
            'global_deadline_seconds':args.deadline_seconds,'physical_full_fs':'/full (dedicated 16 MiB tmpfs)',
            'excluded':['performance campaigns','native io_uring capability','full Mac fast suite'],
            'optional_skip':OPTIONAL_SKIP},indent=2));return 0
    if not sys.platform.startswith('linux') or os.geteuid()==0:
        parser.error('Execution requires Linux and a nonroot user; use --plan for read-only inspection')
    if socket.gethostname()!='ddq-phase4-lifecycle':parser.error('Unexpected hostname')
    if args.source_revision=='unspecified':parser.error('An explicit supplied source revision is required')
    full=Path('/full')
    if os.environ.get('SVSIM_FULL_FS')!=str(full) or not full.is_dir() or any(full.iterdir()):
        parser.error('SVSIM_FULL_FS must be an empty dedicated /full mount')
    mounts=Path('/proc/self/mountinfo').read_text().splitlines()
    if not any(row.split()[4]=='/full' and ' - tmpfs ' in row for row in mounts):
        parser.error('/full is not a dedicated tmpfs mount')
    st=os.statvfs(full)
    if st.f_blocks*st.f_frsize!=16*1024**2:parser.error('/full must be exactly 16 MiB')
    interfaces=sorted(path.name for path in Path('/sys/class/net').iterdir())
    if interfaces!=['lo']:parser.error('Expected only loopback in the network-none container')
    args.output.mkdir(parents=True,exist_ok=True)
    result=args.output/'summary.json'
    if result.exists():parser.error('Output already contains summary.json; use a fresh output directory')
    root=Path(__file__).resolve().parents[1]
    report={'schema_version':1,'supplied_source_revision':args.source_revision,
        'source':source_inventory(root),'platform':platform.platform(),'hostname':socket.gethostname(),
        'uid':os.geteuid(),'gid':os.getegid(),'python':sys.version,'image':os.environ.get('DDQ_IMAGE'),
        'image_id':os.environ.get('DDQ_IMAGE_ID'),'deadline_seconds':args.deadline_seconds,
        'stage_timeout_seconds':args.stage_timeout_seconds,'planned_stages':planned,
        'selected_stages':[stage.name for stage in selected],'intentionally_omitted_stages':omitted,
        'container_contract':{'cpus':4,'memory_bytes':8*1024**3,'network':'none','interfaces':interfaces,
            'cgroup_observed':{name:Path('/sys/fs/cgroup',name).read_text().strip()
                for name in ('cpu.max','memory.max','memory.swap.max') if Path('/sys/fs/cgroup',name).is_file()}},
        'capacity_claim':'observed software admission; strict_capacity_guarantee=false',
        'power_loss_claim':'segment model and native SQLite fault points, not physical power loss',
        'uring_claim':'not exercised','stages':[],'status':'running'}
    def save():result.write_text(json.dumps(report,indent=2)+'\n')
    save();started=time.monotonic()
    env={**os.environ,'PYTHONPATH':str(root/'src'),'OMP_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1',
        'MKL_NUM_THREADS':'1','VECLIB_MAXIMUM_THREADS':'1','PYTHONDONTWRITEBYTECODE':'1','PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1'}
    # This opt-in is solely the SQLite test VFS. io_uring remains unrequested.
    env.pop('DDQ_NATIVE_URING',None)
    try:
        report['linux_probe']=direct_and_birth_probe(args.output);save()
        for stage in selected:
            remaining=args.deadline_seconds-(time.monotonic()-started)
            if remaining<=0:raise TimeoutError('Global deadline exhausted before '+stage.name)
            junit=args.output/(stage.name+'.xml');log=args.output/(stage.name+'.log')
            command=[sys.executable,'-m','pytest','-q','-r','a','-p','no:cacheprovider',
                '--junitxml='+str(junit),*stage.nodes]
            stage_env={**env,'SVSIM_TEST_BACKEND':stage.backend,'SVSIM_FULL_FS':'/full'}
            if stage.name=='native-vfs-fault-points':stage_env['SVSIM_VFS_TEST_FAULTS']='1'
            else:stage_env.pop('SVSIM_VFS_TEST_FAULTS',None)
            row={'name':stage.name,'backend_env':stage.backend,'command':command,'status':'running',
                 'timeout_seconds':min(args.stage_timeout_seconds or stage.seconds,remaining),'log':log.name,'junit':junit.name}
            report['stages'].append(row);save();tick=time.monotonic()
            print(json.dumps({'stage':stage.name,'status':'running'}),flush=True)
            with log.open('w') as output:
                process=subprocess.Popen(command,cwd=root,env=stage_env,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
                try:row['exit_code']=process.wait(timeout=row['timeout_seconds'])
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid,signal.SIGKILL);process.wait();row['status']='timeout';raise
            row['seconds']=time.monotonic()-tick
            row['tests']=read_junit(junit)
            row['status']=('passed' if row['exit_code']==0 and not row['tests']['failed'] and not row['tests']['unexpected_skips'] else 'failed')
            save();print(json.dumps(row),flush=True)
            if row['status']!='passed':raise RuntimeError('Stage failed: '+stage.name)
        report['status']='passed'
    except Exception as exc:
        report['status']='failed';report['error']=repr(exc)
        if report['stages'] and report['stages'][-1]['status']=='running':
            report['stages'][-1].update(status='failed',error=repr(exc))
    finally:
        report['seconds']=time.monotonic()-started
        report['not_run']=[stage.name for stage in selected if stage.name not in {row['name'] for row in report['stages']}]
        report['optional_skips']=[item for row in report['stages'] for item in row.get('tests',{}).get('skipped',[])]
        save()
    print(json.dumps({'status':report['status'],'summary':str(result),'seconds':report['seconds']}),flush=True)
    return 0 if report['status']=='passed' else 1


if __name__=='__main__':raise SystemExit(main())
