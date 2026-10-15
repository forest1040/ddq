"""Persisted inventory and owner-dead recovery evidence for state exports."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import stat

from .dataset_lease import DirectoryLease
from .catalog_admission import catalog_birth_identity
from .disk_budget import DiskAdmissionError

ROOT_SCHEMA='CREATE TABLE export_roots(path TEXT PRIMARY KEY, device INTEGER NOT NULL, inode INTEGER NOT NULL, birth TEXT NOT NULL)'
OPERATIONS={'file':'state-export','shard':'state-export-shard','manifest':'state-export-manifest'}


def export_inventory_roots(con):
    row=con.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='export_roots'").fetchone()
    if row is None:return []
    if re.sub(r'\s+','',row[0].lower())!=re.sub(r'\s+','',ROOT_SCHEMA.lower()):
        raise DiskAdmissionError('Export inventory schema changed')
    if con.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='export_roots'").fetchone():
        raise DiskAdmissionError('Export inventory trigger schema changed')
    rows=con.execute('SELECT path,device,inode,birth FROM export_roots ORDER BY path LIMIT 257').fetchall()
    if len(rows)>256:raise DiskAdmissionError('Export root limit exceeded')
    result=[]
    for path,device,inode,birth in rows:
        root=Path(path)
        if not root.is_absolute() or root.is_symlink() or root.resolve()!=root:
            raise DiskAdmissionError('Export root is aliased')
        info=root.stat()
        if (not stat.S_ISDIR(info.st_mode) or (info.st_dev,info.st_ino)!=(device,inode)
                or catalog_birth_identity(root,info)!=birth):
            raise DiskAdmissionError('Export root identity changed')
        result.append({'path':path,'device':device,'inode':inode,'birth':birth})
    return result


def register_export_root(budget,path):
    budget._require_writer()
    root=Path(path).resolve(strict=True)
    info=root.stat()
    if not root.is_dir() or info.st_dev!=budget.roots[0].stat().st_dev:
        raise DiskAdmissionError('State export must stay on the managed storage volume')
    birth=catalog_birth_identity(root,info)
    if birth is None:raise DiskAdmissionError('Export root birth identity is unavailable')
    with budget._ledger_transaction():
        budget.db.execute(ROOT_SCHEMA.replace('CREATE TABLE','CREATE TABLE IF NOT EXISTS',1))
        rows=export_inventory_roots(budget.db)
        if str(root) not in {r['path'] for r in rows}:
            if len(rows)>=256:raise DiskAdmissionError('Export root limit exceeded')
            budget.db.execute('INSERT INTO export_roots VALUES(?,?,?,?)',(str(root),info.st_dev,info.st_ino,birth))
    return root


def prepare_export(budget,path):
    """Bind the existing output ancestor before any output or state mutation."""
    target=Path(os.path.abspath(path))
    if target.is_symlink():raise DiskAdmissionError('State export target cannot be a symlink')
    target=target.parent.resolve()/target.name
    ancestor=target.parent
    while not ancestor.exists():ancestor=ancestor.parent
    if ancestor.stat().st_dev!=budget.roots[0].stat().st_dev:
        raise DiskAdmissionError('State export must stay on the managed storage volume')
    if target.exists() and target.stat().st_dev!=ancestor.stat().st_dev:
        raise DiskAdmissionError('State export target is on another volume')
    root=register_export_root(budget,ancestor)
    return target,root


def intent_for(dataset,root,target,temporary,kind):
    dataset=Path(dataset).resolve(strict=True);root=Path(root).resolve(strict=True)
    di,ri=dataset.stat(),root.stat()
    dbirth,rbirth=catalog_birth_identity(dataset,di),catalog_birth_identity(root,ri)
    if dbirth is None or rbirth is None:raise DiskAdmissionError('Export birth identity is unavailable')
    return {'purpose':'state-export','dataset_root':str(dataset),'dataset_device':di.st_dev,'dataset_inode':di.st_ino,'dataset_birth':dbirth,
            'export_root':str(root),'export_device':ri.st_dev,'export_inode':ri.st_ino,'export_birth':rbirth,
            'target':str(Path(target).relative_to(root)),'temporary':str(Path(temporary).relative_to(root)),'kind':kind}


def normalize_intent(budget,link,amount):
    keys={'purpose','dataset_root','dataset_device','dataset_inode','export_root','export_device','export_inode',
          'target','temporary','kind','dataset_birth','export_birth'}
    if set(link)!=keys or link['kind'] not in OPERATIONS:raise ValueError('Invalid export intent')
    roots=export_inventory_roots(budget.db)
    if {'path':link['export_root'],'device':link['export_device'],'inode':link['export_inode'],'birth':link['export_birth']} not in roots:
        raise DiskAdmissionError('Export intent root is not registered')
    root=Path(link['export_root']);dataset=Path(link['dataset_root'])
    info=dataset.stat()
    if dataset.resolve()!=dataset or not dataset.is_dir() or (info.st_dev,info.st_ino)!=(link['dataset_device'],link['dataset_inode']) or catalog_birth_identity(dataset,info)!=link['dataset_birth']:
        raise DiskAdmissionError('Export dataset identity changed')
    registered=[Path(p) for p, in budget.db.execute('SELECT path FROM roots')]
    if not any(dataset.is_relative_to(p) for p in registered):raise DiskAdmissionError('Export dataset is unmanaged')
    if info.st_dev!=root.stat().st_dev:raise DiskAdmissionError('Export spans volumes')
    for name in ('target','temporary'):
        path=Path(link[name])
        if path.is_absolute() or '..' in path.parts or not path.parts:raise ValueError('Invalid export intent path')
        full=root/path
        if full.is_symlink() or full.parent.resolve()!=full.parent:raise DiskAdmissionError('Export path is aliased')
        if full.exists() and (not full.is_file() or full.stat().st_nlink!=1 or full.stat().st_dev!=info.st_dev):
            raise DiskAdmissionError('Export target is not an exclusive regular file')
    if (root/link['temporary']).exists():raise DiskAdmissionError('Export temporary already exists')
    if link['target']==link['temporary'] or not re.search(r'\.[0-9a-f]{32}\.tmp(?:/|$)',link['temporary']):
        raise ValueError('Invalid export temporary generation')
    encoded=json.dumps({**link,'expected_bytes':amount},sort_keys=True,separators=(',',':'))
    if len(encoded.encode())>4096:raise ValueError('Export intent exceeds metadata limit')
    return encoded


def _plan_recovery(token,intent,scope):
    required={'purpose','dataset_root','dataset_device','dataset_inode','dataset_birth','export_root',
              'export_device','export_inode','export_birth','target','temporary','kind','expected_bytes'}
    if set(intent)!=required:raise DiskAdmissionError('Export intent schema changed')
    if re.fullmatch(r'[0-9a-f]{32}',token.get('owner') or '') is None:
        raise DiskAdmissionError('Export owner is unbound')
    if intent.get('purpose')!='state-export' or token['operation']!=OPERATIONS.get(intent.get('kind')):
        raise DiskAdmissionError('Export purpose mismatch')
    if intent.get('expected_bytes')!=token['bytes']:raise DiskAdmissionError('Export size mismatch')
    rows=export_inventory_roots(scope['budget'].db)
    if {'path':intent['export_root'],'device':intent['export_device'],'inode':intent['export_inode'],'birth':intent['export_birth']} not in rows:
        raise DiskAdmissionError('Export root is unbound')
    generation=token.get('generation')
    if re.fullmatch(r'[0-9a-f]{32}',generation or '') is None or not re.search(r'\.'+re.escape(generation)+r'\.tmp(?:/|$)',intent['temporary']):
        raise DiskAdmissionError('Export writer generation is unbound')
    dataset=Path(intent['dataset_root']);info=dataset.stat()
    if (dataset not in scope['stores'] or (info.st_dev,info.st_ino)!=(intent['dataset_device'],intent['dataset_inode'])
            or catalog_birth_identity(dataset,info)!=intent['dataset_birth']):
        raise DiskAdmissionError('Export dataset is unbound')
    leased={Path(l.path) for l in scope['leases']} | {Path(scope['ledger_lease'].path)}
    if Path(intent['export_root']) not in leased:raise DiskAdmissionError('Export inventory is not leased')
    if info.st_dev!=intent['export_device']:raise DiskAdmissionError('Export spans volumes')
    artifacts=[]
    for key in ('target','temporary'):
        relative=Path(intent[key]);path=Path(intent['export_root'])/relative
        if relative.is_absolute() or '..' in relative.parts or path.parent.resolve()!=path.parent:
            raise DiskAdmissionError('Export artifact path is aliased')
        try:info=path.lstat()
        except FileNotFoundError:continue
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_dev!=intent['export_device']:
            raise DiskAdmissionError('Export artifact is not an exclusive regular file')
        if key=='temporary' and info.st_size>token['bytes']:
            # After a whole directory exchange this may be an older generation;
            # it is preserved and charged, not treated as newly written bytes.
            if intent['kind']=='file':raise DiskAdmissionError('Export temporary exceeds reservation')
        birth=catalog_birth_identity(path,info)
        if birth is None:raise DiskAdmissionError('Export artifact birth identity is unavailable')
        artifacts.append({'path':str(path),'device':info.st_dev,'inode':info.st_ino,'birth':birth,'bytes':info.st_size})
    return {'class':'export','attribution':{'intent':intent,'artifacts':artifacts,'artifacts_preserved':True}}


def plan_recovery(token,intent,scope):
    try:return _plan_recovery(token,intent,scope)
    except (KeyError,TypeError,ValueError,OSError) as exc:
        if isinstance(exc,DiskAdmissionError):raise
        raise DiskAdmissionError('Export intent inspection failed: '+str(exc)) from exc


def recheck_recovery(token,intent,scope):
    plan_recovery(token,intent,scope)


@contextmanager
def export_lease(root):
    lease=DirectoryLease(root)
    try:yield lease
    finally:lease.close()
