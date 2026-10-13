"""Physical owner descriptors; a remote path is never opened by a reader."""
from dataclasses import asdict, dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class StoragePlacement:
    store_id: str
    owner_rank: int
    host: str
    volume_id: str
    root: str
    version: int = 1

    def __post_init__(self):
        if self.owner_rank < 0 or not self.store_id or not self.host or self.version != 1:
            raise ValueError('Invalid storage placement')
        if not Path(self.root).is_absolute():
            raise ValueError('Storage placement root must be absolute')

    def to_data(self):
        return asdict(self)

    @classmethod
    def local(cls, store_id, rank, host, root):
        root = Path(root).resolve()
        kind=filesystem_type(root)
        if kind in {'nfs','nfs4','cifs','smbfs','smb3','fuse.sshfs','sshfs','lustre','gpfs'}:
            raise ValueError('Node-local amplitude storage requires a local filesystem')
        return cls(store_id, rank, host, str(os.stat(root).st_dev), str(root))


def resolve_owner_root(entry, rank, *, host=None):
    """Resolve legacy shared roots or this rank's explicitly placed store."""
    if isinstance(entry, str):
        return Path(entry)
    matches = [StoragePlacement(**p) for p in entry if p['owner_rank'] == rank]
    if len(matches) != 1:
        raise OSError('Missing or duplicate storage owner placement')
    owner = matches[0]
    if host is not None and owner.host != host:
        raise OSError('Storage owner moved to another host; explicit migration required')
    path = Path(owner.root)
    if host is not None and (not path.exists() or str(os.stat(path).st_dev) != owner.volume_id):
        raise OSError('Storage owner volume unavailable; explicit migration required')
    return path


def filesystem_type(path):
    """Best-effort mount type discovery without shell or privileged operations."""
    import re
    import subprocess
    import sys
    path=Path(path).resolve()
    candidates=[]
    if sys.platform.startswith('linux'):
        try:
            for line in Path('/proc/self/mountinfo').read_text().splitlines():
                left,right=line.split(' - ',1)
                mount=re.sub(r'\\([0-7]{3})',lambda m:chr(int(m.group(1),8)),left.split()[4])
                candidates.append((Path(mount),right.split()[0]))
        except (OSError,ValueError,IndexError): return None
    elif sys.platform == 'darwin':
        try:
            result=subprocess.run(['/sbin/mount'],capture_output=True,text=True,timeout=5,check=True)
            for line in result.stdout.splitlines():
                match=re.match(r'.* on (.*) \(([^, )]+)',line)
                if match: candidates.append((Path(match[1]),match[2]))
        except (OSError,subprocess.SubprocessError): return None
    matching=[(len(mount.parts),kind) for mount,kind in candidates if path.is_relative_to(mount)]
    return max(matching)[1] if matching else None
