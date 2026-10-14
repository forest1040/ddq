"""Run from the repository with uv run python <this file> after capture."""
from hashlib import sha256
import json
from pathlib import Path
import xml.etree.ElementTree as ET

folder=Path(__file__).resolve().parent
repo=folder.parents[2]
def results(directory,name,expected,collection=False):
    root=ET.parse(directory/f'{name}.xml').getroot()
    cases=root.findall('.//testcase')
    assert len(cases)==expected,(directory,name,len(cases))
    assert not any(c.find(tag) is not None for c in cases for tag in ('failure','error','skipped'))
    if collection:
        ids={c.attrib['classname'].replace('.','/')+'.py::'+c.attrib['name'] for c in cases}
        collected={line.strip() for line in (directory/'collection.txt').read_text().splitlines() if line.startswith('tests/')}
        assert ids==collected,(ids-collected,collected-ids)
    return {'passed':len(cases),'failures':0,'errors':0,'skipped':0,
            'seconds':sum(float(s.attrib['time']) for s in root.findall('testsuite'))}

mac=folder/'mac/final';linux=folder/'linux/final'
summary={'mac':results(mac,'regression',184,True),
         'linux':results(linux,'regression',181,True),
         'linux-native':results(linux,'native',8)}
mac_source=json.loads((mac/'source-hashes.json').read_text())
linux_source={}
for line in (linux/'source-hashes.txt').read_text().splitlines():
    digest,name=line.split('  ',1);linux_source[name]=digest
assert mac_source==linux_source
assert all(sha256((repo/f).read_bytes()).hexdigest()==digest for f,digest in mac_source.items())
receipts=json.loads((linux/'recovery-receipts.json').read_text())
assert receipts
for receipt in receipts:
    raw=json.dumps(receipt['proof'],sort_keys=True,separators=(',',':'),ensure_ascii=False)
    assert receipt['evidence'].endswith(':'+sha256(raw.encode()).hexdigest())
capabilities=json.loads((linux/'native-catalog-capabilities.json').read_text())
assert len(capabilities)==4
c_source=sha256((repo/'src/svsim/native/sqlite_budget_vfs.c').read_bytes()).hexdigest()
assert all(row['capability']['source_sha256']==c_source for row in capabilities)
(folder/'merged-results.json').write_text(json.dumps(summary,indent=2)+'\n')
manifest={'date':'2026-10-14','scope':'P4-009 offline checkpoint-linked reservation recovery, single-host correctness',
          'source_matches_workspace':True,'results':summary,
          'recovery_receipts':len(receipts),'actual_native_catalogs':len(capabilities),
          'resources':{'cpus':4,'memory_bytes':8*1024**3,'pids':512,'shm_bytes':256*1024**2,'network':'none'}}
manifest['artifacts']={str(p.relative_to(folder)):sha256(p.read_bytes()).hexdigest()
    for p in sorted(folder.rglob('*')) if p.is_file() and p.name!='manifest.json'}
(folder/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps(summary,indent=2));print('Source/collection/receipt verification complete:',len(receipts),'receipts')
