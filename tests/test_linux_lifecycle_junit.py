"""Backend-specific non-applicability must not hide a missing fault test."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET

import pytest


SOURCE=Path(__file__).resolve().parents[1]/'examples/linux_phase4_lifecycle.py'
spec=importlib.util.spec_from_file_location('lifecycle_junit_contract',SOURCE)
runner=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=runner
spec.loader.exec_module(runner)


def junit(tmp_path,name,reason):
    root=ET.Element('testsuite')
    ET.SubElement(root,'testcase',name='real-payload-oracle')
    case=ET.SubElement(root,'testcase',name=name)
    ET.SubElement(case,'skipped',message=reason)
    path=tmp_path/'result.xml';ET.ElementTree(root).write(path)
    return path


@pytest.mark.parametrize('backend,allowed',[('direct',True),('buffered',False),(None,False)])
def test_aligned_direct_fault_skip_requires_explicit_backend(tmp_path,backend,allowed):
    path=junit(tmp_path,runner.DIRECT_SKIP,runner.DIRECT_SKIP_REASON)
    result=runner.read_junit(path,backend=backend)
    assert result['cases']==2 and result['failed']==0 and len(result['skipped'])==1
    assert (not result['unexpected_skips'])==allowed


@pytest.mark.parametrize('name,reason',[
    ('different_test',runner.DIRECT_SKIP_REASON),
    (runner.DIRECT_SKIP,runner.DIRECT_SKIP_REASON+' '),
    (runner.DIRECT_SKIP,'O_DIRECT unavailable'),
    (runner.DIRECT_SKIP,'MPI launcher unavailable'),
    (runner.OPTIONAL_SKIP,'filesystem birth-time resolution cannot distinguish this replacement')])
def test_direct_backend_does_not_allow_other_missing_evidence(tmp_path,name,reason):
    path=junit(tmp_path,name,reason)
    assert len(runner.read_junit(path,backend='direct')['unexpected_skips'])==1


def test_exact_bounded_inode_nonarrival_skip_remains_allowed(tmp_path):
    path=junit(tmp_path,runner.OPTIONAL_SKIP,runner.OPTIONAL_SKIP_REASON)
    assert not runner.read_junit(path,backend='buffered')['unexpected_skips']


@pytest.mark.parametrize('backend,expected_cases,success',[('direct',2,True),('buffered',2,False),('direct',3,False)])
def test_saved_junit_recheck_is_read_only_and_rejects_wrong_backend_or_count(tmp_path,backend,expected_cases,success):
    path=junit(tmp_path,runner.DIRECT_SKIP,runner.DIRECT_SKIP_REASON)
    before=path.read_bytes()
    helper=SOURCE.with_name('recheck_phase4_lifecycle_junit.py')
    result=subprocess.run([sys.executable,str(helper),str(path),'--backend',backend,
        '--expected-cases',str(expected_cases)],capture_output=True,text=True,timeout=5)
    assert (result.returncode==0)==success,result.stderr
    data=json.loads(result.stdout)
    assert data['recheck_only'] is True and data['status']==('passed' if success else 'failed')
    assert path.read_bytes()==before and set(tmp_path.iterdir())=={path}
