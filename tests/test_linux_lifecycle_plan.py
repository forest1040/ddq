"""Read-only retry plans must not silently omit or invent acceptance stages."""
import json
from pathlib import Path
import subprocess
import sys

import pytest

from examples.linux_phase4_lifecycle import read_junit


RUNNER=Path(__file__).resolve().parents[1]/'examples/linux_phase4_lifecycle.py'


def plan(*args):
    return subprocess.run([sys.executable,str(RUNNER),'--plan',*args],capture_output=True,text=True,timeout=5)


def test_retry_plan_preserves_canonical_order_and_marks_intentional_omissions():
    result=plan('--stages','real-enospc-direct,physical-direct','--stage-timeout-seconds','900')
    assert result.returncode==0,result.stderr
    data=json.loads(result.stdout)
    assert data['selected_stages']==['physical-direct','real-enospc-direct']
    assert data['not_run']==data['selected_stages']
    assert len(data['planned_stages'])==6 and len(data['intentionally_omitted_stages'])==4
    assert set(data['planned_stages'])==set(data['selected_stages']+data['intentionally_omitted_stages'])
    assert [stage['seconds'] for stage in data['stages']]==[900,900]
    assert data['global_deadline_seconds']==1200


def test_default_plan_keeps_original_stage_limits():
    result=plan();assert result.returncode==0,result.stderr
    data=json.loads(result.stdout)
    assert data['selected_stages']==data['planned_stages'] and not data['intentionally_omitted_stages']
    assert [stage['seconds'] for stage in data['stages']]==[240,120,240,240,120,120]


@pytest.mark.parametrize('args',[
    ('--stages',''),('--stages','unknown'),('--stages','physical-direct,physical-direct'),
    ('--stages','physical-direct,'),('--stage-timeout-seconds','0'),
    ('--stage-timeout-seconds','1201'),('--deadline-seconds','0'),('--deadline-seconds','1201')])
def test_invalid_retry_plan_is_rejected_before_any_environment_or_test_start(args):
    result=plan(*args)
    assert result.returncode==2 and not result.stdout and 'error:' in result.stderr


@pytest.mark.parametrize(('reason','is_unexpected'),[
    ('filesystem did not reuse the catalog inode within the bounded probe',False),
    ('filesystem birth-time resolution cannot distinguish this replacement',True),
])
def test_inode_reuse_optional_skip_requires_bounded_probe_reason(tmp_path,reason,is_unexpected):
    junit=tmp_path/'report.xml'
    junit.write_text(
        '<testsuite tests="1" skipped="1"><testcase '
        'name="test_reused_linux_inode_is_distinguished_by_birth_identity">'
        f'<skipped message="{reason}" /></testcase></testsuite>',
        encoding='utf-8')

    result=read_junit(junit)
    assert result['skipped']==[{'name':'test_reused_linux_inode_is_distinguished_by_birth_identity',
                                'reason':reason}]
    assert bool(result['unexpected_skips']) is is_unexpected
