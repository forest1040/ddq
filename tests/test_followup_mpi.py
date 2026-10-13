"""Actual Mac-local campaign smoke, with no cluster commands."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


@pytest.mark.skipif(not shutil.which('mpiexec') or importlib.util.find_spec('mpi4py') is None,reason='Local MPI unavailable')
@pytest.mark.parametrize('ranks',[1,2,4])
def test_actual_followup_campaign_keeps_unknown_costs_and_state(tmp_path,ranks):
    output=tmp_path/'report.json'
    env=dict(os.environ,PYTHONPATH='src',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1',
        OMPI_ALLOW_RUN_AS_ROOT='1',OMPI_ALLOW_RUN_AS_ROOT_CONFIRM='1')
    subprocess.run([shutil.which('mpiexec'),'-n',str(ranks),sys.executable,
        'examples/phase4_remaining_campaign.py','--action','all','--scratch',str(tmp_path/'scratch'),
        '--output',str(output),'--repetitions','1','--warmup','0','--wall-seconds','90'],
        check=True,env=env,timeout=120)
    data=json.loads(output.read_text())
    assert not data['cluster_used'] and data['ranks']==ranks
    for group in ('comparison','adaptive'):
        assert all(row['status']=='completed' for row in data[group]['rows'])
        assert all(row['claim']=='unconfirmed' for row in data[group]['comparisons'])
    for row in data['adaptive']['rows']:
        report=row['trials'][0]['raw']['adaptive']
        assert report['switch_count']==0 and report['eligible_candidate_count']==0
        assert not report['positive_gain_verified']
    gradient=data['gradient']
    assert all(row['validated'] and row['success'] for row in gradient['calibration']['observations'])
    assert gradient['production_cost_count']==0
    assert gradient['auto_diagnostic']['method']=='adjoint'
    assert gradient['auto_diagnostic']['cost_origin']=='INSUFFICIENT_BUILD_BASELINE'
