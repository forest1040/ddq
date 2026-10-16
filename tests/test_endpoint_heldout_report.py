"""Report arithmetic preserves unknowns and distinguishes API wall from stage time."""
from pathlib import Path
import runpy

import pytest

metrics=runpy.run_path(str(Path(__file__).resolve().parents[1]/'examples/p4_002_endpoint_heldout.py'),
                       run_name='endpoint_report_only')['metrics']


def test_unknown_and_error_rows_remain_in_denominator_without_zero_error_credit():
    rows=[{'prediction':{'expected_seconds':2.,'lower_seconds':1.,'upper_seconds':3.},'actual_wall_seconds':1.,'actual_stage_seconds':2.},
          {'prediction':{'expected_seconds':3.,'lower_seconds':1.,'upper_seconds':3.},'actual_wall_seconds':2.,'actual_stage_seconds':3.},
          {'prediction':{'expected_seconds':None,'unknown_reasons':['UNMEASURED']},'actual_wall_seconds':4.},
          {'prediction':{'expected_seconds':5.},'error':'actual endpoint failed'}]
    report=metrics(rows)
    assert report['checks']==4 and report['answered']==2 and report['answer_fraction']==.5
    assert report['errors']==1 and report['unknown']==1 and report['unknown_reasons']=={'UNMEASURED':1}
    assert report['absolute_error_seconds']=={'median':1.,'p95':1.,'worst':1.}
    assert report['absolute_relative_error']['median']==.75
    assert report['absolute_relative_error']['p95']==pytest.approx(.975)
    assert report['interval_coverage']==1.
    assert metrics(rows,'actual_stage_seconds')['absolute_error_seconds']['worst']==0.


def test_all_unknown_has_no_manufactured_accuracy_or_coverage():
    report=metrics([{'prediction':{'expected_seconds':None,'unknown_reasons':['INSUFFICIENT_SAMPLES']},'actual_wall_seconds':1.}])
    assert report['answered']==0 and report['answer_fraction']==0.
    assert report['absolute_relative_error']=={'median':None,'p95':None,'worst':None}
    assert report['interval_coverage'] is None and report['interval_checks']==0
