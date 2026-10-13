"""Mac-local MPI oracle for snapshots, collective failures and proposals."""
import json
import sys
from pathlib import Path

import numpy as np
from mpi4py import MPI

from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig
from svsim.kernels import eager_reference
from svsim.mpi_transport import CollectiveError
from svsim.runtime_cost import CostSample


comm=MPI.COMM_WORLD
rank=comm.Get_rank()
config=DistributedConfig(str(Path(sys.argv[1])/f'owner-{rank}'),2,
    ram_budget_bytes_per_rank=4*1024**2,frame_policy='materialize',
    disk_quota_bytes=64*1024**2)
policy=AsyncPolicy(io_engine='cooperative',swap_policy='S2',lookahead_gates=4)
with AsyncDistributedSimulator(6,config,comm=comm,policy=policy) as sim:
    sim.runtime.configure_eta(max_bytes=65536,scan_items=2)
    for kind,target,angle in [('H',0,None),('RX',1,.2),('RY',5,.3),('RZ',0,.4),
                              ('H',2,None),('RX',5,-.1),('RY',1,.7),('H',3,None)]:
        sim.gate(kind,(target,),**({'angle':angle} if angle is not None else {}))
    binding=sim.bind()
    expected=eager_reference(sim.sequencer.operations,6,sim.assets,binding)
    # Injected values exercise arithmetic and rank MAX, never performance claims.
    sim.runtime.set_eta_cost_view('baseline',lambda task:CostSample('oracle',.1,.1,.1))
    duration=.2+.1*rank
    sim.runtime.set_eta_cost_view('observed',lambda task:CostSample('oracle',duration,duration,duration))
    report=sim.runtime.update_eta(endpoint_frontier=3,collective=True)
    groups=len(sim.runtime.remaining_extractor._published.window)
    assert report['remaining_sources']==4
    assert report['rank_max_expected_seconds']['observed']==max(comm.allgather(duration*groups))
    assert not report['eligible_for_control']
    assert report['cost_view_sources']['observed']=='diagnostic-explicit'
    candidates=sim.runtime.search_candidates(collective=True)
    ids=tuple(row['proposal_id'] for row in candidates)
    assert all(other==ids for other in comm.allgather(ids))
    assert candidates[0]['classification']=='reference'
    assert not any(row['eligible'] for row in candidates)
    # One local analysis error must reach every rank before the next collective.
    def failing(task):
        if rank==min(1,comm.Get_size()-1):raise ValueError('injected ETA failure')
        return CostSample('oracle',1,1,1)
    sim.runtime.set_eta_cost_view('observed',failing)
    try:sim.runtime.update_eta(collective=True)
    except CollectiveError:pass
    else:raise AssertionError('ETA failure was not voted collectively')
    sim.runtime.set_eta_cost_view('observed',lambda task:CostSample('oracle',duration,duration,duration))
    sim.materialize(3,binding=binding)
    assert sim.runtime.latest_eta['remaining_sources']==0
    assert sim.runtime.latest_eta['rank_max_expected_seconds']['observed']==0
    # Incremental scan reports unknown tail without inventing future fusion.
    report=sim.runtime.update_eta(endpoint_frontier=7,collective=True)
    assert report['remaining_sources']==4
    sim.materialize(binding=binding)
    actual=sim.state(allow_gather=True)
    error=None
    if rank==0:
        try:np.testing.assert_allclose(actual,expected,atol=2e-11,rtol=2e-11)
        except Exception as exc:error=str(exc)
    sim.transport.vote(error)
    assert sim.runtime.latest_eta['remaining_sources']==0
    assert sim.budget.peak<=config.ram_budget_bytes_per_rank
    sim.gate('H',(0,))
    assert sim.runtime.latest_eta['stale']
    assert sim.runtime.remaining_seconds is None
    assert sim.runtime.update_eta()['remaining_sources']==1
if rank==0:print(json.dumps({'kind':'remaining-work-correctness','ranks':comm.Get_size(),'pass':True}),flush=True)
