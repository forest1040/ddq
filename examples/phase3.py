"""Collective MPI demo and bounded-memory file-backend measurement.

uv sync --locked --extra mpi
mpiexec -n 4 .venv/bin/python -m mpi4py examples/phase3.py --directory /tmp/phase3
"""
import argparse
from dataclasses import asdict
import json
from pathlib import Path
from time import perf_counter

import numpy as np
from mpi4py import MPI
from svsim import DistributedConfig, DistributedSimulator, ObservableIR, PauliTerm, ParameterSymbol


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',required=True)
    parser.add_argument('--num-qubits',type=int,default=8)
    parser.add_argument('--block-bits',type=int,default=2)
    parser.add_argument('--budget-kib',type=int,default=512)
    parser.add_argument('--backend',choices=('buffered','direct'),default='buffered')
    parser.add_argument('--amplitude-store',choices=('file','ram'),default='file')
    parser.add_argument('--exchange',choices=('packed','full','auto'),default='auto')
    parser.add_argument('--calibrate',action='store_true')
    parser.add_argument('--gradient',action='store_true')
    parser.add_argument('--output')
    args=parser.parse_args()
    comm=MPI.COMM_WORLD; rank=comm.Get_rank(); n=args.num_qubits
    config=DistributedConfig(args.directory,args.block_bits,
        ram_budget_bytes_per_rank=args.budget_kib*1024,metadata_reserve_bytes=64*1024,
        tile_bytes=16*1024,backend=args.backend,mcx_exchange=args.exchange,
        amplitude_store=args.amplitude_store)
    theta=ParameterSymbol('theta',parameter_id='phase3-demo-theta')
    observable=ObservableIR((PauliTerm(0,1),))
    begin=perf_counter()
    with DistributedSimulator(n,config,comm=comm) as sim:
        profile=sim.calibrate_network(repetitions=2) if args.calibrate else None
        sim.gate('RY',(0,),angle=theta)
        for q in range(1,n): sim.gate('H',(q,))
        sim.gate('MCX',(n-1,),controls=(0,1),control_values=(1,0))
        binding=sim.bind({theta:0.37})
        started=perf_counter(); sim.materialize(binding=binding)
        execute=sim.transport.max_value(perf_counter()-started)
        started=perf_counter(); value=sim.expectation(observable)
        expectation=sim.transport.max_value(perf_counter()-started)
        np.testing.assert_allclose(value,np.cos(.37),atol=1e-10)
        derivative=None; gradient_seconds=None
        if args.gradient:
            started=perf_counter(); derivative=sim.gradient(observable)[theta.parameter_id]
            gradient_seconds=sim.transport.max_value(perf_counter()-started)
            np.testing.assert_allclose(derivative,-np.sin(.37),atol=1e-9)
        started=perf_counter(); snapshot=sim.commit_snapshot()
        checkpoint=sim.transport.max_value(perf_counter()-started)
        rows=comm.gather({'rank':rank,'managed_peak_bytes':sim.budget.peak,
            'send_payload_bytes':sim.transport.send_bytes,'recv_payload_bytes':sim.transport.recv_bytes,
            'metadata_bytes':sim.transport.metadata_bytes,'storage_read_bytes':sim.trace.read_bytes,
            'storage_write_bytes':sim.trace.write_bytes,
            'segment_bytes':sum(p.stat().st_size for p in (sim.store.directory/'state').glob('segment-*.bin'))},root=0)
        environment=sim.environment
    started=perf_counter()
    with DistributedSimulator.resume(config,snapshot,comm=comm) as sim:
        restored=sim.expectation(observable)
        np.testing.assert_allclose(restored,value,atol=1e-10)
        restart=sim.transport.max_value(perf_counter()-started)
    elapsed=comm.allreduce(perf_counter()-begin,op=MPI.MAX)
    if rank==0:
        report={'schema_version':1,'scope':'same-host' if len({r['host'] for r in environment})==1 else 'multi-host',
            'num_qubits':n,'block_bits':args.block_bits,'ranks':comm.Get_size(),'state_bytes':16*(1<<n),
            'budget_bytes_per_rank':config.ram_budget_bytes_per_rank,'backend':args.backend,
            'amplitude_store':args.amplitude_store,'exchange':args.exchange,'full_state_gather':False,
            'expectation':value,'gradient':derivative,'execute_seconds_max':execute,
            'expectation_seconds_max':expectation,'gradient_seconds_max':gradient_seconds,
            'checkpoint_seconds_max':checkpoint,'restart_expectation_seconds_max':restart,
            'end_to_end_seconds_max':elapsed,'rank_metrics':rows,'environment':environment,
            'network_profile':asdict(profile) if profile else None}
        text=json.dumps(report,ensure_ascii=False,indent=2)
        if args.output:
            Path(args.output).parent.mkdir(parents=True,exist_ok=True)
            Path(args.output).write_text(text+'\n')
        print(text)


if __name__=='__main__': main()
