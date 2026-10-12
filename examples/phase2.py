"""Phase-aware frame, exact binding, prefix sharing and streaming observation."""
import argparse
from svsim import StoredSimulator,StorageConfig,ParameterSymbol,ExactPi,ObservableIR,PauliTerm


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',help='New storage directory')
    parser.add_argument('--backend',choices=('buffered','direct'),default='buffered')
    args = parser.parse_args()
    # The 1 MiB state exceeds the 256 KiB managed allocation budget.
    config = StorageConfig(args.directory,6,ram_budget_bytes=256*1024,metadata_reserve_bytes=128*1024,
                           backend=args.backend,alignment=4096 if args.backend=='direct' else 64)
    theta = ParameterSymbol('theta')
    observable = ObservableIR((PauliTerm.from_string('Z'+'I'*14+'Z'),))
    with StoredSimulator(16,config,frame_policy='keep',alignment_policy='auto') as sim:
        sim.gate('H',(0,))
        sim.gate('CX',(0,15))
        sim.gate('T',(0,))
        sim.gate('RY',(0,),angle=theta)
        sim.gate('PAULI_ROT',(0,1),paulis=('X','Y'),angle=theta/2)
        prefix = sim.commit_prefix((theta,))
        for value in (ExactPi(1,2),.3):
            binding = sim.bind({theta:value})
            with sim.fork(prefix,binding=binding) as run:
                run.materialize()
                expectation = run.expectation(observable)
                snapshot = run.commit_snapshot()
                print(f'binding={binding.binding_id[:12]} <Z0 Z15>={expectation:.12f} snapshot={snapshot.snapshot_id}')
                print(f'managed peak={run.budget.peak} budget={config.ram_budget_bytes} bytes')


if __name__ == '__main__': main()
