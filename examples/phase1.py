"""uv run python examples/phase1.py --storage /tmp/svsim-phase1-demo"""
import argparse
from pathlib import Path
from svsim import StoredSimulator, StorageConfig, ParameterSymbol, PauliTerm, ObservableIR


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--storage',required=True,help='New state directory; an existing state is never overwritten')
    parser.add_argument('--backend',choices=('buffered','direct'),default='buffered')
    args = parser.parse_args()
    # State = 1 MiB, managed budget = 256 KiB. No eager gather is used.
    config = StorageConfig(args.storage,8,ram_budget_bytes=256*1024,backend=args.backend)
    with StoredSimulator(16,config) as sim:
        theta = ParameterSymbol('theta')
        sim.gate('H',(0,)); sim.gate('CX',(0,15)); sim.gate('RY',(0,),angle=theta)
        prefix = sim.commit_prefix((theta,))
        observable = ObservableIR((PauliTerm.from_string('Z'+'I'*14+'Z'),))
        for angle in (.3,.7):
            with sim.fork(prefix,binding=sim.bind({theta:angle})) as run:
                value = run.expectation(observable)
                snapshot = run.commit_snapshot()
                print(f'theta={angle:.1f}, expectation={value:.12f}, snapshot={snapshot.snapshot_id}')
                print(f'managed peak={run.budget.peak} / {config.ram_budget_bytes} bytes, reads={run.trace.read_bytes}, writes={run.trace.write_bytes}')
                print(f'trace={run.trace.path}')
    print(f'prefix snapshot={prefix.snapshot_id}, storage={Path(args.storage)}')


if __name__ == '__main__': main()
