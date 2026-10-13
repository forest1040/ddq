"""Capacity/time planning model; this does not run a chemistry calculation.

uv run --locked --extra mpi python docs/slides/estimate_vqe.py \
    --output docs/slides/vqe_estimates.json --measure-python-kernel
"""
import argparse
from dataclasses import asdict, dataclass
import json
from math import comb
from pathlib import Path

GIB=1<<30
TIB=1<<40


@dataclass(frozen=True)
class Case:
    name: str
    molecule: str
    active_electrons: int
    active_spatial_orbitals: int
    nodes: int
    hamiltonian_terms_assumed: int
    ram_gib_per_node: int
    ssd_tib_per_node: int
    network_gib_per_second_per_node: float | None


CASES=(
    Case('A-RAM','H2O',8,12,1,5000,512,0,None),
    Case('B-SSD','N2',10,14,1,20000,64,32,None),
    Case('C-Ethernet','N2',10,16,8,50000,64,64,8),
    Case('D-InfiniBand','N2',10,16,8,50000,64,64,16),
)


def estimate(case, *, kernel_rate=100_000_000):
    if kernel_rate<=0: raise ValueError('kernel_rate must be positive')
    n=2*case.active_spatial_orbitals
    state=16*(1<<n); shard=state/case.nodes; s=shard/GIB
    b=min(24,n); upper=n-b; rank_bits=case.nodes.bit_length()-1
    assert upper>=rank_bits
    masks=lambda bits: sum(comb(bits,j) for j in range(min(4,bits)+1))
    groups=masks(upper); remote=groups-masks(upper-rank_bits)
    # Upper-X grouping is valid for one-/two-body JW Hamiltonians (<=4 X/Y).
    # Reuse for O psi is a Phase 4 target; Phase 3 does not guarantee this.
    rotations=128; iterations=100
    kernel=case.hamiltonian_terms_assumed*(1<<n)/case.nodes/kernel_rate
    memory=2*case.hamiltonian_terms_assumed*s/32
    compute=max(kernel,memory)
    disk=2*groups*s/2 if case.ssd_tib_per_node else 0
    network=remote*s/case.network_gib_per_second_per_node if remote else 0
    evaluation=compute+disk+network
    per_rotation_memory=2*s/32
    per_rotation_disk=2*s/2 if case.ssd_tib_per_node else 0
    per_rotation_network=2*s/case.network_gib_per_second_per_node if remote else 0
    forward=rotations*(per_rotation_memory+per_rotation_disk+per_rotation_network)
    checkpoint=2*s/2 if case.ssd_tib_per_node else 0
    generator_cpu=max(rotations*(1<<n)/case.nodes/kernel_rate,2*rotations*s/32)
    generator_disk=2*rotations*s/2 if case.ssd_tib_per_node else 0
    generator_network=2*rotations*s/case.network_gib_per_second_per_node if remote else 0
    generator=generator_cpu+generator_disk+generator_network
    # One energy, O psi, forward, and two inverse streams per iteration.
    iteration_raw=3*forward+2*evaluation+generator+checkpoint
    iteration=1.05*iteration_raw
    peak_ssd=1.25*(1+3*rotations+1)*shard/TIB if case.ssd_tib_per_node else 0
    ram_required=1.25*(4*s+16) if not case.ssd_tib_per_node else 64
    assert peak_ssd<=case.ssd_tib_per_node
    assert ram_required<=case.ram_gib_per_node
    return {**asdict(case),'qubits':n,'state_gib':state/GIB,'shard_gib_per_node':s,
        'block_bits':b,'upper_x_groups_bound':groups,'remote_groups_bound':remote,
        'selected_double_excitations':16,'pauli_rotations':rotations,'parameters':16,
        'iterations_assumed':iterations,'peak_ssd_tib_per_node':peak_ssd,
        'ram_required_gib_per_node':ram_required,'energy_compute_seconds':compute,
        'energy_disk_seconds':disk,'energy_network_seconds':network,
        'gradient_generator_inner_seconds':generator,
        'forward_seconds':forward,'energy_only_seconds':forward+evaluation,
        'energy_gradient_iteration_seconds':iteration,'total_100_iterations_seconds':iterations*iteration,
        'kernel_rate_assumed':kernel_rate,
        'time_model':'planned-parallel-native-kernel; no overlap; unmeasured',
        'retention':'delete completed Binding scratch before admitting next; keep scalar results',
        'fit_under_assumptions':True}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True)
    parser.add_argument('--measure-python-kernel',action='store_true')
    args=parser.parse_args()
    report={'model_version':1,'assumptions':{'kernel_amplitude_terms_per_second_per_node':100_000_000,
        'memory_gib_per_second_per_node':32,'ssd_gib_per_second_per_node':2,
        'reserve_ratio':1.25,'time_overhead_ratio':1.05,'overlap_credit':False,
        'hamiltonian_integrals_generated':False,'convergence_guaranteed':False},
        'cases':[estimate(case) for case in CASES]}
    report['throughput_sensitivity']=[{'case':case.name,
        'total_days_at_50m':estimate(case,kernel_rate=50_000_000)['total_100_iterations_seconds']/86400,
        'total_days_at_200m':estimate(case,kernel_rate=200_000_000)['total_100_iterations_seconds']/86400}
        for case in CASES]
    if args.measure_python_kernel:
        from time import perf_counter
        import numpy as np
        from svsim.distributed_kernels import pauli_inner
        b=16; a=np.ones(1<<b,dtype=np.complex128); partner=a.copy()
        pauli_inner(a,partner,3,5,0,b)
        samples=[]
        for _ in range(3):
            start=perf_counter();pauli_inner(a,partner,3,5,0,b)
            samples.append(perf_counter()-start)
        elapsed=float(np.median(samples))
        report['local_python_kernel_measurement']={'block_bits':b,'samples_seconds':samples,
            'median_seconds':elapsed,'amplitude_terms_per_second':len(a)/elapsed,
            'scope':'one local Python kernel; excludes MPI/IO/Frame; not a chemistry benchmark'}
    path=Path(args.output);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    for row in report['cases']:
        print(row['name'], 'energy min',round(row['energy_only_seconds']/60,2),
              'E+grad hours',round(row['energy_gradient_iteration_seconds']/3600,2),
              '100iter days',round(row['total_100_iterations_seconds']/86400,2),
              'SSD TiB/node',round(row['peak_ssd_tib_per_node'],4))


if __name__=='__main__': main()
