"""Iterative QPE / second-order Trotter planning model, not a simulator.

uv run --locked --extra mpi python docs/slides/estimate_chemistry.py \
  --output docs/slides/chemistry_estimates.json
"""
import argparse
from dataclasses import asdict, dataclass
import json
from math import ceil, log2, pi
from pathlib import Path
GIB=1<<30
TIB=1<<40
YEAR_SECONDS=365.25*86400

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

def estimate(case, *, energy_target_ha=.001, delta_max=.01, kernel_rate=100_000_000):
    if min(energy_target_ha,delta_max,kernel_rate)<=0: raise ValueError('Positive model inputs required')
    lam=100; phase_budget=energy_target_ha/2
    tau=pi/(2*lam); bits=ceil(log2(4*lam/phase_budget))
    base_steps=ceil(tau/delta_max); repetitions=32
    steps=base_steps*((1<<bits)-1)
    controlled_factors=2*case.hamiltonian_terms_assumed*steps
    rotations=2*controlled_factors
    system_qubits=2*case.active_spatial_orbitals; full_qubits=system_qubits+1
    state=16*(1<<full_qubits); shard=state/case.nodes; s=shard/GIB
    block_bits=min(24,full_qubits-(case.nodes.bit_length()-1))
    ram_required=1.25*(2*s+16+1)
    cache_fit=2*s<=32 or not case.ssd_tib_per_node
    assert cache_fit and ram_required<=case.ram_gib_per_node
    per_rotation=max((1<<full_qubits)/case.nodes/kernel_rate,2*s/32)
    per_rotation_network=2*s/case.network_gib_per_second_per_node if case.nodes>1 else 0
    checkpoint=(bits+1)*2*s/2 if case.ssd_tib_per_node else 0
    run_seconds=1.05*(rotations*(per_rotation+per_rotation_network)+checkpoint)
    disk_peak=1.25*4*shard/TIB if case.ssd_tib_per_node else 0
    slot_payload=max(4096,16*(1<<block_bits))
    slot_multiplier=slot_payload/(16*(1<<block_bits))
    append_peak=1.25*(rotations+1)*shard*slot_multiplier/TIB if case.ssd_tib_per_node else 0
    assert disk_peak<=case.ssd_tib_per_node
    return {**asdict(case),'system_qubits':system_qubits,'phase_bits':bits,
        'simulated_qubits':full_qubits,'qpe_ancillas':1,'state_gib':state/GIB,
        'shard_gib_per_node':s,'block_bits':block_bits,'energy_target_ha':energy_target_ha,
        'phase_budget_ha':phase_budget,'trotter_budget_ha':energy_target_ha/2,
        'phase_grid_ha':4*lam/(1<<bits),'spectral_half_width_ha_assumed':lam,
        'tau_inverse_ha':tau,'delta_max_inverse_ha':delta_max,
        'base_trotter_steps':base_steps,'actual_step_inverse_ha':tau/base_steps,
        'trotter_steps_per_run':steps,'controlled_pauli_factors_per_run':controlled_factors,
        'native_rotations_per_run':rotations,'independent_runs':repetitions,
        'assumed_ground_overlap':.25,'assumed_good_phase_probability':.4,
        'probability_at_least_one_ground_good_phase':1-(1-.25*.4)**repetitions,
        'ram_required_gib_per_node':ram_required,'planned_disk_peak_tib_per_node':disk_peak,
        'append_only_disk_peak_tib_per_node':append_peak,
        'planned_fit_with_reclamation':True,
        'phase3_append_only_disk_fits':append_peak<=case.ssd_tib_per_node,
        'qpe_runtime_implemented':False,'trotter_error_certified':False,
        'per_rotation_compute_memory_seconds':per_rotation,
        'per_rotation_network_seconds':per_rotation_network,
        'checkpoint_io_seconds_per_run':checkpoint,'one_run_seconds':run_seconds,
        'one_run_years':run_seconds/YEAR_SECONDS,
        'all_runs_years':repetitions*run_seconds/YEAR_SECONDS,
        'kernel_update_rate_assumed':kernel_rate,
        'time_model':'planned native / balanced parallel / RAM-resident working states; no overlap',
        'requires':'IPEA measurement/reset, compressed repeat plan, bounded root retirement / working store'}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True)
    parser.add_argument('--energy-mha',type=float,default=1)
    parser.add_argument('--delta-max',type=float,default=.01)
    args=parser.parse_args()
    rows=[estimate(c,energy_target_ha=args.energy_mha/1000,delta_max=args.delta_max) for c in CASES]
    report={'model_version':2,'algorithm':'iterative QPE / second-order symmetric Trotter',
        'assumptions':{'hamiltonian_integrals_generated':False,'precision_certified':False,
            'controlled_rotation_lowering':2,'rotation_updates_per_second_per_node':100_000_000,
            'memory_gib_per_second_per_node':32,'ssd_gib_per_second_per_node':2,
            'reserve_ratio':1.25,'time_overhead_ratio':1.05,'overlap_credit':False,
            'bounded_working_store_implemented':False,'qpe_runtime_implemented':False},
        'cases':rows,'sensitivity':[{'case':c.name,
            'years_32_runs_delta_002':estimate(c,delta_max=.02)['all_runs_years'],
            'years_32_runs_delta_0005':estimate(c,delta_max=.005)['all_runs_years'],
            'years_32_runs_energy_10mha':estimate(c,energy_target_ha=.01)['all_runs_years']}
            for c in CASES]}
    Path(args.output).write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    for row in rows:
        print(row['name'],'qubits',row['simulated_qubits'],'bits',row['phase_bits'],
            'native rotations',row['native_rotations_per_run'],
            'one run years',round(row['one_run_years'],2),
            '32 runs years',round(row['all_runs_years'],2),
            'planned SSD TiB',row['planned_disk_peak_tib_per_node'],
            'append SSD TiB',round(row['append_only_disk_peak_tib_per_node'],2))

if __name__=='__main__': main()
