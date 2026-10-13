"""Combine matched local measurements with conditional chemistry projections.

No benchmark is launched here. Cluster input is summarized without host names,
paths, job IDs or library build strings in the public output.
"""
import argparse
import json
from pathlib import Path

from estimate_chemistry import CASES as QPE_CASES, estimate as qpe_estimate, YEAR_SECONDS
from estimate_vqe import CASES as VQE_CASES, estimate as vqe_estimate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    folder = Path(__file__).resolve().parent
    local = json.loads((folder/'performance_remeasurement.json').read_text())
    qulacs = json.loads((folder/'qulacs_ram_measurement.json').read_text())
    qulacs_sources = [('distributed wheel', qulacs)]
    source_files = ['performance_remeasurement.json', 'qulacs_ram_measurement.json',
                    'phase4_remeasurement.json', 'vqe_estimates.json',
                    'chemistry_estimates.json', 'estimate_chemistry.py', 'estimate_vqe.py']
    for filename in ('qulacs_omp_single_measurement.json', 'qulacs_omp_single_29_measurement.json',
                     'qulacs_omp_ram_measurement.json'):
        if (folder/filename).exists():
            qulacs_sources.append(('OpenMP source build', json.loads((folder/filename).read_text())))
            source_files.append(filename)
    old_vqe = json.loads((folder/'vqe_estimates.json').read_text())
    rows = local['rows']
    single_inner = next(r for r in rows if
        (r['block_bits'], r['kernel'], r['processes']) == (16, 'pauli_inner', 1))
    previous = old_vqe.get('previous_local_python_kernel_measurement',
                            old_vqe['local_python_kernel_measurement'])
    vqe_projections = []; qpe_projections = []
    for processes in (1, 16):
        inner = next(r for r in rows if (r['block_bits'], r['kernel'], r['processes']) ==
                     (16, 'pauli_inner', processes))
        # Two controlled factors are lowered to native rotations. Use the
        # slower of local and pair rotation examples for this sensitivity.
        rotation = min(r['aggregate_updates_per_second_median'] for r in rows if
                       r['block_bits'] == 16 and r['processes'] == processes and
                       r['kernel'] in ('rotation_local', 'rotation_pair'))
        for case in VQE_CASES:
            result = vqe_estimate(case, kernel_rate=inner['aggregate_updates_per_second_median'])
            vqe_projections.append({'case':case.name, 'independent_processes_per_node':processes,
                'kernel_rate':inner['aggregate_updates_per_second_median'],
                'total_100_iterations_days':result['total_100_iterations_seconds']/86400,
                'scope':'CPU-rate sensitivity only; target I/O/network/reuse assumptions retained; '
                        'balanced execution and replicated Mac-class CPUs not verified'})
        for case in QPE_CASES:
            result = qpe_estimate(case, kernel_rate=rotation)
            qpe_projections.append({'case':case.name, 'independent_processes_per_node':processes,
                'kernel_rate':rotation, 'one_run_years':result['one_run_years'],
                'all_runs_years':result['all_runs_years'],
                'scope':'CPU-rate sensitivity only; original RAM/network assumptions retained; '
                        'not measured end-to-end QPE or distributed throughput'})
    qulacs_projections = []
    for build, measurement in qulacs_sources:
      for row in measurement['rows']:
        if row['kernel'] != 'controlled_symmetric_trotter_slice': continue
        for case in QPE_CASES[:2]:
            base = qpe_estimate(case)
            if base['simulated_qubits'] != row['qubits']: continue
            # Measured whole-memory native gate time already includes RAM.
            # No additional RAM, SSD or MPI costs for the one-node RAM case.
            seconds = 1.05*base['native_rotations_per_run']*row['seconds_per_native_rotation']
            times = [1.05*base['native_rotations_per_run']*x['wall_seconds']/32
                     for x in row['samples']]
            qulacs_projections.append({'case':case.name, 'storage':'RAM only', 'build':build,
                'system_qubits':base['system_qubits'], 'simulated_qubits':row['qubits'],
                'state_gib':row['state_bytes']/2**30, 'threads_requested':row['threads_requested'],
                'cpu_wall_ratio_median':row['cpu_wall_ratio_median'],
                'native_rotations_per_run':base['native_rotations_per_run'],
                'measured_seconds_per_native_rotation':row['seconds_per_native_rotation'],
                'one_run_years':seconds/YEAR_SECONDS, 'all_runs_years':32*seconds/YEAR_SECONDS,
                'one_run_years_sample_min':min(times)/YEAR_SECONDS,
                'one_run_years_sample_max':max(times)/YEAR_SECONDS,
                'scope':'linear extrapolation of eight synthetic terms; no molecular Hamiltonian, '
                        'error certification, state preparation, IPEA measurement/reset or fusion'})
    cluster = []
    internal = folder.parent/'internal/phase4_cluster_results'
    for name, provider in [('psm2-2','PSM2'),('psm2-4','PSM2'),
                           ('ofi-4','OFI/PSM2'),('ethernet-4','Ethernet/TCP')]:
        result = json.loads((internal/f'{name}.json').read_text())
        cluster.append({'nodes':len(set(result['hosts'])), 'ranks':result['ranks'],
            'provider':provider, 'transfer_bytes':result['transfer_bytes'],
            'transfer_seconds_rank_max':result['transfer_seconds_max'],
            'transfer_median_ms':1000*result['transfer_median_seconds'],
            'small_message_payload_gib_per_second':result['transfer_bytes']/2**30/result['transfer_median_seconds'],
            'scope':'one 1 MiB transfer via bounded simulator protocol incl control/ack; '
                    'three trials without separate warmup; not streaming bandwidth or InfiniBand'})
    report = {'schema_version':1, 'date':'2026-10-13',
        'unchanged_target_models':{'qpe':'chemistry_estimates.json', 'vqe':'vqe_estimates.json'},
        'python_inner_old_rate':previous['amplitude_terms_per_second'],
        'python_inner_new_rate':single_inner['aggregate_updates_per_second_median'],
        'python_inner_new_over_old':single_inner['aggregate_updates_per_second_median']/previous['amplitude_terms_per_second'],
        'vqe_cpu_sensitivity':vqe_projections, 'qpe_cpu_sensitivity':qpe_projections,
        'qulacs_one_node_ram_qpe':qulacs_projections, 'cluster_small_transfer_comparison':cluster,
        'sources':source_files,
        'qpe_conditions':{'energy_target_ha':.001,'delta_max_inverse_ha':.01,
            'phase_bits':20,'trotter_order':2,'algorithm':'IPEA','independent_runs':32,
            'overhead_ratio_assumed':1.05,'precision_certified':False}}
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    for row in qulacs_projections:
        print(row['case'], row['threads_requested'], 'threads:', round(row['one_run_years'],3),
              'years/run;',round(row['all_runs_years'],3),'years/32 runs')


if __name__ == '__main__': main()
