"""Test selection: ``pytest -m "not slow"`` is the fast pre-commit subset.

Slow tests repeat a path that a faster representative already covers: 4-rank
variants of MPI suites (1/2-rank variants stay), subprocess campaigns, the
largest parametrized sizes and redundant negative scenarios. The full suite
remains the acceptance run.
"""
import re

import pytest

MPI_SUITES = ('test_phase3_mpi.py', 'test_phase4_mpi.py', 'test_phase4_lifecycle.py', 'test_disk_coordinator.py',
              'test_checkpoint_recovery_mpi.py', 'test_remaining_work_mpi.py')
SLOW_PATTERNS = (
    r'test_followup_mpi\.py::',                                  # full campaign subprocesses
    r'test_phase1_benchmark\.py::test_phase1_comparison_report',
    r'test_adaptive_replan\.py::test_mpi_slowdown',
    r'test_adaptive_replan\.py::test_negative_cases_do_not_switch\[(observe|recommend|missing-baseline)\]',
    r'test_stored_numerics\.py::test_stored_streaming_mixed_local_joint_and_controlled_dense_matches_oracle\[(10|12)\]',
    r'test_stored_numerics\.py::test_structured_exact_stored_execution_matches_native\[4\]',
    r'test_tile_transpose\.py::test_tile_route_fits_when_full_block_does_not\[14\]',
    r'test_phase2_runtime\.py::test_random_8q_words_include_unaligned_controlled_and_dense_fallback\[(2\d|3\d)\]',
    r'test_stored_budget\.py::test_state_larger_than_budget_executes_exports_and_streams',
    r'test_adaptive_replan\.py::test_negative_cases_do_not_switch\[no-checkpoint-cost\]',
    # One representative rank count / backend / policy per MPI scenario stays in the fast subset.
    r'test_phase3_mpi\.py::test_distributed_state_observables_and_checkpoint\[1\]',
    r'test_phase4_mpi\.py::test_async_exact_state_resume_and_rank_local_manifests\[(buffered-S0-2|buffered-S2-\d|direct-S0-\d|direct-S2-1)\]',
    r'test_phase4_mpi\.py::test_async_file_state_larger_than_managed_budget\[2\]',
    r'test_checkpoint_recovery_mpi\.py::test_collective_checkpoint_recovery\[(1-(True|False)-True|1-True-False|2-False-\w+|2-True-False)\]',
    r'test_disk_coordinator\.py::test_shared_volume_dataset_fork_and_gradient\[(direct-\d|buffered-1)\]',
    r'test_phase4_lifecycle\.py::test_node_local_collective_lifecycle\[1\]',
    r'test_remaining_work_mpi\.py::test_remaining_work_collective_order_and_actual_state\[1\]',
    r'test_phase5_timing\.py::test_two_rank_runs_share_one_dataset_and_validate\[sync\]',
    r'test_stored_numerics\.py::test_materialization_modes_match_eager_oracle\[(batching|deferred-no-frame)\]',
    r'test_stored_numerics\.py::test_structured_exact_stored_execution_matches_native\[3\]',
    r'test_tile_transpose\.py::test_tile_route_fits_when_full_block_does_not\[13\]',
    r'test_disk_reservation_ledger\.py::test_nonoverlapping_root_limit_still_rejects_257th_root',
)
SLOW = re.compile('|'.join(SLOW_PATTERNS))
FOUR_RANKS = re.compile(r'\[(?:[^\]]*-)?4(?:-[^\]]*)?\]$')


def pytest_configure(config):
    config.addinivalue_line('markers', 'slow: covered by a faster representative; excluded by -m "not slow"')


def pytest_collection_modifyitems(config, items):
    for item in items:
        name = item.nodeid.split('/')[-1]
        if SLOW.search(name) or (name.split('::')[0] in MPI_SUITES and FOUR_RANKS.search(name)):
            item.add_marker(pytest.mark.slow)
