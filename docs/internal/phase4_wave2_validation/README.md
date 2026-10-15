# Phase 4 wave 2 validation (2026-10-15)

Three isolated worktrees implemented catalog file attribution, evaluation baseline qualification, and a representative comparison-matrix validator. Luna handled the matrix; the parent integrated and reviewed all changes. P4-002 and P4-009 remain IN_PROGRESS; subsequent work continues toward their original acceptance conditions.

## Final validation

- Mac `uv run pytest -m "not slow" -q`: **1081 passed, 36 skipped, 83 deselected**, 208.97 seconds. Full output: `mac-fast-final.txt`.
- Linux buffered: **196 passed, 1 deselected**, 229.99 seconds.
- Linux direct: **196 passed, 1 deselected**, 192.83 seconds.
- Direct preflight asserted `linux-o-direct`, performed aligned write/fsync/readback, and obtained a real non-null statx birthtime with a `catalog-v3` owner. The real separate-device `/dev/shm` catalog refusal test passed in both backend suites.

`linux/` contains both pytest logs, JUnit XML, environment metadata and mounted-source hashes. All runtime sources and all tests executed in Linux match the integrated sources. Among the 245 mounted files, 244 match the final tree; the sole difference is `tests/test_adaptive_replan.py`, which was subsequently made deterministic and was **not selected** in either Linux run. Runtime source was unchanged by that final test-only fix. The Linux runs use the source after the parent-alias correction. Mac final includes the deterministic integration test and the explicit VERIFY-failure regression.

## Commands and scope

The user-approved transfer contains only `src/`, `tests/`, and `examples/`, excludes bytecode, and targets the previously approved `ent:/tmp/ddq-p4-linux-followup-20261015-tmori/context/`. No project metadata or Docker definitions were exported. Image-provided dependencies/configuration are used. [run_linux.sh](run_linux.sh) records the actual finite command, including selected test files, fixed hostname, existing non-root image, read-only source mounts, separate scratch/output, `--network none`, four CPU and eight GiB limits. `SVSIM_TEST_BACKEND` selects buffered/direct; each backend pytest is limited to 870 seconds and its preflight to 30 seconds.

The Linux tests cover catalog identity/admission, checkpoint recovery including parent-path alias compatibility, uncommitted/private recovery and GC, adaptive evaluator, layout-cost contracts, cost registry, representative matrix, replan cost and coordinator runtime/MPI. They do not run a Linux layout-performance campaign or certify the earlier O_DIRECT gain predictions. Mac MPI positive/negative functional smoke is separately recorded in [mac-baseline-quality](../phase4_layout_migration/mac-baseline-quality/README.md); one pair is not performance-significance evidence.

## Failures retained and corrected

`mac-initial-parent-alias-failure.txt` records the first full suite: one checkpoint API compatibility failure. Parent directory symlinks are now canonicalized; DB-file symlinks and mismatched birth identity remain refused. `mac-verify-jitter-failure.txt` records the second full suite: the real executor exceeded VERIFY's measured envelope, and runtime correctly refused. The integration test now supplies explicit injected evaluation timing while preserving the real executor, injection driver, numerical oracle, and unchanged runtime safety thresholds. A complementary deterministic upper-overrun test requires VERIFY_FAILED. The third full suite above passed.

The first Linux buffered run, before the parent-alias fix, passed 172 cases. Its direct run was stopped after the final-source correction became available; neither is counted as final evidence. The two complete final-source runs in `linux/` supersede them.
