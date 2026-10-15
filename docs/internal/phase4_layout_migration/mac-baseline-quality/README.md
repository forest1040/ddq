# Baseline quality smoke (2026-10-15)

Mac, same-host MPI 2 rank, buffered, MPI injection 0.1 seconds/call, warmup 0 and paired 1. These runs check functionality and conservative refusal; one pair is not performance significance evidence.

- `source-state.json`: revision, dirty status and SHA256 of the four relevant source files used for final calibration and both final campaigns. Unit tests from other agents were allowed in the background; no other performance campaign ran concurrently.
- `profile/executor-profile-rank*.json`: final calibration, 24 gates and 12 swaps; each rank exported three certified rows. Calibration was rerun after fixing no-op injection accounting. The profile fingerprint identifies the precommit revision; it must be recalibrated after integration changes the revision.
- `sustained.json`: fixed execute 6.084 s, auto 1.479 s, auto switched once and passed VERIFY. Both prefixes passed evaluation-only certification with held-out coverage 1.0. Predicted lower gain 2.300 s, observed gain of this pair 4.605 s.
- `missing-baseline.json`: both arms completed oracle validation, switched zero times, and reported all 32 events as `INSUFFICIENT_BASELINE`, despite attaching the executor profile.
- `sustained-initial-noop-refusal.json`: first diagnostic run, before the final source state. The new noninjected-prefix filter exposed that no-op resource tickets incorrectly added bookkeeping time to `resource_injected_seconds`; both arms therefore refused. Kept as diagnostic evidence, not folded into the final pair. It predates the planned-delay accounting fix and the additional `baseline_quality` field in `events_head`; the original calibration for this diagnostic run was replaced by final calibration.

Commands (run from this worktree, with `PYTHONPATH=src`, `OMP_NUM_THREADS=1` and the shared venv Python):

```sh
mpiexec -n 2 /Users/tmori/work3/ddq/.venv/bin/python examples/p4_004_layout_migration.py --output docs/internal/phase4_layout_migration/mac-baseline-quality/profile --gates 24 --swaps 12
mpiexec -n 2 /Users/tmori/work3/ddq/.venv/bin/python examples/phase4_remaining_campaign.py --action adaptive --resource mpi --injection-delay .1 --repetitions 1 --warmup 0 --output docs/internal/phase4_layout_migration/mac-baseline-quality/sustained.json --scratch /private/tmp/ddq-migration-quality-sustained-final --executor-profile docs/internal/phase4_layout_migration/mac-baseline-quality/profile
mpiexec -n 2 /Users/tmori/work3/ddq/.venv/bin/python examples/phase4_remaining_campaign.py --action adaptive --scenario missing-baseline --resource mpi --injection-delay .1 --repetitions 1 --warmup 0 --output docs/internal/phase4_layout_migration/mac-baseline-quality/missing-baseline.json --scratch /private/tmp/ddq-migration-quality-missing --executor-profile docs/internal/phase4_layout_migration/mac-baseline-quality/profile
```

An initial sandbox launch could not bind localhost sockets and was rerun under the approved escalation. One missing-baseline command used an unrecognized option, exited in argparse before running, and was corrected to `--scenario`; it contributes no trial.

Linux O_DIRECT was not rerun. No claim is made that the earlier device variability or VERIFY envelope issue is resolved on that device.
