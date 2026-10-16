# Phase 4 wave 3 validation (2026-10-16)

The user requested continuation until P4-009 and P4-002 are DONE. This evidence
is an implementation checkpoint; neither item is declared complete here.

## Mac integration

- Fixed production source through `9eb426f`: `PYTHONPATH=src .venv/bin/python -m pytest -q -m 'not slow'`:
  **1317 passed, 38 skipped, 83 deselected, 280.88 seconds**. Full log: [mac-fast-final.txt](mac-fast-final.txt).
- The subsequently added incomplete-overhead guard changes only the campaign
  evaluator and its regression: **5 passed**, 0.32 seconds. It keeps a missing
  pair inconclusive even when the successful subset has a one-sided CI.
- Reader/bootstrap merge validation: **98 passed, 1 skipped**, 34.64 seconds;
  [mac-reader-bootstrap.txt](mac-reader-bootstrap.txt). The read-only shard
  provenance check and the mkdir-before-admission prohibition both remain active.
- Initial failures are retained: [six regressions](mac-fast-initial-failure.txt),
  [collective export settlement race](mac-fast-export-race.txt), and
  [standalone reader plus old diagnostic assertion](mac-fast-reader-and-diagnostic.txt).
  The export regression now deliberately delays a peer's release; adding a
  barrier to the caller was not the fix.

## Linux

The user-approved export contains **only src/, tests/, examples/** to the
existing `ent:/tmp/ddq-p4-linux-followup-20261015-tmori/context/`. No Docker,
project metadata, credentials or repository metadata were exported.
[run_linux.sh](run_linux.sh) uses the existing fixed-hostname non-root image,
read-only code mounts, network disabled, 4 CPUs, 8 GiB RAM and finite timeouts.

- [Initial integration](linux-initial/): buffered **305 passed, 2 deselected**,
  537.87 seconds. O_DIRECT **304 passed, 1 failed, 2 deselected**, 562.78 seconds.
  The failure was the same shared-ledger export-settlement race found on Mac.
  The direct run first proved `linux-o-direct` write/fsync/readback and statx birthtime.
- [First follow-up](linux-functional-initial/): **76 passed, 1 failed**, 58.02 seconds.
  The cross-volume test already refused before mutation, but its regex expected
  the later volume check instead of the new earlier managed-root check. Both
  valid early rejection reasons are now accepted.
- [Revised selected tests](linux-functional-v2/): buffered **83 passed**,
  158.11 seconds, including bootstrap, private coordinator, migration intent,
  checkpoint chunks, real MPI and export. Full logs/JUnit/source hashes are saved.

Source manifests are authoritative for each run. The initial integration source
is earlier than several follow-ups; it is not presented as final-source acceptance.
Among 269 files in the v2 manifest, four differ from the final Mac production
source: private_disk_budget.py, storage.py, stored_runtime.py and
 test_checkpoint_recovery.py. These are the later standalone-reader/fork fixes;
new Linux validation for those and final lifecycle checks remains pending.

## Runtime measurements (running)

[run_performance.sh](run_performance.sh) records the finite sequential campaign.
It uses a separate immutable `wave3-performance-context-v2` snapshot, so later
source syncs cannot change a running trial. Source hashes accompany results.
Each P=1/2 and buffered/direct configuration runs small and long workloads,
2 warmup pairs + 7 measured pairs with seed 20261016, followed by held-out checks
for six workloads, S0/S1/S2, file/RAM and fractions 0.4/0.6/0.8 (one trial each).
The long workload is explicitly 120 layers. Each command has a 3600-second limit.

At this checkpoint the first configuration is still running. Seven small measured
pairs are complete, but the campaign and long-workload result are incomplete;
no final overhead or accuracy claim is made. Failure, timeout and unknown records
will be retained. The source evaluator used in the running snapshot predates the
incomplete-CI guard: any incomplete summary must be treated as inconclusive.
`reported_extraction_seconds` is a subset of `capture_wall_seconds`; they must
not be added together. Empty backend identity arrays do not prove amplitude
I/O used O_DIRECT. Actual I/O capability has a separate readback proof above.

## Slides and remaining acceptance

The added slide was visually checked and LuaLaTeX builds 40 pages without overfull
warnings. TeX records the final Mac and revised buffered test counts.
Actual cross-host submission is prepared separately in
[phase4_crosshost_acceptance](../phase4_crosshost_acceptance/README.md), and remains
unsubmitted pending the required new-job authorization. Final Linux lifecycle
checks and completed held-out/overhead measurements are also outstanding.


## Follow-up: cross-host startup correction and restartable measurement (2026-10-16)

PBS 265179 reached two nodes but failed in singleton MPI diagnostics before any worker ran. The launcher now avoids MPI initialization outside mpiexec and returns failure logs through an EXIT trap. Its local regression passed 8 cases. The separately authorized retry, PBS 265180, passed all 8 actual cross-host cases (2 hosts / 4 ranks; buffered/direct × run/mismatch/unsupported/recover), Exit_status 0, walltime 33 s. [Complete evidence](../phase4_crosshost_acceptance/job-265180/README.md) retains source checks, capacity/cleanup logs and the first failed attempt.

The 120-layer performance v2 campaign was resource-aborted before any long measured pair: the first long warmup pair took 1315 s, so 9 pairs could not fit its 3600 s command budget. [All partial rows](linux-performance-v2-aborted/README.md) are retained and inconclusive. The independent [v3 plan](performance-v3-plan.md) fixes long layers at 20 while retaining warmup 2 / paired measured 7, and uses fresh per-attempt output/scratch. A new result audit preserves unknown/error outcomes and reports absolute seconds and relative errors; extraction is nested in capture time, never added twice.

The follow-up focused fast checks passed **16 tests** (overhead incomplete-CI guard, exact v3 artifact grid, PBS launcher failure modes), [log](mac-wave4-focused.txt). Production core remains 9eb426f, already covered by 1317 passed / 38 skipped / 83 deselected. The latest additions here are validation tooling and documentation. TeX/PDF was rebuilt successfully (40 pages, no overfull boxes) to record the actual cross-host result. P4-002 and P4-009 stay IN_PROGRESS until the outstanding Linux lifecycle/checkpoint and endpoint/measurement results are reviewed.


## P4-009 final Linux and cross-host acceptance (2026-10-17)

The final disk-admission production source is `9eb426f`. Every `src/` file in the saved final Linux inventories matches that source. Later changes through validation `26ddac5` affect test tooling, reporting and documentation. The original 009A–D criteria and writer coverage are reviewed in [the acceptance matrix](../../phase4_p4_009_acceptance_2026-10-16.md); the [selection audit](../../phase4_p4_009_test_selection_coverage_2026-10-16.md) explains all 38 Mac skips and 83 slow exclusions rather than waiving them.

| Final evidence | Passed | Non-applicable skips | Notes |
|---|---:|---:|---|
| [Linux contracts](linux-lifecycle-initial/README.md) | 119 | 0 | latest reader/fork/bootstrap/birth/sidecar/append/migration; actual inode reuse test passed |
| Native SQLite fault points (same initial run) | 13 | 0 | no unavailable-native skip accepted |
| Buffered physical lifecycle (same initial run) | 73 | 0 | private recovery, GC, segment kills/model, extent boundaries |
| [Direct physical lifecycle retry](linux-lifecycle-direct-retry/README.md) | 72 | 1 | exact sub-page short-write test is non-applicable to O_DIRECT; aligned failures remain tested |
| [Physical ENOSPC](linux-lifecycle-enospc/README.md) | 30 | 0 | 15 each buffered/direct, dedicated 16 MiB tmpfs |
| [Complete MPI checkpoint kill/ENOSPC matrix](linux-checkpoint-matrix/README.md) | 60 | 12 | 30 pass each backend; decision rank1 is non-applicable, exact names/reasons checked |
| Total Linux follow-up | **367** | **13** | no test failures; original timeout and failed validation-guard reports preserved |

The direct retry's pytest exit was 0 but its original generic skip guard reported failure. [Read-only recheck](linux-lifecycle-direct-retry/acceptance-recheck.json) accepts only the exact test name/reason on backend direct; the original XML/summary were not rewritten and their hashes are recorded. The first 240-second timeout likewise remains in the initial artifact. This is an explained acceptance review, not a claim that the original launcher was green.

Separately, the explicitly authorized [PBS retry 265180](../phase4_crosshost_acceptance/job-265180/README.md) passed all **8** cases on two actual physical hosts/four MPI ranks, with pinned source checks, capacity checks, cleanup and numerical recovery/fork/gradient oracles. Its first singleton-MPI failure is retained. P4-009 can close using these results together with the existing Mac semantic/guard/power-loss-model evidence. Hardware power-loss/torn-write guarantees and unmanaged filesystem metadata remain outside the original software-admission claim; strict_capacity_guarantee stays false. P4-002 and the other unfinished Phase 4 items remain separate.


The integrated endpoint-prelude core fix `83cfc36` subsequently passed the full Mac fast suite: **1351 passed / 38 skipped / 83 deselected**, 292.77 s ([raw log](mac-wave5-fast-full.txt)); the unchanged disk implementation remains covered, while P4-002's final bounded-analysis review and held-out measurement continue. Validation-focused checks passed **33** before the later container-cleanup-only driver change ([raw log](mac-wave5-focused.txt)). TeX/PDF was rebuilt to **41 pages**, no overfull boxes, and the new P4-009 DONE / M1 slide was visually checked. This intermediate closure is not the final P4-002 acceptance.
