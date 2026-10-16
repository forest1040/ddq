# Final Linux lifecycle: initial finite attempt

Production core 9eb426f; supplied validation revision e33c1b4 plus scratch mount. Fixed image SHA256 is in summary.json. 4 CPU / 8 GiB / nonroot / network none / dedicated 16 MiB tmpfs.

Completed: linux-contracts 119 passed, native-vfs-fault-points 13 passed, physical-buffered 73 passed, all without skips. Physical-direct reached its declared 240-second stage deadline and was killed; no final JUnit was produced for that stage, so its partial progress is not counted as acceptance. The two physical ENOSPC stages were not started. Whole attempt status is failed, total 480.614 s. The successful stages remain usable with their exact source manifest and JUnit; this is not a whole-suite pass.

The retry uses a fresh output/scratch and selects only physical-direct with a predeclared 1150-second stage budget and 1200-second whole-command limit. Physical ENOSPC and the full MPI checkpoint matrix are separate follow-ups. All initial diagnostics are retained; runner changes affect validation only, not production source.
