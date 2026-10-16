# Final physical ENOSPC: passed

Production core 9eb426f, validation revision ca01d57. Buffered 15 passed / 0 skipped (3.275 s); direct 15 passed / 0 skipped (3.225 s). Dedicated empty container-local 16 MiB tmpfs; nonroot / network none / 4 CPU / 8 GiB. Each stage budget was declared 500 s and the whole command 1200 s. The 6.868-second run passed.

The suite covers catalog/trace/journal/sidecar/extent, admission after an external process consumes actual filesystem space, min-free/exhausted refusal before mutation, old-cut/recovery checks. The direct label applies to the extent payload; SQLite/append/JSON are their own usual I/O paths. Source inventory, real O_DIRECT fd/readback/birth/native-VFS probe, JUnit and all logs are retained. This is real kernel ENOSPC, distinct from logical quota or a fault mock.
