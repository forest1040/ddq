# PBS 265179: MPI startup failure

Explicitly authorized on 2026-10-16. Source overlay SHA256 9cdb8c95f8b6d801d79f58ec35d360d37ee5ac442b2e986f1213bd977481014e (production 9eb426f). PBS placed the job on CPUcluster009 and CPUcluster010; bootstrap verified the overlay and local capacity on both hosts.

The launcher then imported mpi4py outside mpiexec with pml=cm/mtl=psm2. That singleton import attempted MPI_Init, which failed because OMPI_MCA_orte_precondition_transports was absent. PBS Exit_status=14; walltime=7 s. None of the eight runtime worker cases ran. There is no cross-host acceptance result and no results archive. stdout, stderr and qstat history are retained verbatim.

The next launcher must avoid singleton MPI initialization and return diagnostic logs on early failure. A new job requires the concrete per-job approval specified in docs/phase4_remaining_design.md section 7.
