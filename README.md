This repository contains a collection of scripts and utilities for performing various computational chemistry and high-performance computing (HPC) tasks. The tools are primarily designed to work within a PBS Pro cluster environment.

---

### Scripts

The following scripts are included in this repository:

* **`scfcheck`**: A Python script to visually check the progress of calculations (particularly optimization runs) from Gaussian log files.
* **`convergence`**: A Python script that extracts and analyzes data from Gaussian log files (energy, max and RMS displacements, max and RMS gradients) to evaluate the convergence status of the job.
* **`rescheck`**: A Python script that checks the availability of cores and memory on the Alabama Supercomputer HPC cluster and suggests suitable queues for a job.
* **`clusterstats`**: A Python script that calculates and displays cluster utilization statistics by parsing `pbsnodes` output. It tracks compute and GPU resources separately.
* **`rung16`**: A bash script for submitting Gaussian 16 jobs to the Alabama Supercomputer ASA-X cluster managed by PBS Pro. It automatically detects required cores and memory from the input file and selects an appropriate queue.
* **`runinteractive`**: A bash script for submitting interactive jobs on a PBS Pro cluster, with options to check for resource availability and set job parameters.
* **`getorcaG`**: A bash script that extracts the Gibbs free energy from ORCA output files.
