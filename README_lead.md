1. __Purpose and Scope__

This document outlines the scope and architectural approach of the dragen_align_pa pipeline.

The pipeline's purpose is to manage DRAGEN genomic alignment jobs on the Illumina Connected Analytics (ICA) platform. It is not a self-contained workflow; rather, it is a pipeline manager that handles the full lifecycle of processing jobs that run on an external system.

The pipeline's scope covers:

* Uploading source data (FASTQ or CRAM files) from GCS to ICA.
* Validating FASTQ data integrity before processing.
* Submitting and monitoring the DRAGEN alignment jobs on ICA.
* Downloading all results (CRAM, gVCF, QC metrics) from ICA back to GCS.
* Running internal QC (Somalier, MultiQC) on the results.
* Automated cleanup of all data (source and generated) from the ICA platform.

2. __Architectural Approach__

The pipeline is designed to safely manage long-running jobs on an external platform (ICA) from within our internal pipeline system (Hail Batch).

3. __Pipeline & State Management__

The core of the pipeline is a "state machine" that controls the external jobs.

When a DRAGEN job is submitted to ICA, the pipeline writes a state file to GCS that contains the external job's ID.

This approach makes the pipeline resumable. If our internal Hail Batch job is interrupted, re-launching it will cause the pipeline to find this state file, read the job ID, and simply resume monitoring the already-running job instead of submitting a duplicate.

4. __Explicit Cancellation__ 
Cancelling a job is an active process.

To cancel, a user sets a "cancel" flag in the pipeline's configuration file and re-launches the pipeline.

The pipeline manager detects this flag, reads the job ID from the GCS state file, and sends an explicit "abort" command to the ICA platform. It then deletes the state file, "resetting" the pipeline for that sample.

5. __Hybrid Tooling__ 
The pipeline uses the best tool for each task:

ICA SDK (Python): Used for all API interactions, such as submitting jobs, checking ob status, and getting download links.

CLIs (Command Line Tools): Used for heavy-lifting operations. For example, large CRAM iles are uploaded using the icav2 CLI, which is more robust for large files than the ython SDK.

6. __Key Workflow Features__

Conditional Input Paths The pipeline has two different behaviors based on the configured input reads_type:

FASTQ Path: This path includes a validation step. It first runs a separate md5 checksum job on all FASTQs in ICA. It then compares these checksums to those in a manifest file provided by the sequencing facility. The main DRAGEN alignment will not begin unless all files are present and all checksums match.

CRAM Path: This is a simpler path used for re-alignment. It focuses on uploading the
source CRAM file to ICA to prepare it for the DRAGEN job.


7. __Data Download & Verification__

Download stages are optimized to stream data directly from  ICA to GCS without landing on the job's local disk. For key outputs (like the final CRAM), the pipeline verifies the file's integrity by checking its MD5 checksum as it's being downloaded.

8. __Automated Cleanup__ 
A final stage is included to manage costs. It gathers the IDs of all files and folders created during the run (both the uploaded source files and the DRAGEN results) and sends a command to delete them all from the ICA platform.

---

<sub><sup>This README was generated in part by Gemini 2.5 Pro.</sup></sub>
