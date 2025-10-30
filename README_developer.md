# DRAGEN Align PA Pipeline - Developer Documentation

## 1\. Overview

`dragen_align_pa` is a `cpg-flow` pipeline for orchestrating DRAGEN alignment/realignment on the Illumina Connected Analytics (ICA) platform.

This pipeline functions as a stateful "manager" or "wrapper." Its primary responsibility is to manage data ingress/egress and remotely submit, monitor, and manage the lifecycle of external jobs on the ICA platform. It is not a self-contained workflow.

## 2\. Architecture & Core Components

  * **Framework:** The pipeline is structured as a standard `cpg-flow` workflow. The DAG, stage dependencies, and Metamist outputs are defined in `src/dragen_align_pa/stages.py`.
  * **Entrypoint:** The pipeline is packaged as a Python script entrypoint named `dragen_align_pa`, which resolves to `dragen_align_pa.run_workflow:cli_main`.
  * **Build Environment:** The `Dockerfile` uses `cpg_hail_gcloud` as its base and installs all necessary external dependencies for interacting with ICA, including the `ica-sdk-python`, the `icav2` CLI, the vendored `popgen_cli` wheel, and `somalier`.

### Core Abstractions

This pipeline's logic is heavily abstracted to handle its stateful, external-platform nature.

1.  **ICA Interaction (`ica_utils.py`):**
    This module is the single source of truth for all ICA interactions, both SDK and CLI. It uses `google.cloud.secretmanager` to fetch ICA credentials (`projectID` and `apiKey`). Key functions include:

      * `check_object_already_exists` / `create_upload_object_id`: These functions prevent data re-uploads by checking for existing files/folders in ICA before creating new ones.
      * `stream_file_to_gcs_and_verify`: Used by download stages. This function gets a pre-signed URL from ICA, streams the file directly to a GCS blob, and simultaneously calculates the MD5 hash for verification against the expected hash (if provided).

2.  **State Management (`ica_pipeline_manager.py`):**
    This is the heart of the pipeline. The `manage_ica_pipeline_loop` function is a generic, polling-based state machine. It bridges the gap between the Hail Batch job and the external ICA job by:

      * Checking for a pre-existing output JSON (e.g., `{sg_name}_pipeline_id_and_arguid.json`) to see if a job has already been submitted.
      * If no file exists, it calls a `submit_function_factory` to launch the job and writes the new `pipeline_id` to the JSON file.
      * If a file *does* exist, it reads the `pipeline_id` and polls `monitor_dragen_pipeline.run()`.
      * It explicitly checks the `ica.management.cancel_cohort_run` config flag to enter a cancellation branch, calling `cancel_ica_pipeline_run.run()`.
      * It handles `FAILED` and `FAILEDFINAL` states, supporting retries.

3.  **Large File I/O (`upload_data_to_ica.py`):**
    The Python `ica-sdk` has limitations on file upload sizes. To handle multi-gigabyte CRAM files, this job uses a hybrid CLI approach:

    1.  A `PythonJob` is initialized.
    2.  Inside the job, `gcloud storage cp` downloads the CRAM from GCS to the job's local disk.
    3.  `icav2 projectdata upload` (the CLI tool) is then called to upload the local file to ICA, which correctly handles large-scale, resumable uploads.

## 3\. Pipeline Workflow (DAG)

The workflow has two distinct entry paths based on the `workflow.reads_type` config value.

### Path 1: `reads_type = "fastq"`

This path is for MD5 validation and preparation of FASTQ inputs.

1.  **PrepareIcaForDragenAnalysis**: Creates analysis output folders in ICA.
2.  **FastqIntakeQc**: Manages the "MD5 Checksum" external pipeline using the `ica_pipeline_manager`.
3.  **DownloadMd5Results**: Downloads the `all_md5.txt` from the completed MD5 job.
4.  **ValidateMd5Sums**: Compares the ICA-generated checksums against the manifest file specified in the config (`workflow.manifest_gcp_path`). Fails if a mismatch is found.
5.  **MakeFastqFileList**: Generates the DRAGEN-required `fastq_list.csv` from assay metadata.
6.  **UploadFastqFileList**: Uploads the generated `fastq_list.csv` to ICA.

### Path 2: `reads_type = "cram"`

This path is for CRAM realignment.

1.  **PrepareIcaForDragenAnalysis**: Creates analysis output folders in ICA.
2.  **UploadDataToIca**: Uploads the source CRAM from GCS to ICA using the hybrid CLI method.

### Common Processing Path

Both paths converge, providing the necessary inputs to `ManageDragenPipeline`.

1.  **ManageDragenPipeline**: The main state-machine job. It submits the appropriate DRAGEN pipeline (FASTQ or CRAM) and polls for completion.
2.  **ManageDragenMlr**: A subsequent state-machine job that submits the MLR pipeline via `popgen-cli` and monitors for completion.
3.  **Download Stages**: A set of parallel stages that download specific outputs using the streaming/verifying method (`DownloadCramFromIca`, `DownloadGvcfFromIca`, `DownloadMlrGvcfFromIca`, `DownloadDataFromIca`).
4.  **QC Stages**:
      * `SomalierExtract`: Runs `somalier extract` on the newly downloaded CRAM.
      * `RunMultiQc`: Aggregates all QC metrics from `DownloadDataFromIca` and `SomalierExtract`.
5.  **DeleteDataInIca**: A final cleanup stage that collects all FIDs (both source data and generated data) and deletes them from ICA to manage storage costs.

## 4\. ICA Pipeline Contracts

The pipeline is configured to use two different underlying DRAGEN pipeline definitions in ICA, which have different input parameter contracts. The logic in `run_align_genotype_with_dragen.py` correctly handles this branching.

  * **FASTQ Pipeline** (e.g., `dragen_3_7_8`):

      * Expects a *single*, multi-value input: `qc_coverage_region_beds`.
      * The code correctly passes `[qc_cov_region_1_id, qc_cov_region_2_id]` to this single parameter.
      * Associated arguments (e.g., `--qc-coverage-reports-1`) are passed via the `additional_args` parameter.

  * **CRAM Pipeline**:

      * Expects *two distinct*, single-value inputs: `qc_coverage_region_1` and `qc_coverage_region_2`.
      * The code correctly passes the file IDs to these separate parameters.

This logic is crucial as it was the source of the `Could not open BED file` error.

## 5\. Configuration

All configurable parameters are defined in `config/dragen_align_pa_defaults.toml`.

**Key configuration options:**

  * `[workflow]`:
      * `input_cohorts`: List of Metamist cohort IDs.
      * `last_stages`: Defines the terminal stage(s) for the `cpg-flow` runner (e.g., `['RunMultiQc']`).
      * `reads_type`: Critical. Must be `"fastq"` or `"cram"`.
      * `manifest_gcp_path`: Required if `reads_type = "fastq"`. Points to the manifest CSV containing checksums.
  * `[ica.projects]`: Defines the ICA project IDs for alignment and MLR.
  * `[ica.management]`:
      * `cancel_cohort_run`: `true`/`false`. This is the state toggle for initiating a cancellation on the next run.
  * `[ica.pipelines]`: Contains the ICA pipeline definition IDs (e.g., `dragen_3_7_8`, `md5_pipeline_id`). The correct alignment pipeline ID is selected dynamically using `config_retrieve(['workflow', 'reads_type'])` as the key.
  * `[ica.cram_references]`:
      * `old_cram_reference`: Required if `reads_type = "cram"`. This string (e.g., `"dragmap"`) is used as a key to look up the corresponding ICA folder ID from this same section.

## 6\. Execution & State Management

Launch the pipeline via `analysis-runner`. The `dragen_align_pa` command is the script entrypoint. The `--output-dir` is unused by the workflow but required by `analysis-runner`.

```bash
analysis-runner \
--dataset <your-dataset> \
--access test \
--config <path/to/your-config.toml> \
--output-dir '' \
--description "DRAGEN alignment for <your-cohort>" \
--image "australia-southeast1-docker.pkg.dev/cpg-common/images/dragen_align_pa:<image-tag>" \
dragen_align_pa
```

### Resuming a Run

The pipeline is inherently resumable. The `ManageDragenPipeline` stage (and other "manager" stages) first checks for the existence of its output `.json` file (e.g., `{sg_name}_pipeline_id_and_arguid.json`).

  * **If file exists:** The job reads the `pipeline_id`, skips submission, and moves directly to polling the status of that `pipeline_id`.
  * **If file does not exist:** The job attempts to submit a new pipeline.

This allows the `analysis-runner` job to be safely re-launched; it will automatically reconnect to the external jobs it was managing.

### Cancelling a Run

To cancel an in-progress ICA run:

1.  Stop the `analysis-runner` (Hail Batch) job.
2.  Set `ica.management.cancel_cohort_run = true` in the TOML config.
3.  Re-launch the pipeline.
4.  The `manage_ica_pipeline_loop` will detect this flag, read the `pipeline_id` from the JSON file, and call `cancel_ica_pipeline_run.run()` to send an abort request to the ICA API.

---

<sub><sup>This README was generated in part by Gemini 2.5 Pro.</sup></sub>
