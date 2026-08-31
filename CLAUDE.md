# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install with dev dependencies
pip install .[test]

# Lint and type-check (runs ruff + mypy via pre-commit)
pre-commit run --all-files

# Run tests (currently disabled in CI)
pytest test

# Bump version (updates Dockerfile, config TOML, README, creates git tag)
bump2version <major|minor|patch>
```

Linting uses **ruff** (line length 120, single quotes) and **mypy** with strict settings. Configuration is in `pyproject.toml`.

Docker images are built automatically by GitHub Actions:
- Production (`australia-southeast1-docker.pkg.dev/cpg-common/images/dragen_align_pa:<tag>`): on merge to main
- Dev (`australia-southeast1-docker.pkg.dev/cpg-common/images-dev/dragen_align_pa:<branch>-<tag>`): on push to non-main branches

## What This Project Does

A **CPG-Flow pipeline manager** that orchestrates DRAGEN genomic alignment jobs on the **Illumina Connected Analytics (ICA)** platform. It handles data upload (FASTQ or CRAM) to ICA, submits DRAGEN and MLR (Machine Learning Recalibration) pipelines, monitors them to completion, downloads results (CRAM, gVCF, QC) back to GCS, runs Somalier fingerprinting and MultiQC, and optionally cleans up ICA storage.

The pipeline runs via `analysis-runner` (CPG-internal job runner) using config files that override `config/dragen_align_pa_defaults.toml`.

## Architecture

### Two input paths

- **FASTQ path**: validates checksums against a Metamist manifest, generates `fastq_list.csv`, uploads files to ICA
- **CRAM path**: uploads the CRAM directly to ICA for realignment

Both paths converge at `ManageDragenPipeline`.

### Stage DAG (`src/dragen_align_pa/stages.py`)

```
PrepareIcaForDragenAnalysis
├─→ FastqIntakeQc (FASTQ only)
│   ├─→ DownloadMd5Results → ValidateMd5Sums
│   └─→ MakeFastqFileList → UploadFastqFileList
└─→ UploadDataToIca (CRAM only)
    └─→ ManageDragenPipeline  ← convergence point
        ├─→ ManageDragenMlr
        ├─→ DownloadCramFromIca
        ├─→ DownloadGvcfFromIca
        └─→ DownloadMlrGvcfFromIca
            ├─→ DownloadDataFromIca → RunMultiQc
            └─→ SomalierExtract → DeleteDataInIca (optional)
```

Stages that don't apply to the current `READS_TYPE` return `None` from `expected_outputs()`, effectively skipping themselves.

### Key modules

| File | Role |
|------|------|
| `run_workflow.py` | Entrypoint; initialises CPG-Flow workflow |
| `stages.py` | All 16 CPG-Flow stage definitions and dependencies |
| `ica_utils.py` | High-level ICA business logic (SDK + CLI) |
| `ica_api_utils.py` | Low-level ICA REST API wrappers |
| `ica_cli_utils.py` | `icav2` CLI execution helpers |
| `utils.py` | GCS path helpers, manifest queries, subprocess wrappers |
| `constants.py` | `READS_TYPE`, `BUCKET`, `DRAGEN_VERSION` |
| `jobs/ica_pipeline_manager.py` | Generic reusable state-machine loop for ICA pipelines |

### State machine pattern (`jobs/ica_pipeline_manager.py`)

`manage_ica_pipeline_loop()` is reused by `ManageDragenPipeline`, `ManageDragenMlr`, and `FastqIntakeQc`. It:
1. Checks GCS for a JSON state file containing the ICA pipeline run ID
2. Submits a new pipeline if none exists; resumes monitoring if one does
3. Polls until terminal state, supports cancellation and retry flags

This state-file pattern is the mechanism for pipeline resumability — do not remove state file writes.

### ICA access pattern

Large CRAM uploads use the `icav2` CLI (bypasses SDK upload size limits). API-heavy operations (status polling, pipeline submission, folder creation) use the ICA Python SDK. Both are coordinated in `jobs/upload_data_to_ica.py`.

Downloads stream directly from ICA presigned URLs to GCS blobs while computing MD5 in parallel — files are never stored locally.

## Configuration

`config/dragen_align_pa_defaults.toml` is the base config. All keys can be overridden by caller-supplied TOML files passed to `analysis-runner`. Key sections:

- `[workflow]`: cohorts, `reads_type` (`fastq`/`cram`), `last_stages`, `skip_stages`
- `[ica.pipelines]`: ICA pipeline IDs for DRAGEN, MLR, MD5 check
- `[ica.management]`: `cancel_pipeline`, `allow_retry`, `force_resubmit` flags
- `[ica.projects]`: ICA project IDs
- `[ica.cram_references]`: Reference genome folder IDs on ICA

## DRAGEN Pipeline Reference

For pipeline-specific reference (enabled modules, expected output files, ICA parameter conventions,
config keys), see `docs/dragen_pipeline_reference.md`.

## DRAGEN 3.7 Guide

Full guide: `docs/dragen_guide.md` (21 103 lines).
Use `Read` with `offset`/`limit` to jump directly to the relevant section.

### Section map — topics relevant to this pipeline

**QC output files and metrics**

| Topic | Line | Heading |
|-------|------|---------|
| QC metrics overview + output CSV names | 13066 | `## QC Metrics and Coverage/Callability Reports` |
| Coverage metrics report format | 13756 | `#### Coverage Metrics Report` |
| Callability report | 13530 | `### Callability Report` |
| Ploidy estimator output metrics | 12799 | `#### Ploidy Estimator Output Metrics` |
| SV statistics output files | 12512 | `### Statistics Output File` |
| CNV output files (seg + VCF) | 9630 | `### Output Files` |
| Read trimming metrics | 5276 | `### Read Trimming Metrics` |
| FastQC metrics output | 5473 | `### FastQC Metrics Output` |

**Caller options and behavior**

| Topic | Line | Heading |
|-------|------|---------|
| Small variant calling overview | 5831 | `## Small Variant Calling` |
| gVCF mode + GQ bands | 6419 | `#### gVCF and Joint VCF Mode` |
| Germline hard filtering (DRAGENHardQUAL) | 8141 | `### Germline Variant Small Hard Filtering` |
| Joint detection of overlapping variants | 6690 | `### Joint Detection of Overlapping Variants` |
| CNV pipeline + SLM segmentation | 8660 | `## Copy Number Variant Calling` |
| SV calling overview | 11419 | `## Structural Variant Calling` |
| SV VCF output format | 11958 | `### Structural Variant VCF Output` |
| CYP2D6 caller | 11142 | `## CYP2D6 Caller` |
| Repeat expansion (ExpansionHunter) | 10853 | `## Repeat Expansion Detection with Expansion Hunter` |
| Ploidy calling | 12674 | `## Ploidy Calling` |
| Read trimming (poly-G) | 5243 | `## Read Trimming` |

**Input / reference**

| Topic | Line | Heading |
|-------|------|---------|
| Input file types (FASTQ, CRAM, fastq_list) | 3799 | `### Input File Types` |
| Hash table generation + background | 764 | `## Prepare a Reference Genome` |
| CRAM reference requirements | 3799 | within `### Input File Types` |

**Full option reference (for flag lookup)**

| Topic | Line | Heading |
|-------|------|---------|
| General / software options | 18529 | `## General Software Options` |
| Mapper options | 19300 | `## Mapper Options` |
| Aligner options | 19363 | `## Aligner Options` |
| Variant caller options | 19678 | `## Variant Caller Options` |
| CNV caller options | 20279 | `## CNV Caller Options` |
| SV caller options | 20675 | `## Structural Variant Caller Options` |
| CYP2D6 options | 20770 | `## CYP2D6_CommandLine_fDG` |
| Repeat expansion options | 20787 | `## Repeat Expansion Detection Options` |
