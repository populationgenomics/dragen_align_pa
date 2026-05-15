# Unified DRAGEN ICA Pipeline Migration — Design

**Date:** 2026-05-11
**Integration branch:** `dragen-unified-dev` (cut from `main`; design doc and all migration work live here)
**Feature branches:** cut from `dragen-unified-dev`, merge back via PR
**Scope:** Replace the two existing per-input-type ICA pipelines (`ica.pipelines.cram`, `ica.pipelines.fastq`) with a single unified pipeline (`DRAGEN378-custom-unified-F2-v1`, ICA pipeline ID `18a4baab-a12f-415d-ba8e-10b5bf6834d0`) that handles CRAM/FASTQ inputs and WGS/WES sequencing types, and switch from per-sequencing-group ICA submissions to per-batch submissions.

**ICA pipeline definition:** The authoritative XML for `DRAGEN378-custom-unified-F2-v1` is checked in alongside this document at [`dragen378-custom-unified-f2-v1.xml`](./dragen378-custom-unified-f2-v1.xml). It enumerates the pipeline's input codes (`crams`, `cram_reference`, `fastqs`, `fastq_list`, `qc_coverage_region_beds`, `qc_cross_cont_vcf`, `ref_tar`, `additional_files`) and top-level parameters (`enable_map_align`, `output_format`, `enable_variant_caller`, `enable_sv`, `enable_cnv`, `dragen_reports`, `error_strategy`, `additional_args`). Section 3 below derives the submitter's data-input and parameter shapes from this XML; treat the XML as the source of truth if they ever diverge.

## 1. Stage graph

```
PrepareIcaForDragenAnalysis (Cohort)
        │
        ├──► FastqIntakeQc (Cohort)         [FASTQ only — unchanged]
        │        └──► DownloadMd5Results
        │               └──► ValidateMd5Sums
        │                     └──► MakeFastqFileList     [per-SG CSVs; unchanged]
        │                                                  ✱ NOTE: UploadFastqFileList is REMOVED;
        │                                                  the submitter concatenates per-SG CSVs into
        │                                                  one combined per-batch CSV and uploads inline.
        │
        ├──► UploadDataToIca (SG)           [CRAM only — unchanged]
        │
        └──► ManageDragenPipeline (Cohort)  ✱ rewritten: batched submitter
                ├──► ManageDragenMlr (Cohort)                  ✱ outputs land in parent batch's per-SG folder
                ├──► DownloadCramFromIca (SG)                  ✱ resolves batch from state file
                ├──► DownloadGvcfFromIca (SG)                  ✱ resolves batch from state file
                ├──► DownloadMlrGvcfFromIca (SG)               ✱ same get_ica_sample_folder helper
                                                                 (MLR outputs live in the Dragen batch folder)
                ├──► ReheaderMlrGvcf (SG)                      [unchanged; produces the final registered gVCF]
                ├──► DownloadDataFromIca (SG)                  ✱ per-sample artefacts only
                ├──► DownloadBatchArtefactsFromIca (Cohort)    ✱ NEW: passfail.json / summary.json / reports/
                ├──► SomalierExtract (SG)                      [unchanged]
                └──► DeleteDataInIca (Cohort)                  [out of scope for this PR — handled separately]
```

Net change: one stage (`ManageDragenPipeline`) gains batching logic. Four download stages learn to resolve their batch via a state file. One new cohort-level stage (`DownloadBatchArtefactsFromIca`) handles per-batch artefacts. `DeleteDataInIca` is deferred to a separate work item; the pipeline accepts an interim sub-optimal cleanup state because production runs continue on `main` until the migration is complete.

## 2. Config schema

### Namespacing convention

Stage-specific config lives under `[dragen_align_pa.<stage_name_snake_case>]` (e.g. `[dragen_align_pa.manage_dragen_pipeline]` below). Pipeline-wide infrastructure stays under its existing namespace (`[workflow]`, `[ica.projects]`, `[ica.management]`, `[ica.data_prep]`, `[ica.tags]`, `[ica.api]`, `[images]`, `[manifest]`). This makes stage-owned keys grep-discoverable from the stage class name and keeps reordering within the TOML predictable. Follow-up: `[ica.mlr]` should migrate to `[dragen_align_pa.manage_dragen_mlr]` in Task 19b (Segment B), since that task already edits `manage_dragen_mlr.py` and is the natural place to update its config callsites. `[ica.pipelines.md5]` is out of scope for this migration (the MD5 stage isn't being touched).

### Hardcoded in code (always sent, regardless of WGS/WES or CRAM/FASTQ)

Submitter assembles `additional_args` from:

```
--read-trimmers polyg
--soft-read-trimmers none
--vc-hard-filter 'DRAGENHardQUAL:all:QUAL<5.0;LowDepth:all:DP<=1'
--vc-frd-max-effective-depth 40
--vc-enable-joint-detection true
--qc-coverage-ignore-overlaps true
--qc-coverage-count-soft-clipped-bases true
--qc-coverage-reports-1 cov_report,cov_report
--qc-coverage-filters-1 'mapq<1,bq<0,mapq<1,bq<0'
--vc-gvcf-gq-bands 10 20 30 40
--vc-emit-ref-confidence GVCF
--vc-enable-vcf-output false
--enable-map-align-output true
--enable-duplicate-marking true
--enable-cyp2d6 true
--repeat-genotype-enable true
```

### Top-level pipeline parameters (built in code, sent every run)

`enable_map_align=true`, `output_format=CRAM`, `enable_variant_caller=true`, `enable_sv=true`, `enable_cnv=true`, `dragen_reports=false`, `error_strategy=auto`.

### Config — divergent values + user overrides

```toml
[dragen_align_pa.manage_dragen_pipeline]
pipeline_id = "18a4baab-a12f-415d-ba8e-10b5bf6834d0"
batch_size  = 5

[dragen_align_pa.manage_dragen_pipeline.presets.genome]
cnv_segmentation_mode = "SLM"
additional_args  = "--cnv-enable-self-normalization true"
additional_files = []

[dragen_align_pa.manage_dragen_pipeline.presets.exome]
cnv_segmentation_mode = "HSLM"
additional_args = "--sv-exome true --sv-call-regions-bed <bed-name> --vc-target-bed <bed-name> --cnv-target-bed <bed-name> --cnv-target-factor-threshold 5 --cnv-enable-self-normalization false"
additional_files = []     # ICA file IDs for SV call regions BED, target BED, PoN files

[dragen_align_pa.manage_dragen_pipeline.user]
additional_args  = ""
additional_files = []
```

At submission time the final `additional_args` parameter is:

```
HARDCODED_COMMON + " --cnv-segmentation-mode " + preset.cnv_segmentation_mode + " " + preset.additional_args + " " + user.additional_args
```

and the `additional_files` pipeline input is `preset.additional_files + user.additional_files`.

Final WES preset values (PoN file IDs, target BED IDs, etc.) land via subsequent edits — config stubs ship empty.

### Removed keys

- `ica.pipelines.cram`
- `ica.pipelines.fastq`
- `ica.qc.coverage_region_1`
- `ica.qc.coverage_region_2`

### Replaced keys

- `ica.qc.coverage_region_beds = ["fil.…", "fil.…"]` (ordered list, 0–3 entries; replaces individual region keys)

### Unchanged

`[ica.management]`, `[ica.tags]`, `[ica.projects]`, `[ica.data_prep]`, `[ica.api]`, `[ica.pipelines.md5]`, `ica.pipelines.md5_pipeline_id`, `ica.pipelines.dragen_version`, `ica.pipelines.dragen_ht_id`, `[ica.cram_references]`, `[ica.mlr]`, `ica.qc.cross_cont_vcf` (still optional, kept).

## 3. Submission and batching

### Batching algorithm

1. Sort cohort SG names lexicographically (stable, deterministic across re-runs).
2. Chunk into batches of `batch_size`. The final batch may be smaller.
3. Batches numbered `0..N-1`. Zero-padded to width 4 (`:04d`) — large enough for 10000 batches (≈ 50000 SGs at `batch_size=5`) without lex-sort breakage. The plan deliberately picks fixed width 4 rather than variable `max(2, len(str(N-1)))` so the format is uniform across persisted files and human-readable paths.

### Per-batch ICA submission inputs

| Pipeline input code        | Source                                                                                                          | Notes                                                                                          |
|----------------------------|-----------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `ref_tar`                  | `ica.pipelines.dragen_ht_id`                                                                                    | Single ID, same as today                                                                       |
| `crams` (multi)            | `cram_fid` from each SG's `{sg}_fids.json`                                                                      | CRAM mode only; list of N IDs per batch                                                        |
| `cram_reference`           | `ica.cram_references[ica.cram_references.old_cram_reference]`                                                   | CRAM mode only; folder ID, same as today                                                       |
| `fastqs` (multi)           | All FASTQ file IDs for the SGs in this batch, from cohort `fastq_ids_outpath`                                   | FASTQ mode only                                                                                |
| `fastq_list` (single CSV)  | Per-batch combined CSV (concatenated from per-SG CSVs in-memory inside the submitter, uploaded just before submission) | FASTQ mode only; one CSV per batch                                                             |
| `qc_coverage_region_beds`  | `ica.qc.coverage_region_beds`                                                                                   | 0–3 file IDs                                                                                   |
| `qc_cross_cont_vcf`        | `ica.qc.cross_cont_vcf`                                                                                         | Optional in new XML; still passed if configured                                                |
| `additional_files` (multi) | `preset.additional_files + user.additional_files`                                                               | Same set for every batch in a cohort                                                           |

### Parameters

The seven fixed top-level params (Section 2) + the assembled `additional_args` string. `user_reference = f"{cohort.name}-batch{batch_index:04d}_{ar_guid}_"`.

### Per-batch FASTQ list CSV

FASTQ-mode submissions always send both `fastqs` and `fastq_list` (never rely on the pipeline's filename-based auto-detection). Reason: production samples have 16+ FASTQ files each, so explicit RGSM grouping in a CSV is required to map files to samples deterministically.

Per decision in brainstorming: `MakeFastqFileList` continues to emit per-SG CSVs unchanged. The cohort-level submitter concatenates the per-SG CSVs for each batch in-memory and uploads exactly one combined CSV per batch. Concatenation strategy: write the header once, then append the data rows from each per-SG CSV. The per-batch CSV's filename is `{cohort}_batch{N}_fastq_list.csv`.

### SDK call

The new pipeline is still a Nextflow ICA pipeline. The existing `submit_nextflow_analysis` helper in `ica_api_utils.py` and `CreateNextflowAnalysis` / `NextflowAnalysisInput` bodies stay; `run_align_genotype_with_dragen.py` (renamed: `submit_dragen_batch.py`) builds list-valued `dataIds` instead of single-element ones.

The icav2 CLI invocation `projectpipelines start nextflow $PIPELINEID --input <code>:fid1,fid2,... --parameters <code>:value` corresponds directly to `create_nextflow_analysis` with `AnalysisDataInput(parameterCode=..., dataIds=[...])` and `AnalysisParameterInput(code=..., value=...)` respectively.

## 4. State files

### Per-SG state — `{sg_name}_pipeline_id_and_arguid.json`

Existing path; schema extended:

```json
{
  "pipeline_id": "abc-…",
  "ar_guid": "xyz",
  "user_reference": "COH0001-batch0000_xyz_",
  "batch_index": 0
}
```

Downstream per-SG stages continue to look this up via `inputs.as_dict(stage=ManageDragenPipeline)[f"{sg}_pipeline_id_and_arguid"]`. The file just carries more fields.

### Cohort batches index — `{cohort_name}_batches.json` (new)

```json
{
  "schema_version": 1,
  "batch_size": 5,
  "n_batches": 4,
  "batches": [
    {
      "batch_index": 0,
      "retry_generation": 0,
      "user_reference": "COH0001-batch0000_xyz_",
      "pipeline_id": "abc-…",
      "ar_guid": "xyz",
      "analysis_output_folder_fid": "fol.…",
      "fastq_list_fid": "fil.…",
      "cram_fids": null,
      "sg_names": ["CPG_00001", "CPG_00002", "CPG_00003", "CPG_00004", "CPG_00005"],
      "status": "SUCCEEDED",
      "passfail_seen": true,
      "passfail": {"CPG_00001": "Success", "CPG_00002": "Fail", …},
      "has_been_retried": false,
      "error_strategy": "auto"
    }
  ]
}
```

`retry_generation` is `0` for initial batches and `1` for retry batches; resume logic keys off `retry_generation` + `status` (NOT `has_been_retried`) so in-flight retry batches survive a crash. `error_strategy` is recorded per batch because single-sample retry batches use `continue` (the unified pipeline's `auto` terminates single-sample runs).

Authoritative cohort-level view used by the submitter loop, retry logic, and `DownloadBatchArtefactsFromIca`. Per-SG files are derived from this and exist for compatibility with the existing per-SG download-stage I/O contract.

### Path construction

ICA output path for an SG (used by every per-SG download stage):

```
/{BUCKET_NAME}/{ica.data_prep.output_folder}/{user_reference}-{pipeline_id}/{sg_name}/
```

Note: ICA names the analysis folder `<user_reference>-<pipeline_id>` — i.e. it inserts a single `-` between the submitted `user_reference` and the analysis `pipeline_id`. Because our `user_reference` ends with `_` (per Task 13 of the implementation plan: `f'{batch.name}_{ar_guid}_'`), the rendered folder name is `…_-<pipeline_id>`, which matches the convention in today's code. The explicit hyphen in the path-construction template above (and in `get_ica_sample_folder`, in `_on_succeeded`'s `analysis_folder_name`, and in the demo-bundle fixture) all reflect this single, consistent rule.

### Management flag semantics under batching

- `monitor_previous=true`: reads `{cohort}_batches.json`, picks up monitoring of in-flight batches. If the file doesn't exist, raises.
- `force_resubmit=true`: deletes `{cohort}_batches.json` and all per-SG state files, preserves AR GUID per batch (lifted up from per-SG), re-batches the cohort, submits fresh. Deletion of the versioned per-SG state file is necessary here — it's the only way to achieve a clean state for the re-batch.
- `cancel_cohort_run=true`: aborts every batch with a known `pipeline_id`, marks them `CANCELLED` in the batches file, **preserves the per-SG state files**. The versioned per-SG state file is the single source of per-SG truth and is not deleted unless forced; preserving it keeps AR GUIDs available for a future `force_resubmit=true` (the only sanctioned recovery path after cancel) to harvest and reuse. CANCELLED is a **terminal** state: the orchestrator raises `CohortCancelled` after cancel so the cohort run terminates cleanly; any subsequent rerun without `force_resubmit=true` also raises `CohortCancelled` as soon as ANY CANCELLED batch is detected (regardless of whether other batches succeeded). This unconditional terminal behaviour means downstream Download stages never read per-SG state files for cancelled SGs — they can't see an "analysis aborted" / "analysis not found" ICA error unless a future caller deliberately bypasses the orchestrator's guard.
- Mutually exclusive: `force_resubmit=true` and `cancel_cohort_run=true` cannot be set together; the orchestrator raises a `ValueError` with a suggested cancel→wait→force_resubmit sequence.
- Precedence: if both `force_resubmit=true` and `monitor_previous=true` are set, `force_resubmit` wins and `monitor_previous` is logged-as-ignored.

In addition, the shared `manage_ica_pipeline_loop` mirrors its in-memory `FAILED_FINAL` and `CANCELLED` target transitions into `{cohort}_batches.json` via an `on_status_change` callback supplied by the DRAGEN orchestrator. Without this mirror, the loop's terminal transitions never reach the batches file, leaving entries stuck at `INPROGRESS` and breaking both the per-sample retry path's batch-level-FAILED branch and the resume-skip filter. MLR does not supply this callback (its targets have no equivalent cohort-level state file).

## 5. Per-SG download path resolution

New helper:

```python
def get_ica_sample_folder(pipeline_id_arguid_path: cpg_utils.Path, sg_name: str) -> str:
    """Resolve the ICA folder containing a single SG's batch output.

    Reads the per-SG state file (extended schema) and constructs:
        /{bucket}/{output_folder}/{user_reference}-{pipeline_id}/{sg_name}/
    """
```

| Stage                            | Target type | Reads from                              | Downloads                                                                |
|----------------------------------|-------------|-----------------------------------------|--------------------------------------------------------------------------|
| `DownloadCramFromIca`            | SG          | `get_ica_sample_folder(state, sg)`      | `{sg}.cram`, `{sg}.cram.crai`                                            |
| `DownloadGvcfFromIca`            | SG          | same                                    | `{sg}.hard-filtered.gvcf.gz`, `.tbi`                                     |
| `DownloadMlrGvcfFromIca`         | SG          | Dragen per-SG state file via `get_ica_sample_folder` (MLR outputs land in the parent Dragen batch's per-SG folder per the implementation plan) | `{sg}.hard-filtered.recal.gvcf.gz`, `.tbi` (to `category='tmp'`) |
| `ReheaderMlrGvcf`                | SG          | n/a (consumes base + recal gVCF)        | produces final `recal_gvcf/{sg}.hard-filtered.recal.gvcf.gz[.tbi]` — registered as the cohort's final gVCF via `analysis_type='gvcf'`. Unchanged by migration. |
| `DownloadDataFromIca`            | SG          | `get_ica_sample_folder(state, sg)`      | per-sample artefacts (everything in `<sample_id>/` minus CRAM/gVCF)      |
| `DownloadBatchArtefactsFromIca`  | Cohort      | `{cohort}_batches.json`                 | `passfail.json`, `summary.json`, `reports/` — once per batch             |
| `SomalierExtract`                | SG          | unchanged                               |                                                                          |

Per-SG stages keep their decorators (`analysis_type='cram'`, `analysis_keys=['cram']`, etc.) so Metamist registration is unchanged.

### GCS output layout

Per-SG outputs land at their existing paths (`gs://{bucket}/ica/{DRAGEN_VERSION}/output/{cram,base_gvcf,recal_gvcf,dragen_metrics}/{sg_name}/...`).

New sibling path for per-batch artefacts:

```
gs://{bucket}/ica/{DRAGEN_VERSION}/output/dragen_batch_metrics/{cohort_name}_batch{NN}/
    ├── passfail.json
    ├── summary.json
    └── reports/
```

## 6. Retry and error handling

### Constraint: MLR's monitoring loop must stay unchanged

`manage_ica_pipeline_loop` in `ica_pipeline_manager.py` is shared between the main DRAGEN pipeline and `ManageDragenMlr`. MLR continues to monitor **per sequencing group** (`is_mlr_pipeline=True`, `allow_retry=False`, one ICA submission per SG) and that behavior must remain bit-for-bit identical post-migration. The loop is therefore generalized — not rewritten — to accept batch targets alongside the existing SG targets.

### Add a `Batch` target type

Introduce a lightweight target class internal to this module (not a cpg-flow target type — purely a vehicle for the monitoring loop):

```python
@dataclass
class Batch:
    """Internal target representing a batch of SGs for the unified DRAGEN pipeline."""
    cohort_name: str
    batch_index: int
    sg_names: list[str]

    @property
    def name(self) -> str:
        return f"{self.cohort_name}-batch{self.batch_index:04d}"
```

`ProcessingTarget` widens to `Cohort | SequencingGroup | Batch`. Only `.name` is consumed by the loop, so no other protocol surface is needed.

### Loop generalization (additive only)

The existing `manage_ica_pipeline_loop` state machine (PENDING / INPROGRESS / SUCCEEDED / FAILED_RETRYING / FAILED_FINAL / CANCELLED, the 5% threshold, the `force_resubmit` / `cancel_cohort_run` / `monitor_previous` handling) is preserved verbatim. Two purely additive hooks let the DRAGEN caller plug in batch-aware behaviour without touching MLR's call site:

```python
def manage_ica_pipeline_loop(
    ...,
    on_succeeded: Callable[[MonitoredTarget], None] | None = None,
    ...,
) -> None:
```

- `on_succeeded`: optional callback invoked when a target transitions to `SUCCEEDED`. DRAGEN uses it to download `passfail.json` from the batch root and persist per-SG `Success`/`Fail` into `{cohort}_batches.json`. MLR omits this callback; its behaviour is unchanged.
- Whole-target retry (existing `allow_retry` flag) is preserved exactly. For batches it applies when the ICA-level analysis itself reports `FAILED` / `FAILEDFINAL` (infrastructure failure, not per-sample DRAGEN failure). MLR keeps `allow_retry=False`.

### Per-sample retry orchestration (DRAGEN-only, outside the loop)

Per-sample retry lives in `manage_dragen_pipeline.py`, not in the shared loop:

1. **First pass**: build initial `Batch` targets from the cohort, call `manage_ica_pipeline_loop(targets=initial_batches, on_succeeded=record_passfail, ...)`.
2. **After the loop returns**: read `{cohort}_batches.json`, collect every SG marked `Fail` (whether from `passfail.json` of a successful batch or from a `FAILED` batch where every SG is implicitly `Fail`).
3. **If any failures remain and none have been retried**: chunk them into one or more retry `Batch` targets (`batch_index = n_initial + i`), mark them and their SGs `has_been_retried=true` in the batches file, call `manage_ica_pipeline_loop` again with just the retry batches.
4. **No further retries**. SGs still marked `Fail` after the retry pass move to `FAILED_FINAL`.

This keeps the loop a pure per-target state machine and confines per-sample reasoning to the caller — which is the only caller that has the concept of "samples inside a target".

### Threshold

```
n_failed_final / len(cohort_sgs) > 0.05  →  raise, write errors.log
```

Computed by `manage_dragen_pipeline.py` after the retry pass (not inside the shared loop, since the loop's existing 5% check is over targets and the DRAGEN denominator must be SGs). The existing cancellation-raises-immediately behavior in the shared loop is preserved.

### passfail.json parsing detail

`passfail.json` maps `sample_id → "Success"|"Fail"` where `sample_id` is the RGSM. In our pipeline RGSM equals `sg_name` because:

- FASTQ mode: `MakeFastqFileList` writes RGSM = SG name in each row.
- CRAM mode: the CRAM-to-BAM conversion preserves the RG line whose SM tag was set when the original CRAM was generated — this must be verified during implementation.

Concrete example. For a batch of 5 SGs `[sample1, sample2, sample3, sample4, sample5]`:

- FASTQ mode — the batch CSV row for `sample3` looks like:
  ```
  sample3,1,gs://.../sample3_L1_R1.fastq.gz,gs://.../sample3_L1_R2.fastq.gz,...
  ```
- DRAGEN writes `passfail.json` at the batch root, keyed by RGSM:
  ```json
  {"sample1":"Success","sample2":"Success","sample3":"Fail","sample4":"Success","sample5":"Success"}
  ```
- The orchestrator joins `sample_id → sg_name` directly (no munging) and records the per-SG verdict into `{cohort}_batches.json` via `BatchesFile.record_passfail`. Retry granularity is therefore per-SG: `sample3` alone forms a one-SG retry batch; `sample1/2/4/5` are not re-run. See §6 "Per-sample retry orchestration" for the retry-batch construction rules and the `error_strategy=continue` requirement for single-sample retry batches.

The file is small (KB-scale); fetched in-memory by a new `fetch_passfail_from_ica` helper that resolves the file ID via `ica_api_utils.find_file_id_by_name` and reads the presigned URL with `requests.get(...).json()`. No GCS or disk staging — the dict is consumed directly by `BatchesFile.record_passfail`.

## 7. Validation plan

### Branch strategy

A long-lived integration branch `dragen-unified-dev` is cut from `main` and acts as the merge base for all migration work. `main` is left untouched so production runs keep going on the existing pipeline.

Workflow:

- `dragen-unified-dev` is initialised from `main`.
- `delete-multiqc` retargets onto `dragen-unified-dev` and lands as its own PR (the MultiQC removal becomes the first reviewed change on the integration branch rather than its base).
- Each subsequent unit of migration work lands as its own short-lived feature branch off `dragen-unified-dev`, reviewed and merged back via PR. This avoids chained branches and lets reviewers see one focused change at a time.
- Once the migration is end-to-end validated, `dragen-unified-dev` is merged to `main` in one go.

Implementing the migration as one or several PRs onto `-dev` is a per-piece judgement call during implementation planning — the design above is small enough to fit in one PR but cleanly splits into (a) config schema + state files + submitter, (b) per-SG download path resolver + new batch-artefacts stage, (c) per-sample retry orchestration, if multi-PR review is preferred.

### Synthetic fixture script

Real ICA outputs (the downloaded `ica-demo-bundle/` and any large generated artefacts) must not be committed to git. Instead, a shell script generates a minimal synthetic version of the same directory structure on demand — populated with empty files where bytes don't matter, and small valid-JSON manifests where they do. Unit tests invoke this script (via pytest fixture) to materialise the layout in a `tmp_path`.

**Location**: `tests/fixtures/generate_demo_bundle.sh` (new). The `tests/` dir is new — it ships in this PR alongside the unit tests.

**Gitignore additions**:
- `/ica-demo-bundle/` — real downloaded data
- `/tests/fixtures/ica-demo-bundle/` — script default output (when run without redirecting)

**Script** (`tests/fixtures/generate_demo_bundle.sh`):

```bash
#!/usr/bin/env bash
# Generate a synthetic ICA analysis-output folder matching the layout that
# DRAGEN378-custom-unified-F2-v1 produces. Files are empty stubs; only the
# tiny JSON manifests at the batch root carry real content. Use as a unit-test
# fixture. Do NOT commit the generated output.
#
# Usage:
#   generate_demo_bundle.sh [OUTPUT_ROOT] [SAMPLE_ID ...]
#
# Environment overrides:
#   USER_REFERENCE  default: "test-WGS-2samples-"
#   PIPELINE_ID     default: a fixed dummy UUID
#   FAILED_SAMPLES  space-separated subset of SAMPLES to mark "Fail" in passfail.json

set -euo pipefail

OUTPUT_ROOT="${1:-./tests/fixtures/ica-demo-bundle}"
shift || true
SAMPLES=("$@")
if [[ ${#SAMPLES[@]} -eq 0 ]]; then
    SAMPLES=("CPG_00001" "CPG_00002")
fi

USER_REFERENCE="${USER_REFERENCE:-COH0001-batch0000_test-guid_}"
PIPELINE_ID="${PIPELINE_ID:-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}"
FAILED_SAMPLES="${FAILED_SAMPLES:-}"

# Production convention: `{user_reference}-{pipeline_id}` — the hyphen is
# required (user_reference ends with `_` so the folder is `…_-{pipeline_id}`).
# Matches `get_ica_sample_folder` in `utils.py` so the synthetic bundle is a
# faithful production-layout fixture.
ANALYSIS_DIR="${OUTPUT_ROOT}/analysis/${USER_REFERENCE}-${PIPELINE_ID}"

mkdir -p "${ANALYSIS_DIR}/reports/report_files/samples"
mkdir -p "${ANALYSIS_DIR}/ica_logs/analysis"
mkdir -p "${ANALYSIS_DIR}/ica_logs/work"

# Batch-root manifests
echo '{"system.iap.timestamp":"2026-05-11T00:00:00Z","system.iap.tes":""}' \
    > "${ANALYSIS_DIR}/_tags.json"

# passfail.json — every sample "Success" unless listed in FAILED_SAMPLES
n_failed=0
{
    echo "{"
    last_idx=$((${#SAMPLES[@]} - 1))
    for i in "${!SAMPLES[@]}"; do
        sample="${SAMPLES[$i]}"
        status="Success"
        if [[ " ${FAILED_SAMPLES} " == *" ${sample} "* ]]; then
            status="Fail"
            n_failed=$((n_failed + 1))
        fi
        sep=","
        [[ $i -eq $last_idx ]] && sep=""
        printf '    "%s": "%s"%s\n' "$sample" "$status" "$sep"
    done
    echo "}"
} > "${ANALYSIS_DIR}/passfail.json"

n_completed=$((${#SAMPLES[@]} - n_failed))
cat > "${ANALYSIS_DIR}/summary.json" <<EOF
{
    "num_samples_total": ${#SAMPLES[@]},
    "num_samples_completed": ${n_completed},
    "num_samples_failed": ${n_failed}
}
EOF

# Per-sample subfolders with the file set the download stages care about
for sample in "${SAMPLES[@]}"; do
    sample_dir="${ANALYSIS_DIR}/${sample}"
    mkdir -p "${sample_dir}/logs" "${sample_dir}/sv/workspace" "${sample_dir}/sv/results"

    touch "${sample_dir}/${sample}.cram" \
          "${sample_dir}/${sample}.cram.crai" \
          "${sample_dir}/${sample}.cram.md5sum" \
          "${sample_dir}/${sample}.hard-filtered.gvcf.gz" \
          "${sample_dir}/${sample}.hard-filtered.gvcf.gz.tbi" \
          "${sample_dir}/${sample}.hard-filtered.gvcf.gz.md5sum" \
          "${sample_dir}/${sample}.sv.vcf.gz" \
          "${sample_dir}/${sample}.sv.vcf.gz.tbi" \
          "${sample_dir}/${sample}.cnv_metrics.csv" \
          "${sample_dir}/${sample}.sv_metrics.csv" \
          "${sample_dir}/${sample}.mapping_metrics.csv" \
          "${sample_dir}/${sample}.vc_metrics.csv" \
          "${sample_dir}/${sample}.wgs_coverage_metrics.csv" \
          "${sample_dir}/${sample}.target_bed_coverage_metrics.csv" \
          "${sample_dir}/${sample}.fragment_length_hist.csv" \
          "${sample_dir}/${sample}-replay.json"
done

echo "Generated: ${ANALYSIS_DIR}"
```

**Pytest fixture wiring** (in `tests/conftest.py`):

```python
import shutil
import subprocess
from pathlib import Path

import pytest

FIXTURE_SCRIPT = Path(__file__).parent / "fixtures" / "generate_demo_bundle.sh"


@pytest.fixture
def demo_bundle(tmp_path: Path) -> Path:
    """Materialise a synthetic ICA analysis output bundle under tmp_path.

    Returns the path to the synthesised analysis directory.
    """
    subprocess.run(
        [str(FIXTURE_SCRIPT), str(tmp_path), "CPG_00001", "CPG_00002"],
        check=True,
        env={
            "USER_REFERENCE": "COH0001-batch00_test-guid_",
            "PIPELINE_ID": "00000000-1111-2222-3333-444444444444",
        },
    )
    return tmp_path / "analysis" / "COH0001-batch00_test-guid_00000000-1111-2222-3333-444444444444"
```

The fixture deliberately mirrors the user-reference / pipeline-id convention from Section 4 so path-construction tests can use the bundle directly.

### Phased local validation

1. **Unit-test path construction**: synthesize a per-SG state file pointing at the demo `user_reference` + `pipeline_id`; assert `get_ica_sample_folder` returns the path matching the demo bundle layout. Use the `demo_bundle` pytest fixture above to materialise the structure under `tmp_path`.
2. **Dry-run a small cohort**: an existing `local_test/*.toml` adapted to new config keys, run with `--dry_run` to exercise stage-graph construction without hitting ICA.
3. **Small real cohort (2 SGs, single batch)**: end-to-end FASTQ run with one batch of two SGs. Verify CRAM + gVCF in per-SG GCS paths; `passfail.json`/`summary.json`/`reports/` under `dragen_batch_metrics/{cohort}_batch00/`; Metamist registration per SG via existing decorators.
4. **Two-batch cohort (e.g. 7 SGs, batch_size=5)**: verify batching, two ICA submissions, both finish, downloads land per-SG, batch artefacts land per-batch.
5. **Forced-failure cohort**: poison one SG's input so the pipeline marks it `Fail` in `passfail.json`; verify the retry path forms a new batch with just that SG, retry attempt is honored, final state reflected in the batches file.
6. **Resume test**: kill the analysis-runner job mid-flight; re-launch with `monitor_previous=true`; verify monitoring resumes from the batches file.

### Correctness check against existing pipeline

Pick one cohort already processed by the current `3.2.x` pipeline; rerun under the new code; bytewise-compare gVCFs (after sorting headers if order differs), CRAMs (`samtools quickcheck` + header diff), and Metamist analysis registrations. Diff goes in the PR description.

### Out of scope for this PR (separate follow-ups)

- `DeleteDataInIca` rewrite for batched outputs (interim leaves stage in a sub-optimal state; live cleanup continues on `main`).
- `README.md` rewrite (new pipeline overview, drop MultiQC references, refresh DAG figure).
- Refresh `local_test/*.toml` examples.
- Regenerate `workflow_dag.svg` / `workflow_dag.dot`.
- Pipeline version bump (likely 4.0.0).

## Open items deferred to implementation

- Verify the CRAM input's `cram_reference` directory input still accepts the existing `ica.cram_references.dragmap` / `gatk` folder IDs unchanged.
- Confirm RGSM = `sg_name` is preserved through CRAM-to-BAM conversion for CRAM-mode inputs.
- Confirm the final assembled `additional_args` string fits within any ICA parameter-length limit.
- Final WES preset values (PoN, target BED, SV call regions BED) — supplied by user in follow-on edits.
