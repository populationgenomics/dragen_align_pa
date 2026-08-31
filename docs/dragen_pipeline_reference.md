# DRAGEN Pipeline Reference

DRAGEN version 3.7.8, running as an ICA Nextflow pipeline.

For detailed option semantics, consult the DRAGEN 3.7 guide (path and section map in `CLAUDE.md`).
For ICA submission code, see `src/dragen_align_pa/jobs/run_align_genotype_with_dragen.py`.

---

## Enabled Modules

Set in `_build_common_parameters()` (`run_align_genotype_with_dragen.py:165`). All values are strings.

| Module | ICA param code | Value | Notes |
|--------|---------------|-------|-------|
| Mapping + alignment | `enable_map_align` | `true` | |
| CRAM output | `enable_map_align_output` | `true` | Excluded from bulk download |
| Output format | `output_format` | `CRAM` | |
| Duplicate marking | `enable_duplicate_marking` | `true` | Marks only, does not remove |
| Small variant calling | `enable_variant_caller` | `true` | |
| gVCF mode | `vc_emit_ref_confidence` | `GVCF` | gVCF only; excluded from bulk download |
| VCF output | `vc_enable_vcf_output` | `false` | No VCF, only gVCF |
| CNV calling | `enable_cnv` | `true` | |
| CNV segmentation | `cnv_segmentation_mode` | `SLM` | Shifting Level Models |
| SV calling | `enable_sv` | `true` | |
| CYP2D6 | `enable_cyp2d6` | `true` | Pharmacogenomics; guide L11142 |
| Repeat expansion | `repeat_genotype_enable` | `true` | ExpansionHunter; guide L10853 |
| HTML reports | `dragen_reports` | `false` | Disabled |

---

## Mode-Specific Settings

### FASTQ mode (`reads_type = "fastq"`)

**Data inputs** (`AnalysisDataInput`):

| `parameterCode` | Content |
|----------------|---------|
| `fastqs` | List of individual FASTQ ICA file IDs |
| `fastq_list` | FASTQ list CSV ICA file ID |
| `qc_coverage_region_beds` | Both BED ICA file IDs as a list (region 1, region 2) |

**`additional_args`**:
```
--qc-coverage-reports-1 cov_report,cov_report
--qc-coverage-filters-1 'mapq<1,bq<0,mapq<1,bq<0'
--vc-gvcf-gq-bands 13 20 30 40
```

### CRAM mode (`reads_type = "cram"`)

**Data inputs** (`AnalysisDataInput`):

| `parameterCode` | Content |
|----------------|---------|
| `crams` | CRAM ICA file ID |
| `cram_reference` | Reference FASTA folder ID (currently `dragmap`) — a folder, not a file |
| `qc_coverage_region_1` | BED ICA file ID |
| `qc_coverage_region_2` | BED ICA file ID |

**`additional_args`**:
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
--vc-gvcf-gq-bands 13 20 30 40
```

### Common data inputs (both modes)

| `parameterCode` | Content |
|----------------|---------|
| `ref_tar` | DRAGEN hash table ICA file ID — a pre-built hash table, NOT a FASTA (guide L764) |
| `qc_cross_cont_vcf` | Cross-contamination VCF ICA file ID |

---

## Expected Output Files

`<prefix>` = sequencing group name (e.g. `CPG123456`). All files land in ICA at:
```
/<bucket>/<output_folder>/<sg>/<sg><ar_guid>-<pipeline_id>/<sg>/
```
then streamed to GCS under `dragen_metrics/<sg>/` by `DownloadDataFromIca`.

### QC metric CSVs — expected

MultiQC collects these via `rglob('*.csv')` — there is no validation step; missing files are silently
absent from the report rather than causing an error.

| File pattern | Module | Guide section |
|-------------|--------|--------------|
| `<prefix>.mapping_metrics.csv` | Mapping / alignment | L13066 |
| `<prefix>.vc_metrics.csv` | Variant calling | L13066 |
| `<prefix>.time_metrics.csv` | Runtime | L13066 |
| `<prefix>.wgs_coverage_metrics.csv` | WGS coverage summary | L13756 |
| `<prefix>.wgs_fine_hist.csv` | WGS coverage fine histogram | L13756 |
| `<prefix>.wgs_hist.csv` | WGS coverage histogram | L13756 |
| `<prefix>.wgs_overall_mean_cov.csv` | WGS overall mean coverage | L13756 |
| `<prefix>.wgs_contig_mean_cov.csv` | WGS per-contig mean coverage | L13756 |
| `<prefix>.wgs_ploidy.csv` | WGS predicted ploidy | L13756 |
| `<prefix>.<region1>_coverage_metrics.csv` | Coverage region 1 summary | L13756 |
| `<prefix>.<region1>_fine_hist.csv` | Coverage region 1 fine histogram | L13756 |
| `<prefix>.<region1>_hist.csv` | Coverage region 1 histogram | L13756 |
| `<prefix>.<region1>_overall_mean_cov.csv` | Coverage region 1 overall mean | L13756 |
| `<prefix>.<region1>_contig_mean_cov.csv` | Coverage region 1 per-contig mean | L13756 |
| `<prefix>.<region1>_ploidy.csv` | Coverage region 1 predicted ploidy | L13756 |
| `<prefix>.<region2>_coverage_metrics.csv` | Coverage region 2 summary | L13756 |
| `<prefix>.<region2>_fine_hist.csv` | Coverage region 2 fine histogram | L13756 |
| `<prefix>.<region2>_hist.csv` | Coverage region 2 histogram | L13756 |
| `<prefix>.<region2>_overall_mean_cov.csv` | Coverage region 2 overall mean | L13756 |
| `<prefix>.<region2>_contig_mean_cov.csv` | Coverage region 2 per-contig mean | L13756 |
| `<prefix>.<region2>_ploidy.csv` | Coverage region 2 predicted ploidy | L13756 |
| `<prefix>.cnv_metrics.csv` | CNV | L8660 |
| `<prefix>.ploidy_estimation_metrics.csv` | Ploidy estimator | L12799 |
| `<prefix>.fragment_length_hist.csv` | Insert-length histogram | L5002 |

### QC metric CSVs — conditional

| File pattern | Condition | Guide section |
|-------------|-----------|--------------|
| `<prefix>.trim_metrics.csv` | CRAM mode only (`--read-trimmers polyg`) | L5276 |
| `<prefix>.fastqc_metrics.csv` | If DRAGEN FastQC enabled (check pipeline config) | L5473 |

### Variant / call files (downloaded by bulk stage)

| File pattern | Module | Notes |
|-------------|--------|-------|
| `<prefix>.cnv.vcf.gz` + `.tbi` | CNV | guide L9649 |
| `<prefix>.seg.called.merged` | CNV | Final segment calls; guide L9632 |
| `<prefix>.sv.vcf.gz` + `.tbi` | SV (top-level) | guide L11958 |
| `sv/results/variants/diploidSV.vcf.gz` + `.tbi` | SV germline calls | guide L11983 |
| `sv/results/variants/candidateSV.vcf.gz` + `.tbi` | SV candidates (unscored) | guide L11983 |
| `sv/results/stats/alignmentStatsSummary.txt` | SV fragment-length quantiles | guide L12512 |
| `sv/results/stats/svLocusGraphStats.tsv` | SV locus graph stats | guide L12512 |
| `sv/results/stats/svCandidateGenerationStats.tsv` | SV candidate generation stats | guide L12512 |
| `sv/results/stats/svCandidateGenerationStats.xml` | SV candidate generation stats (XML) | guide L12512 |
| `sv/results/stats/diploidSV.sv_metrics.csv` | SV passing call counts | guide L12512 |
| `<prefix>.cyp2d6.tsv` | CYP2D6 star-allele genotype | guide L11334 |
| `<prefix>.repeats.vcf.gz` + `.tbi` | Repeat expansion (ExpansionHunter) | guide L10939 |
| `<prefix>.roh.bed` | Runs of homozygosity (default-enabled) | guide L6794 |
| `<prefix>.<region1>_cov_report.bed` | Coverage region 1 full coverage report | guide L13751 |
| `<prefix>.<region2>_cov_report.bed` | Coverage region 2 full coverage report | guide L13751 |

### Excluded from bulk download (separate stages)

| File | Stage |
|------|-------|
| `<prefix>.cram` + `.cram.crai` | `DownloadCramFromIca` |
| `<prefix>.hard-filtered.gvcf.gz` + `.tbi` | `DownloadGvcfFromIca` |
| MLR gVCF | `DownloadMlrGvcfFromIca` |

The exclusion logic is in `ica_utils.list_and_filter_ica_files()` — it rejects anything ending in
`.cram`, `.cram.crai`, `.gvcf.gz`, `.gvcf.gz.tbi`.

---

## ICA Parameter Conventions

**Direct parameters** (`AnalysisParameterInput`) map to DRAGEN CLI flags but use underscores
and no `--` prefix. All values must be strings:
```python
AnalysisParameterInput(code='enable_sv', value='true')   # → --enable-sv true
AnalysisParameterInput(code='output_format', value='CRAM')  # → --output-format CRAM
```

**`additional_args`** passes a freeform string of raw CLI flags verbatim to DRAGEN. Use this for
any flag not exposed as a first-class ICA pipeline parameter.

**Data input IDs**:
- `fil.*` = ICA file ID
- `fol.*` = ICA folder ID
- `ref_tar` must be a file ID pointing to the hash table, not a FASTA path

**`userReference`** format: `{sg_name}_{ar_guid}_` (e.g. `CPG123_abc123_`). Must be unique per run;
also used to construct the ICA output folder path.

---

## ICA Platform Reference

### API

Base URL: `https://ica.illumina.com/ica/rest/api/`  
Auth header: `X-API-Key`  
Content-Type: `application/vnd.illumina.v3+json`

Key endpoints (see ICA help docs for full reference):

| Operation | Endpoint |
|-----------|----------|
| Submit Nextflow analysis | `POST /projects/{projectId}/analysis:nextflow` |
| List project data | `GET /projects/{projectId}/data` |
| List project pipelines | `GET /projects/{projectId}/pipelines` |
| Discover pipeline input params | `GET /pipelines/{pipelineId}/inputParameters` |
| List storage options | `GET /analysisStorages` |

### Request body structure (raw API)

```json
{
  "userReference": "SGname_arguid_",
  "pipelineId": "uuid",
  "analysisStorageId": "uuid",
  "outputParentFolderId": "fol.xxx",
  "analysisInput": {
    "inputs": [{"parameterCode": "ref_tar", "dataIds": ["fil.xxx"]}],
    "parameters": [{"code": "enable_sv", "value": "true"}]
  },
  "tags": {"technicalTags": [], "userTags": [], "referenceTags": []}
}
```

**Note on `analysisStorageId`**: the ICA API docs list this as a required field. The codebase omits it
from `CreateNextflowAnalysis` — the `icasdk` SDK likely supplies a default. If submissions start
failing with a storage-related error, this is the first thing to check. Use `GET /analysisStorages`
to list available options.

### Output folder naming

ICA names the output folder: `{userReference}-{pipelineCode}-{GUID}`. The codebase constructs
`userReference` as `{sg_name}_{ar_guid}_`, which is why the ICA path looks like
`{sg_name}_{ar_guid}_-{pipelineCode}-{GUID}/{sg_name}/`.

Only files written to the Nextflow `publishDir = 'out'` directory are captured by ICA and appear in
the project's data folder. Files written elsewhere in the job environment are discarded.

### FPGA compute resources

DRAGEN runs on FPGA instances. The pipeline uses:
- Preset: `fpga2-medium` (annotation: `scheduler.illumina.com/presetSize`)
- Scratch: `1TiB` (annotation: `volumes.illumina.com/scratchSize`)

These are baked into the ICA pipeline definition, not configurable from the submission payload.

### Useful ICA help doc pages

- Nextflow DRAGEN tutorial: `https://help.ica.illumina.com/tutorials/nextflow/nextflow-dragen-pipeline.md`
- API introduction: `https://help.ica.illumina.com/tutorials/api-introduction.md`
- Nextflow pipeline details: `https://help.ica.illumina.com/project/p-flow/f-pipelines/pi-nextflow.md`
- Analysis execution: `https://help.ica.illumina.com/project/p-flow/f-analyses.md`
- Data management: `https://help.ica.illumina.com/project/p-data.md`
- JSON input form syntax: `https://help.ica.illumina.com/project/p-flow/f-pipelines/json-based-input-forms/inputform.json-syntax.md`
- CLI data transfer: `https://help.ica.illumina.com/command-line-interface/cli-datatransfer.md`

---

## Config Key Reference

```toml
[ica.pipelines]
dragen_version   # label only — not sent to ICA API
fastq            # ICA pipeline UUID for FASTQ mode
cram             # ICA pipeline UUID for CRAM mode
dragen_ht_id     # ICA file ID → ref_tar data input (hash table)

[ica.qc]
cross_cont_vcf       # ICA file ID → qc_cross_cont_vcf data input
coverage_region_1    # ICA file ID → first coverage BED
coverage_region_2    # ICA file ID → second coverage BED

[ica.cram_references]
old_cram_reference   # which reference key to use: 'dragmap' or 'gatk'
dragmap              # ICA folder ID for Dragmap FASTA + FAI
gatk                 # ICA folder ID for GATK FASTA + FAI (currently empty)
```
