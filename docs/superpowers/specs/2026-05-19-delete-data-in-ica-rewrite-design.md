# DeleteDataInIca Rewrite — Design

**Date:** 2026-05-19
**Branch:** `dragen-delete-data-rewrite` (cut from `dragen-stage-rewiring`; once PR #52 merges, GitHub will retarget the follow-up PR onto `dragen-unified-dev`)
**Scope:** Rewrite `DeleteDataInIca` and `jobs/delete_data_in_ica.py` for the unified DRAGEN pipeline's batched layout, with delete-verification (not fire-and-forget) and correct handling of the two ICA-project topology.

This is the explicit follow-up to the deferral noted in `2026-05-11-unified-dragen-pipeline-design.md` §1 ("`DeleteDataInIca` is deferred to a separate work item") and §7 ("`DeleteDataInIca` rewrite for batched outputs — interim leaves stage in a sub-optimal state; live cleanup continues on `main`").

## 1. What gets deleted, in which project

The pipeline puts data into ICA via two paths and reads it out via one. Cleanup must mirror that asymmetry.

| Item | Owning project | FID source (existing stage) | Delete in project |
|---|---|---|---|
| Cohort-level analysis output folder (cascades: per-batch analysis subfolders, per-SG output subfolders, per-batch FASTQ list CSVs at `{output_folder}/{cohort_name}/{cohort_name}_batch{NN}_fastq_list.csv`) | DRAGEN runs project (`ica.projects.dragen_align`) | `PrepareIcaForDragenAnalysis` (single Path → `{"analysis_output_fid": "fol.XXX"}`) | DRAGEN runs project |
| Uploaded source CRAMs (one FID per SG) | DRAGEN runs project | `UploadDataToIca` (`inputs.as_path_by_target` → dict of per-SG Paths, each holding `{"cram_fid": "fil.XXX"}`) | DRAGEN runs project |
| Linked source FASTQs (one FID per file) | **Supplier project** (newly configured `ica.projects.fastq_source_project_id`) | `FastqIntakeQc.fastq_ids_outpath` (newline-delimited `file_id` per line) | **Supplier project** |
| ICA Analysis records (per-batch metadata entries) | DRAGEN runs project metadata registry | — | **Not deleted.** Metadata only — no storage cost. Orphan entries are acceptable. |

Cascade semantics under ICA: deleting the parent cohort folder propagates removal to every child file and subfolder (per-batch analysis output folders, per-SG output subfolders inside them, per-batch FASTQ list CSVs). Operator-observed and documented.

Project switching under the icasdk: there is no global `enter` concept. Each API call takes `projectId` in `path_params`; switching projects is just threading a different `projectId` value. A single `get_ica_api_client()` connection serves both projects because authentication is API-key-based, not project-scoped.

## 2. Stage shape

`DeleteDataInIca` (CohortStage) — required-stages list unchanged (already correctly waits for all downloads + post-processing). The fence comment / "TEMPORARILY BROKEN" docstring is removed.

```python
@stage(required_stages=[
    PrepareIcaForDragenAnalysis,
    UploadDataToIca,
    DownloadCramFromIca, DownloadGvcfFromIca, DownloadMlrGvcfFromIca, DownloadDataFromIca,
    ReheaderMlrGvcf, SomalierExtract, FastqIntakeQc,
])
class DeleteDataInIca(CohortStage):
    """Delete cohort outputs + source CRAMs/FASTQs from ICA to release storage."""

    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        return get_pipeline_path(filename=f'{cohort.name}_delete_complete.json')

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput:
        cohort_analysis_output_fid_path = inputs.as_path(
            target=cohort, stage=PrepareIcaForDragenAnalysis,
        )
        cram_fid_paths_dict: dict[str, cpg_utils.Path] | None = None
        fastq_ids_list_path: cpg_utils.Path | None = None
        if READS_TYPE == 'cram':
            cram_fid_paths_dict = inputs.as_path_by_target(stage=UploadDataToIca)
        elif READS_TYPE == 'fastq':
            fastq_ids_list_path = inputs.as_path(
                target=cohort, stage=FastqIntakeQc, key='fastq_ids_outpath',
            )

        output_path = self.expected_outputs(cohort=cohort)
        job = initialise_python_job(job_name='DeleteDataInIca', target=cohort, tool_name='ICA')
        job.image(image=get_driver_image())
        job.call(
            delete_data_in_ica.run,
            cohort_name=cohort.name,
            output_path=output_path,
            cohort_analysis_output_fid_path=cohort_analysis_output_fid_path,
            cram_fid_paths_dict=cram_fid_paths_dict,
            fastq_ids_list_path=fastq_ids_list_path,
        )
        return self.make_outputs(target=cohort, data=output_path, jobs=job)
```

**Key change vs current:** `inputs.as_path(...)` (single Path) for the cohort folder where the old code had `inputs.as_dict(...)` (dict of per-SG paths). The dict shape went away with Task 17.

**Note** on the expected-output rename: `_delete_placeholder.txt` → `_delete_complete.json` mirrors `ManageDragenPipeline`'s `_pipeline_complete.json` convention, and the JSON payload carries a small audit summary (counts per project, deleted cohort folder FID) for free.

## 3. Job logic

`delete_data_in_ica.run`:

1. **Resolve FIDs** by source:
   - Read `cohort_analysis_output_fid_path` → one folder FID.
   - If CRAM mode: read each per-SG `cram_fid` from the dict of paths → list of file FIDs.
   - If FASTQ mode: read line-delimited FIDs from `fastq_ids_list_path` → list of file FIDs.

2. **Two project-scoped passes** through `_delete_and_verify`:
   - Runs project: `[cohort_folder_fid, *cram_fids]`. The cohort folder cascades to everything inside; CRAM FIDs are siblings (uploaded by `UploadDataToIca` to a different parent).
   - Supplier project (only if FASTQ mode): `fastq_fids`.

3. **Aggregate failures** from both passes; if any, write a TSV log file and raise.

4. **On success**, write the JSON marker `{cohort.name}_delete_complete.json` (deterministic output for cpg-flow):

   ```json
   {
       "cohort_name": "COH0001",
       "runs_project": {"cohort_folder": "fol.abc...", "cram_count": 50},
       "fastq_source_project": {"fastq_count": 100}
   }
   ```

   `cram_count` is `0` in FASTQ mode; `fastq_source_project` is absent in CRAM mode.

## 4. `_delete_and_verify`

```python
@dataclass(frozen=True)
class DeleteFailure:
    project_id: str
    fid: str
    kind: str           # 'still_present (status=X)' | 'verify_failed'
    context: str        # repr of delete-time exception (if any), plus verify-time exception


def _delete_and_verify(
    api_instance: project_data_api.ProjectDataApi,
    project_id: str,
    fids: list[str],
    settle_seconds: int = 60,
) -> list[DeleteFailure]:
    failures: list[DeleteFailure] = []
    delete_errors: dict[str, str] = {}

    # Phase 1: fire deletes
    for fid in fids:
        path_params = {'projectId': project_id, 'dataId': fid}
        try:
            api_instance.delete_data(path_params=path_params)
        except ApiValueError:
            pass  # known spurious-but-accepted case (icasdk returns None from a non-Optional)
        except ApiException as e:
            delete_errors[fid] = repr(e)

    # Let ICA's async delete propagate; the object goes 'DELETING' before disappearing.
    time.sleep(settle_seconds)

    # Phase 2: verify each FID — accept 404 OR status=='DELETING' as success
    for fid in fids:
        path_params = {'projectId': project_id, 'dataId': fid}
        try:
            response = api_instance.get_project_data(path_params=path_params)
            status = response.body['data']['details'].get('status', 'UNKNOWN')
            if status == 'DELETING':
                continue
            failures.append(DeleteFailure(
                project_id, fid, f'still_present (status={status})', delete_errors.get(fid, ''),
            ))
        except ApiException as e:
            if e.status == 404:
                continue
            failures.append(DeleteFailure(
                project_id, fid, 'verify_failed',
                f'{repr(e)} | delete: {delete_errors.get(fid, "")}',
            ))

    return failures
```

**Why the `time.sleep(60)`**: ICA's `delete_data` is asynchronous — the API call returns immediately but the object transitions through `DELETING` before final removal. Verifying immediately after a delete can race the state machine. 60s is the operational guideline confirmed by the user; tests pass `settle_seconds=0` to skip it.

**Why two phases (not interleaved)**: doing all deletes first, then all verifies, gives ICA loop-iteration time + the 60s sleep to settle all of them in parallel rather than serializing one settle period per FID. For a cohort with 50 CRAMs or hundreds of FASTQs this is the difference between 60s and 50–N minutes.

**Status enum** (`icasdk/model/data_details.py`): `PARTIAL`, `AVAILABLE`, `ARCHIVING`, `ARCHIVED`, `UNARCHIVING`, `DELETING`. `DELETING` is the only one we accept as "delete in flight". Everything else on a verify lookup means the delete did not take.

## 5. Failure reporting

On any failure (across both project passes):

```python
if failures:
    error_log_path = get_pipeline_path(filename=f'{cohort_name}_delete_errors.log')
    with error_log_path.open('w') as fh:
        fh.write('project_id\tfid\tkind\tcontext\n')
        for f in failures:
            fh.write(f'{f.project_id}\t{f.fid}\t{f.kind}\t{f.context}\n')
    raise RuntimeError(
        f'{len(failures)} FIDs failed cleanup; see {error_log_path}. '
        f'Re-run the stage to retry (already-deleted items return 404 quickly).'
    )
```

**TSV not JSON**: matches the existing `tmp_errors.log` text-line style elsewhere in the package; greppable and spreadsheet-importable without a parser.

**Conditional output**: the log file is NOT in `expected_outputs` (mirrors `ManageDragenPipeline`'s `_mlr_errors.log` / `errors.log` pattern — variable-existence files don't participate in cpg-flow's expected-outputs check). The deterministic output is the success marker only.

**Log AND raise**: log gives details; raise tells cpg-flow the stage failed so it surfaces in the dashboard. The `RuntimeError` message names the log's GCS path so the operator finds it directly from the cpg-flow error.

## 6. Idempotency

- Re-running the stage after partial failure: the cohort folder, CRAM FIDs, and FASTQ FIDs that were already deleted return 404 on the delete call (which raises `ApiException` — handled) and 404 on the verify lookup (which is the success path).
- Re-running after full success: the success marker exists from the first run; cpg-flow short-circuits.
- No state file is needed beyond the success marker — the verify step is the source of truth.

## 7. Config

New key under existing `[ica.projects]`:

```toml
[ica.projects]
dragen_align = "main_dragen_project"
dragen_mlr = "mlr_job_project"
dragen_mlr_project_id = "mlr_project_id"
fastq_source_project_id = "supplier_project_id"   # NEW
```

Read **lazily** inside `delete_data_in_ica.run`, only when `fastq_ids_list_path is not None`. CRAM-mode cohorts don't need it set — missing key is fine and won't be touched. Tests stub it in `_TEST_CONFIG` only when exercising the FASTQ path.

## 8. Tests

New file `tests/test_delete_data_in_ica.py` with `MagicMock`-based fixtures for the icasdk `ProjectDataApi`. All tests pass `settle_seconds=0` to skip the real sleep.

1. **CRAM mode, all-clean** — `delete_data` succeeds, `get_project_data` raises 404 for every FID. Assert: no log file written; success marker written; `projectId` in every call equals the DRAGEN runs project; supplier project is never queried.
2. **FASTQ mode, all-DELETING** — `get_project_data` returns 200 with `status='DELETING'` for every FID. Assert: both project IDs exercised correctly; no failures; marker payload includes `fastq_count`.
3. **Spurious `ApiValueError` on delete, 404 on verify** — exact scenario the user described from manual API testing. Assert: zero failures.
4. **Real failure** — `get_project_data` returns `status='AVAILABLE'` for one CRAM. Assert: log file contains that one row; `RuntimeError` raised with the log path in the message; success marker NOT written.
5. **Mixed** — five FIDs: three succeed (404 verify), one is `DELETING`, one is `AVAILABLE`. Assert: only the one bad FID is in the log; other four are silent.
6. **Idempotent re-run** — first run leaves one FID failed; second run with the same inputs sees 404 for the previously-deleted FIDs and a fresh delete-attempt for the leftover. Assert: clean exit on second run.

## 9. Out of scope

- Deletion of ICA Analysis records (metadata only — no storage cost; orphan records are acceptable).
- Cleanup of GCS state (per-SG state files, `{cohort}_batches.json`, the new success marker, the conditional error log). GCS lifecycle / explicit clean-up is a separate operational concern.
- Cleanup of pre-migration legacy per-SG analysis output folders. Cohorts processed by `main` use a different layout; cleanup of their leftover storage is operational, not pipeline-driven.
- Bulk-delete optimisation (e.g. ICA's hypothetical multi-FID delete endpoint). Per-FID is fine; the loop is bounded by cohort size.
