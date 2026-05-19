# DeleteDataInIca Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `DeleteDataInIca` + `jobs/delete_data_in_ica.py` to clean ICA storage for the unified DRAGEN pipeline's batched layout, with delete-verification and correct two-project handling.

**Architecture:** Two project-scoped passes (DRAGEN runs project for cohort folder + CRAMs; supplier project for linked FASTQs). Each pass fires all deletes, sleeps 60s for ICA's async state machine, then verifies each FID via `get_project_data` (404 or `status='DELETING'` = success). Failures aggregated into a TSV log; success writes a small JSON marker. See spec at `docs/superpowers/specs/2026-05-19-delete-data-in-ica-rewrite-design.md`.

**Tech Stack:** Python 3.11, icasdk, cpg-flow, pytest. Mocks via `unittest.mock.MagicMock`.

---

## File structure

- **Modify:** `src/dragen_align_pa/jobs/delete_data_in_ica.py` — rewrite entirely; ~150 lines (was 82). Adds `DeleteFailure` dataclass + `_delete_and_verify` private helper + rewritten `run`.
- **Modify:** `src/dragen_align_pa/stages.py` — rewire `DeleteDataInIca.queue_jobs` (single-Path input from `PrepareIcaForDragenAnalysis`) and `expected_outputs` (rename to `_delete_complete.json`); drop the "TEMPORARILY BROKEN" docstring.
- **Modify:** `config/dragen_align_pa_defaults.toml` — add `fastq_source_project_id` under `[ica.projects]`.
- **Create:** `tests/test_delete_data_in_ica.py` — new file, six scenarios per spec §8.

Each file has one clear responsibility (cleanup job; stage wiring; config; tests). No structural refactoring beyond the rewrite.

## Task 1: Failing test scaffold — CRAM-mode happy path

**Files:**
- Create: `tests/test_delete_data_in_ica.py`

- [ ] **Step 1: Write the test file**

```python
"""Tests for the DeleteDataInIca cleanup job.

The job has two ICA project-scoped passes (DRAGEN runs project +
supplier project for FASTQ mode). Tests mock the icasdk ProjectDataApi
to control delete/verify outcomes per FID. settle_seconds=0 is threaded
through every test so the 60s production sleep doesn't run.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dragen_align_pa.jobs import delete_data_in_ica


RUNS_PROJECT_ID = 'runs-project-id'
FASTQ_PROJECT_ID = 'fastq-supplier-project-id'


def _write_cohort_fid(tmp_path: Path, fid: str = 'fol.cohort_xyz') -> Path:
    path = tmp_path / 'cohort_analysis_output_fid.json'
    path.write_text(json.dumps({'analysis_output_fid': fid}))
    return path


def _write_cram_fids(tmp_path: Path, sg_to_fid: dict[str, str]) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for sg_name, fid in sg_to_fid.items():
        p = tmp_path / f'{sg_name}_cram_fid.json'
        p.write_text(json.dumps({'cram_fid': fid}))
        paths[sg_name] = p
    return paths


def _write_fastq_fid_list(tmp_path: Path, fids: list[str]) -> Path:
    p = tmp_path / 'fastq_ids.txt'
    p.write_text('\n'.join(fids))
    return p


def _make_api_instance(
    verify_404_fids: set[str] | None = None,
    verify_deleting_fids: set[str] | None = None,
    verify_available_fids: set[str] | None = None,
    delete_raises: dict[str, Exception] | None = None,
) -> MagicMock:
    """Build a MagicMock `ProjectDataApi` whose delete_data + get_project_data
    behave per the maps above. Unmapped FIDs default to delete-OK + verify-404."""
    verify_404_fids = verify_404_fids or set()
    verify_deleting_fids = verify_deleting_fids or set()
    verify_available_fids = verify_available_fids or set()
    delete_raises = delete_raises or {}

    api = MagicMock()

    def fake_delete(path_params):
        fid = path_params['dataId']
        if fid in delete_raises:
            raise delete_raises[fid]
        return MagicMock()

    api.delete_data.side_effect = fake_delete

    def fake_get(path_params):
        from icasdk import ApiException
        fid = path_params['dataId']
        if fid in verify_deleting_fids:
            resp = MagicMock()
            resp.body = {'data': {'details': {'status': 'DELETING'}}}
            return resp
        if fid in verify_available_fids:
            resp = MagicMock()
            resp.body = {'data': {'details': {'status': 'AVAILABLE'}}}
            return resp
        # default = 404 not found
        exc = ApiException(status=404, reason='Not Found')
        raise exc

    api.get_project_data.side_effect = fake_get
    return api


@pytest.fixture
def patched_env(monkeypatch):
    """Stub get_ica_secrets + get_ica_api_client + config_retrieve for the
    fastq source project lookup. The api_instance MagicMock is returned via
    a closure so each test can swap it per scenario."""
    state: dict[str, MagicMock] = {'api_instance': MagicMock()}

    monkeypatch.setattr(
        'dragen_align_pa.jobs.delete_data_in_ica.ica_api_utils.get_ica_secrets',
        lambda: {'projectID': RUNS_PROJECT_ID, 'apiKey': 'stub'},
    )
    fake_client = MagicMock()
    fake_client.__enter__ = MagicMock(return_value=fake_client)
    fake_client.__exit__ = MagicMock(return_value=False)
    monkeypatch.setattr(
        'dragen_align_pa.jobs.delete_data_in_ica.ica_api_utils.get_ica_api_client',
        lambda: fake_client,
    )

    def fake_config_retrieve(key, default=None):
        if tuple(key) == ('ica', 'projects', 'fastq_source_project_id'):
            return FASTQ_PROJECT_ID
        return default

    monkeypatch.setattr(
        'dragen_align_pa.jobs.delete_data_in_ica.config_retrieve',
        fake_config_retrieve,
    )

    # Patch the ProjectDataApi constructor so it returns our mock api_instance.
    monkeypatch.setattr(
        'dragen_align_pa.jobs.delete_data_in_ica.project_data_api.ProjectDataApi',
        lambda _client: state['api_instance'],
    )
    return state


def test_cram_mode_happy_path_writes_marker_no_log(tmp_path: Path, patched_env):
    """All deletes succeed, all verifies return 404. Marker is written.
    No error log exists. Supplier project is never queried (CRAM-only)."""
    cohort_fid = 'fol.cohort_001'
    cram_fids = {'SYN00001': 'fil.cram_001', 'SYN00002': 'fil.cram_002'}

    api = _make_api_instance()  # default: all 404 on verify
    patched_env['api_instance'] = api

    cohort_path = _write_cohort_fid(tmp_path, fid=cohort_fid)
    cram_paths = _write_cram_fids(tmp_path, cram_fids)
    marker_path = tmp_path / 'COH0001_delete_complete.json'

    delete_data_in_ica.run(
        cohort_name='COH0001',
        output_path=marker_path,
        cohort_analysis_output_fid_path=cohort_path,
        cram_fid_paths_dict=cram_paths,
        fastq_ids_list_path=None,
        settle_seconds=0,
    )

    assert marker_path.exists(), 'success marker should be written'
    payload = json.loads(marker_path.read_text())
    assert payload['cohort_name'] == 'COH0001'
    assert payload['runs_project']['cohort_folder'] == cohort_fid
    assert payload['runs_project']['cram_count'] == 2
    assert 'fastq_source_project' not in payload

    # No error log file in the same directory
    error_log = tmp_path / 'COH0001_delete_errors.log'
    assert not error_log.exists()

    # Every delete_data and get_project_data call used the RUNS project
    for call in api.delete_data.call_args_list + api.get_project_data.call_args_list:
        assert call.kwargs['path_params']['projectId'] == RUNS_PROJECT_ID
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/test_delete_data_in_ica.py::test_cram_mode_happy_path_writes_marker_no_log -v`

Expected: `FAILED` — either `TypeError: run() got an unexpected keyword argument 'cohort_name'` (signature change) OR `AttributeError` on `delete_data_in_ica.config_retrieve` (the symbol doesn't exist yet inside the module). Either is fine — confirms we're driving the rewrite from the test.

- [ ] **Step 3: Commit the failing test**

```bash
git add tests/test_delete_data_in_ica.py
git commit -m "test: failing CRAM happy-path test for DeleteDataInIca rewrite"
```

---

## Task 2: Implement the new `delete_data_in_ica.run`

**Files:**
- Modify: `src/dragen_align_pa/jobs/delete_data_in_ica.py` (replace entire contents)

- [ ] **Step 1: Replace the file contents**

```python
"""Delete cohort outputs + source CRAMs/FASTQs from ICA to release storage.

Two project-scoped passes:
- DRAGEN runs project: the cohort-level analysis output folder (cascades
  to per-batch analyses, per-SG outputs, per-batch FASTQ list CSVs) + the
  per-SG uploaded CRAM file IDs.
- Supplier project (FASTQ mode only): the linked FASTQ file IDs from
  `FastqIntakeQc`'s outpath.

Each pass fires all deletes, sleeps `settle_seconds` (default 60) for ICA's
async delete state machine, then verifies via `get_project_data` (404 or
`status='DELETING'` is success). Failures across both passes are aggregated
into a TSV log at `get_pipeline_path('{cohort}_delete_errors.log')`; on any
failure the job raises so the cpg-flow stage shows red.
"""

import json
import time
from dataclasses import dataclass
from typing import Any, Literal

import cpg_utils
from cpg_utils.config import config_retrieve
from icasdk.apis.tags import project_data_api
from icasdk.exceptions import ApiException, ApiValueError
from loguru import logger

from dragen_align_pa import ica_api_utils
from dragen_align_pa.utils import get_pipeline_path

_DELETING_STATUS = 'DELETING'


@dataclass(frozen=True)
class DeleteFailure:
    """One row in the failure log. `kind` is human-readable for the TSV."""

    project_id: str
    fid: str
    kind: str
    context: str

    def as_tsv(self) -> str:
        return f'{self.project_id}\t{self.fid}\t{self.kind}\t{self.context}'


def _read_cohort_folder_fid(path: cpg_utils.Path) -> str:
    with path.open() as fh:
        return json.load(fh)['analysis_output_fid']


def _read_cram_fids(paths: dict[str, cpg_utils.Path]) -> list[str]:
    fids: list[str] = []
    for path in paths.values():
        with path.open() as fh:
            fids.append(json.load(fh)['cram_fid'])
    logger.info(f'Collected {len(fids)} CRAM FIDs across {len(paths)} SGs')
    return fids


def _read_fastq_fids(path: cpg_utils.Path) -> list[str]:
    with path.open() as fh:
        fids = [line.strip() for line in fh if line.strip()]
    logger.info(f'Collected {len(fids)} FASTQ FIDs from {path}')
    return fids


def _delete_and_verify(
    api_instance: project_data_api.ProjectDataApi,
    project_id: str,
    fids: list[str],
    settle_seconds: int,
) -> list[DeleteFailure]:
    """Fire all deletes, wait for ICA's async state machine, then verify each."""
    if not fids:
        return []

    failures: list[DeleteFailure] = []
    delete_errors: dict[str, str] = {}

    for fid in fids:
        path_params = {'projectId': project_id, 'dataId': fid}
        try:
            api_instance.delete_data(path_params=path_params)
        except ApiValueError:
            # icasdk returns None from a non-Optional signature; the call
            # actually succeeded. Verify will confirm.
            pass
        except ApiException as e:
            delete_errors[fid] = repr(e)

    # ICA's delete is async — wait for state to advance to DELETING (or 404).
    if settle_seconds > 0:
        logger.info(f'Sleeping {settle_seconds}s for ICA delete propagation in project {project_id}')
        time.sleep(settle_seconds)

    for fid in fids:
        path_params = {'projectId': project_id, 'dataId': fid}
        try:
            response = api_instance.get_project_data(path_params=path_params)
            status = response.body['data']['details'].get('status', 'UNKNOWN')
            if status == _DELETING_STATUS:
                continue
            failures.append(DeleteFailure(
                project_id=project_id,
                fid=fid,
                kind=f'still_present (status={status})',
                context=delete_errors.get(fid, ''),
            ))
        except ApiException as e:
            if getattr(e, 'status', None) == 404:
                continue
            failures.append(DeleteFailure(
                project_id=project_id,
                fid=fid,
                kind='verify_failed',
                context=f'{e!r} | delete: {delete_errors.get(fid, "")}',
            ))

    return failures


def _write_failure_log(cohort_name: str, failures: list[DeleteFailure]) -> cpg_utils.Path:
    error_log_path = get_pipeline_path(filename=f'{cohort_name}_delete_errors.log')
    with error_log_path.open('w') as fh:
        fh.write('project_id\tfid\tkind\tcontext\n')
        for f in failures:
            fh.write(f.as_tsv() + '\n')
    return error_log_path


def _write_success_marker(
    output_path: cpg_utils.Path,
    cohort_name: str,
    cohort_folder_fid: str,
    cram_count: int,
    fastq_count: int | None,
) -> None:
    payload: dict[str, Any] = {
        'cohort_name': cohort_name,
        'runs_project': {
            'cohort_folder': cohort_folder_fid,
            'cram_count': cram_count,
        },
    }
    if fastq_count is not None:
        payload['fastq_source_project'] = {'fastq_count': fastq_count}
    with output_path.open('w') as fh:
        json.dump(payload, fh)


def run(
    cohort_name: str,
    output_path: cpg_utils.Path,
    cohort_analysis_output_fid_path: cpg_utils.Path,
    cram_fid_paths_dict: dict[str, cpg_utils.Path] | None,
    fastq_ids_list_path: cpg_utils.Path | None,
    settle_seconds: int = 60,
) -> None:
    """Entry point — called from the `DeleteDataInIca` stage's PythonJob.

    `settle_seconds` is a parameter (not a constant) so unit tests can pass 0
    to skip the production 60s wait. Production callers always use the default.
    """
    secrets: dict[Literal['projectID', 'apiKey'], str] = ica_api_utils.get_ica_secrets()
    runs_project_id = secrets['projectID']

    cohort_folder_fid = _read_cohort_folder_fid(cohort_analysis_output_fid_path)
    cram_fids = _read_cram_fids(cram_fid_paths_dict) if cram_fid_paths_dict else []
    fastq_fids = _read_fastq_fids(fastq_ids_list_path) if fastq_ids_list_path else []

    runs_project_fids = [cohort_folder_fid, *cram_fids]

    failures: list[DeleteFailure] = []
    with ica_api_utils.get_ica_api_client() as api_client:
        api_instance = project_data_api.ProjectDataApi(api_client)

        failures += _delete_and_verify(
            api_instance=api_instance,
            project_id=runs_project_id,
            fids=runs_project_fids,
            settle_seconds=settle_seconds,
        )

        if fastq_fids:
            fastq_project_id: str = config_retrieve(
                ['ica', 'projects', 'fastq_source_project_id'],
            )
            failures += _delete_and_verify(
                api_instance=api_instance,
                project_id=fastq_project_id,
                fids=fastq_fids,
                settle_seconds=settle_seconds,
            )

    if failures:
        error_log_path = _write_failure_log(cohort_name, failures)
        raise RuntimeError(
            f'{len(failures)} FIDs failed cleanup; see {error_log_path}. '
            f'Re-run the stage to retry (already-deleted items return 404 quickly).',
        )

    _write_success_marker(
        output_path=output_path,
        cohort_name=cohort_name,
        cohort_folder_fid=cohort_folder_fid,
        cram_count=len(cram_fids),
        fastq_count=len(fastq_fids) if fastq_ids_list_path is not None else None,
    )
```

- [ ] **Step 2: Run the Task 1 test, expect PASS**

Run: `pytest tests/test_delete_data_in_ica.py::test_cram_mode_happy_path_writes_marker_no_log -v`

Expected: `PASSED`.

- [ ] **Step 3: Run lint on the new module**

Run: `pre-commit run --files src/dragen_align_pa/jobs/delete_data_in_ica.py`

Expected: ruff + mypy pass. If ruff complains about the `del sg_name` line, drop it (and adjust the loop variable handling).

- [ ] **Step 4: Commit**

```bash
git add src/dragen_align_pa/jobs/delete_data_in_ica.py
git commit -m "Rewrite delete_data_in_ica.run for batched layout + verify"
```

---

## Task 3: Add remaining test scenarios

**Files:**
- Modify: `tests/test_delete_data_in_ica.py` (append five new tests)

- [ ] **Step 1: Append the FASTQ-mode happy path test**

```python
def test_fastq_mode_uses_supplier_project_and_writes_marker(tmp_path: Path, patched_env):
    """FASTQ-mode happy path: runs project handles cohort folder (no CRAMs);
    supplier project handles all FASTQ FIDs. Marker includes fastq_count."""
    cohort_fid = 'fol.cohort_002'
    fastq_fids = ['fil.fastq_001', 'fil.fastq_002', 'fil.fastq_003']

    # Verify returns DELETING for everything — delete-in-flight = success.
    api = _make_api_instance(verify_deleting_fids={cohort_fid, *fastq_fids})
    patched_env['api_instance'] = api

    cohort_path = _write_cohort_fid(tmp_path, fid=cohort_fid)
    fastq_path = _write_fastq_fid_list(tmp_path, fastq_fids)
    marker_path = tmp_path / 'COH0002_delete_complete.json'

    delete_data_in_ica.run(
        cohort_name='COH0002',
        output_path=marker_path,
        cohort_analysis_output_fid_path=cohort_path,
        cram_fid_paths_dict=None,
        fastq_ids_list_path=fastq_path,
        settle_seconds=0,
    )

    assert marker_path.exists()
    payload = json.loads(marker_path.read_text())
    assert payload['runs_project']['cram_count'] == 0
    assert payload['fastq_source_project']['fastq_count'] == 3

    runs_calls = [c for c in api.delete_data.call_args_list
                  if c.kwargs['path_params']['projectId'] == RUNS_PROJECT_ID]
    fastq_calls = [c for c in api.delete_data.call_args_list
                   if c.kwargs['path_params']['projectId'] == FASTQ_PROJECT_ID]
    assert len(runs_calls) == 1, 'one cohort folder delete in runs project'
    assert len(fastq_calls) == 3, 'one delete per FASTQ FID in supplier project'
```

- [ ] **Step 2: Append the spurious-ApiValueError test**

```python
def test_spurious_apivalueerror_with_404_verify_is_success(tmp_path: Path, patched_env):
    """User-reported scenario: delete_data raises ApiValueError but actually
    deletes. Verify returns 404 → counted as success, no log."""
    from icasdk.exceptions import ApiValueError

    cohort_fid = 'fol.cohort_003'
    api = _make_api_instance(
        delete_raises={cohort_fid: ApiValueError('spurious')},
    )
    patched_env['api_instance'] = api

    cohort_path = _write_cohort_fid(tmp_path, fid=cohort_fid)
    marker_path = tmp_path / 'COH0003_delete_complete.json'

    delete_data_in_ica.run(
        cohort_name='COH0003',
        output_path=marker_path,
        cohort_analysis_output_fid_path=cohort_path,
        cram_fid_paths_dict=None,
        fastq_ids_list_path=None,
        settle_seconds=0,
    )

    assert marker_path.exists()
```

- [ ] **Step 3: Append the still-present (real failure) test**

```python
def test_real_failure_writes_log_and_raises(tmp_path: Path, patched_env, monkeypatch):
    """One CRAM FID still has status=AVAILABLE after delete + settle. The
    failure is logged to {cohort}_delete_errors.log and RuntimeError raises."""
    cohort_fid = 'fol.cohort_004'
    bad_cram = 'fil.cram_bad'
    good_cram = 'fil.cram_good'

    api = _make_api_instance(verify_available_fids={bad_cram})
    patched_env['api_instance'] = api

    cohort_path = _write_cohort_fid(tmp_path, fid=cohort_fid)
    cram_paths = _write_cram_fids(tmp_path, {'SYN1': bad_cram, 'SYN2': good_cram})
    marker_path = tmp_path / 'COH0004_delete_complete.json'

    # Redirect get_pipeline_path so the error log lands under tmp_path.
    error_log_path = tmp_path / 'COH0004_delete_errors.log'
    monkeypatch.setattr(
        'dragen_align_pa.jobs.delete_data_in_ica.get_pipeline_path',
        lambda filename: error_log_path,
    )

    with pytest.raises(RuntimeError, match=r'1 FIDs failed cleanup'):
        delete_data_in_ica.run(
            cohort_name='COH0004',
            output_path=marker_path,
            cohort_analysis_output_fid_path=cohort_path,
            cram_fid_paths_dict=cram_paths,
            fastq_ids_list_path=None,
            settle_seconds=0,
        )

    assert error_log_path.exists()
    lines = error_log_path.read_text().splitlines()
    assert lines[0] == 'project_id\tfid\tkind\tcontext'
    assert any(bad_cram in line and 'still_present' in line for line in lines[1:])
    assert not any(good_cram in line for line in lines[1:])
    assert not marker_path.exists(), 'marker NOT written on failure'
```

- [ ] **Step 4: Append the mixed-outcomes test**

```python
def test_mixed_outcomes_logs_only_failures(tmp_path: Path, patched_env, monkeypatch):
    """Five FIDs: cohort folder (DELETING=ok), two CRAMs (404=ok),
    one CRAM still AVAILABLE (fail), one FASTQ also AVAILABLE (fail).
    Log contains exactly the two failures; raise mentions count=2."""
    cohort_fid = 'fol.cohort_005'
    crams = {'SYN1': 'fil.cram_ok', 'SYN2': 'fil.cram_stuck'}
    fastqs = ['fil.fastq_ok', 'fil.fastq_stuck']

    api = _make_api_instance(
        verify_deleting_fids={cohort_fid},
        verify_available_fids={'fil.cram_stuck', 'fil.fastq_stuck'},
    )
    patched_env['api_instance'] = api

    cohort_path = _write_cohort_fid(tmp_path, fid=cohort_fid)
    cram_paths = _write_cram_fids(tmp_path, crams)
    fastq_path = _write_fastq_fid_list(tmp_path, fastqs)
    marker_path = tmp_path / 'COH0005_delete_complete.json'

    error_log_path = tmp_path / 'COH0005_delete_errors.log'
    monkeypatch.setattr(
        'dragen_align_pa.jobs.delete_data_in_ica.get_pipeline_path',
        lambda filename: error_log_path,
    )

    with pytest.raises(RuntimeError, match=r'2 FIDs failed cleanup'):
        delete_data_in_ica.run(
            cohort_name='COH0005',
            output_path=marker_path,
            cohort_analysis_output_fid_path=cohort_path,
            cram_fid_paths_dict=cram_paths,
            fastq_ids_list_path=fastq_path,
            settle_seconds=0,
        )

    lines = error_log_path.read_text().splitlines()[1:]
    assert len(lines) == 2
    assert any('fil.cram_stuck' in line for line in lines)
    assert any('fil.fastq_stuck' in line for line in lines)
```

- [ ] **Step 5: Append the idempotent-rerun test**

```python
def test_idempotent_rerun_after_full_deletion(tmp_path: Path, patched_env):
    """Re-running after a clean prior run: every delete_data call raises
    ApiException(status=404), every get_project_data also raises 404.
    No failures, marker written normally."""
    from icasdk.exceptions import ApiException

    cohort_fid = 'fol.cohort_006'
    crams = {'SYN1': 'fil.cram_001'}

    not_found = ApiException(status=404, reason='Not Found')
    api = _make_api_instance(
        delete_raises={cohort_fid: not_found, 'fil.cram_001': not_found},
    )
    # default get_project_data behavior is 404 — matches re-run scenario.
    patched_env['api_instance'] = api

    cohort_path = _write_cohort_fid(tmp_path, fid=cohort_fid)
    cram_paths = _write_cram_fids(tmp_path, crams)
    marker_path = tmp_path / 'COH0006_delete_complete.json'

    delete_data_in_ica.run(
        cohort_name='COH0006',
        output_path=marker_path,
        cohort_analysis_output_fid_path=cohort_path,
        cram_fid_paths_dict=cram_paths,
        fastq_ids_list_path=None,
        settle_seconds=0,
    )

    assert marker_path.exists()
```

- [ ] **Step 6: Run all six tests**

Run: `pytest tests/test_delete_data_in_ica.py -v`

Expected: 6 passed.

- [ ] **Step 7: Commit**

```bash
git add tests/test_delete_data_in_ica.py
git commit -m "test: FASTQ-mode, spurious-error, failure, mixed, idempotent cases"
```

---

## Task 4: Rewire the `DeleteDataInIca` stage class

**Files:**
- Modify: `src/dragen_align_pa/stages.py` (the `DeleteDataInIca` class, around line 670)

- [ ] **Step 1: Replace the class body**

Find the existing `class DeleteDataInIca(CohortStage):` and replace from `class DeleteDataInIca` through the end of its `queue_jobs` method with:

```python
@stage(
    required_stages=[
        PrepareIcaForDragenAnalysis,
        UploadDataToIca,
        DownloadCramFromIca,
        DownloadGvcfFromIca,
        DownloadMlrGvcfFromIca,
        DownloadDataFromIca,
        ReheaderMlrGvcf,
        SomalierExtract,
        FastqIntakeQc,
    ],
)
class DeleteDataInIca(CohortStage):
    """Delete cohort outputs + source CRAMs/FASTQs from ICA to release storage.

    Two project-scoped passes: DRAGEN runs project for the cohort folder
    (cascades to per-batch analyses + per-SG outputs + per-batch FASTQ list
    CSVs) and the uploaded source CRAMs; supplier project for linked FASTQs
    (FASTQ mode only). Each pass verifies delete via `get_project_data`
    after a 60s settle for ICA's async state machine.
    """

    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        return get_pipeline_path(filename=f'{cohort.name}_delete_complete.json')

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput:
        cohort_analysis_output_fid_path: cpg_utils.Path = inputs.as_path(
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

        output_path: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        ica_delete_job: PythonJob = initialise_python_job(
            job_name='DeleteDataInIca',
            target=cohort,
            tool_name='ICA',
        )
        ica_delete_job.image(image=get_driver_image())
        ica_delete_job.call(
            delete_data_in_ica.run,
            cohort_name=cohort.name,
            output_path=output_path,
            cohort_analysis_output_fid_path=cohort_analysis_output_fid_path,
            cram_fid_paths_dict=cram_fid_paths_dict,
            fastq_ids_list_path=fastq_ids_list_path,
        )

        return self.make_outputs(target=cohort, data=output_path, jobs=ica_delete_job)
```

Differences vs. the previous body:
- Docstring no longer mentions "TEMPORARILY BROKEN".
- `expected_outputs` filename changes from `_delete_placeholder.txt` (under `get_prep_path`) to `_delete_complete.json` (under `get_pipeline_path` — sibling of `_pipeline_complete.json`).
- `inputs.as_dict(target=cohort, stage=PrepareIcaForDragenAnalysis)` becomes `inputs.as_path(...)` (the upstream stage now returns a single Path).
- `job.call(...)` passes the new kwargs (`cohort_name`, `output_path`); no `settle_seconds` override (production uses the 60s default).
- Adds `ica_delete_job.image(image=get_driver_image())` — the previous code omitted this; every other stage that uses `initialise_python_job` also calls `.image()` afterwards. Without it the job runs against an empty image config.

- [ ] **Step 2: Verify the stage graph still imports**

Run: `pytest --collect-only -q`

Expected: 127 tests collected (121 baseline + 6 new from Tasks 1–3). No collection errors.

- [ ] **Step 3: Run lint**

Run: `pre-commit run --files src/dragen_align_pa/stages.py`

Expected: ruff + mypy pass.

- [ ] **Step 4: Commit**

```bash
git add src/dragen_align_pa/stages.py
git commit -m "Rewire DeleteDataInIca stage to single cohort folder + new marker name"
```

---

## Task 5: Add the new config key

**Files:**
- Modify: `config/dragen_align_pa_defaults.toml`

- [ ] **Step 1: Add `fastq_source_project_id` under `[ica.projects]`**

Find the existing block:

```toml
[ica.projects]
dragen_align = "main_dragen_project"
dragen_mlr = "mlr_job_project"
dragen_mlr_project_id = "mlr_project_id"
```

Replace with:

```toml
[ica.projects]
dragen_align = "main_dragen_project"
dragen_mlr = "mlr_job_project"
dragen_mlr_project_id = "mlr_project_id"
# Supplier project where third-party FASTQ uploads live; we operate against this
# project (not `dragen_align`) to delete linked FASTQs in `DeleteDataInIca`.
# Only consulted in FASTQ mode.
fastq_source_project_id = "fastq_source_project_id"
```

- [ ] **Step 2: Commit**

```bash
git add config/dragen_align_pa_defaults.toml
git commit -m "Add [ica.projects] fastq_source_project_id for FASTQ cleanup"
```

---

## Task 6: Final verification + push + open PR

- [ ] **Step 1: Run the pinned pre-commit hooks across all changed files**

Run: `pre-commit run --files src/dragen_align_pa/jobs/delete_data_in_ica.py src/dragen_align_pa/stages.py tests/test_delete_data_in_ica.py config/dragen_align_pa_defaults.toml`

Expected: ruff (v0.15.0), mypy (v1.19.1), cpg-id-checker, all other hooks pass.

- [ ] **Step 2: Run the full test suite**

Run: `pytest -q`

Expected: 128 passed (122 from `dragen-stage-rewiring` + 6 new in `test_delete_data_in_ica.py`).

- [ ] **Step 3: Push the branch**

Run: `git push -u origin dragen-delete-data-rewrite`

Expected: branch published, tracking origin/dragen-delete-data-rewrite.

- [ ] **Step 4: Open the PR against `dragen-stage-rewiring`**

```bash
gh pr create --base dragen-stage-rewiring \
  --title "Rewrite DeleteDataInIca for the unified pipeline layout" \
  --body "$(cat <<'EOF'
## Summary

- Rewrite `DeleteDataInIca` + `jobs/delete_data_in_ica.py` for the unified pipeline's batched layout: one cohort-level analysis folder replaces N per-SG analysis folders; cleanup needs to switch projects (CRAMs in the DRAGEN runs project; linked FASTQs in the supplier project introduced via the new `[ica.projects] fastq_source_project_id` config key).
- Replace the fire-and-forget delete with a per-project pass that fires deletes, waits 60s for ICA's async state machine to advance, then verifies each FID via `get_project_data` (404 OR `status='DELETING'` = success). Failures aggregated into a TSV at `get_pipeline_path('{cohort}_delete_errors.log')`; on any failure the stage raises (cpg-flow shows red).
- New tests cover CRAM + FASTQ happy paths, the spurious-ApiValueError-but-actually-deleted case, single-FID real failure, mixed outcomes, and idempotent re-run.

Closes the follow-up deferred by `2026-05-11-unified-dragen-pipeline-design.md` §1 + §7. Design: `docs/superpowers/specs/2026-05-19-delete-data-in-ica-rewrite-design.md`.

> Base is `dragen-stage-rewiring` (PR #52). GitHub will auto-retarget to `dragen-unified-dev` when #52 merges.

## Test plan

- [x] `pre-commit run` — pinned ruff v0.15.0 + mypy v1.19.1 pass on every changed file
- [x] `pytest -q` — 128 tests pass (6 new in `test_delete_data_in_ica.py`)
- [x] `pytest --collect-only` — full stage graph imports cleanly with the rewired stage
- [ ] Smoke test in ICA UI: after a real cohort run finishes, manually trigger `DeleteDataInIca` and confirm the cohort folder + linked FASTQs disappear (or enter DELETING) within ~60s

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: a PR URL printed; gh records the new PR against this branch.

---

## Out of scope (not in this PR)

- Deletion of ICA Analysis records (metadata-only orphans — no storage cost).
- Cleanup of GCS state files (per-SG state, `{cohort}_batches.json`, the new success marker, the conditional error log). Lifecycle is separate.
- Cleanup of pre-migration legacy per-SG analysis output folders (cohorts processed by `main`).
- Bulk-delete optimisation (per-FID loop is fine; cohorts are bounded).
