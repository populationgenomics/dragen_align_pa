"""Backfill entry point: copy externally-produced outputs from -upload into -main,
register them in metamist in a guaranteed order, then delete the -upload sources.

Covers the pure units (relative-filename maps shared with the download stages, the
-upload source path builder, registration ordering, the submit-time stage-selection
and source-staging guards) and runs the transfer functions against a fake `gcloud`
on PATH, so the skip-vs-fail and verify-before-rm branches are proven by behavior.
"""

import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from dragen_align_pa import backfill_registration, backfill_transfer, run_workflow, stages, utils, validator


def test_backfill_source_path_uses_upload_bucket(monkeypatch):
    calls: list[tuple[str, str | None]] = []

    def fake_dataset_path(suffix: str, category: str | None = None) -> str:
        calls.append((suffix, category))
        return f'gs://cpg-test-dataset-main-upload/{suffix}'

    monkeypatch.setattr(utils, 'dataset_path', fake_dataset_path)

    result = utils.get_backfill_source_path('cram/SG1.cram')

    assert calls == [('output/cram/SG1.cram', 'upload')]
    assert str(result) == 'gs://cpg-test-dataset-main-upload/output/cram/SG1.cram'


def test_cram_output_filenames_shape():
    assert utils.cram_output_filenames('SG1') == {
        'cram': 'cram/SG1.cram',
        'crai': 'cram/SG1.cram.crai',
    }


def test_base_gvcf_output_filenames_shape():
    assert utils.base_gvcf_output_filenames('SG1') == {
        'gvcf': 'base_gvcf/SG1.hard-filtered.gvcf.gz',
        'gvcf_tbi': 'base_gvcf/SG1.hard-filtered.gvcf.gz.tbi',
    }


def test_recal_gvcf_output_filenames_shape():
    assert utils.recal_gvcf_output_filenames('SG1') == {
        'gvcf': 'recal_gvcf/SG1.hard-filtered.recal.gvcf.gz',
        'gvcf_tbi': 'recal_gvcf/SG1.hard-filtered.recal.gvcf.gz.tbi',
        'gvcf_md5': 'recal_gvcf/SG1.hard-filtered.recal.gvcf.gz.md5sum',
        'gvcf_tbi_md5': 'recal_gvcf/SG1.hard-filtered.recal.gvcf.gz.tbi.md5sum',
    }


# --- Transfer functions ----------------------------------------------------------------
#
# Each test installs a fake `gcloud` dispatch script on PATH; the transfer functions
# exec it via subprocess (no shell). The fake logs every invocation to gcloud_calls.log
# so tests can assert exactly which operations ran (in particular: that `rm` did or
# did not).

_PAIR = ('gs://up/output/cram/SG1.cram', 'gs://main/ica/v/output/cram/SG1.cram')


def _install_fake_gcloud(tmp_path: Path, monkeypatch, describe_case_body: str) -> None:
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir(exist_ok=True)
    fake_gcloud = bin_dir / 'gcloud'
    fake_gcloud.write_text(
        f"""#!/bin/bash
echo "$@" >> gcloud_calls.log
if [ "$1 $2 $3" == 'storage objects describe' ]; then
    case "$4" in
{describe_case_body}
    esac
elif [ "$1 $2" == 'storage cp' ] || [ "$1 $2" == 'storage rm' ]; then
    exit 0
fi
"""
    )
    fake_gcloud.chmod(0o755)
    monkeypatch.setenv('PATH', f'{bin_dir}:{os.environ["PATH"]}')
    monkeypatch.chdir(tmp_path)  # gcloud_calls.log lands in cwd


def _gcloud_calls(tmp_path: Path) -> str:
    log = tmp_path / 'gcloud_calls.log'
    return log.read_text() if log.exists() else ''


def test_copy_files_succeeds_when_checksums_match(tmp_path, monkeypatch):
    source, destination = _PAIR
    _install_fake_gcloud(tmp_path, monkeypatch, f"""
        '{source}') echo 'abc123' ;;
        '{destination}') echo 'abc123' ;;
""")

    backfill_transfer.copy_files([_PAIR])

    assert f'storage cp --no-clobber {source} {destination}' in _gcloud_calls(tmp_path)


def test_copy_files_fails_when_destination_checksum_differs(tmp_path, monkeypatch):
    # A pre-existing stale destination survives --no-clobber; the checksum
    # comparison must fail the job rather than report a successful copy.
    source, destination = _PAIR
    _install_fake_gcloud(tmp_path, monkeypatch, f"""
        '{source}') echo 'abc123' ;;
        '{destination}') echo 'zzz999' ;;
""")

    with pytest.raises(ValueError, match='mismatch'):
        backfill_transfer.copy_files([_PAIR])


def test_copy_files_fails_when_checksum_output_is_empty(tmp_path, monkeypatch):
    # `--format='value(...)'` prints nothing (exit 0) for an unknown field; an
    # empty-vs-empty comparison must not certify anything.
    _install_fake_gcloud(tmp_path, monkeypatch, """
        *) exit 0 ;;
""")

    with pytest.raises(ValueError, match=r'[Ee]mpty'):
        backfill_transfer.copy_files([_PAIR])


def test_copy_files_passes_hostile_paths_verbatim(tmp_path, monkeypatch):
    # No shell is involved: a `$(...)` in a path reaches gcloud as one literal
    # argument and is never expanded.
    source = 'gs://up/output/cram/$(touch pwned).cram'
    destination = 'gs://main/ica/v/output/cram/SG1.cram'
    _install_fake_gcloud(tmp_path, monkeypatch, """
        *) echo 'abc123' ;;
""")

    backfill_transfer.copy_files([(source, destination)])

    assert not (tmp_path / 'pwned').exists()
    assert f'storage cp --no-clobber {source} {destination}' in _gcloud_calls(tmp_path)


def test_verify_files_passes_on_matching_checksums(tmp_path, monkeypatch):
    source, destination = _PAIR
    _install_fake_gcloud(tmp_path, monkeypatch, f"""
        '{source}') echo 'abc123' ;;
        '{destination}') echo 'abc123' ;;
""")

    backfill_transfer.verify_files([_PAIR])

    # Verification never copies or deletes anything.
    assert 'storage cp' not in _gcloud_calls(tmp_path)
    assert 'storage rm' not in _gcloud_calls(tmp_path)


def test_verify_files_fails_on_checksum_mismatch(tmp_path, monkeypatch):
    source, destination = _PAIR
    _install_fake_gcloud(tmp_path, monkeypatch, f"""
        '{source}') echo 'abc123' ;;
        '{destination}') echo 'zzz999' ;;
""")

    with pytest.raises(ValueError, match='mismatch'):
        backfill_transfer.verify_files([_PAIR])


def test_delete_files_removes_source_when_checksums_match(tmp_path, monkeypatch):
    source, destination = _PAIR
    _install_fake_gcloud(tmp_path, monkeypatch, f"""
        '{source}') echo 'abc123' ;;
        '{destination}') echo 'abc123' ;;
""")

    backfill_transfer.delete_files([_PAIR], results_file=tmp_path / 'results.txt')

    assert f'storage rm {source}' in _gcloud_calls(tmp_path)
    assert (tmp_path / 'results.txt').read_text() == f'deleted {source}\n'


def test_delete_files_aborts_before_rm_on_checksum_mismatch(tmp_path, monkeypatch):
    source, destination = _PAIR
    _install_fake_gcloud(tmp_path, monkeypatch, f"""
        '{source}') echo 'abc123' ;;
        '{destination}') echo 'zzz999' ;;
""")

    with pytest.raises(ValueError, match='mismatch'):
        backfill_transfer.delete_files([_PAIR], results_file=tmp_path / 'results.txt')

    assert 'storage rm' not in _gcloud_calls(tmp_path)


def test_delete_files_skips_source_that_is_genuinely_absent(tmp_path, monkeypatch):
    # gcloud's GcsNotFoundError builds the message from the request URL's
    # percent-encoded resource path, so the object name arrives with %2F for
    # every slash (verified in SDK 577.0.0 api_lib/storage/errors.py).
    source, destination = _PAIR
    encoded_source = 'gs://up/output%2Fcram%2FSG1.cram'
    _install_fake_gcloud(tmp_path, monkeypatch, f"""
        '{source}') echo 'ERROR: (gcloud.storage.objects.describe) {encoded_source} not found: 404.' >&2; exit 1 ;;
        '{destination}') echo 'abc123' ;;
""")

    backfill_transfer.delete_files([_PAIR], results_file=tmp_path / 'results.txt')

    assert 'storage rm' not in _gcloud_calls(tmp_path)
    assert (tmp_path / 'results.txt').read_text() == f'already-absent {source}\n'


def test_delete_files_also_accepts_an_unencoded_not_found_message(tmp_path, monkeypatch):
    # Guard against gcloud switching to (or some paths already using) the raw URL.
    source, destination = _PAIR
    _install_fake_gcloud(tmp_path, monkeypatch, f"""
        '{source}') echo 'ERROR: {source} not found: 404.' >&2; exit 1 ;;
        '{destination}') echo 'abc123' ;;
""")

    backfill_transfer.delete_files([_PAIR], results_file=tmp_path / 'results.txt')

    assert (tmp_path / 'results.txt').read_text() == f'already-absent {source}\n'


def test_delete_files_fails_on_not_found_wording_that_lacks_the_source_url(tmp_path, monkeypatch):
    # gcloud's real not-found error names the URL; auth errors ("Your default
    # credentials were not found") don't. Wording alone must not count as absence.
    source, destination = _PAIR
    _install_fake_gcloud(tmp_path, monkeypatch, f"""
        '{source}') echo 'ERROR: Your default credentials were not found.' >&2; exit 1 ;;
        '{destination}') echo 'abc123' ;;
""")

    with pytest.raises(subprocess.CalledProcessError):
        backfill_transfer.delete_files([_PAIR], results_file=tmp_path / 'results.txt')

    assert 'storage rm' not in _gcloud_calls(tmp_path)


def test_delete_files_fails_on_transient_describe_error(tmp_path, monkeypatch):
    # A 503 failure is NOT absence: the job must fail so the stage re-runs,
    # instead of silently orphaning the -upload file forever.
    source, destination = _PAIR
    _install_fake_gcloud(tmp_path, monkeypatch, f"""
        '{source}') echo 'ERROR: 503 backend error' >&2; exit 1 ;;
        '{destination}') echo 'abc123' ;;
""")

    with pytest.raises(subprocess.CalledProcessError):
        backfill_transfer.delete_files([_PAIR], results_file=tmp_path / 'results.txt')

    assert 'storage rm' not in _gcloud_calls(tmp_path)


def test_delete_files_fails_when_gcloud_is_missing(tmp_path, monkeypatch):
    # A missing gcloud binary must fail loudly, never classify as object absence.
    # PATH is reduced to a single empty directory: subprocess exec's the argv
    # directly (no shell), so nothing else is needed — and anything broader
    # picks up the real gcloud on CI runners (/usr/bin/gcloud on ubuntu images).
    empty_bin = tmp_path / 'bin'
    empty_bin.mkdir()
    monkeypatch.setenv('PATH', str(empty_bin))
    monkeypatch.chdir(tmp_path)

    with pytest.raises(FileNotFoundError):
        backfill_transfer.delete_files([_PAIR], results_file=tmp_path / 'results.txt')


def test_delete_files_fails_before_rm_when_checksum_output_is_empty(tmp_path, monkeypatch):
    _install_fake_gcloud(tmp_path, monkeypatch, """
        *) exit 0 ;;
""")

    with pytest.raises(ValueError, match=r'[Ee]mpty'):
        backfill_transfer.delete_files([_PAIR], results_file=tmp_path / 'results.txt')

    assert 'storage rm' not in _gcloud_calls(tmp_path)


def test_delete_files_fails_when_destination_is_missing(tmp_path, monkeypatch):
    source, destination = _PAIR
    _install_fake_gcloud(tmp_path, monkeypatch, f"""
        '{source}') echo 'abc123' ;;
        '{destination}') echo 'ERROR: {destination} not found: 404.' >&2; exit 1 ;;
""")

    with pytest.raises(subprocess.CalledProcessError):
        backfill_transfer.delete_files([_PAIR], results_file=tmp_path / 'results.txt')

    assert 'storage rm' not in _gcloud_calls(tmp_path)


def test_transfer_cli_parses_pairs_json(tmp_path, monkeypatch):
    source, destination = _PAIR
    _install_fake_gcloud(tmp_path, monkeypatch, f"""
        '{source}') echo 'abc123' ;;
        '{destination}') echo 'abc123' ;;
""")
    monkeypatch.setattr(
        'sys.argv',
        ['backfill_transfer', 'copy', '--pairs-json', json.dumps([list(_PAIR)])],
    )

    backfill_transfer.main()

    assert f'storage cp --no-clobber {source} {destination}' in _gcloud_calls(tmp_path)


# --- Registration ----------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _no_preexisting_analyses(monkeypatch):
    """Default the metamist lookups to 'nothing registered yet, ordering fine'.

    Tests that exercise the dedup or the post-registration ordering check
    override these explicitly.
    """
    monkeypatch.setattr(backfill_registration, '_existing_completed_outputs', lambda *_args: set())
    monkeypatch.setattr(backfill_registration, '_assert_recal_is_latest', lambda *_args: None)


def test_registration_orders_cram_then_base_gvcf_then_recal(monkeypatch):
    registered: list[tuple[str, str]] = []

    def fake_complete_analysis_job(output: str, analysis_type: str, *args: object) -> None:  # noqa: ARG001
        registered.append((output, analysis_type))

    monkeypatch.setattr(backfill_registration, 'complete_analysis_job', fake_complete_analysis_job)

    backfill_registration.run(
        cram='gs://main/cram/SG1.cram',
        base_gvcf='gs://main/base_gvcf/SG1.hard-filtered.gvcf.gz',
        recal_gvcf='gs://main/recal_gvcf/SG1.hard-filtered.recal.gvcf.gz',
        sg_id='CPG_000001',
        project_name='test-dataset',
        meta={'stage': 'BackfillGvcfsFromUpload'},
    )

    assert registered == [
        ('gs://main/cram/SG1.cram', 'cram'),
        ('gs://main/base_gvcf/SG1.hard-filtered.gvcf.gz', 'gvcf'),
        ('gs://main/recal_gvcf/SG1.hard-filtered.recal.gvcf.gz', 'gvcf'),
    ]


def test_registration_passes_analysis_arguments_and_returns_marker(monkeypatch):
    calls: list[dict] = []

    def fake_complete_analysis_job(
        output,
        analysis_type,
        cohort_ids,
        sg_ids,
        project_name,
        meta,
    ):
        calls.append(
            {
                'output': output,
                'analysis_type': analysis_type,
                'cohort_ids': cohort_ids,
                'sg_ids': sg_ids,
                'project_name': project_name,
                'meta': meta,
            }
        )

    monkeypatch.setattr(backfill_registration, 'complete_analysis_job', fake_complete_analysis_job)

    marker = backfill_registration.run(
        cram='gs://main/SG1.cram',
        base_gvcf='gs://main/base.g.vcf.gz',
        recal_gvcf='gs://main/recal.g.vcf.gz',
        sg_id='CPG_000001',
        project_name='test-dataset',
        meta={'stage': 'BackfillGvcfsFromUpload'},
    )

    assert all(call['cohort_ids'] == [] for call in calls)
    assert all(call['sg_ids'] == ['CPG_000001'] for call in calls)
    assert all(call['project_name'] == 'test-dataset' for call in calls)
    assert all(call['meta'] == {'stage': 'BackfillGvcfsFromUpload'} for call in calls)
    assert marker == {
        'sg_id': 'CPG_000001',
        'registered': ['gs://main/SG1.cram', 'gs://main/base.g.vcf.gz', 'gs://main/recal.g.vcf.gz'],
    }


def test_registration_skips_outputs_already_registered_in_metamist(monkeypatch):
    # A Hail Batch retry or a mid-trio failure replays the CLI; outputs that
    # already have a completed analysis must not be registered twice.
    registered: list[tuple[str, str]] = []

    def fake_complete_analysis_job(output: str, analysis_type: str, *args: object) -> None:  # noqa: ARG001
        registered.append((output, analysis_type))

    monkeypatch.setattr(backfill_registration, 'complete_analysis_job', fake_complete_analysis_job)
    monkeypatch.setattr(
        backfill_registration,
        '_existing_completed_outputs',
        lambda *_args: {('cram', 'gs://main/SG1.cram'), ('gvcf', 'gs://main/base.g.vcf.gz')},
    )

    marker = backfill_registration.run(
        cram='gs://main/SG1.cram',
        base_gvcf='gs://main/base.g.vcf.gz',
        recal_gvcf='gs://main/recal.g.vcf.gz',
        sg_id='CPG_000001',
        project_name='test-dataset',
        meta={'stage': 'BackfillGvcfsFromUpload'},
    )

    assert registered == [('gs://main/recal.g.vcf.gz', 'gvcf')]
    # The marker still records the full registered state of the SG.
    assert marker['registered'] == ['gs://main/SG1.cram', 'gs://main/base.g.vcf.gz', 'gs://main/recal.g.vcf.gz']


def test_registration_reregisters_recal_when_the_base_gvcf_was_newly_registered(monkeypatch):
    # A prior partial ICA run can leave the recal registered but not the base.
    # Registering the base and skipping the recal would make the base the
    # latest gvcf analysis — sg.gvcf would resolve to the non-MLR file — so the
    # recal must be re-registered whenever the base was registered after it.
    registered: list[tuple[str, str]] = []

    def fake_complete_analysis_job(output: str, analysis_type: str, *args: object) -> None:  # noqa: ARG001
        registered.append((output, analysis_type))

    monkeypatch.setattr(backfill_registration, 'complete_analysis_job', fake_complete_analysis_job)
    monkeypatch.setattr(
        backfill_registration,
        '_existing_completed_outputs',
        lambda *_args: {('gvcf', 'gs://main/recal.g.vcf.gz')},
    )

    backfill_registration.run(
        cram='gs://main/SG1.cram',
        base_gvcf='gs://main/base.g.vcf.gz',
        recal_gvcf='gs://main/recal.g.vcf.gz',
        sg_id='CPG_000001',
        project_name='test-dataset',
        meta={'stage': 'BackfillGvcfsFromUpload'},
    )

    assert registered == [
        ('gs://main/SG1.cram', 'cram'),
        ('gs://main/base.g.vcf.gz', 'gvcf'),
        ('gs://main/recal.g.vcf.gz', 'gvcf'),
    ]


def test_registration_does_not_reregister_recal_when_only_the_cram_was_new(monkeypatch):
    # The cram is a different analysis type with no ordering interplay with the
    # gvcf rows; a new cram must not force a duplicate recal registration.
    registered: list[tuple[str, str]] = []

    def fake_complete_analysis_job(output: str, analysis_type: str, *args: object) -> None:  # noqa: ARG001
        registered.append((output, analysis_type))

    monkeypatch.setattr(backfill_registration, 'complete_analysis_job', fake_complete_analysis_job)
    monkeypatch.setattr(
        backfill_registration,
        '_existing_completed_outputs',
        lambda *_args: {('gvcf', 'gs://main/base.g.vcf.gz'), ('gvcf', 'gs://main/recal.g.vcf.gz')},
    )

    backfill_registration.run(
        cram='gs://main/SG1.cram',
        base_gvcf='gs://main/base.g.vcf.gz',
        recal_gvcf='gs://main/recal.g.vcf.gz',
        sg_id='CPG_000001',
        project_name='test-dataset',
        meta={'stage': 'BackfillGvcfsFromUpload'},
    )

    assert registered == [('gs://main/SG1.cram', 'cram')]


def test_run_checks_recal_is_the_latest_gvcf_after_registering(monkeypatch):
    checked: list[tuple[str, str, str]] = []

    def fake_complete_analysis_job(*args: object) -> None:  # noqa: ARG001
        pass

    monkeypatch.setattr(backfill_registration, 'complete_analysis_job', fake_complete_analysis_job)
    monkeypatch.setattr(
        backfill_registration,
        '_assert_recal_is_latest',
        lambda sg_id, recal_gvcf, project_name: checked.append((sg_id, recal_gvcf, project_name)),
    )

    backfill_registration.run(
        cram='gs://main/SG1.cram',
        base_gvcf='gs://main/base.g.vcf.gz',
        recal_gvcf='gs://main/recal.g.vcf.gz',
        sg_id='CPG_000001',
        project_name='test-dataset',
        meta={'stage': 'BackfillGvcfsFromUpload'},
    )

    assert checked == [('CPG_000001', 'gs://main/recal.g.vcf.gz', 'test-dataset')]


def test_completed_analyses_queries_the_registration_project(monkeypatch):
    # Without a project filter, metamist returns analyses from every project the
    # SG appears in, while registration and sg.gvcf resolution are project-scoped.
    captured: list[dict] = []

    def fake_query(document, variables):  # noqa: ARG001
        captured.append(variables)
        return {'sequencingGroups': [{'analyses': []}]}

    monkeypatch.setattr(backfill_registration, 'query', fake_query)

    rows = backfill_registration._completed_analyses('CPG_000001', 'test-dataset')

    assert rows == []
    assert captured == [{'sgId': 'CPG_000001', 'project': 'test-dataset'}]


# Bound at import, before the autouse fixture replaces the module attribute with
# a no-op, so the ordering-check tests exercise the real implementation.
_REAL_ASSERT_RECAL_IS_LATEST = backfill_registration._assert_recal_is_latest


def _stub_completed_analyses(monkeypatch, rows: list[dict]) -> None:
    monkeypatch.setattr(backfill_registration, '_completed_analyses', lambda *_args: rows)


def test_assert_recal_is_latest_passes_when_recal_is_the_last_response_row(monkeypatch):
    _stub_completed_analyses(
        monkeypatch,
        [
            {'type': 'gvcf', 'output': 'gs://main/base.g.vcf.gz'},
            {'type': 'cram', 'output': 'gs://main/SG1.cram'},
            {'type': 'gvcf', 'output': 'gs://main/recal.g.vcf.gz'},
        ],
    )

    _REAL_ASSERT_RECAL_IS_LATEST('CPG_000001', 'gs://main/recal.g.vcf.gz', 'test-dataset')


def test_assert_recal_is_latest_fails_when_the_base_gvcf_postdates_it(monkeypatch):
    # A concurrent run (or a pre-existing bad ordering the dedup skipped over)
    # must surface loudly rather than leave sg.gvcf resolving to the base file.
    _stub_completed_analyses(
        monkeypatch,
        [
            {'type': 'gvcf', 'output': 'gs://main/recal.g.vcf.gz'},
            {'type': 'gvcf', 'output': 'gs://main/base.g.vcf.gz'},
        ],
    )

    with pytest.raises(RuntimeError, match='latest'):
        _REAL_ASSERT_RECAL_IS_LATEST('CPG_000001', 'gs://main/recal.g.vcf.gz', 'test-dataset')


def test_registration_passes_an_isolated_meta_dict_per_call(monkeypatch):
    # cpg-flow's complete_analysis_job mutates the meta dict it receives (pops
    # keys, adds size); one shared dict would leak mutations between calls.
    captured: list[dict] = []

    def fake_complete_analysis_job(output, analysis_type, cohort_ids, sg_ids, project_name, meta):  # noqa: ARG001
        captured.append(dict(meta))
        meta['size'] = 12345  # simulate the in-place mutation

    monkeypatch.setattr(backfill_registration, 'complete_analysis_job', fake_complete_analysis_job)

    backfill_registration.run(
        cram='gs://main/SG1.cram',
        base_gvcf='gs://main/base.g.vcf.gz',
        recal_gvcf='gs://main/recal.g.vcf.gz',
        sg_id='CPG_000001',
        project_name='test-dataset',
        meta={'stage': 'BackfillGvcfsFromUpload'},
    )

    assert all(meta == {'stage': 'BackfillGvcfsFromUpload'} for meta in captured)


def _registration_cli_argv(marker_file: Path) -> list[str]:
    return [
        'backfill_registration',
        '--cram', 'gs://main/SG1.cram',
        '--base-gvcf', 'gs://main/base.g.vcf.gz',
        '--recal-gvcf', 'gs://main/recal.g.vcf.gz',
        '--sg-id', 'CPG_000001',
        '--project-name', 'test-dataset',
        '--meta-json', '{"stage": "BackfillGvcfsFromUpload"}',
        '--marker-file', str(marker_file),
        '--marker-gcs-path', 'gs://main/ica/v/output/backfill_registration/CPG_000001.json',
    ]


def test_registration_cli_writes_marker_file_as_pure_json(monkeypatch, tmp_path, capsys):
    # complete_analysis_job logs to stdout, so the marker must be written to an
    # explicit file, not captured from stdout redirection.
    def fake_complete_analysis_job(*args: object) -> None:  # noqa: ARG001
        print('Created Analysis(id=1, type=gvcf) log noise')

    monkeypatch.setattr(backfill_registration, 'complete_analysis_job', fake_complete_analysis_job)
    monkeypatch.setattr(backfill_registration, '_read_existing_marker', lambda path: None)  # noqa: ARG005
    marker_file = tmp_path / 'marker.json'
    monkeypatch.setattr('sys.argv', _registration_cli_argv(marker_file))

    backfill_registration.main()

    capsys.readouterr()  # log noise goes to stdout, not the marker
    assert json.loads(marker_file.read_text())['sg_id'] == 'CPG_000001'


def test_registration_cli_exits_early_when_gcs_marker_exists(monkeypatch, tmp_path):
    # A stage re-queue with the marker present (e.g. a copied file went missing)
    # must not re-register anything; the existing marker content is preserved.
    existing_marker = '{"sg_id": "CPG_000001", "registered": ["gs://prior"]}'

    def fail_complete_analysis_job(*args: object) -> None:  # noqa: ARG001
        raise AssertionError('must not register when the marker already exists')

    monkeypatch.setattr(backfill_registration, 'complete_analysis_job', fail_complete_analysis_job)
    monkeypatch.setattr(backfill_registration, '_read_existing_marker', lambda path: existing_marker)  # noqa: ARG005
    marker_file = tmp_path / 'marker.json'
    monkeypatch.setattr('sys.argv', _registration_cli_argv(marker_file))

    backfill_registration.main()

    assert marker_file.read_text() == existing_marker


# --- Wiring and config guards -----------------------------------------------------------


def test_normal_mode_wiring_keeps_somalier_on_the_ica_download():
    # The test config leaves backfill disabled, so importing stages must wire
    # SomalierExtract to DownloadCramFromIca and leave the backfill stages out
    # of its dependencies.
    assert stages.BACKFILL_MODE is False
    assert stages._SOMALIER_CRAM_SOURCE is stages.DownloadCramFromIca
    # In backfill mode SomalierExtract additionally depends on BackfillGvcfsFromUpload
    # (whose copy job certifies the cram); normal mode keeps the single ICA dependency.
    assert [stages.DownloadCramFromIca] == stages._SOMALIER_REQUIRED_STAGES


def test_wiring_selectors_cover_both_modes():
    # The import-time constants only ever exercise one branch per test session
    # (conftest pins backfill off), so the selector functions are tested directly.
    assert stages.somalier_cram_source(backfill_mode=True) is stages.BackfillCramFromUpload
    assert stages.somalier_cram_source(backfill_mode=False) is stages.DownloadCramFromIca
    assert stages.somalier_required_stages(backfill_mode=True) == [
        stages.BackfillCramFromUpload,
        stages.BackfillGvcfsFromUpload,
    ]
    assert stages.somalier_required_stages(backfill_mode=False) == [stages.DownloadCramFromIca]


def test_terminal_stages_cover_both_modes():
    assert run_workflow.terminal_stages(backfill_mode=True) == [
        stages.BackfillGvcfsFromUpload,
        stages.SomalierExtract,
        stages.DeleteBackfillUpload,
    ]
    assert run_workflow.terminal_stages(backfill_mode=False) == [stages.DeleteDataInIca]


def test_registration_project_name_matches_metamists_endswith_rule(monkeypatch):
    # metamist's get_metamist_proj bumps to -test when the name does not already
    # END with -test; a substring rule ('test' in name) would leave a dataset like
    # 'testdata' unbumped here while metamist writes to 'testdata-test', making
    # the dedup and latest-recal queries read a different project than rows land in.
    monkeypatch.setattr(backfill_registration, 'get_access_level', lambda: 'test')

    assert backfill_registration.registration_project_name('testdata') == 'testdata-test'
    assert backfill_registration.registration_project_name('dataset') == 'dataset-test'
    assert backfill_registration.registration_project_name('dataset-test') == 'dataset-test'


def test_registration_project_name_is_unchanged_at_full_access(monkeypatch):
    monkeypatch.setattr(backfill_registration, 'get_access_level', lambda: 'full')

    assert backfill_registration.registration_project_name('testdata') == 'testdata'
    assert backfill_registration.registration_project_name('dataset') == 'dataset'


def _selection_config(monkeypatch, key: str, names: list[str]) -> None:
    def fake_config_retrieve(config_key, default=None):
        if tuple(config_key) == ('workflow', key):
            return names
        return default

    monkeypatch.setattr(validator, 'config_retrieve', fake_config_retrieve)


def test_backfill_stage_selection_rejects_the_shipped_last_stages_default(monkeypatch):
    # The defaults TOML ships last_stages=['DownloadDataFromIca'] for the ICA flow;
    # the validator must fail loud at submit with backfill-specific instructions.
    _selection_config(monkeypatch, 'last_stages', ['DownloadDataFromIca'])

    with pytest.raises(ValueError, match='DownloadDataFromIca'):
        validator.assert_backfill_stage_selection()


def test_backfill_stage_selection_rejects_backfill_names_that_prune_registration(monkeypatch):
    # last_stages=['SomalierExtract'] would prune BackfillGvcfsFromUpload, so the
    # run would complete green with nothing registered. The backfill graph is
    # fixed: any stage selection is rejected, backfill names included.
    _selection_config(monkeypatch, 'last_stages', ['SomalierExtract'])

    with pytest.raises(ValueError, match='SomalierExtract'):
        validator.assert_backfill_stage_selection()


def test_backfill_stage_selection_rejects_only_stages(monkeypatch):
    _selection_config(monkeypatch, 'only_stages', ['DownloadDataFromIca'])

    with pytest.raises(ValueError, match='only_stages'):
        validator.assert_backfill_stage_selection()


def test_backfill_stage_selection_rejects_skipping_a_backfill_stage(monkeypatch):
    # Skipping a requested backfill stage aborts the workflow at graph build
    # (missing expected outputs); the delete opt-out is the delete_upload flag.
    _selection_config(monkeypatch, 'skip_stages', ['DeleteBackfillUpload'])

    with pytest.raises(ValueError, match='delete_upload'):
        validator.assert_backfill_stage_selection()


def test_backfill_stage_selection_accepts_empty_selection_and_ica_skip_names(monkeypatch):
    # The shipped skip_stages=['DeleteDataInIca'] names no backfill stage and is
    # harmless; empty first/last/only_stages is the required configuration.
    _selection_config(monkeypatch, 'skip_stages', ['DeleteDataInIca'])

    validator.assert_backfill_stage_selection()


def test_backfill_rejects_a_nonempty_output_prefix(monkeypatch):
    # A non-empty analysis-runner --output-dir relocates every destination, the
    # registration marker and the delete record, defeating output reuse and the
    # duplicate-registration protection.
    def fake_config_retrieve(config_key, default=None):
        if tuple(config_key) == ('workflow', 'output_prefix'):
            return 'run2'
        return default

    monkeypatch.setattr(validator, 'config_retrieve', fake_config_retrieve)

    with pytest.raises(ValueError, match='output_prefix'):
        validator.assert_backfill_output_prefix_empty()


def test_backfill_accepts_an_empty_output_prefix(monkeypatch):
    def fake_config_retrieve(config_key, default=None):  # noqa: ARG001
        return default

    monkeypatch.setattr(validator, 'config_retrieve', fake_config_retrieve)

    validator.assert_backfill_output_prefix_empty()


# --- Submit-time staging check ----------------------------------------------------------
#
# missing_backfill_sources is pure set logic over pre-listed object names, so the
# output-reuse branches are covered without GCS; the assert wrapper is tested with a
# stubbed prefix listing to pin the single-error message shape.


def _all_rel_filenames(sg_name: str) -> set[str]:
    return {
        *utils.cram_output_filenames(sg_name).values(),
        *utils.base_gvcf_output_filenames(sg_name).values(),
        *utils.recal_gvcf_output_filenames(sg_name).values(),
    }


def _fake_sg(name: str, sg_id: str) -> SimpleNamespace:
    return SimpleNamespace(name=name, id=sg_id)


def test_staging_check_passes_for_a_fresh_run_with_everything_staged():
    sgs = [_fake_sg('SG1', 'CPG_A'), _fake_sg('SG2', 'CPG_B')]
    staged = _all_rel_filenames('SG1') | _all_rel_filenames('SG2')

    missing, unexpected = validator.missing_backfill_sources(sgs, staged=staged, ingested=set())  # type: ignore[arg-type]

    assert missing == []
    assert unexpected == []


def test_staging_check_aggregates_missing_sources_across_sequencing_groups():
    sgs = [_fake_sg('SG1', 'CPG_A'), _fake_sg('SG2', 'CPG_B')]
    staged = (_all_rel_filenames('SG1') | _all_rel_filenames('SG2')) - {
        'cram/SG1.cram',
        'recal_gvcf/SG2.hard-filtered.recal.gvcf.gz.tbi',
    }

    missing, _ = validator.missing_backfill_sources(sgs, staged=staged, ingested=set())  # type: ignore[arg-type]

    assert missing == ['cram/SG1.cram', 'recal_gvcf/SG2.hard-filtered.recal.gvcf.gz.tbi']


def test_staging_check_allows_deleted_sources_for_a_fully_ingested_sg():
    # All destinations and the registration marker exist, so both copy stages are
    # REUSEd and the (legitimately already deleted) sources are not required.
    ingested = _all_rel_filenames('SG1') | {'backfill_registration/CPG_A.json'}

    missing, unexpected = validator.missing_backfill_sources(
        [_fake_sg('SG1', 'CPG_A')],  # type: ignore[arg-type]
        staged=set(),
        ingested=ingested,
    )

    assert missing == []
    assert unexpected == []


def test_staging_check_requires_every_source_when_only_the_marker_is_missing():
    # Copied but never registered: the gVCF stage re-runs, and its copy job also
    # re-certifies the cram against its -upload source, so all eight are required.
    ingested = _all_rel_filenames('SG1')

    missing, _ = validator.missing_backfill_sources([_fake_sg('SG1', 'CPG_A')], staged=set(), ingested=ingested)  # type: ignore[arg-type]

    assert missing == sorted(_all_rel_filenames('SG1'))


def test_staging_check_requires_only_cram_sources_when_only_the_cram_destination_is_missing():
    cram_rel = set(utils.cram_output_filenames('SG1').values())
    ingested = (_all_rel_filenames('SG1') - cram_rel) | {'backfill_registration/CPG_A.json'}

    missing, _ = validator.missing_backfill_sources([_fake_sg('SG1', 'CPG_A')], staged=set(), ingested=ingested)  # type: ignore[arg-type]

    assert missing == sorted(cram_rel)


def test_staging_check_reports_misnamed_staged_objects_as_unexpected():
    staged = (_all_rel_filenames('SG1') - {'cram/SG1.cram'}) | {'cram/SG1.crm'}

    missing, unexpected = validator.missing_backfill_sources([_fake_sg('SG1', 'CPG_A')], staged=staged, ingested=set())  # type: ignore[arg-type]

    assert missing == ['cram/SG1.cram']
    assert unexpected == ['cram/SG1.crm']


def _stub_prefix_listings(monkeypatch, staged: set[str], ingested: set[str]) -> None:
    def fake_rel_names(dir_for, prefixes):  # noqa: ARG001
        return staged if dir_for is validator.get_backfill_source_path else ingested

    monkeypatch.setattr(validator, '_rel_names_under', fake_rel_names)


def test_assert_staging_raises_one_error_naming_missing_urls_and_unexpected_objects(monkeypatch):
    _stub_prefix_listings(
        monkeypatch,
        staged=(_all_rel_filenames('SG1') - {'cram/SG1.cram'}) | {'cram/SG1.crm'},
        ingested=set(),
    )
    monkeypatch.setattr(utils, 'dataset_path', lambda suffix, category=None: f'gs://up/{suffix}')  # noqa: ARG005
    cohort = SimpleNamespace(id='COH1', get_sequencing_groups=lambda: [_fake_sg('SG1', 'CPG_A')])

    with pytest.raises(RuntimeError, match=r'gs://up/output/cram/SG1\.cram') as exc_info:
        validator.assert_backfill_sources_staged(cohort)  # type: ignore[arg-type]

    assert 'cram/SG1.crm' in str(exc_info.value)


def test_assert_staging_passes_and_tolerates_objects_staged_for_another_cohort(monkeypatch):
    _stub_prefix_listings(monkeypatch, staged=_all_rel_filenames('SG1') | {'cram/OtherSG.cram'}, ingested=set())
    cohort = SimpleNamespace(id='COH1', get_sequencing_groups=lambda: [_fake_sg('SG1', 'CPG_A')])

    validator.assert_backfill_sources_staged(cohort)  # type: ignore[arg-type]
