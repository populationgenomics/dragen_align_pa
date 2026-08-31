"""Backfill entry point: copy externally-produced outputs from -upload into -main,
register them in metamist in a guaranteed order, then delete the -upload sources.

Covers the pure units (relative-filename maps shared with the download stages, the
-upload source path builder, registration ordering) and executes the generated bash
command scripts against a fake `gcloud` on PATH, so the skip-vs-fail and
verify-before-rm branches are proven by behavior rather than string matching.
"""

import json
import os
import subprocess
from pathlib import Path

import pytest

from dragen_align_pa import backfill_registration, stages, utils, validator
from dragen_align_pa.jobs import backfill


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


# --- Command-script execution harness -------------------------------------------------
#
# Each test installs a fake `gcloud` dispatch script on PATH and runs the generated
# command under bash. The fake logs every invocation to gcloud_calls.log so tests can
# assert exactly which operations ran (in particular: that `rm` did or did not).

_PAIR = ('gs://up/output/cram/SG1.cram', 'gs://main/ica/v/output/cram/SG1.cram')


def _run_script(tmp_path: Path, script: str, fake_gcloud_body: str) -> subprocess.CompletedProcess:
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir(exist_ok=True)
    fake_gcloud = bin_dir / 'gcloud'
    fake_gcloud.write_text('#!/bin/bash\necho "$@" >> gcloud_calls.log\n' + fake_gcloud_body)
    fake_gcloud.chmod(0o755)
    return subprocess.run(  # noqa: S603
        ['/bin/bash', '-c', script],  # noqa: S607
        env=os.environ | {'PATH': f'{bin_dir}:{os.environ["PATH"]}'},
        capture_output=True,
        text=True,
        cwd=tmp_path,
        check=False,
    )


def _gcloud_calls(tmp_path: Path) -> str:
    log = tmp_path / 'gcloud_calls.log'
    return log.read_text() if log.exists() else ''


# Fake gcloud: describe answers per-URL from DESCRIBE_<n> case branches; cp/rm succeed.
def _fake_gcloud(describe_case_body: str) -> str:
    return f"""
if [ "$1 $2 $3" == 'storage objects describe' ]; then
    case "$4" in
{describe_case_body}
    esac
elif [ "$1 $2" == 'storage cp' ] || [ "$1 $2" == 'storage rm' ]; then
    exit 0
fi
"""


def test_copy_command_succeeds_when_checksums_match(tmp_path):
    source, destination = _PAIR
    fake = _fake_gcloud(f"""
        '{source}') echo 'abc123' ;;
        '{destination}') echo 'abc123' ;;
""")

    result = _run_script(tmp_path, backfill.copy_command([_PAIR]), fake)

    assert result.returncode == 0, result.stderr
    assert f'storage cp --no-clobber {source} {destination}' in _gcloud_calls(tmp_path)


def test_copy_command_fails_when_destination_checksum_differs(tmp_path):
    # A pre-existing stale destination survives --no-clobber; the checksum
    # comparison must fail the job rather than report a successful copy.
    source, destination = _PAIR
    fake = _fake_gcloud(f"""
        '{source}') echo 'abc123' ;;
        '{destination}') echo 'zzz999' ;;
""")

    result = _run_script(tmp_path, backfill.copy_command([_PAIR]), fake)

    assert result.returncode != 0
    assert 'mismatch' in result.stderr.lower()


def test_copy_command_quotes_awkward_paths(tmp_path):
    # shlex-quoted arguments must reach gcloud intact even with a quote in the path.
    source = "gs://up/output/cram/SG'1.cram"
    destination = 'gs://main/ica/v/output/cram/SG1.cram'
    fake = _fake_gcloud("""
        *) echo 'abc123' ;;
""")

    result = _run_script(tmp_path, backfill.copy_command([(source, destination)]), fake)

    assert result.returncode == 0, result.stderr
    assert f'storage cp --no-clobber {source} {destination}' in _gcloud_calls(tmp_path)


def _run_delete(tmp_path: Path, fake_gcloud_body: str) -> subprocess.CompletedProcess:
    script = f'RESULTS={tmp_path}/results.txt\n{backfill.delete_command([_PAIR])}'
    return _run_script(tmp_path, script, fake_gcloud_body)


def test_delete_command_removes_source_when_checksums_match(tmp_path):
    source, destination = _PAIR
    fake = _fake_gcloud(f"""
        '{source}') echo 'abc123' ;;
        '{destination}') echo 'abc123' ;;
""")

    result = _run_delete(tmp_path, fake)

    assert result.returncode == 0, result.stderr
    assert f'storage rm {source}' in _gcloud_calls(tmp_path)
    assert (tmp_path / 'results.txt').read_text() == f'deleted {source}\n'


def test_delete_command_aborts_before_rm_on_checksum_mismatch(tmp_path):
    source, destination = _PAIR
    fake = _fake_gcloud(f"""
        '{source}') echo 'abc123' ;;
        '{destination}') echo 'zzz999' ;;
""")

    result = _run_delete(tmp_path, fake)

    assert result.returncode != 0
    assert 'storage rm' not in _gcloud_calls(tmp_path)


def test_delete_command_skips_source_that_is_genuinely_absent(tmp_path):
    source, destination = _PAIR
    fake = _fake_gcloud(f"""
        '{source}') echo 'ERROR: {source} not found: 404.' >&2; exit 1 ;;
        '{destination}') echo 'abc123' ;;
""")

    result = _run_delete(tmp_path, fake)

    assert result.returncode == 0, result.stderr
    assert 'storage rm' not in _gcloud_calls(tmp_path)
    assert (tmp_path / 'results.txt').read_text() == f'already-absent {source}\n'


def test_delete_command_fails_on_transient_describe_error(tmp_path):
    # A 503/auth failure is NOT absence: the job must fail so the stage re-runs,
    # instead of silently orphaning the -upload file forever.
    source, destination = _PAIR
    fake = _fake_gcloud(f"""
        '{source}') echo 'ERROR: 503 backend error' >&2; exit 1 ;;
        '{destination}') echo 'abc123' ;;
""")

    result = _run_delete(tmp_path, fake)

    assert result.returncode != 0
    assert '503' in result.stderr
    assert 'storage rm' not in _gcloud_calls(tmp_path)


def test_delete_command_fails_when_destination_is_missing(tmp_path):
    source, destination = _PAIR
    fake = _fake_gcloud(f"""
        '{source}') echo 'abc123' ;;
        '{destination}') echo 'ERROR: {destination} not found: 404.' >&2; exit 1 ;;
""")

    result = _run_delete(tmp_path, fake)

    assert result.returncode != 0
    assert 'storage rm' not in _gcloud_calls(tmp_path)


# --- Registration ----------------------------------------------------------------------


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


def test_registration_cli_writes_marker_file_as_pure_json(monkeypatch, tmp_path, capsys):
    # complete_analysis_job logs to stdout, so the marker must be written to an
    # explicit file, not captured from stdout redirection.
    def fake_complete_analysis_job(*args: object) -> None:  # noqa: ARG001
        print('Created Analysis(id=1, type=gvcf) log noise')

    monkeypatch.setattr(backfill_registration, 'complete_analysis_job', fake_complete_analysis_job)
    marker_file = tmp_path / 'marker.json'
    monkeypatch.setattr(
        'sys.argv',
        [
            'backfill_registration',
            '--cram', 'gs://main/SG1.cram',
            '--base-gvcf', 'gs://main/base.g.vcf.gz',
            '--recal-gvcf', 'gs://main/recal.g.vcf.gz',
            '--sg-id', 'CPG_000001',
            '--project-name', 'test-dataset',
            '--meta-json', '{"stage": "BackfillGvcfsFromUpload"}',
            '--marker-file', str(marker_file),
        ],
    )

    backfill_registration.main()

    capsys.readouterr()  # log noise goes to stdout, not the marker
    assert json.loads(marker_file.read_text())['sg_id'] == 'CPG_000001'


# --- Wiring and config guards -----------------------------------------------------------


def test_normal_mode_wiring_keeps_somalier_on_the_ica_download():
    # The test config leaves backfill disabled, so importing stages must wire
    # SomalierExtract to DownloadCramFromIca and leave the backfill stages out
    # of its dependencies.
    assert stages.BACKFILL_MODE is False
    assert stages._SOMALIER_CRAM_SOURCE is stages.DownloadCramFromIca


def test_backfill_stage_selection_rejects_ica_stage_in_last_stages(monkeypatch):
    # The defaults TOML ships last_stages=['DownloadDataFromIca'], which names a
    # stage that doesn't exist in the backfill graph; the validator must fail loud
    # at submit rather than let cpg-flow abort with a generic message.
    def fake_config_retrieve(key, default=None):
        if tuple(key) == ('workflow', 'last_stages'):
            return ['DownloadDataFromIca']
        return default

    monkeypatch.setattr(validator, 'config_retrieve', fake_config_retrieve)

    with pytest.raises(ValueError, match='DownloadDataFromIca'):
        validator.assert_backfill_stage_selection()


def test_backfill_stage_selection_accepts_backfill_stage_names(monkeypatch):
    def fake_config_retrieve(key, default=None):
        if tuple(key) == ('workflow', 'last_stages'):
            return ['SomalierExtract']
        return default

    monkeypatch.setattr(validator, 'config_retrieve', fake_config_retrieve)

    validator.assert_backfill_stage_selection()
