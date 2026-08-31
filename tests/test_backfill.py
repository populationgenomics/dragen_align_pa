"""Backfill entry point: copy externally-produced outputs from -upload into -main,
register them in metamist in a guaranteed order, then delete the -upload sources.

Covers the pure units: relative-filename maps shared with the download stages,
the -upload source path builder, the bash command builders for the copy/verify
and verify/delete jobs, and the in-process registration ordering (base gvcf
strictly before recal gvcf, so `sg.gvcf` resolves to the recal file).
"""

from dragen_align_pa import backfill_registration, stages, utils
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


def test_copy_command_copies_without_clobber_then_verifies_each_destination():
    pairs = [
        ('gs://up/output/cram/SG1.cram', 'gs://main/ica/v/output/cram/SG1.cram'),
        ('gs://up/output/cram/SG1.cram.crai', 'gs://main/ica/v/output/cram/SG1.cram.crai'),
    ]

    command = backfill.copy_command(pairs)

    assert 'set -euo pipefail' in command
    for source, destination in pairs:
        assert f"gcloud storage cp --no-clobber '{source}' '{destination}'" in command
        assert f"gcloud storage objects describe '{destination}'" in command


def test_delete_command_compares_sizes_then_removes_source():
    pairs = [('gs://up/output/cram/SG1.cram', 'gs://main/ica/v/output/cram/SG1.cram')]

    command = backfill.delete_command(pairs)

    assert 'set -euo pipefail' in command
    source, destination = pairs[0]
    assert f"gcloud storage objects describe '{source}'" in command
    assert f"gcloud storage objects describe '{destination}'" in command
    assert f"gcloud storage rm '{source}'" in command
    # A source/destination size mismatch must abort before the rm.
    assert 'exit 1' in command
    assert command.index('exit 1') < command.index('gcloud storage rm')


def test_registration_registers_base_gvcf_strictly_before_recal(monkeypatch):
    registered: list[str] = []

    def fake_complete_analysis_job(output: str, *args: object, **kwargs: object) -> None:  # noqa: ARG001
        registered.append(output)

    monkeypatch.setattr(backfill_registration, 'complete_analysis_job', fake_complete_analysis_job)

    backfill_registration.run(
        base_gvcf='gs://main/base_gvcf/SG1.hard-filtered.gvcf.gz',
        recal_gvcf='gs://main/recal_gvcf/SG1.hard-filtered.recal.gvcf.gz',
        sg_id='CPG_000001',
        project_name='test-dataset',
        meta={'stage': 'BackfillGvcfsFromUpload'},
    )

    assert registered == [
        'gs://main/base_gvcf/SG1.hard-filtered.gvcf.gz',
        'gs://main/recal_gvcf/SG1.hard-filtered.recal.gvcf.gz',
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
        base_gvcf='gs://main/base.g.vcf.gz',
        recal_gvcf='gs://main/recal.g.vcf.gz',
        sg_id='CPG_000001',
        project_name='test-dataset',
        meta={'stage': 'BackfillGvcfsFromUpload'},
    )

    assert all(call['analysis_type'] == 'gvcf' for call in calls)
    assert all(call['cohort_ids'] == [] for call in calls)
    assert all(call['sg_ids'] == ['CPG_000001'] for call in calls)
    assert all(call['project_name'] == 'test-dataset' for call in calls)
    assert all(call['meta'] == {'stage': 'BackfillGvcfsFromUpload'} for call in calls)
    assert marker == {
        'sg_id': 'CPG_000001',
        'registered': ['gs://main/base.g.vcf.gz', 'gs://main/recal.g.vcf.gz'],
    }


def test_normal_mode_wiring_keeps_somalier_on_the_ica_download():
    # The test config leaves backfill disabled, so importing stages must wire
    # SomalierExtract to DownloadCramFromIca and leave the backfill stages out
    # of its dependencies.
    assert stages.BACKFILL_MODE is False
    assert stages._SOMALIER_CRAM_SOURCE is stages.DownloadCramFromIca
