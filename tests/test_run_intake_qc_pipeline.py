"""Unit tests for the MD5 intake-QC pipeline submission body."""

from types import SimpleNamespace

from dragen_align_pa.jobs import run_intake_qc_pipeline


def test_run_md5_pipeline_forwards_the_computed_chunk_size(monkeypatch):
    """The caller-computed chunk size must reach ICA as the pipeline's chunk_size parameter."""
    config = {
        ('ica', 'pipelines', 'md5_pipeline_id'): 'pip-md5',
        ('ica', 'tags', 'technical_tags'): [],
        ('ica', 'tags', 'user_tags'): [],
        ('ica', 'tags', 'reference_tags'): [],
    }
    monkeypatch.setattr(
        run_intake_qc_pipeline.cpg_utils.config,
        'config_retrieve',
        lambda key, default=None: config.get(tuple(key), default),
    )
    monkeypatch.setattr(run_intake_qc_pipeline.ica_api_utils, 'get_ica_api_key', lambda: 'key')

    captured: dict = {}

    def _capture_submit(*, api_instance, path_params, body, header_params):  # noqa: ARG001
        captured['body'] = body
        return 'analysis-1'

    monkeypatch.setattr(run_intake_qc_pipeline.ica_api_utils, 'submit_nextflow_analysis', _capture_submit)

    result = run_intake_qc_pipeline.run_md5_pipeline(
        cohort_name='COH1',
        fastq_list_file_id='fil.list',
        api_instance=SimpleNamespace(),
        path_parameters={'projectId': 'proj-1'},
        ar_guid='guid',
        md5_outputs_folder_id='fol.out',
        chunk_size=76,
    )

    assert result == 'analysis-1'
    parameters = {str(p['code']): str(p['value']) for p in captured['body']['analysisInput']['parameters']}
    assert parameters['chunk_size'] == '76'
