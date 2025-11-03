"""
Unit tests for the refactored helper functions in the pipeline.
"""

import json
from unittest import mock

import pytest

from dragen_align_pa.jobs.manage_dragen_mlr import _mlr_build_popgen_cli_command

# Functions to test
from dragen_align_pa.jobs.run_align_genotype_with_dragen import (
    _build_common_parameters,
    _build_cram_specific_inputs,
)
from dragen_align_pa.utils import validate_cli_path_input

# --- Tests for dragen_align_pa.utils ---


def test_validate_cli_path_input_safe():
    """
    Tests that validate_cli_path_input passes for safe, common paths.
    """
    try:
        validate_cli_path_input('gs://my-bucket/path/file_1.txt', 'test_path_1')
        validate_cli_path_input('/local/path/file-with-dashes.vcf.gz', 'test_path_2')
        validate_cli_path_input('simple_filename', 'test_path_3')
    except ValueError:
        pytest.fail('validate_cli_path_input raised ValueError unexpectedly on safe paths')


def test_validate_cli_path_input_unsafe():
    """
    Tests that validate_cli_path_input raises ValueError for unsafe strings.
    """
    unsafe_paths = [
        'file; rm -rf /',
        'file && echo "pwned"',
        '$(ls)',
        'file | malicious_script.sh',
        'file with spaces',
        '"quoted_file"',
        'file_with_newline\n',
    ]

    for path in unsafe_paths:
        with pytest.raises(ValueError, match='Potential unsafe characters'):
            validate_cli_path_input(path, 'test_unsafe_path')


# --- Tests for run_align_genotype_with_dragen.py ---


def test_build_common_parameters():
    """
    Tests that _build_common_parameters returns the expected list of parameters.
    """
    params = _build_common_parameters()
    assert isinstance(params, list)
    assert len(params) == 13  # Should have 13 common parameters

    # Check for a few key parameters
    param_codes = {p.code for p in params}
    assert 'enable_map_align' in param_codes
    assert 'enable_variant_caller' in param_codes
    assert 'vc_emit_ref_confidence' in param_codes

    # Check a specific value
    gvcf_param = next(p for p in params if p.code == 'vc_emit_ref_confidence')
    assert gvcf_param.value == 'GVCF'


@mock.patch('dragen_align_pa.jobs.run_align_genotype_with_dragen.config_retrieve')
def test_build_cram_specific_inputs(mock_config_retrieve):
    """
    Tests that _build_cram_specific_inputs builds the correct objects.
    We mock config_retrieve and the file open/read.
    """
    # Mock the config call for the CRAM reference
    mock_config_retrieve.side_effect = [
        'my_old_ref',  # First call (old_cram_reference)
        'ref_id_123',  # Second call (lookup for 'my_old_ref')
    ]

    # Mock the cram_ica_fids_path file
    mock_path = mock.Mock()
    mock_file_content = json.dumps({'cram_fid': 'cram_id_456'})
    mock_path.open.return_value = mock.MagicMock(
        __enter__=lambda: mock.MagicMock(read=lambda: mock_file_content),
        __exit__=lambda *a: None,
    )

    # Call the function
    data, params = _build_cram_specific_inputs(
        cram_ica_fids_path=mock_path,
        qc_cov_region_1_id='qc1_id',
        qc_cov_region_2_id='qc2_id',
    )

    # --- Test Data Inputs ---
    assert isinstance(data, list)
    assert len(data) == 4
    data_codes = {d.parameterCode for d in data}
    assert 'crams' in data_codes
    assert 'cram_reference' in data_codes
    assert 'qc_coverage_region_1' in data_codes

    # Check dataId values
    cram_data = next(d for d in data if d.parameterCode == 'crams')
    assert cram_data.dataIds == ['cram_id_456']

    ref_data = next(d for d in data if d.parameterCode == 'cram_reference')
    assert ref_data.dataIds == ['ref_id_123']

    qc1_data = next(d for d in data if d.parameterCode == 'qc_coverage_region_1')
    assert qc1_data.dataIds == ['qc1_id']

    # --- Test Parameter Inputs ---
    assert isinstance(params, list)
    assert len(params) == 1
    assert params[0].code == 'additional_args'
    assert '--read-trimmers polyg' in params[0].value


# --- Tests for manage_dragen_mlr.py ---


@mock.patch('dragen_align_pa.jobs.manage_dragen_mlr.config_retrieve')
def test_mlr_build_popgen_cli_command(mock_config_retrieve):
    """
    Tests that the MLR popgen-cli command is built correctly.
    """
    # Mock the config call for analysis_instance_tier
    mock_config_retrieve.return_value = 'standard'

    cmd = _mlr_build_popgen_cli_command(
        local_config_path='/tmp/config.json',
        output_analysis_json_folder='output_json_folder',
        run_id='test_run_id',
        sample_id='test_sample_id',
        mlr_hash_table='ica://path/to/hashtable',
        output_folder_url='ica://path/to/output',
        cram_url='ica://path/to/cram',
        gvcf_url='ica://path/to/gvcf',
    )

    assert isinstance(cmd, list)
    assert cmd[0] == 'popgen-cli'
    assert cmd[1] == 'dragen-mlr'
    assert cmd[2] == 'submit'

    # Check for key arguments
    assert '--run-id' in cmd
    assert cmd[cmd.index('--run-id') + 1] == 'test_run_id'

    assert '--input-align-file-url' in cmd
    assert cmd[cmd.index('--input-align-file-url') + 1] == 'ica://path/to/cram'

    assert '--analysis-instance-tier' in cmd
    assert cmd[cmd.index('--analysis-instance-tier') + 1] == 'standard'

    # Ensure config_retrieve was called
    mock_config_retrieve.assert_called_with(['ica', 'mlr', 'analysis_instance_tier'])
