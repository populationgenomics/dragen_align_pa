import json
from pathlib import Path

from dragen_align_pa.jobs.parse_passfail import parse_passfail_file


def test_parse_passfail_all_success(demo_bundle: Path):
    result = parse_passfail_file(demo_bundle / 'passfail.json')
    assert result == {'CPG00001': 'Success', 'CPG00002': 'Success'}


def test_parse_passfail_with_failure(demo_bundle_with_failure: Path):
    result = parse_passfail_file(demo_bundle_with_failure / 'passfail.json')
    assert result == {'CPG00001': 'Success', 'CPG00002': 'Fail'}
