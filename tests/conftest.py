from pathlib import Path

import pytest

from tests.fixtures.generate_demo_bundle import generate_demo_bundle

DEMO_USER_REFERENCE = 'COH0001-batch0000_test-guid_'
DEMO_PIPELINE_ID = '00000000-1111-2222-3333-444444444444'
DEMO_SAMPLES = ('CPG00001', 'CPG00002')


@pytest.fixture
def demo_bundle(tmp_path: Path) -> Path:
    """Materialise a synthetic ICA analysis output bundle under tmp_path."""
    return generate_demo_bundle(
        output_root=tmp_path,
        samples=DEMO_SAMPLES,
        user_reference=DEMO_USER_REFERENCE,
        pipeline_id=DEMO_PIPELINE_ID,
    )


@pytest.fixture
def demo_bundle_with_failure(tmp_path: Path) -> Path:
    """Materialise a synthetic bundle where CPG00002 is marked Fail."""
    return generate_demo_bundle(
        output_root=tmp_path,
        samples=DEMO_SAMPLES,
        user_reference=DEMO_USER_REFERENCE,
        pipeline_id=DEMO_PIPELINE_ID,
        failed_samples=('CPG00002',),
    )
