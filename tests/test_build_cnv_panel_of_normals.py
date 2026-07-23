"""Tests for the CNV panel-of-normals builder script.

The script lives under `scripts/` (not the package), so it's loaded by path.
The one behaviour worth pinning is that the JSON block it prints for an operator
to paste into `ICA_PON_FILE_IDS` round-trips cleanly through the consumer,
`constants_registry.resolve_cnv_normals_panel` — if the emitter and resolver
ever drift, this test catches it.
"""

import importlib.util
import json
from pathlib import Path

from dragen_align_pa.constants import constants_registry

_BUILDER_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'build_cnv_panel_of_normals.py'


def _load_builder():
    spec = importlib.util.spec_from_file_location('build_cnv_panel_of_normals', _BUILDER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_registration_snippet_round_trips_through_resolver(capsys, monkeypatch):
    """The block the builder prints must merge into ICA_PON_FILE_IDS and resolve
    cleanly — the emitted `count_file_ids` list excludes the normals-list file and
    preserves count order, and the resolver returns list-first then counts."""
    builder = _load_builder()
    # build_panel returns {basename: fil_id}, including the <panel>.normals.txt list file.
    file_ids = {
        'panel-z.normals.txt': 'fil.list',
        'sgA_pon.target.counts.gc-corrected.gz': 'fil.c1',
        'sgB_pon.target.counts.gc-corrected.gz': 'fil.c2',
    }
    builder._print_registration_snippet('panel-z', file_ids)

    # The builder's status line goes through loguru (stderr), so capsys stdout holds
    # only the `# --- CNV PON: … ---` comment (no brace) then the JSON block — the
    # first '{' is therefore the start of the JSON object.
    out = capsys.readouterr().out
    parsed = json.loads(out[out.index('{') :])
    assert parsed == {
        'panel-z': {'pon_list_file': 'fil.list', 'count_file_ids': ['fil.c1', 'fil.c2']},
    }

    # Round-trip through the actual consumer.
    monkeypatch.setattr('dragen_align_pa.constants.ica_constants.ICA_PON_FILE_IDS', parsed)
    list_basename, resolved_ids = constants_registry.resolve_cnv_normals_panel('panel-z')
    assert list_basename == 'panel-z.normals.txt'
    assert resolved_ids == ['fil.list', 'fil.c1', 'fil.c2']
