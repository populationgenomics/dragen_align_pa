"""Unit tests for `ica_utils.list_ica_files`.

The helper walks an ICA folder via repeated `parentFolderPath` queries
(plus subfolder traversal when `recursive=True`). These tests use a
MagicMock api_instance configured to respond to (parentFolderPath, type)
pairs deterministically — no live ICA calls.
"""

from unittest.mock import MagicMock

import icasdk
import pytest

from dragen_align_pa.ica_utils import list_ica_files


def _file_item(name: str, fid: str) -> dict:
    return {'data': {'id': fid, 'details': {'name': name}}}


def _folder_item(name: str) -> dict:
    return {'data': {'details': {'name': name}}}


def _make_api(children: dict[tuple[str, str], list[dict]]) -> MagicMock:
    """Build a MagicMock api_instance that returns `children[parent, type]`.

    The mapping is keyed on (parentFolderPath, type). Anything not in the
    mapping returns an empty page. Single-page responses only (no pagination
    in these tests — see the dedicated pagination tests).
    """

    def fake_list(path_params, query_params):  # noqa: ARG001
        parent = query_params['parentFolderPath']
        type_ = query_params['type']
        items = children.get((parent, type_), [])
        return MagicMock(body={'items': items, 'nextPageToken': None})

    api = MagicMock()
    api.get_project_data_list.side_effect = fake_list
    return api


def test_list_ica_files_non_recursive_returns_leaf_names():
    """Default (recursive=False) returns just the leaf names of files at
    the top level. No subfolder traversal."""
    children = {
        ('/base/', 'FILE'): [
            _file_item('a.txt', 'fid_a'),
            _file_item('b.txt', 'fid_b'),
        ],
    }
    api = _make_api(children)

    result = list_ica_files(
        api_instance=api, path_parameters={'projectId': 'p'}, base_ica_folder_path='/base/',
    )
    assert sorted(result) == [('a.txt', 'fid_a'), ('b.txt', 'fid_b')]


def test_list_ica_files_non_recursive_does_not_traverse_subfolders():
    """With recursive=False, files in subfolders are NOT returned even
    when the subfolders exist. The FOLDER query is skipped entirely."""
    children = {
        ('/base/', 'FILE'): [_file_item('top.html', 'fid_top')],
        ('/base/', 'FOLDER'): [_folder_item('sub')],
        ('/base/sub/', 'FILE'): [_file_item('hidden.csv', 'fid_hidden')],
    }
    api = _make_api(children)

    result = list_ica_files(
        api_instance=api, path_parameters={'projectId': 'p'}, base_ica_folder_path='/base/',
    )

    assert result == [('top.html', 'fid_top')]
    # Verify the FOLDER query was never issued — only the top-level FILE query.
    calls = api.get_project_data_list.call_args_list
    types = [c.kwargs['query_params']['type'] for c in calls]
    assert types == ['FILE']


def test_list_ica_files_recursive_returns_relative_paths():
    """Files inside nested subfolders return a relative_path that includes
    every intermediate folder name, separated by `/`."""
    children = {
        ('/base/', 'FILE'): [_file_item('top.html', 'fid_top')],
        ('/base/', 'FOLDER'): [_folder_item('report_files')],
        ('/base/report_files/', 'FILE'): [_file_item('mid.csv', 'fid_mid')],
        ('/base/report_files/', 'FOLDER'): [_folder_item('samples')],
        ('/base/report_files/samples/', 'FILE'): [
            _file_item('a.csv', 'fid_a'),
            _file_item('b.csv', 'fid_b'),
        ],
        ('/base/report_files/samples/', 'FOLDER'): [],
    }
    api = _make_api(children)

    result = list_ica_files(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        base_ica_folder_path='/base/',
        recursive=True,
    )

    assert sorted(result) == sorted([
        ('top.html', 'fid_top'),
        ('report_files/mid.csv', 'fid_mid'),
        ('report_files/samples/a.csv', 'fid_a'),
        ('report_files/samples/b.csv', 'fid_b'),
    ])


def test_list_ica_files_empty_base_folder_returns_empty_list():
    api = _make_api({})
    result = list_ica_files(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        base_ica_folder_path='/empty/',
        recursive=True,
    )
    assert result == []


def test_list_ica_files_normalises_trailing_slash():
    """The base path should be normalised to end with `/` — passing
    `/base` (no trailing slash) is treated the same as `/base/`."""
    children = {('/base/', 'FILE'): [_file_item('a.txt', 'fid_a')]}
    api = _make_api(children)

    result = list_ica_files(
        api_instance=api, path_parameters={'projectId': 'p'}, base_ica_folder_path='/base',
    )
    assert result == [('a.txt', 'fid_a')]


def test_list_ica_files_skips_items_with_missing_name_or_id():
    """Defensive: malformed API items (no `name` or no `id`) are skipped
    with a warning rather than crashing the walk."""
    children = {
        ('/base/', 'FILE'): [
            _file_item('ok.txt', 'fid_ok'),
            {'data': {'id': 'fid_no_name', 'details': {}}},  # no name
            {'data': {'details': {'name': 'no_id.txt'}}},  # no id
        ],
        ('/base/', 'FOLDER'): [],
    }
    api = _make_api(children)

    result = list_ica_files(
        api_instance=api, path_parameters={'projectId': 'p'}, base_ica_folder_path='/base/',
    )
    assert result == [('ok.txt', 'fid_ok')]


def test_list_ica_files_propagates_api_exception_mid_walk():
    """An ApiException from the SDK during the walk propagates to the
    caller — the helper is not transactional, so re-running is the recovery
    path."""
    api = MagicMock()
    api.get_project_data_list.side_effect = icasdk.ApiException(status=500, reason='boom')

    with pytest.raises(icasdk.ApiException, match='boom'):
        list_ica_files(
            api_instance=api,
            path_parameters={'projectId': 'p'},
            base_ica_folder_path='/base/',
            recursive=True,
        )


def test_list_ica_files_handles_pagination_for_files():
    """Multi-page FILE listings are concatenated correctly across pages."""
    page_a = MagicMock(body={
        'items': [_file_item('a.txt', 'fid_a')],
        'nextPageToken': 'token-1',
    })
    page_b = MagicMock(body={
        'items': [_file_item('b.txt', 'fid_b')],
        'nextPageToken': None,
    })
    page_folders = MagicMock(body={'items': [], 'nextPageToken': None})

    api = MagicMock()
    # File listing returns two pages; folder listing returns empty.
    file_pages = iter([page_a, page_b])

    def fake_list(path_params, query_params):  # noqa: ARG001
        if query_params['type'] == 'FILE':
            return next(file_pages)
        return page_folders

    api.get_project_data_list.side_effect = fake_list

    result = list_ica_files(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        base_ica_folder_path='/base/',
        recursive=True,
    )
    assert sorted(result) == [('a.txt', 'fid_a'), ('b.txt', 'fid_b')]


def test_list_ica_files_handles_pagination_for_folders():
    """Multi-page FOLDER listings are concatenated correctly across pages,
    and the walker recurses into every folder from every page."""
    base_files = MagicMock(body={'items': [], 'nextPageToken': None})
    folders_page_a = MagicMock(body={
        'items': [_folder_item('alpha')],
        'nextPageToken': 'fld-token-1',
    })
    folders_page_b = MagicMock(body={
        'items': [_folder_item('beta')],
        'nextPageToken': None,
    })
    # Each subfolder has one file and no sub-subfolders.
    alpha_files = MagicMock(body={
        'items': [_file_item('a.csv', 'fid_a')],
        'nextPageToken': None,
    })
    beta_files = MagicMock(body={
        'items': [_file_item('b.csv', 'fid_b')],
        'nextPageToken': None,
    })
    empty_folders = MagicMock(body={'items': [], 'nextPageToken': None})

    api = MagicMock()
    folder_pages_at_base = iter([folders_page_a, folders_page_b])

    def fake_list(path_params, query_params):  # noqa: ARG001
        parent = query_params['parentFolderPath']
        type_ = query_params['type']
        if parent == '/base/' and type_ == 'FILE':
            return base_files
        if parent == '/base/' and type_ == 'FOLDER':
            return next(folder_pages_at_base)
        if parent == '/base/alpha/' and type_ == 'FILE':
            return alpha_files
        if parent == '/base/beta/' and type_ == 'FILE':
            return beta_files
        return empty_folders

    api.get_project_data_list.side_effect = fake_list

    result = list_ica_files(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        base_ica_folder_path='/base/',
        recursive=True,
    )
    assert sorted(result) == [('alpha/a.csv', 'fid_a'), ('beta/b.csv', 'fid_b')]
