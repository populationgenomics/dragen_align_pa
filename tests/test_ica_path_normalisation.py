"""Tests that ICA folder/file paths are normalised to a single leading AND
trailing slash before reaching the ICA API, regardless of how the caller
slashes its input.

ICA's `parentFolderPath`/`filePath` queries throws an API error when
the slash convention is off, so this invariant is load-bearing. The helpers
each normalise their own query, so a caller passing `foo/bar`, `/foo/bar`,
`/foo/bar/`, or `//foo/bar//` must all produce the same canonical path. These
tests capture the query_params handed to a MagicMock api_instance — no live
ICA calls.
"""

from unittest.mock import MagicMock

import pytest

from dragen_align_pa import ica_api_utils, ica_utils

# Every variant must normalise to the same canonical `/foo/bar/`.
_PARENT_VARIANTS = ['foo/bar', '/foo/bar', '/foo/bar/', '//foo/bar//', 'foo/bar/']


def _capture_api(body: dict) -> tuple[MagicMock, list[dict]]:
    """MagicMock api_instance that records each query_params and returns `body`."""
    calls: list[dict] = []

    def fake_list(path_params, query_params):  # noqa: ARG001
        calls.append(query_params)
        return MagicMock(body=body)

    api = MagicMock()
    api.get_project_data_list.side_effect = fake_list
    return api, calls


@pytest.mark.parametrize('parent', _PARENT_VARIANTS)
def test_find_file_id_by_name_normalises_parent_folder_path(parent: str):
    api, calls = _capture_api({'items': [{'data': {'id': 'fid_x'}}]})

    result = ica_api_utils.find_file_id_by_name(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        parent_folder_path=parent,
        file_name='all_md5.txt',
    )

    assert result == 'fid_x'
    assert calls[0]['parentFolderPath'] == '/foo/bar/'


@pytest.mark.parametrize('parent', _PARENT_VARIANTS)
def test_get_file_details_from_ica_normalises_parent_folder_path(parent: str):
    api, calls = _capture_api({'items': [{'data': {'id': 'fid_x', 'details': {'status': 'AVAILABLE'}}}]})

    result = ica_api_utils.get_file_details_from_ica(
        api_instance=api,
        path_params={'projectId': 'p'},
        ica_folder_path=parent,
        file_name='sample.cram',
    )

    assert result is not None
    assert calls[0]['parentFolderPath'] == '/foo/bar/'


@pytest.mark.parametrize('parent', _PARENT_VARIANTS)
def test_check_object_already_exists_normalises_file_path(parent: str):
    api, calls = _capture_api({'items': [{'data': {'id': 'fid_x', 'details': {'status': 'AVAILABLE'}}}]})

    ica_api_utils.check_object_already_exists(
        api_instance=api,
        path_params={'projectId': 'p'},
        file_name='thing.csv',
        folder_path=parent,
        object_type='FILE',
    )

    assert calls[0]['filePath'] == ['/foo/bar/thing.csv']


@pytest.mark.parametrize('parent', _PARENT_VARIANTS)
def test_list_ica_files_normalises_leading_and_trailing_slash(parent: str):
    api, calls = _capture_api({'items': [], 'nextPageToken': None})

    ica_utils.list_ica_files(
        api_instance=api,
        path_parameters={'projectId': 'p'},
        base_ica_folder_path=parent,
    )

    assert calls[0]['parentFolderPath'] == '/foo/bar/'


@pytest.mark.parametrize('folder', _PARENT_VARIANTS)
def test_create_upload_object_id_normalises_path_on_create(folder: str):
    """When the object doesn't yet exist, both the existence-check `filePath`
    and the `CreateData` `folderPath` carry a single leading+trailing slash."""
    api, calls = _capture_api({'items': []})  # not found → proceeds to create
    api.create_data_in_project.return_value = MagicMock(
        body={'data': {'id': 'new_fid', 'details': {'status': 'AVAILABLE'}}},
    )

    file_id, status = ica_utils.create_upload_object_id(
        api_instance=api,
        path_params={'projectId': 'p'},
        folder_name='myfolder',
        file_name='thing.csv',
        folder_path=folder,
        object_type='FILE',
    )

    assert (file_id, status) == ('new_fid', 'AVAILABLE')
    # Existence check reflects the normalised folder_path.
    assert calls[0]['filePath'] == ['/foo/bar/thing.csv']
    # CreateData body carries a single leading+trailing slash (no double slash).
    body = api.create_data_in_project.call_args.kwargs['body']
    assert body['folderPath'] == '/foo/bar/'
