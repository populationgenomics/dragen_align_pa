"""Low-level path primitives — the single home for path-string discipline.

Holds the value types and pure parsers every path scheme is built from (deps: `constants`,
`constants_registry`, and `cpg_utils`). `IcaPath.output_root` and `IcaPath.as_url` read config
at call time — the output folder and, via `constants_registry`, the configured project name;
the remaining methods are pure string ops.

- `IcaPath` — the ICA path value type owning ICA's three incompatible slash conventions.
- `gcs_bucket_and_key` / `gcs_relative_key` — split a `gs://` path into its bucket and key.

The config-reading path *builders* that compose these (`ica_run_path`, `get_output_path`,
…) live a layer up in `ica_utils` / `utils`.
"""

from dataclasses import dataclass

import cpg_utils
from cpg_utils.config import config_retrieve

from dragen_align_pa.constants.ica_constants import BUCKET_NAME
from dragen_align_pa.constants.constants_registry import ica_project_name


@dataclass(frozen=True)
class IcaPath:
    """An ICA path assembled from segments, with slash discipline owned by the type.

    ICA needs three incompatible string forms for the same logical path.
    `IcaPath` makes the caller pick a form explicitly, so the wrong slash convention is
    hard to write:

    - `as_folder()`   → `/a/b/`             leading + trailing slash (ICA REST folder form)
    - `as_file(name)` → `/a/b/name`         leading slash, no trailing (ICA REST file form)
    - `as_url(role)`  → `ica://<name>/a/b`  scheme + project *name* resolved for `role` from
      the configured `[ica.projects].project_root` family, no trailing slash

    Build with `IcaPath.output_root()` or `IcaPath.from_relpath(...)` and join segments
    with `/`. Segments may themselves contain `/`; empty parts are dropped so slashes
    never double up.

    `str()` / f-string interpolation deliberately raise: there is no single correct
    string form, so a bare `str(ica_path)` is always a bug. Call a terminal method.
    """

    _segments: tuple[str, ...]

    @staticmethod
    def _split(*parts: str) -> tuple[str, ...]:
        """Flatten `parts` into clean segments, dropping empties and stray slashes."""
        return tuple(segment for part in parts for segment in part.split('/') if segment)

    @classmethod
    def under_bucket(cls, *segments: str) -> 'IcaPath':
        """`IcaPath` rooted at the GCS bucket: `/{BUCKET_NAME}/…`.

        The primitive behind `output_root`; use it directly for ICA folders that hang off a
        non-output config folder (e.g. the upload staging area, rooted at `upload_folder`).

        Args:
            segments: Path segments below the bucket; each may contain `/`, empties dropped.
        """
        return cls(cls._split(BUCKET_NAME, *segments))

    @classmethod
    def output_root(cls) -> 'IcaPath':
        """Build the ICA output root `/{BUCKET_NAME}/{output_folder}`.

        `[ica.data_prep][output_folder]` is read at call time, so the root stays monkeypatchable
        in tests and configurable per run.

        Raises:
            cpg_utils.config.ConfigError: If `[ica.data_prep][output_folder]` is unset.
        """
        return cls.under_bucket(config_retrieve(['ica', 'data_prep', 'output_folder']))

    @classmethod
    def from_relpath(cls, relpath: str) -> 'IcaPath':
        """Build an `IcaPath` from a project-relative path; leading/trailing/repeated slashes are ignored."""
        return cls(cls._split(relpath))

    def __truediv__(self, segment: str) -> 'IcaPath':
        """Return a new `IcaPath` with `segment` appended — the `/` operator.

        A `segment` containing `/` is split into several segments; empty parts are dropped so
        joins never produce `//`. The original is left unchanged (the type is frozen).
        """
        return IcaPath(self._segments + self._split(segment))

    def as_folder(self) -> str:
        """Render the ICA REST folder form `/a/b/` (leading + trailing slash), or `/` when empty."""
        if not self._segments:
            return '/'
        return '/' + '/'.join(self._segments) + '/'

    def as_file(self, filename: str) -> str:
        """Render the ICA REST file form `/a/b/filename` (leading, no trailing slash).

        Args:
            filename: The file's basename; surrounding slashes are stripped.

        Raises:
            ValueError: If `filename` is empty once surrounding slashes are stripped.
        """
        name = filename.strip('/')
        if not name:
            raise ValueError(f'as_file() needs a non-empty filename, got {filename!r}')
        return self.as_folder() + name

    def as_url(self, project_role: str) -> str:
        """Render the `ica://<project-name>/a/b` URL for this path (no trailing slash).

        The role resolves to the ICA project *name* via `ica_project_name` (reads the configured
        `[ica.projects].project_root` family) — not the numeric project ID REST `path_params`
        use. Resolution fails loud on a missing/unknown family rather than producing an
        `ica://None/...` URL that fails opaquely inside ICA.

        Args:
            project_role: The ICA role (a `constants_registry.REQUIRED_ICA_ROLES` value, e.g.
                `ROLE_DRAGEN_MLR`) to resolve to an ICA project name.

        Raises:
            cpg_utils.config.ConfigError: If `[ica.projects].project_root` is unset.
        """
        return f'ica://{ica_project_name(project_role)}/' + '/'.join(self._segments)

    def __str__(self) -> str:
        """Refuse to render a default string form.

        Raises:
            TypeError: Always. The slash convention differs by consumer, so a bare
                `str(ica_path)` is always a bug — call `as_folder`, `as_file`, or `as_url`.
        """
        raise TypeError(
            'IcaPath has no default string form — the slash convention differs by consumer. '
            'Call .as_folder(), .as_file(name), or .as_url(role).',
        )


def gcs_bucket_and_key(path: cpg_utils.Path | str) -> tuple[str, str]:
    """Split a `gs://` path into its bucket and object key.

    The `gs://` scheme is required so a malformed path fails loud here rather than silently
    yielding a wrong split.

    Args:
        path: A `gs://bucket/key` path (a `cpg_utils.Path` or its string form).

    Returns:
        `(bucket, key)`, e.g. `gs://bkt/a/b/c` → `('bkt', 'a/b/c')`.

    Raises:
        ValueError: If `path` does not start with the `gs://` scheme.
    """
    text = str(path)
    if not text.startswith('gs://'):
        raise ValueError(f'Expected a gs:// path, got {text!r}')
    bucket, _, key = text.removeprefix('gs://').partition('/')
    return bucket, key


def gcs_relative_key(path: cpg_utils.Path | str) -> str:
    """Return the object key of a `gs://` path — everything after `gs://<bucket>/`.

    Args:
        path: A `gs://bucket/key` path (a `cpg_utils.Path` or its string form).

    Returns:
        The bucket-relative object key, e.g. `gs://bkt/a/b/c` → `a/b/c`.

    Raises:
        ValueError: If `path` does not start with the `gs://` scheme.
    """
    return gcs_bucket_and_key(path)[1]
