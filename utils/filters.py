from __future__ import annotations

import os


def path_accepted(
    path: str,
    *,
    include_extensions: set[str] | None = None,
    exclude_extensions: set[str] | None = None,
    include_directories: set[str] | None = None,
    exclude_directories: set[str] | None = None,
) -> bool:
    """Return True if path passes all extension and directory filters.

    Extension sets should contain lowercase dot-prefixed strings (e.g. {'.py', '.md'}).
    Directory sets should contain path prefixes to match against (e.g. {'src', 'docs'}).

    Rules (each filter is skipped when its set is None or empty):
    - include_extensions: path must have one of the listed extensions
    - exclude_extensions: path must NOT have any of the listed extensions
    - include_directories: path must start with one of the listed prefixes
    - exclude_directories: path must NOT start with any of the listed prefix
    """
    ext = os.path.splitext(path)[1].lower()

    if include_extensions and ext not in include_extensions:
        return False
    if exclude_extensions and ext in exclude_extensions:
        return False

    # Normalise path separators for consistent prefix matching
    norm = path.replace("\\", "/")

    if include_directories:
        if not any(norm.startswith(d.rstrip("/") + "/") or norm == d for d in include_directories):
            return False
    if exclude_directories:
        if any(norm.startswith(d.rstrip("/") + "/") or norm == d for d in exclude_directories):
            return False

    return True
