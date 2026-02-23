"""Path utilities for barecat archive paths."""

import os
import os.path as osp


def resolve_index_path(path):
    """Resolve the index database path for a barecat archive.

    Supports both old format ({path}-sqlite-index) and new format ({path} directly).
    For new archives, uses the new format where the path IS the index file.

    Args:
        path: The archive path as given by the user (str or Path).

    Returns:
        The actual path to the SQLite index file.
    """
    path = os.fspath(path)
    # New format: the path itself is the index file
    if osp.exists(path) and not osp.isdir(path):
        return path
    # Old format: path with -sqlite-index suffix
    old_format_path = f'{path}-sqlite-index'
    if osp.exists(old_format_path):
        return old_format_path
    # New archive: use new format
    return path


def normalize_path(path):
    """Normalize an archive path (strip leading slashes, handle '.')."""
    x = osp.normpath(path).lstrip('/')
    return '' if x == '.' else x


def get_parent(path):
    """Get the parent directory of a path. Returns sentinel for root."""
    if path == '':
        # root already, has no parent
        return b'\x00'
    return path.rpartition('/')[0]


def partition_path(path):
    """Split path into (parent, basename)."""
    if path == '':
        # root already, has no parent
        return b'\x00', path
    parts = path.rpartition('/')
    return parts[0], parts[2]


def get_ancestors(path):
    """Yield all ancestor paths from root ('') down to and including path itself."""
    yield ''
    for i in range(len(path)):
        if path[i] == '/':
            yield path[:i]
    if path:
        yield path
