"""High-level convenience functions for opening Barecat archives."""

import functools
import warnings

from .core.barecat import Barecat


def open(path, mode='r', auto_codec=False, threadsafe_reader=True):
    """Open a Barecat archive.

    Args:
        path: Path to the archive (without suffix).
        mode: 'r' (read), 'r+' (read-write), 'w+' (overwrite), 'a+' (append), 'x+' (exclusive create).
        auto_codec: **Deprecated.** Use :class:`DecodedView` instead. Will be removed in 1.0.
        threadsafe_reader: Use thread-local storage for read mode. Default: True.

    Returns:
        Barecat: The opened archive.
    """
    if auto_codec:
        warnings.warn(
            "auto_codec is deprecated and will be removed in version 1.0. "
            "Use DecodedView instead: dec = DecodedView(bc); dec['file.json'] = data",
            DeprecationWarning,
            stacklevel=2,
        )
    if mode == 'r':
        return Barecat(path, readonly=True, threadsafe=threadsafe_reader, auto_codec=auto_codec)
    elif mode == 'w+':
        return Barecat(
            path,
            readonly=False,
            overwrite=True,
            exist_ok=True,
            append_only=False,
            auto_codec=auto_codec,
        )
    elif mode == 'r+':
        return Barecat(
            path,
            readonly=False,
            overwrite=False,
            exist_ok=True,
            append_only=False,
            auto_codec=auto_codec,
        )
    elif mode == 'a+':
        return Barecat(
            path,
            readonly=False,
            overwrite=False,
            exist_ok=True,
            append_only=True,
            auto_codec=auto_codec,
        )
    elif mode == 'ax+':
        return Barecat(
            path,
            readonly=False,
            overwrite=False,
            exist_ok=False,
            append_only=True,
            auto_codec=auto_codec,
        )
    elif mode == 'x+':
        return Barecat(
            path,
            readonly=False,
            overwrite=False,
            exist_ok=False,
            append_only=False,
            auto_codec=auto_codec,
        )
    else:
        raise ValueError(f"Invalid mode: {mode}")


def get_cached_reader(path, auto_codec=True):
    """Get a thread-locally cached read-only Barecat reader.

    Each thread/process gets its own cached instance. Useful for multi-threaded
    data loading where each worker needs its own Barecat handle.
    """
    import multiprocessing_utils

    # Thread-local LRU cache
    local = multiprocessing_utils.local()
    if not hasattr(local, '_cache'):
        @functools.lru_cache()
        def _open(path, auto_codec):
            return Barecat(path, readonly=True, auto_codec=auto_codec)
        local._cache = _open

    return local._cache(path, auto_codec)
