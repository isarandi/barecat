"""High-level convenience functions for opening Barecat archives."""

import functools
import os.path as osp
import warnings

from .core.barecat import Barecat
from .core.paths import resolve_index_path


def open(path, mode='r', auto_codec=False, threadsafe_reader=True):
    """Open a Barecat archive.

    .. deprecated::
        Use ``Barecat(path, readonly=True)`` or ``Barecat(path, readonly=False)`` directly.

    Args:
        path: Path to the archive (without suffix).
        mode: 'r' (read), 'r+' (read-write), 'w+' (overwrite), 'a+' (append), 'x+' (exclusive create).
        auto_codec: **Deprecated.** Use :class:`DecodedView` instead. Will be removed in 1.0.
        threadsafe_reader: Use thread-local storage for read mode. Default: True.

    Returns:
        Barecat: The opened archive.
    """
    warnings.warn(
        'barecat.open() is deprecated and will be removed in version 1.0. '
        'Use Barecat(path, readonly=True) or Barecat(path, readonly=False) directly.',
        DeprecationWarning,
        stacklevel=2,
    )
    if auto_codec:
        warnings.warn(
            'auto_codec is deprecated and will be removed in version 1.0. '
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
        if not osp.exists(resolve_index_path(path)):
            raise FileNotFoundError(
                f"Archive not found: {path}. Mode 'r+' requires an existing archive; "
                f"use 'w+' or 'a+' to create one."
            )
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
        raise ValueError(f'Invalid mode: {mode}')


def get_cached_reader(path, auto_codec=None):
    """Get a thread-locally cached read-only Barecat reader.

    Each thread/process gets its own cached instance. Useful for multi-threaded
    data loading where each worker needs its own Barecat handle.

    Args:
        path: Path to the archive.
        auto_codec: Whether to automatically decode files based on extension.
            Currently defaults to True for backwards compatibility, but the default
            will change to False in version 1.0. Pass True or False explicitly to
            silence the deprecation warning. Use DecodedView for new code.
    """
    import multiprocessing_utils

    if auto_codec is None:
        warnings.warn(
            'get_cached_reader() currently defaults to auto_codec=True, but this '
            'default will change to False in version 1.0. Pass auto_codec=True '
            'explicitly to keep the current behavior, or auto_codec=False '
            '(and use DecodedView) to adopt the new default.',
            FutureWarning,
            stacklevel=2,
        )
        auto_codec = True
    elif auto_codec:
        warnings.warn(
            'auto_codec=True in get_cached_reader() is deprecated and will be removed '
            'in version 1.0. Use DecodedView instead.',
            DeprecationWarning,
            stacklevel=2,
        )

    return _cached_reader_open(path, auto_codec)


def _threadlocal_decorate(decorator):
    import multiprocessing_utils

    def my_decorator(fun):
        local = multiprocessing_utils.local()

        @functools.wraps(fun)
        def wrapper(*args, **kwargs):
            if not hasattr(local, 'fn'):
                local.fn = decorator(fun)
            return local.fn(*args, **kwargs)

        return wrapper

    return my_decorator


@_threadlocal_decorate(functools.lru_cache())
def _cached_reader_open(path, auto_codec=True):
    return Barecat(path, readonly=True, auto_codec=auto_codec)
