import argparse
import functools
import glob
import itertools
import os
import os.path as osp
from datetime import datetime


def read_file(input_path, mode='r'):
    with open(input_path, mode) as f:
        return f.read()


def remove(path):
    path = os.fspath(path)
    # New format: path is the index file directly
    # Old format: path-sqlite-index is the index file
    index_paths = [path, f'{path}-sqlite-index']
    shard_paths = glob.glob(f'{path}-shard-?????')
    # SQLite can create journal, wal, and shm files
    sqlite_extras = []
    for index_path in index_paths:
        sqlite_extras.extend(
            [
                f'{index_path}-journal',
                f'{index_path}-wal',
                f'{index_path}-shm',
            ]
        )
    for p in index_paths + shard_paths + sqlite_extras:
        if osp.exists(p):
            os.remove(p)


def exists(path):
    path = os.fspath(path)
    # New format: path is the index file directly
    # Old format: path-sqlite-index is the index file
    if osp.exists(path) and not osp.isdir(path):
        return True
    if osp.exists(f'{path}-sqlite-index'):
        return True
    shard_paths = glob.glob(f'{path}-shard-?????')
    return len(shard_paths) > 0


# From `more-itertools` package.
def chunked(iterable, n, strict=False):
    """Break *iterable* into lists of length *n*:

        >>> list(chunked([1, 2, 3, 4, 5, 6], 3))
        [[1, 2, 3], [4, 5, 6]]

    By the default, the last yielded list will have fewer than *n* elements
    if the length of *iterable* is not divisible by *n*:

        >>> list(chunked([1, 2, 3, 4, 5, 6, 7, 8], 3))
        [[1, 2, 3], [4, 5, 6], [7, 8]]

    To use a fill-in value instead, see the :func:`grouper` recipe.

    If the length of *iterable* is not divisible by *n* and *strict* is
    ``True``, then ``ValueError`` will be raised before the last
    list is yielded.

    """
    iterator = iter(functools.partial(take, n, iter(iterable)), [])
    if strict:
        if n is None:
            raise ValueError('n must not be None when using strict mode.')

        def ret():
            for chunk in iterator:
                if len(chunk) != n:
                    raise ValueError('iterable is not divisible by n.')
                yield chunk

        return iter(ret())
    else:
        return iterator


def take(n, iterable):
    """Return first *n* items of the iterable as a list.

        >>> take(3, range(10))
        [0, 1, 2]

    If there are fewer than *n* items in the iterable, all of them are
    returned.

        >>> take(10, range(3))
        [0, 1, 2]

    """
    return list(itertools.islice(iterable, n))


def raise_if_readonly(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.readonly:
            raise PermissionError('This function is not allowed in readonly mode')
        return method(self, *args, **kwargs)

    return wrapper


def raise_if_append_only(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.append_only:
            raise PermissionError('This function is not allowed in append-only mode')
        return method(self, *args, **kwargs)

    return wrapper


def raise_if_readonly_or_append_only(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.readonly or self.append_only:
            raise PermissionError('This function is not allowed in read-only or append-only mode')
        return method(self, *args, **kwargs)

    return wrapper


def parse_size(size):
    if size is None:
        return None
    units = dict(K=1024, M=1024**2, G=1024**3, T=1024**4)
    size = size.upper()

    for unit, factor in units.items():
        if unit in size:
            return int(float(size.replace(unit, '')) * factor)

    return int(size)


def datetime_to_ns(dt):
    return int(dt.timestamp() * 1e9)


def ns_to_datetime(ns):
    return datetime.fromtimestamp(ns / 1e9)


class BoolAction(argparse.Action):
    """Action to parse boolean arguments with --arg and --no-arg variants."""

    def __init__(self, option_strings, dest, default=False, required=False, help=None):
        positive_opts = option_strings
        if not all(opt.startswith('--') for opt in positive_opts):
            raise ValueError('Boolean arguments must be prefixed with --')
        if any(opt.startswith('--no-') for opt in positive_opts):
            raise ValueError(
                'Boolean arguments cannot start with --no-, the --no- version will be '
                'auto-generated'
            )

        negative_opts = ['--no-' + opt[2:] for opt in positive_opts]
        opts = [*positive_opts, *negative_opts]
        super().__init__(
            opts, dest, nargs=0, const=None, default=default, required=required, help=help
        )

    def __call__(self, parser, namespace, values, option_string=None):
        if option_string.startswith('--no-'):
            setattr(namespace, self.dest, False)
        else:
            setattr(namespace, self.dest, True)
