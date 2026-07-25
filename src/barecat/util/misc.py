import argparse
import functools
import glob
import re
import itertools
import math
import os
import os.path as osp
from datetime import datetime


def read_file(input_path, mode='r'):
    with open(input_path, mode) as f:
        return f.read()


# SQLite's default variable limit is 32766 (999 on builds older than 3.32); stay
# well under it so an IN-list never trips OperationalError: too many SQL variables.
SQL_VARIABLE_CHUNK = 900


def sqlite_uri(path, mode=None):
    """Build a SQLite URI filename for a filesystem path.

    SQLite parses a URI filename as a URI, so '?' begins the query part, '#' a
    fragment and '%' an escape sequence: a path containing any of the three names
    a different file, or fails to open, unless it is percent-encoded. Only those
    three are encoded — encoding anything else would equally change the filename.
    """
    p = os.fspath(path)
    # '%' first, or the escapes introduced below would themselves be escaped
    for ch, escape in (('%', '%25'), ('?', '%3f'), ('#', '%23')):
        p = p.replace(ch, escape)
    return f'file:{p}?mode={mode}' if mode is not None else f'file:{p}'


def quote_sql_identifier(name):
    """Quote an identifier (schema/table name) for interpolation into SQL."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def find_shard_paths(path):
    """Find the shard files of an archive, sorted numerically.

    Matches any number of digits (shard numbering grows past five digits at shard
    100000), rejects non-numeric strays like 'X-shard-backup', and sorts numerically
    (lexicographic order breaks exactly at the 5-to-6 digit boundary).
    """
    prefix = f'{os.fspath(path)}-shard-'
    numbered = []
    for p in glob.glob(f'{glob.escape(prefix)}*'):
        suffix = p[len(prefix) :]
        if suffix.isdigit():
            numbered.append((int(suffix), p))
    return [p for _, p in sorted(numbered)]


def remove(path):
    path = os.fspath(path)
    # New format: path is the index file directly
    # Old format: path-sqlite-index is the index file
    index_paths = [path, f'{path}-sqlite-index']
    shard_paths = find_shard_paths(path)
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
    shard_paths = find_shard_paths(path)
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


# GNU coreutils' convention (du, ls --block-size, dd): a bare letter is binary,
# a 'B' suffix is decimal, and 'iB' is binary. Bare K/M/G/T keep their 1024-based
# meaning so existing shard sizes do not silently change.
_SIZE_UNITS = {
    'B': 1,
    'K': 1024,
    'KB': 1000,
    'KIB': 1024,
    'M': 1024**2,
    'MB': 1000**2,
    'MIB': 1024**2,
    'G': 1024**3,
    'GB': 1000**3,
    'GIB': 1024**3,
    'T': 1024**4,
    'TB': 1000**4,
    'TIB': 1024**4,
    'P': 1024**5,
    'PB': 1000**5,
    'PIB': 1024**5,
}


def parse_size(size):
    """Parse a size like '10', '512K', '1.5GiB' or '2GB'.

    The suffix must come last: the old substring search accepted '1K5' as
    15 * 1024, and any suffix it did not know (KB, MiB) crashed.
    """
    if size is None:
        return None
    if isinstance(size, int):
        return size

    text = size.strip().upper()
    match = re.fullmatch(r'\s*([0-9]*\.?[0-9]+)\s*([A-Z]*)\s*', text)
    if match is None:
        raise ValueError(f'Cannot parse size {size!r}')
    number, suffix = match.groups()

    if suffix == '':
        factor = 1
    elif suffix in _SIZE_UNITS:
        factor = _SIZE_UNITS[suffix]
    else:
        raise ValueError(
            f'Unknown size suffix {suffix!r} in {size!r}. Use one of: '
            + ', '.join(sorted(_SIZE_UNITS))
        )
    return int(float(number) * factor)


def datetime_to_ns(dt):
    # Not int(dt.timestamp() * 1e9): scaling the float loses precision (~256 ns at
    # current epoch values, degrading over time). Whole seconds up to 2**53 are
    # exact in a float, and the sub-second part comes from dt.microsecond directly.
    return math.floor(dt.timestamp()) * 1_000_000_000 + dt.microsecond * 1_000


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
