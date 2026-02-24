import contextlib
import copy
import itertools
import os
import os.path as osp
import sqlite3
import sys
from datetime import datetime
from typing import Iterable, Iterator, Optional, Union

from ..util import misc

from .types import BarecatDirInfo, BarecatFileInfo, BarecatEntryInfo, Order

from ..core.types import SCHEMA_VERSION_MAJOR, SCHEMA_VERSION_MINOR

from ..exceptions import (
    BarecatError,
    DirectoryNotEmptyBarecatError,
    FileExistsBarecatError,
    FileNotFoundBarecatError,
    IsADirectoryBarecatError,
    NotADirectoryBarecatError,
)
from ..util.misc import datetime_to_ns
from ..core.paths import normalize_path
from ..util.glob_helper import GlobHelper
from ..maintenance.merge import IndexMergeHelper
from contextlib import AbstractContextManager


class Index(AbstractContextManager):
    """Manages the SQLite database storing metadata about the files and directories in the Barecat
    archive.

    Args:
        path: Path to the SQLite database file (e.g., 'archive.barecat').
        shard_size_limit: Maximum size of a shard in bytes (int) or as a string like '1G'.
            If None, the shard size is unlimited.
        bufsize: Buffer size for fetching rows.
        readonly: Whether to open the index in read-only mode.
    """

    def __init__(
        self,
        path: Union[str, os.PathLike],
        shard_size_limit: Union[int, str, None] = None,
        bufsize: Optional[int] = None,
        readonly: bool = True,
        wal: bool = False,
        readonly_is_immutable: bool = False,
    ):
        path = os.fspath(path)
        is_new = not osp.exists(path)
        self.path = path
        self.readonly = readonly
        try:
            if self.readonly and readonly_is_immutable:
                mode = 'ro&immutable=1'
            elif self.readonly:
                mode = 'ro'
            else:
                mode = 'rwc'
            self.conn = sqlite3.connect(
                f'file:{path}?mode={mode}',
                uri=True,
                check_same_thread=not self.readonly,
                cached_statements=1024,
            )
        except sqlite3.OperationalError as e:
            if readonly and not osp.exists(path):
                raise FileNotFoundError(
                    f'Index file {path} does not exist, so cannot be opened in readonly mode.'
                ) from e
            else:
                raise RuntimeError(f'Could not open index {path}') from e

        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.cursor.arraysize = bufsize if bufsize is not None else 128
        self.fetcher = Fetcher(self.conn, self.cursor, bufsize=bufsize)
        self.fetch_one = self.fetcher.fetch_one
        self.fetch_one_or_raise = self.fetcher.fetch_one_or_raise
        self.fetch_all = self.fetcher.fetch_all
        self.fetch_iter = self.fetcher.fetch_iter
        self.fetch_many = self.fetcher.fetch_many

        self._shard_size_limit_cached = None

        if is_new:
            sql_dir = osp.join(osp.dirname(__file__), '../sql')
            self.cursor.executescript(misc.read_file(f'{sql_dir}/schema.sql'))
            self.cursor.executescript(misc.read_file(f'{sql_dir}/indexes.sql'))
            self.cursor.executescript(misc.read_file(f'{sql_dir}/triggers.sql'))
            with self.no_triggers():
                self.cursor.execute(
                    "INSERT INTO dirs (path, uid, gid, mtime_ns) VALUES ('', ?, ?, ?)",
                    (os.getuid(), os.getgid(), datetime_to_ns(datetime.now())),
                )

        if self.readonly:
            # self.cursor.execute('PRAGMA journal_mode=OFF')
            # self.cursor.execute('PRAGMA synchronous=OFF')
            if readonly_is_immutable:
                self.cursor.execute('PRAGMA locking_mode=EXCLUSIVE')
            self.cursor.execute('PRAGMA cache_size=-64000')
        else:
            self.cursor.execute('PRAGMA recursive_triggers = ON')
            self.cursor.execute('PRAGMA foreign_keys = ON')

            if wal:
                self.cursor.execute('PRAGMA journal_mode = WAL')

            self._triggers_enabled = True
            if shard_size_limit is not None:
                self.shard_size_limit = shard_size_limit

        self.cursor.execute('PRAGMA busy_timeout = 5000')
        self.cursor.execute('PRAGMA temp_store = memory')
        self.cursor.execute('PRAGMA mmap_size = 30000000000')

        if not is_new:
            self._check_schema_version()

        self.is_closed = False
        self._glob_helper = GlobHelper(self)
        self._merge_helper = IndexMergeHelper(self)

    def _check_schema_version(self):
        """Check that the database schema version is compatible with this code.

        Raises BarecatError if the schema major version doesn't match.
        Warns if the schema minor version is newer.
        Archives without config table are treated as one major version below current.
        """
        try:
            self.cursor.execute("SELECT value_int FROM config WHERE key='schema_version_major'")
            row = self.cursor.fetchone()
            if row is None:
                # Config table exists but no version entry - treat as old
                db_major = SCHEMA_VERSION_MAJOR - 1
                db_minor = 0
            else:
                db_major = int(row[0])
                self.cursor.execute(
                    "SELECT value_int FROM config WHERE key='schema_version_minor'"
                )
                minor_row = self.cursor.fetchone()
                db_minor = int(minor_row[0]) if minor_row else 0
        except sqlite3.OperationalError:
            # Config table doesn't exist - ancient format (pre-0.1)
            db_major = -1
            db_minor = 0

        if db_major > SCHEMA_VERSION_MAJOR:
            raise BarecatError(
                f'Database schema version {db_major}.{db_minor} is newer than '
                f'supported version {SCHEMA_VERSION_MAJOR}.{SCHEMA_VERSION_MINOR}. '
                'Please upgrade barecat: pip install --upgrade barecat'
            )

        if db_major < SCHEMA_VERSION_MAJOR:
            raise BarecatError(
                f'Database schema version {db_major}.{db_minor} is older than '
                f'supported version {SCHEMA_VERSION_MAJOR}.{SCHEMA_VERSION_MINOR}. '
                'Please run: barecat upgrade <archive>'
            )

        # Extract archive path (handles both old format with -sqlite-index suffix and new format)
        archive_path = self.path.removesuffix('-sqlite-index')

        if db_minor > SCHEMA_VERSION_MINOR:
            print(
                f'Warning: Schema version {db_major}.{db_minor} is newer than supported '
                f'{SCHEMA_VERSION_MAJOR}.{SCHEMA_VERSION_MINOR}. Some features may not work. '
                'Consider: pip install --upgrade barecat',
                file=sys.stderr,
            )

        if db_minor < SCHEMA_VERSION_MINOR:
            if db_major == 0 and db_minor < 3:
                print(
                    f'Warning: Schema {db_major}.{db_minor} has a trigger bug that may cause '
                    f'incorrect directory statistics if directories were moved or deleted. '
                    f'Consider: barecat upgrade {archive_path}',
                    file=sys.stderr,
                )
            else:
                print(
                    f'Warning: Schema version is outdated ({db_major}.{db_minor} < '
                    f'{SCHEMA_VERSION_MAJOR}.{SCHEMA_VERSION_MINOR}). '
                    f'Consider: barecat upgrade {archive_path}',
                    file=sys.stderr,
                )

    # READING
    def lookup_file(self, path: str, normalized: bool = False) -> BarecatFileInfo:
        """Look up a file by its path.

        Args:
            path: Path of the file.
            normalized: Whether the path is already normalized. If False, the path will be
                normalized before the lookup.

        Returns:
            The file info object.

        Raises:
            FileNotFoundBarecatError: If the file is not found.
        """

        if not normalized:
            path = normalize_path(path)
        try:
            result = self.fetch_one_or_raise(
                """
                SELECT shard, offset, size, crc32c, mode, uid, gid, mtime_ns
                FROM files WHERE path=?
                """,
                (path,),
                rowcls=BarecatFileInfo,
            )
            result._path = path
            return result
        except LookupError:
            raise FileNotFoundBarecatError(path)

    def lookup_files(self, paths: list[str]) -> list[BarecatFileInfo]:
        """Look up multiple files by their paths.

        Args:
            paths: Iterable of file paths.

        Returns:
            An iterator over the file info objects.

        Raises:
            FileNotFoundBarecatError: If any of the files are not found.
        """
        if not paths:
            return []
        normalized_paths = [normalize_path(p) for p in paths]
        placeholders = ','.join('?' for _ in normalized_paths)
        query = f"""
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
            FROM files WHERE path IN ({placeholders})
        """
        finfos = self.fetch_all(query, tuple(normalized_paths), rowcls=BarecatFileInfo)
        found_files = {finfo.path: finfo for finfo in finfos}
        results = []
        for path in normalized_paths:
            try:
                results.append(found_files[path])
            except KeyError:
                raise FileNotFoundBarecatError(path)
        return results

    def lookup_dir(self, dirpath: str) -> BarecatDirInfo:
        """Look up a directory by its path.

        Args:
            dirpath: Path of the directory.

        Returns:
            The directory info object.

        Raises:
            FileNotFoundBarecatError: If the directory is not found.
        """
        dirpath = normalize_path(dirpath)
        try:
            result = self.fetch_one_or_raise(
                """
                SELECT num_subdirs, num_files, size_tree, num_files_tree, mode, uid, gid, mtime_ns
                FROM dirs WHERE path=?
                """,
                (dirpath,),
                rowcls=BarecatDirInfo,
            )
            result._path = dirpath
            return result
        except LookupError:
            raise FileNotFoundBarecatError(f'Directory {dirpath} not found in index')

    def lookup(self, path: str) -> BarecatEntryInfo:
        """Look up a file or directory by its path.

        Args:
            path: Path of the file or directory.

        Returns:
            The file or directory info object.

        Raises:
            FileNotFoundBarecatError: If the file or directory is not found.
        """
        path = normalize_path(path)
        try:
            return self.lookup_file(path)
        except FileNotFoundBarecatError:
            return self.lookup_dir(path)

    def __len__(self):
        """Number of files in the index."""
        return self.num_files

    @property
    def num_files(self):
        """Number of files in the index."""
        return self.fetch_one("SELECT num_files_tree FROM dirs WHERE path=''")[0]

    @property
    def num_dirs(self):
        """Number of directories in the index."""
        return self.fetch_one('SELECT COUNT(*) FROM dirs')[0]

    @property
    def total_size(self):
        """Total size of all files in the index, in bytes."""
        return self.fetch_one("SELECT size_tree FROM dirs WHERE path=''")[0]

    def __iter__(self):
        """Iterate over all file info objects in the index."""
        yield from self.iter_all_fileinfos(order=Order.ANY)

    def __contains__(self, path: str) -> bool:
        """Check if a file exists in the index.

        Args:
            path: Path of the file.

        Returns:
            True if the file exists, False otherwise.
        """
        return self.isfile(path)

    def isfile(self, path: str) -> bool:
        """Check if a file exists in the index.

        Args:
            path: Path of the file. It is normalized before the check.

        Returns:
            True if a file with the given path exists, False otherwise.
        """
        path = normalize_path(path)
        return self.fetch_one('SELECT 1 FROM files WHERE path=?', (path,)) is not None

    def isdir(self, path):
        """Check if a directory exists in the index.

        Args:
            path: Path of the directory. It is normalized before the check.

        Returns:
            True if a directory with the given path exists, False otherwise.
        """

        path = normalize_path(path)
        return self.fetch_one('SELECT 1 FROM dirs WHERE path=?', (path,)) is not None

    def exists(self, path):
        """Check if a file or directory exists in the index.

        Args:
            path: Path of the file or directory. It is normalized before the check.

        Returns:
            True if a file or directory with the given path exists, False otherwise.
        """
        path = normalize_path(path)
        return (
            self.fetch_one(
                """
            SELECT 1
            WHERE EXISTS (SELECT 1 FROM files WHERE path = :path)
               OR EXISTS (SELECT 1 FROM dirs WHERE path = :path)
        """,
                dict(path=path),
            )
            is not None
        )

    def iter_all_fileinfos(
        self, order: Order = Order.ANY, bufsize: Optional[int] = None
    ) -> Iterator[BarecatFileInfo]:
        """Iterate over all file info objects in the index.

        Args:
            order: Order in which to iterate over the files. The default ANY
                uses SQLite's natural rowid order, which is typically address
                order (shard, offset) if the archive was built by linear
                insertion. ANY also streams results immediately, while explicit
                ordering waits for a full sort before returning the first row.
            bufsize: Buffer size for fetching rows.

        Returns:
            An iterator over the file info objects.
        """
        query = """
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
            FROM files"""
        query += order.as_query_text()
        return self.fetch_iter(query, bufsize=bufsize, rowcls=BarecatFileInfo)

    def iter_all_dirinfos(
        self, order: Order = Order.ANY, bufsize: Optional[int] = None
    ) -> Iterator[BarecatDirInfo]:
        query = """
            SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
            mode, uid, gid, mtime_ns FROM dirs"""
        query += order.as_query_text()
        return self.fetch_iter(query, bufsize=bufsize, rowcls=BarecatDirInfo)

    def iter_all_infos(
        self, order: Order = Order.ANY, bufsize: Optional[int] = None
    ) -> Iterator[BarecatEntryInfo]:
        query = """
            SELECT path, NULL AS shard, NULL AS offset, size_tree AS size, NULL AS crc32c,
                   mode, uid, gid, mtime_ns, num_subdirs, num_files, num_files_tree,
                   'dir' AS type
            FROM dirs
            UNION ALL
            SELECT path, shard, offset, size, crc32c,
                   mode, uid, gid, mtime_ns, NULL AS num_subdirs, NULL AS num_files,
                   NULL AS num_files_tree, 'file' AS type
            FROM files"""
        query += order.as_query_text()
        for row in self.fetch_iter(query, bufsize=bufsize):
            if row['type'] == 'dir':
                yield BarecatDirInfo(
                    path=row['path'],
                    num_subdirs=row['num_subdirs'],
                    num_files=row['num_files'],
                    size_tree=row['size'],
                    num_files_tree=row['num_files_tree'],
                    mode=row['mode'],
                    uid=row['uid'],
                    gid=row['gid'],
                    mtime_ns=row['mtime_ns'],
                )
            else:
                yield BarecatFileInfo(
                    path=row['path'],
                    shard=row['shard'],
                    offset=row['offset'],
                    size=row['size'],
                    crc32c=row['crc32c'],
                    mode=row['mode'],
                    uid=row['uid'],
                    gid=row['gid'],
                    mtime_ns=row['mtime_ns'],
                )

    def iter_all_filepaths(
        self, order: Order = Order.ANY, bufsize: Optional[int] = None
    ) -> Iterator[str]:
        query = 'SELECT path FROM files' + order.as_query_text()
        for row in self.fetch_iter(query, bufsize=bufsize):
            yield row['path']

    def iter_all_dirpaths(
        self, order: Order = Order.ANY, bufsize: Optional[int] = None
    ) -> Iterator[str]:
        query = 'SELECT path FROM dirs' + order.as_query_text()
        for row in self.fetch_iter(query, bufsize=bufsize):
            yield row['path']

    def iter_all_paths(
        self, order: Order = Order.ANY, bufsize: Optional[int] = None
    ) -> Iterator[str]:
        query = """
            SELECT path FROM dirs
            UNION ALL
            SELECT path FROM files"""
        query += order.as_query_text()
        for row in self.fetch_iter(query, bufsize=bufsize):
            yield row['path']

    ########## Listdir-like methods ##########
    def _as_dirinfo(self, diritem: Union[BarecatDirInfo, str]):
        return diritem if isinstance(diritem, BarecatDirInfo) else self.lookup_dir(diritem)

    def _as_fileinfo(self, fileitem: Union[BarecatFileInfo, str]):
        return fileitem if isinstance(fileitem, BarecatFileInfo) else self.lookup_file(fileitem)

    @staticmethod
    def _as_path(item: Union[BarecatEntryInfo, str]):
        return normalize_path(item) if isinstance(item, str) else item.path

    def list_direct_fileinfos(
        self, dirpath: str, order: Order = Order.ANY
    ) -> list[BarecatFileInfo]:
        """List the file info objects in a directory (non-recursively).

        Args:
            dirpath: Path of the directory.
            order: Order in which to list the files.

        Returns:
            A list of file info objects.
        """
        dirpath = normalize_path(dirpath)
        query = """
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
            FROM files WHERE parent=?"""
        query += order.as_query_text()
        return self.fetch_all(query, (dirpath,), rowcls=BarecatFileInfo)

    def list_subdir_dirinfos(self, dirpath: str, order: Order = Order.ANY) -> list[BarecatDirInfo]:
        """List the subdirectory info objects contained in a directory (non-recursively).

        Args:
            dirpath: Path of the directory.
            order: Order in which to list the directories.

        Returns:
            A list of directory info objects.
        """
        dirpath = normalize_path(dirpath)
        query = """
            SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
            mode, uid, gid, mtime_ns FROM dirs WHERE parent=?"""
        query += order.as_query_text()
        return self.fetch_all(query, (dirpath,), rowcls=BarecatDirInfo)

    def iter_direct_fileinfos(
        self,
        diritem: Union[BarecatDirInfo, str],
        order: Order = Order.ANY,
        bufsize: Optional[int] = None,
    ) -> Iterator[BarecatFileInfo]:
        """Iterate over the file info objects in a directory (non-recursively).

        Args:
            diritem: Directory info object or path of the directory.
            order: Order in which to iterate over the files.
            bufsize: Buffer size for fetching rows.

        Returns:
            An iterator over the file info objects.
        """
        dinfo = self._as_dirinfo(diritem)
        if dinfo.num_files == 0:
            return iter([])
        query = """
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
            FROM files WHERE parent=?"""
        query += order.as_query_text()
        return self.fetch_iter(query, (dinfo.path,), bufsize=bufsize, rowcls=BarecatFileInfo)

    def iter_subdir_dirinfos(
        self,
        diritem: Union[BarecatDirInfo, str],
        order: Order = Order.ANY,
        bufsize: Optional[int] = None,
    ) -> Iterator[BarecatDirInfo]:
        """Iterate over the subdirectory info objects contained in a directory (non-recursively).

        Args:
            diritem: Directory info object or path of the directory.
            order: Order in which to iterate over the directories.
            bufsize: Buffer size for fetching rows.

        Returns:
            An iterator over the directory info objects.
        """
        dinfo = self._as_dirinfo(diritem)
        if dinfo.num_subdirs == 0:
            return iter([])
        query = """
            SELECT path, num_subdirs, num_files, size_tree, num_files_tree, mode, uid, gid,
            mtime_ns
            FROM dirs WHERE parent=?"""
        query += order.as_query_text()
        return self.fetch_iter(query, (dinfo.path,), bufsize=bufsize, rowcls=BarecatDirInfo)

    def listdir_names(
        self, diritem: Union[BarecatDirInfo, str], order: Order = Order.ANY
    ) -> list[str]:
        """List the names of the files and subdirectories in a directory (non-recursively).

        Args:
            diritem: Directory info object or path of the directory.
            order: Order in which to list the files and directories.

        Returns:
            A list of file and directory names.
        """
        dinfo = self._as_dirinfo(diritem)
        query = """
            SELECT path FROM dirs WHERE parent=:parent
            UNION ALL
            SELECT path FROM files WHERE parent=:parent"""
        query += order.as_query_text()
        rows = self.fetch_all(query, dict(parent=dinfo.path))
        return [osp.basename(row['path']) for row in rows]

    def listdir_infos(
        self, diritem: Union[BarecatDirInfo, str], order: Order = Order.ANY
    ) -> list[BarecatEntryInfo]:
        """List the file and directory info objects in a directory (non-recursively).

        Args:
            diritem: Directory info object or path of the directory.
            order: Order in which to list the files and directories.

        Returns:
            A list of file and directory info objects.
        """
        dinfo = self._as_dirinfo(diritem)
        return self.list_subdir_dirinfos(dinfo.path, order=order) + self.list_direct_fileinfos(
            dinfo.path, order=order
        )

    def iterdir_names(
        self,
        diritem: Union[BarecatDirInfo, str],
        order: Order = Order.ANY,
        bufsize: Optional[int] = None,
    ) -> Iterator[str]:
        """Iterate over the names of the files and subdirectories in a directory (non-recursively).

        Args:
            diritem: Directory info object or path of the directory.
            order: Order in which to iterate over the files and directories.
            bufsize: Buffer size for fetching rows.

        Returns:
            An iterator over the file and directory names.
        """

        dinfo = self._as_dirinfo(diritem)
        query = """
            SELECT path FROM dirs WHERE parent=?
            UNION ALL
            SELECT path FROM files WHERE parent=?"""
        query += order.as_query_text()
        rows = self.fetch_iter(query, (dinfo.path, dinfo.path), bufsize=bufsize)
        return (osp.basename(row['path']) for row in rows)

    def iterdir_infos(
        self,
        diritem: Union[BarecatDirInfo, str],
        order: Order = Order.ANY,
        bufsize: Optional[int] = None,
    ) -> Iterator[BarecatEntryInfo]:
        """Iterate over the file and directory info objects in a directory (non-recursively).

        Args:
            diritem: Directory info object or path of the directory.
            order: Order in which to iterate over the files and directories.
            bufsize: Buffer size for fetching rows.

        Returns:
            An iterator over the file and directory info objects.
        """
        dinfo = self._as_dirinfo(diritem)
        return itertools.chain(
            self.iter_subdir_dirinfos(dinfo, order=order, bufsize=bufsize),
            self.iter_direct_fileinfos(dinfo, order=order, bufsize=bufsize),
        )

    # Glob methods (delegated to GlobHelper)

    def raw_glob_paths(self, pattern, order: Order = Order.ANY):
        return self._glob_helper.raw_glob_paths(pattern, order)

    def raw_iterglob_paths(
        self, pattern, order: Order = Order.ANY, only_files=False, bufsize=None
    ):
        return self._glob_helper.raw_iterglob_paths(pattern, order, only_files, bufsize)

    def raw_iterglob_paths_multi(
        self, patterns, order: Order = Order.ANY, only_files=False, bufsize=None
    ):
        return self._glob_helper.raw_iterglob_paths_multi(patterns, order, only_files, bufsize)

    def glob_paths(
        self,
        pattern: str,
        recursive: bool = False,
        include_hidden: bool = False,
        only_files: bool = False,
    ):
        r"""Glob for paths matching a pattern.

        The glob syntax is equivalent to Python's :py:func:`glob.glob`.

        Args:
            pattern: Glob pattern.
            recursive: Whether to glob recursively. If True, the pattern can contain the ``'/**/'``
                sequence to match any number of directories.
            include_hidden: Whether to include hidden files and directories (those starting with a
                dot).
            only_files: Whether to glob only files and not directories.

        Returns:
            A list of paths.
        """
        return self._glob_helper.glob_paths(pattern, recursive, include_hidden, only_files)

    def iterglob_paths(
        self,
        pattern: str,
        recursive: bool = False,
        include_hidden: bool = False,
        bufsize: Optional[int] = None,
        only_files: bool = False,
    ) -> Iterator[str]:
        r"""Iterate over paths matching a pattern.

        The glob syntax is equivalent to Python's :py:func:`glob.iglob`.

        Args:
            pattern: Glob pattern.
            recursive: Whether to glob recursively. If True, the pattern can contain the ``'/**/'``
                sequence to match any number of directories.
            include_hidden: Whether to include hidden files and directories (those starting with a
                dot).
            bufsize: Buffer size for fetching rows.
            only_files: Whether to glob only files and not directories.

        Returns:
            An iterator over the paths.
        """
        return self._glob_helper.iterglob_paths(
            pattern, recursive, include_hidden, bufsize, only_files
        )

    def raw_iterglob_infos(self, pattern, only_files=False, bufsize=None):
        return self._glob_helper.raw_iterglob_infos(pattern, only_files, bufsize)

    def raw_iterglob_infos_incl_excl(self, patterns, only_files=False, bufsize=None):
        return self._glob_helper.raw_iterglob_infos_incl_excl(patterns, only_files, bufsize)

    def iterglob_infos(
        self,
        pattern: str,
        recursive: bool = False,
        include_hidden: bool = False,
        bufsize: Optional[int] = None,
        only_files: bool = False,
    ) -> Iterator[BarecatEntryInfo]:
        r"""Iterate over file and directory info objects matching a pattern.

        The glob syntax is equivalent to Python's :py:func:`glob.glob`.

        Args:
            pattern: Glob pattern.
            recursive: Whether to glob recursively. If True, the pattern can contain the ``'/**/'``
                sequence to match any number of directories.
            include_hidden: Whether to include hidden files and directories (those starting with a
                dot).
            bufsize: Buffer size for fetching rows.
            only_files: Whether to glob only files and not directories.

        Returns:
            An iterator over the file and directory info objects.
        """
        return self._glob_helper.iterglob_infos(
            pattern, recursive, include_hidden, bufsize, only_files
        )

    def iterglob_infos_incl_excl(
        self,
        rules: list[tuple[str, str]],
        default_include: bool = True,
        only_files: bool = False,
        bufsize: Optional[int] = None,
    ) -> Iterator[BarecatEntryInfo]:
        r"""Iterate over infos matching rsync-style include/exclude rules.

        Uses "first match wins" semantics like rsync: each file is tested against
        rules in order, and the first matching rule determines inclusion/exclusion.

        Args:
            rules: List of (sign, pattern) tuples. sign is '+' for include,
                   '-' for exclude. Patterns use Python glob syntax with ** support.
            default_include: If no rule matches, include (True) or exclude (False).
            only_files: Whether to return only files, not directories.
            bufsize: Buffer size for fetching rows.

        Returns:
            An iterator over matching file/directory info objects.
        """
        return self._glob_helper.iterglob_infos_incl_excl(
            rules, default_include, only_files, bufsize
        )

    ## walking
    def walk_infos(
        self, rootitem: Union[BarecatDirInfo, str], bufsize: int = 32
    ) -> Iterable[tuple[BarecatDirInfo, Iterable[BarecatDirInfo], Iterable[BarecatFileInfo]]]:
        """Walk over the directory tree starting from a directory.

        Args:
            rootitem: Directory info object or path of the root directory.
            bufsize: Buffer size for fetching rows.

        Returns:
            An iterator over tuples of directory info objects, subdirectory info objects, and file
            info objects.

            The tuples are in the format ``(dirinfo, subdirs, files)``, where
                - ``dirinfo`` is the directory info object.
                - ``subdirs`` is a list of subdirectory info objects.
                - ``files`` is a list of file info objects.
        """

        rootinfo = self._as_dirinfo(rootitem)
        dirs_to_walk = iter([rootinfo])

        while (dinfo := next(dirs_to_walk, None)) is not None:
            subdirs = RecallableIter(self.iter_subdir_dirinfos(dinfo, bufsize=bufsize))
            files = self.iter_direct_fileinfos(dinfo, bufsize=bufsize)
            yield dinfo, subdirs, files
            dirs_to_walk = iter(itertools.chain(subdirs, dirs_to_walk))

    def walk_names(
        self, rootitem: Union[BarecatDirInfo, str], bufsize: int = 32
    ) -> Iterable[tuple[str, list[str], list[str]]]:
        """Walk over the directory tree starting from a directory.

        Args:
            rootitem: Directory info object or path of the root directory.
            bufsize: Buffer size for fetching rows.

        Returns:
            An iterator over tuples of directory paths, subdirectory names, and file names.

            The tuples are in the format ``(dirpath, subdirs, files)``, where
                - ``dirpath`` is the path of the directory.
                - ``subdirs`` is a list of subdirectory names.
                - ``files`` is a list of file names.
        """
        for dinfo, subdirs, files in self.walk_infos(rootitem, bufsize=bufsize):
            yield (
                dinfo.path or '.',
                [osp.basename(d.path) for d in subdirs],
                [osp.basename(f.path) for f in files],
            )

    def get_last_file(self):
        """Return the last file in the index, i.e., the one with the highest offset in the last
        shard (shard with largest numerical ID).

        Returns:
            The file info object.

        Raises:
            LookupError: If the index is empty.
        """
        try:
            return self.fetch_one_or_raise(
                """
                SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
                FROM files
                ORDER BY shard DESC, offset DESC LIMIT 1""",
                rowcls=BarecatFileInfo,
            )
        except LookupError:
            raise LookupError('Index is empty, it has no last file')

    def logical_shard_end(self, shard: int) -> int:
        """Return the logical end offset of a shard, which is the index of a byte immediately after
        the last byte of the last file in the shard.

        Args:
            shard: Shard number.

        Returns:
            The logical end offset of the shard.
        """

        # COALESCE guarantees a row is always returned, so result is never None
        return self.fetch_one(
            """
            SELECT coalesce(MAX(offset + size), 0) as end FROM files WHERE shard=:shard
            """,
            dict(shard=shard),
        )[0]

    @property
    def shard_size_limit(self) -> int:
        """The maximum allowed shard size, in bytes. Upon reaching this limit, a new shard is
        created."""
        if self._shard_size_limit_cached is None:
            self._shard_size_limit_cached = self.fetch_one(
                "SELECT value_int FROM config WHERE key='shard_size_limit'"
            )[0]
        return self._shard_size_limit_cached

    @shard_size_limit.setter
    def shard_size_limit(self, value: Union[int, str]):
        """Set the maximum allowed shard size. Upon reaching this limit, a new shard is created.

        Args:
            value: The new shard size limit in bytes, or a string like '1G', '500M', '100K'.
        """
        if self.readonly:
            raise ValueError('Cannot set shard size limit on a read-only index')
        if isinstance(value, str):
            value = misc.parse_size(value)

        if value == self.shard_size_limit:
            return
        if value < self.shard_size_limit:
            largest_shard_size = max(
                (self.logical_shard_end(i) for i in range(self.num_used_shards)), default=0
            )
            if value < largest_shard_size:
                # Wants to shrink
                raise ValueError(
                    f'Trying to set shard size limit as {value}, which is smaller than the largest'
                    f' existing shard size {largest_shard_size}.'
                    f' Increase the shard size limit or re-shard the data first.'
                )

        self.cursor.execute(
            """
            UPDATE config SET value_int=:value WHERE key='shard_size_limit'
            """,
            dict(value=value),
        )
        self._shard_size_limit_cached = value

    @property
    def num_used_shards(self):
        """Number of shards where final, logically empty shards are not counted.

        Returns:
             The maximum shard number of any file, plus one.
        """
        return self.fetch_one('SELECT coalesce(MAX(shard), -1) + 1 FROM files')[0]

    # WRITING
    def add(self, info: BarecatEntryInfo):
        """Add a file or directory to the index.

        Args:
            info: File or directory info object.

        Raises:
            FileExistsBarecatError: If the file or directory already exists.
        """

        if isinstance(info, BarecatFileInfo):
            self.add_file(info)
        else:
            self.add_dir(info)

    def add_file(self, finfo: BarecatFileInfo):
        """Add a file to the index.

        Args:
            finfo: File info object.

        Raises:
            FileExistsBarecatError: If the file already exists.
        """
        try:
            self.cursor.execute(
                """
                INSERT INTO files (
                    path, shard, offset, size,  crc32c, mode, uid, gid, mtime_ns)
                VALUES (:path, :shard, :offset, :size, :crc32c, :mode, :uid, :gid, :mtime_ns)
                """,
                finfo.asdict(),
            )
        except sqlite3.IntegrityError as e:
            if 'Path already exists as file' in str(e):
                raise NotADirectoryBarecatError(
                    f'A parent of {finfo.path!r} exists as a file'
                ) from e
            if 'Path already exists as directory' in str(e):
                raise IsADirectoryBarecatError(finfo.path) from e
            raise FileExistsBarecatError(finfo.path) from e

    def add_files(self, finfos: list[BarecatFileInfo]):
        """Add multiple files to the index.

        Args:
            finfos: List of file info objects.

        Raises:
            FileExistsBarecatError: If any file already exists.
        """
        if not finfos:
            return
        try:
            self.cursor.executemany(
                """
                INSERT INTO files (
                    path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns)
                VALUES (:path, :shard, :offset, :size, :crc32c, :mode, :uid, :gid, :mtime_ns)
                """,
                [finfo.asdict() for finfo in finfos],
            )
        except sqlite3.IntegrityError as e:
            if 'Path already exists as file' in str(e):
                raise NotADirectoryBarecatError('A parent path exists as a file') from e
            if 'Path already exists as directory' in str(e):
                raise IsADirectoryBarecatError('Path exists as a directory') from e
            raise FileExistsBarecatError([finfo.path for finfo in finfos]) from e

    def update_file(
        self,
        path: str,
        new_shard: Optional[int] = None,
        new_offset: Optional[int] = None,
        new_size: Optional[int] = None,
        new_crc32c: Optional[int] = None,
        new_mtime_ns: Optional[int] = None,
        new_mode: Optional[int] = None,
        new_uid: Optional[int] = None,
        new_gid: Optional[int] = None,
    ):
        """Update file metadata in the index.

        Args:
            path: Path of the file.
            new_shard: New shard number. If None, the shard is not updated.
            new_offset: New offset within the shard. If None, the offset is not updated.
            new_size: New size of the file. If None, the size is not updated.
            new_crc32c: New CRC32C checksum of the file. If None, the checksum is not updated.
            new_mtime_ns: New modification time in nanoseconds. If None, not updated.
            new_mode: New file mode. If None, not updated.
            new_uid: New user ID. If None, not updated.
            new_gid: New group ID. If None, not updated.
        """
        path = normalize_path(path)
        update_fields = []
        params = dict(path=path)
        if new_shard is not None:
            update_fields.append('shard = :new_shard')
            params['new_shard'] = new_shard
        if new_offset is not None:
            update_fields.append('offset = :new_offset')
            params['new_offset'] = new_offset
        if new_size is not None:
            update_fields.append('size = :new_size')
            params['new_size'] = new_size
        if new_crc32c is not None:
            update_fields.append('crc32c = :new_crc32c')
            params['new_crc32c'] = new_crc32c
        if new_mtime_ns is not None:
            update_fields.append('mtime_ns = :new_mtime_ns')
            params['new_mtime_ns'] = new_mtime_ns
        if new_mode is not None:
            update_fields.append('mode = :new_mode')
            params['new_mode'] = new_mode
        if new_uid is not None:
            update_fields.append('uid = :new_uid')
            params['new_uid'] = new_uid
        if new_gid is not None:
            update_fields.append('gid = :new_gid')
            params['new_gid'] = new_gid

        if not update_fields:
            return

        update_clause = ', '.join(update_fields)
        self.cursor.execute(
            f"""
            UPDATE files SET {update_clause} WHERE path = :path
            """,
            params,
        )

    def add_dir(self, dinfo: BarecatDirInfo, exist_ok=False):
        """Add a directory to the index.

        Args:
            dinfo: Directory info object.
            exist_ok: Whether to ignore if the directory already exists.

        Raises:
            FileExistsBarecatError: If the directory already exists and `exist_ok` is False.
        """
        if dinfo.path == '' and exist_ok:
            self.cursor.execute(
                """
                UPDATE dirs SET mode=:mode, uid=:uid, gid=:gid, mtime_ns=:mtime_ns
                 WHERE path=''""",
                dinfo.asdict(),
            )
            return

        maybe_replace = 'OR REPLACE' if exist_ok else ''
        try:
            self.cursor.execute(
                f"""
                INSERT {maybe_replace} INTO dirs (path, mode, uid, gid, mtime_ns)
                VALUES (:path, :mode, :uid, :gid, :mtime_ns)
                """,
                dinfo.asdict(),
            )
        except sqlite3.IntegrityError as e:
            if 'Path already exists as file' in str(e):
                raise NotADirectoryBarecatError(
                    f'{dinfo.path!r} or a parent exists as a file'
                ) from e
            raise FileExistsBarecatError(dinfo.path) from e

    def rename(self, old: Union[BarecatEntryInfo, str], new: str, allow_overwrite: bool = False):
        """Rename a file or directory in the index.

        Args:
            old: Path of the file or directory or the file or directory info object.
            new: New path.
            allow_overwrite: if True and a file with path `new` already exists, then it is removed first.
                if False, an exception is raised.

        Raises:
            FileNotFoundBarecatError: If the file or directory is not found.
            FileExistsBarecatError: If the new path already exists and `allow_overwrite` is False.
            IsADirectoryBarecatError: If the new path is a directory.
            DirectoryNotEmptyBarecatError: If the new path is a non-empty directory.
        """
        if isinstance(old, BarecatFileInfo) or (isinstance(old, str) and self.isfile(old)):
            self.rename_file(old, new, allow_overwrite)
        elif isinstance(old, BarecatDirInfo) or (isinstance(old, str) and self.isdir(old)):
            self.rename_dir(old, new, allow_overwrite)
        else:
            raise FileNotFoundBarecatError(old)

    def rename_file(
        self, old: Union[BarecatFileInfo, str], new: str, allow_overwrite: bool = False
    ):
        """Rename a file in the index.

        Args:
            old: Path of the file or the file info object.
            new: New path.

        Raises:
            FileNotFoundBarecatError: If the file is not found.
            FileExistsBarecatError: If the new path already exists and `allow_overwrite` is False.
            IsADirectoryBarecatError: If the new path is a directory.
        """
        old_path = self._as_path(old)
        new_path = normalize_path(new)
        if self.isfile(new_path):
            if allow_overwrite:
                self.remove_file(new_path)
            else:
                raise FileExistsBarecatError(new_path)

        if self.isdir(new_path):
            raise IsADirectoryBarecatError(new_path)

        try:
            self.cursor.execute(
                """
                UPDATE files SET path=:new_path WHERE path=:old_path
                """,
                dict(old_path=old_path, new_path=new_path),
            )
        except sqlite3.IntegrityError:
            raise FileExistsBarecatError(new_path)

    def rename_dir(self, old: Union[BarecatDirInfo, str], new: str, allow_overwrite: bool = False):
        """Rename a directory in the index.

        Args:
            old: Path of the directory or the directory info object.
            new: New path.

        Raises:
            FileNotFoundBarecatError: If the directory is not found.
            FileExistsBarecatError: If the new path already exists.
            NotADirectoryBarecatError: If the new path is a file.
            DirectoryNotEmptyBarecatError: If the new path is a non-empty directory.
        """

        old_path = self._as_path(old)
        new_path = normalize_path(new)
        if old_path == new_path:
            return
        if old_path == '':
            raise BarecatError('Cannot rename the root directory')

        if self.isfile(new_path):
            raise NotADirectoryBarecatError(new_path)

        if self.isdir(new_path):
            if allow_overwrite:
                self.remove_empty_dir(new_path)
            else:
                raise FileExistsBarecatError(new_path)

        dinfo = self._as_dirinfo(old)

        # We temporarily disable foreign keys because we are orphaning the files and dirs in the
        # directory
        with self.no_foreign_keys():
            try:
                # This triggers, and updates ancestors, which is good
                # We do this first, in case the new path already exists
                self.cursor.execute(
                    """
                    UPDATE dirs SET path = :new_path WHERE path = :old_path
                    """,
                    dict(old_path=old_path, new_path=new_path),
                )
            except sqlite3.IntegrityError:
                raise FileExistsBarecatError(new_path)

            if dinfo.num_files > 0 or dinfo.num_subdirs > 0:
                with self.no_triggers():
                    if dinfo.num_files_tree > 0:
                        self.cursor.execute(
                            r"""
                            UPDATE files
                            -- The substring starts with the '/' after the old dirpath
                            -- SQL indexing starts at 1
                            SET path = :new_path || substr(path, length(:old_path) + 1)
                            WHERE path GLOB
                            replace(replace(replace(:old_path, '[', '[[]'), '?', '[?]'), '*', '[*]')
                             || '/*'
                            """,
                            dict(old_path=old_path, new_path=new_path),
                        )
                    if dinfo.num_subdirs > 0:
                        self.cursor.execute(
                            r"""
                            UPDATE dirs
                            SET path = :new_path || substr(path, length(:old_path) + 1)
                            WHERE path GLOB
                            replace(replace(replace(:old_path, '[', '[[]'), '?', '[?]'), '*', '[*]')
                             || '/*'
                            """,
                            dict(old_path=old_path, new_path=new_path),
                        )

    # DELETING
    def remove_file(self, item: Union[BarecatFileInfo, str]):
        """Remove a file from the index.

        Args:
            item: Path of the file or the file info object.

        Raises:
            FileNotFoundBarecatError: If the file is not found.
        """
        path = self._as_path(item)
        self.cursor.execute('DELETE FROM files WHERE path=?', (path,))
        if self.cursor.rowcount == 0:
            raise FileNotFoundBarecatError(path)

    def remove_files(self, items: Iterable[Union[BarecatFileInfo, str]]):
        """Remove multiple files from the index.

        Args:
            items: Paths of the files or the file info objects.

        Raises:
            FileNotFoundBarecatError: If any of the files is not found.
        """
        self.cursor.executemany(
            """
            DELETE FROM files WHERE path=?
            """,
            ((self._as_path(x),) for x in items),
        )

    def remove_empty_dir(self, item: Union[BarecatDirInfo, str]):
        """Remove an empty directory from the index.

        Args:
            item: Path of the directory or the directory info object.

        Raises:
            DirectoryNotEmptyBarecatError: If the directory is not empty.
            FileNotFoundBarecatError: If the directory is not found.
        """
        dinfo = self._as_dirinfo(item)
        if dinfo.num_entries != 0:
            raise DirectoryNotEmptyBarecatError(item)
        self.cursor.execute('DELETE FROM dirs WHERE path=?', (dinfo.path,))

    def remove_recursively(self, item: Union[BarecatDirInfo, str]):
        """Remove a directory and all its contents recursively.

        Args:
            item: Path of the directory or the directory info object.

        Raises:
            FileNotFoundBarecatError: If the directory is not found.
        """
        dinfo = self._as_dirinfo(item)
        if dinfo.path == '':
            raise BarecatError('Cannot remove the root directory')

        if dinfo.num_files > 0 or dinfo.num_subdirs > 0:
            with self.no_triggers():
                # First the files, then the dirs, this way foreign key constraints are not violated
                if dinfo.num_files_tree > 0:
                    self.cursor.execute(
                        r"""
                        DELETE FROM files WHERE path GLOB
                        replace(replace(replace(:dirpath, '[', '[[]'), '?', '[?]'), '*', '[*]')
                         || '/*'
                        """,
                        dict(dirpath=dinfo.path),
                    )
                if dinfo.num_subdirs > 0:
                    self.cursor.execute(
                        r"""
                        DELETE FROM dirs WHERE path GLOB
                        replace(replace(replace(:dirpath, '[', '[[]'), '?', '[?]'), '*', '[*]')
                         || '/*'
                        """,
                        dict(dirpath=dinfo.path),
                    )
        # Now delete the directory itself, triggers will update ancestors, etc.
        self.cursor.execute('DELETE FROM dirs WHERE path=?', (dinfo.path,))

    def chmod(self, path: str, mode: int):
        """Change the mode of a file or directory.

        Args:
            path: Path of the file or directory.
            mode: New mode.

        Raises:
            FileNotFoundBarecatError: If the file or directory is not found.
        """
        path = normalize_path(path)
        self.cursor.execute("""UPDATE files SET mode=? WHERE path=?""", (mode, path))
        if self.cursor.rowcount > 0:
            return

        self.cursor.execute("""UPDATE dirs SET mode=? WHERE path=?""", (mode, path))
        if self.cursor.rowcount == 0:
            raise FileNotFoundBarecatError(f'Path {path} not found in index')

    def chown(self, path: str, uid: int, gid: int):
        """Change the owner and group of a file or directory.

        Args:
            path: Path of the file or directory.
            uid: New user ID.
            gid: New group ID.

        Raises:
            FileNotFoundBarecatError: If the file or directory is not found.
        """

        path = normalize_path(path)
        self.cursor.execute(
            """
            UPDATE files SET uid=?, gid=? WHERE path=?
            """,
            (uid, gid, path),
        )
        if self.cursor.rowcount > 0:
            return

        self.cursor.execute(
            """
            UPDATE dirs SET uid=?, gid=? WHERE path=?
            """,
            (uid, gid, path),
        )
        if self.cursor.rowcount == 0:
            raise FileNotFoundBarecatError(f'Path {path} not found in index')

    def update_mtime(self, path: str, mtime_ns: int):
        """Update the modification time of a file or directory.

        Args:
            path: Path of the file or directory.
            mtime_ns: New modification time in nanoseconds since the Unix epoch.

        Raises:
            FileNotFoundBarecatError: If the file or directory is not found.
        """

        path = normalize_path(path)
        self.cursor.execute(
            """
            UPDATE files SET mtime_ns = :mtime_ns WHERE path = :path
            """,
            dict(path=path, mtime_ns=mtime_ns),
        )
        if self.cursor.rowcount > 0:
            return
        self.cursor.execute(
            """
            UPDATE dirs SET mtime_ns = :mtime_ns WHERE path = :path
            """,
            dict(path=path, mtime_ns=mtime_ns),
        )
        if self.cursor.rowcount == 0:
            raise FileNotFoundBarecatError(f'Path {path} not found in index')

    def find_space(self, path: Union[BarecatFileInfo, str], size: int):
        finfo = self._as_fileinfo(path)
        requested_space = size - finfo.size
        if requested_space <= 0:
            return finfo

        # need to check if there is space in the shard
        result = self.fetch_one(
            """
            SELECT offset FROM files
            WHERE shard = :shard AND offset > :offset
            ORDER BY offset LIMIT 1
            """,
            dict(shard=finfo.shard, offset=finfo.offset),
        )
        space_available = (
            result['offset'] - finfo.offset
            if result is not None
            else self.shard_size_limit - finfo.offset
        )
        if space_available >= requested_space:
            return finfo

        # find first hole large enough:
        result = self.fetch_one(
            """
            SELECT shard, gap_offset FROM (
                SELECT
                    shard,
                    (offset + size) AS gap_offset,
                    LEAD(offset, 1, :shard_size_limit) OVER (PARTITION BY shard ORDER BY offset)
                    AS gap_end
                FROM files)
            WHERE gap_end - gap_offset > :requested_size
            ORDER BY shard, gap_offset
            LIMIT 1
            """,
            dict(requested_size=size - finfo.size, shard_size_limit=self.shard_size_limit),
        )
        if result is not None:
            new_finfo = copy.copy(finfo)
            new_finfo.shard = result['shard']
            new_finfo.offset = result['gap_offset']
            return new_finfo

        # Must start new shard
        new_finfo = copy.copy(finfo)
        new_finfo.shard = self.num_used_shards
        new_finfo.offset = 0
        return new_finfo

    def verify_integrity(self):
        """Verify the integrity of the index.

        This method checks if the number of files, number of subdirectories, size of the directory
        tree, and number of files in the directory tree are correct. It also checks the integrity of
        the SQLite database.

        Returns:
            True if no problems are found, False otherwise.
        """
        is_good = True
        # check if num_subdirs, num_files, size_tree, num_files_tree are correct
        # Uses bottom-up recursive CTE: O(files * avg_depth) instead of O(dirs * files) with GLOB

        # Compute treestats (size_tree, num_files_tree) using recursive CTE
        self.cursor.execute(
            r"""
            CREATE TEMPORARY TABLE tmp_verify_treestats AS
                WITH RECURSIVE file_ancestors AS (
                    SELECT parent AS ancestor, size FROM files
                    UNION ALL
                    SELECT
                        rtrim(rtrim(ancestor, replace(ancestor, '/', '')), '/'),
                        size
                    FROM file_ancestors
                    WHERE ancestor != ''
                )
                SELECT
                    ancestor AS path,
                    SUM(size) AS size_tree,
                    COUNT(*) AS num_files_tree
                FROM file_ancestors
                GROUP BY ancestor
            """
        )

        # Compute direct file counts per directory
        self.cursor.execute(
            r"""
            CREATE TEMPORARY TABLE tmp_verify_file_counts AS
                SELECT parent AS path, COUNT(*) AS num_files
                FROM files GROUP BY parent
            """
        )

        # Compute direct subdir counts per directory
        self.cursor.execute(
            r"""
            CREATE TEMPORARY TABLE tmp_verify_subdir_counts AS
                SELECT parent AS path, COUNT(*) AS num_subdirs
                FROM dirs GROUP BY parent
            """
        )

        # Join all computed stats into a single temp table
        self.cursor.execute(
            r"""
            CREATE TEMPORARY TABLE temp_dir_stats AS
                SELECT
                    d.path,
                    COALESCE(fc.num_files, 0) AS num_files,
                    COALESCE(sc.num_subdirs, 0) AS num_subdirs,
                    COALESCE(ts.size_tree, 0) AS size_tree,
                    COALESCE(ts.num_files_tree, 0) AS num_files_tree
                FROM dirs d
                LEFT JOIN tmp_verify_file_counts fc ON fc.path = d.path
                LEFT JOIN tmp_verify_subdir_counts sc ON sc.path = d.path
                LEFT JOIN tmp_verify_treestats ts ON ts.path = d.path
            """
        )

        res = self.fetch_many(
            """
            SELECT
                dirs.path,
                dirs.num_files,
                temp_dir_stats.num_files AS temp_num_files,
                dirs.num_subdirs,
                temp_dir_stats.num_subdirs AS temp_num_subdirs,
                dirs.size_tree,
                temp_dir_stats.size_tree AS temp_size_tree,
                dirs.num_files_tree,
                temp_dir_stats.num_files_tree AS temp_num_files_tree
            FROM
                dirs
            JOIN
                temp_dir_stats
            ON
                dirs.path = temp_dir_stats.path
            WHERE
                NOT (
                    dirs.num_files = temp_dir_stats.num_files AND
                    dirs.num_subdirs = temp_dir_stats.num_subdirs AND
                    dirs.size_tree = temp_dir_stats.size_tree AND
                    dirs.num_files_tree = temp_dir_stats.num_files_tree
                )
        """,
            bufsize=10,
        )

        if len(res) > 0:
            is_good = False
            print('Mismatch in dir stats:')
            for row in res:
                print('Mismatch:', dict(**row))

        integrity_check_result = self.fetch_all('PRAGMA integrity_check')
        if integrity_check_result[0][0] != 'ok':
            str_result = str([dict(**x) for x in integrity_check_result])
            print('Integrity check failed: \n' + str_result, file=sys.stderr)
            is_good = False
        foreign_keys_check_result = self.fetch_all('PRAGMA foreign_key_check')
        if foreign_keys_check_result:
            str_result = str([dict(**x) for x in foreign_keys_check_result])
            print('Foreign key check failed: \n' + str_result, file=sys.stderr)
            is_good = False

        # Check for paths that exist as both file and directory
        # Scan dirs (smaller) and lookup in files (larger)
        conflicts = self.fetch_all('SELECT path FROM dirs WHERE path IN (SELECT path FROM files)')
        if conflicts:
            is_good = False
            print('Paths exist as both file and directory:')
            for row in conflicts:
                print(f'  {row[0]}')

        # Cleanup temporary tables
        self.cursor.execute('DROP TABLE IF EXISTS tmp_verify_treestats')
        self.cursor.execute('DROP TABLE IF EXISTS tmp_verify_file_counts')
        self.cursor.execute('DROP TABLE IF EXISTS tmp_verify_subdir_counts')
        self.cursor.execute('DROP TABLE IF EXISTS temp_dir_stats')

        return is_good

    def merge_from_other_barecat(
        self,
        source_index_path: str,
        ignore_duplicates: bool = False,
        prefix: str = None,
        update_treestats: bool = True,
    ):
        """Adds the files and directories from another Barecat index to this one.

        Typically used during symlink-based merging. That is, the shards in the source Barecat
        are assumed to be simply be placed next to each other, instead of being merged with the
        existing shards in this index.
        For merging the shards themselves, more complex logic is needed, and that method is
        in the Barecat class.

        Args:
            source_index_path: Path to the source Barecat index.
            ignore_duplicates: Whether to ignore duplicate files and directories.
            prefix: Optional path prefix to prepend to all paths.
            update_treestats: Whether to recompute directory tree statistics after merging.

        Raises:
            sqlite3.DatabaseError: If an error occurs during the operation.
            ValueError: If file/directory path conflicts exist between source and target.
        """
        self._merge_helper.merge_from_other_barecat(
            source_index_path, ignore_duplicates, prefix, update_treestats
        )

    def check_merge_conflicts(self, prefix: str) -> str:
        """Check for file/directory conflicts with attached sourcedb.

        Must be called while sourcedb is attached.

        Args:
            prefix: Path prefix to prepend to source paths (empty string for no prefix).

        Returns:
            SQL expression for transforming source paths with prefix.

        Raises:
            ValueError: If file/directory path conflicts exist.
        """
        return self._merge_helper.check_merge_conflicts(prefix)

    def update_treestats(self):
        """Recompute size_tree and num_files_tree for all directories.

        Uses a bottom-up recursive CTE that expands each file to all its ancestor
        directories, then aggregates. This is O(files * avg_depth) instead of the
        naive GLOB/LIKE join which is O(dirs * files).

        Performance comparison (10k files, 769 dirs, avg depth ~3):

            | Method | Time   | vs CTE    |
            |--------|--------|-----------|
            | GLOB   | 2.53s  | 73x slower|
            | LIKE   | 0.67s  | 19x slower|
            | CTE    | 0.04s  | baseline  |

        On a real dataset (21M files, 341k dirs):
            - CTE took 170s
            - GLOB/LIKE would take hours (O(dirs * files) = 7 trillion comparisons)
        """
        print('Computing treestats (bottom-up recursive CTE)')
        self.cursor.execute(
            r"""
            CREATE TEMPORARY TABLE tmp_treestats AS
                WITH RECURSIVE file_ancestors AS (
                    SELECT parent AS ancestor, size FROM files
                    UNION ALL
                    SELECT
                        rtrim(rtrim(ancestor, replace(ancestor, '/', '')), '/'),
                        size
                    FROM file_ancestors
                    WHERE ancestor != ''
                )
                SELECT
                    ancestor AS path,
                    SUM(size) AS size_tree,
                    COUNT(*) AS num_files_tree
                FROM file_ancestors
                GROUP BY ancestor
            """
        )

        print('Creating temporary tables for file counts')
        self.cursor.execute(
            r"""
            CREATE TEMPORARY TABLE tmp_file_counts AS
                SELECT
                    parent AS path,
                    COUNT(*) AS num_files
                FROM files
                GROUP BY parent
            """
        )

        print('Creating temporary tables for subdir counts')
        self.cursor.execute(
            r"""
            CREATE TEMPORARY TABLE tmp_subdir_counts AS
                SELECT
                    parent AS path,
                    COUNT(*) AS num_subdirs
                FROM dirs
                GROUP BY parent
            """
        )

        print('Updating dirs table with treestats')
        with self.no_triggers():
            self.cursor.execute(
                r"""
                UPDATE dirs
                SET
                    num_files = COALESCE(fc.num_files, 0),
                    num_subdirs = COALESCE(sc.num_subdirs, 0),
                    size_tree = COALESCE(ts.size_tree, 0),
                    num_files_tree = COALESCE(ts.num_files_tree, 0)
                FROM dirs d
                LEFT JOIN tmp_file_counts fc ON fc.path = d.path
                LEFT JOIN tmp_subdir_counts sc ON sc.path = d.path
                LEFT JOIN tmp_treestats ts ON ts.path = d.path
                WHERE dirs.path = d.path;
            """
            )

    def update_dirs(self):
        """Ensure all ancestor directories exist for both files and dirs."""
        self.cursor.execute(
            """
            WITH RECURSIVE
                all_ancestors AS (
                    -- Parents of files (using generated column)
                    SELECT DISTINCT parent AS path FROM files WHERE parent != ''
                    UNION
                    -- Parents of dirs (using generated column)
                    SELECT DISTINCT parent AS path FROM dirs WHERE parent IS NOT NULL
                    UNION
                    -- Walk up the tree (must compute for CTE values)
                    SELECT rtrim(rtrim(path, replace(path, '/', '')), '/')
                    FROM all_ancestors
                    WHERE path LIKE '%/%'
                )
            INSERT OR IGNORE INTO dirs (path)
            SELECT path FROM all_ancestors
            UNION ALL SELECT ''
            """
        )

    @property
    def _triggers_enabled(self):
        return self.fetch_one("SELECT value_int FROM config WHERE key='use_triggers'")[0] == 1

    @_triggers_enabled.setter
    def _triggers_enabled(self, value: bool):
        self.cursor.execute(
            """
            UPDATE config SET value_int=:value WHERE key='use_triggers'
            """,
            dict(value=int(value)),
        )

    @contextlib.contextmanager
    def no_triggers(self):
        """Context manager to temporarily disable triggers.

        Also disables foreign key checks, since triggers are what create parent
        directory rows - without them, FK constraints would fail.

        Note: Does NOT rebuild dirs/treestats on exit. Use bulk_write_mode() for
        that, or call update_dirs() and update_treestats() manually.
        """
        prev_triggers = self._triggers_enabled
        prev_fk = self._foreign_keys_enabled
        if not prev_triggers:
            yield
            return
        try:
            self._triggers_enabled = False
            self._foreign_keys_enabled = False
            yield
        finally:
            self._triggers_enabled = prev_triggers
            self._foreign_keys_enabled = prev_fk

    @contextlib.contextmanager
    def bulk_mode(
        self,
        drop_indexes: bool = False,
        update_dirs_at_end: bool = True,
        update_treestats_at_end: bool = True,
    ):
        """Context manager for bulk operations with automatic cleanup.

        Args:
            drop_indexes: If True, drops indexes for maximum speed (use only for
                fresh/empty archives). If False (default), keeps indexes so that
                ON CONFLICT duplicate handling works (use for merges/updates).
            update_dirs_at_end: If True (default), creates missing ancestor
                directories on exit.
            update_treestats_at_end: If True (default), recomputes directory
                tree statistics on exit.

        On exit:
        - Rebuilds indexes and triggers (if dropped)
        - Re-enables triggers and foreign keys (if not dropped)
        - Creates missing ancestor directories (if update_dirs_at_end)
        - Recomputes directory tree statistics (if update_treestats_at_end)
        """
        if drop_indexes:
            if self.conn.in_transaction:
                self.conn.commit()

            # Disable FK (required because dropping unique index on dirs.path breaks FK check)
            self._foreign_keys_enabled = False

            # Drop triggers (they reference unique indexes via ON CONFLICT)
            self.cursor.execute('DROP TRIGGER IF EXISTS add_file')
            self.cursor.execute('DROP TRIGGER IF EXISTS del_file')
            self.cursor.execute('DROP TRIGGER IF EXISTS move_file')
            self.cursor.execute('DROP TRIGGER IF EXISTS resize_file')
            self.cursor.execute('DROP TRIGGER IF EXISTS add_subdir')
            self.cursor.execute('DROP TRIGGER IF EXISTS del_subdir')
            self.cursor.execute('DROP TRIGGER IF EXISTS move_subdir')
            self.cursor.execute('DROP TRIGGER IF EXISTS resize_dir')

            # Drop indexes
            self.cursor.execute('DROP INDEX IF EXISTS idx_files_path')
            self.cursor.execute('DROP INDEX IF EXISTS idx_dirs_path')
            self.cursor.execute('DROP INDEX IF EXISTS idx_files_parent')
            self.cursor.execute('DROP INDEX IF EXISTS idx_dirs_parent')
            self.cursor.execute('DROP INDEX IF EXISTS idx_files_shard_offset')
            try:
                yield
            finally:
                sql_dir = osp.join(osp.dirname(__file__), '../sql')
                self.cursor.executescript(misc.read_file(f'{sql_dir}/indexes.sql'))
                self.cursor.executescript(misc.read_file(f'{sql_dir}/triggers.sql'))
                self._foreign_keys_enabled = True
                if update_dirs_at_end:
                    self.update_dirs()
                if update_treestats_at_end:
                    self.update_treestats()
        else:
            with self.no_triggers():
                yield
            if update_dirs_at_end:
                self.update_dirs()
            if update_treestats_at_end:
                self.update_treestats()

    @property
    def _foreign_keys_enabled(self):
        return self.fetch_one('PRAGMA foreign_keys')[0] == 1

    @_foreign_keys_enabled.setter
    def _foreign_keys_enabled(self, value):
        # PRAGMA foreign_keys can only be changed outside of a transaction
        if self.conn.in_transaction:
            self.conn.commit()
        self.cursor.execute(f"PRAGMA foreign_keys = {'ON' if value else 'OFF'}")

    @contextlib.contextmanager
    def no_foreign_keys(self):
        prev_setting = self._foreign_keys_enabled
        if not prev_setting:
            yield
            return
        try:
            self._foreign_keys_enabled = False
            yield
        finally:
            self._foreign_keys_enabled = True

    @contextlib.contextmanager
    def attached_database(self, path: str, name: str = 'sourcedb', readonly: bool = True):
        """Context manager to attach another SQLite database temporarily."""
        mode = 'ro' if readonly else 'rw'
        self.cursor.execute(f"ATTACH DATABASE 'file:{path}?mode={mode}' AS {name}")
        try:
            yield
        finally:
            self.conn.commit()
            self.cursor.execute(f'DETACH DATABASE {name}')

    def close(self):
        """Close the index."""
        if self.is_closed:
            return
        self.cursor.close()
        if not self.readonly:
            self.conn.commit()
            self.conn.execute('PRAGMA optimize')
        self.conn.close()
        self.is_closed = True

    def optimize(self):
        """Optimize the index."""
        if not self.readonly:
            self.conn.commit()
            self.conn.execute('ANALYZE')
            self.conn.execute('VACUUM')
            self.conn.execute('PRAGMA optimize')

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager."""
        self.close()

    # This can cause issues when multi-threading
    # def __del__(self):
    #     """Commit when the object is deleted."""
    #     self.close()


class Fetcher:
    def __init__(self, conn, cursor=None, bufsize=None, row_factory=sqlite3.Row):
        self.conn = conn
        if cursor is None:
            self.cursor = conn.cursor()
            self.cursor.arraysize = bufsize if bufsize is not None else 128
        else:
            self.cursor = cursor

        self.bufsize = bufsize if bufsize is not None else self.cursor.arraysize
        self.row_factory = row_factory

    def fetch_iter(self, query, params=(), cursor=None, bufsize=None, rowcls=None):
        # This needs its own cursor because the results are not all fetched in this
        # call, the user may interleave other queries before consuming all results
        # from this generator
        cursor = self.conn.cursor() if cursor is None else cursor
        bufsize = bufsize if bufsize is not None else self.bufsize
        cursor.row_factory = rowcls.row_factory if rowcls is not None else self.row_factory
        cursor.execute(query, params)
        while rows := cursor.fetchmany(bufsize):
            yield from rows

    def fetch_one(self, query, params=(), cursor=None, rowcls=None):
        cursor = self.cursor if cursor is None else cursor
        cursor.row_factory = rowcls.row_factory if rowcls is not None else self.row_factory
        cursor.execute(query, params)
        return cursor.fetchone()

    def fetch_one_or_raise(self, query, params=(), cursor=None, rowcls=None):
        res = self.fetch_one(query, params, cursor, rowcls)
        if res is None:
            raise LookupError()
        return res

    def fetch_all(self, query, params=(), cursor=None, rowcls=None):
        cursor = self.cursor if cursor is None else cursor
        cursor.row_factory = rowcls.row_factory if rowcls is not None else self.row_factory
        cursor.execute(query, params)
        return cursor.fetchall()

    def fetch_many(self, query, params=(), cursor=None, bufsize=None, rowcls=None):
        cursor = self.cursor if cursor is None else cursor
        cursor.row_factory = rowcls.row_factory if rowcls is not None else self.row_factory
        cursor.execute(query, params)
        return cursor.fetchmany(bufsize)


class RecallableIter:
    """An iterable that wraps an iterator so that it can be recalled to the beginning and iterated again."""

    def __init__(self, iterator):
        self.cached_items = []
        self.iterator = iter(iterator)

    def advance(self):
        result = next(self.iterator)
        self.cached_items.append(result)
        return result

    def __iter__(self):
        return self._Iterator(self)

    class _Iterator:
        def __init__(self, recall_iter: 'RecallableIter'):
            self.recallable_iter = recall_iter
            self.next_index = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self.next_index < len(self.recallable_iter.cached_items):
                result = self.recallable_iter.cached_items[self.next_index]
                self.next_index += 1
                return result
            else:
                self.next_index += 1
                return self.recallable_iter.advance()
