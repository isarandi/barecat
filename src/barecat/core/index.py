import contextlib
import copy
import itertools
import os
import os.path as osp
import re
import sqlite3
from datetime import datetime
from typing import Iterable, Iterator, Optional, Union, TYPE_CHECKING

import barecat.util

if TYPE_CHECKING:
    from barecat import BarecatDirInfo, BarecatFileInfo, BarecatEntryInfo, Order
else:
    from barecat.common import BarecatDirInfo, BarecatFileInfo, BarecatEntryInfo, Order

from barecat.exceptions import (
    BarecatError,
    DirectoryNotEmptyBarecatError,
    FileExistsBarecatError,
    FileNotFoundBarecatError,
)
from barecat.glob_to_regex import glob_to_regex
from barecat.util import datetime_to_ns, normalize_path
from contextlib import AbstractContextManager


class Index(AbstractContextManager):
    """Manages the SQLite database storing metadata about the files and directories in the Barecat
    archive.

    Args:
        path: Path to the SQLite database file, including the ``"-sqlite-index"`` suffix.
        shard_size_limit: Maximum size of a shard in bytes. If None, the shard size is unlimited.
        bufsize: Buffer size for fetching rows.
        readonly: Whether to open the index in read-only mode.
    """

    def __init__(
        self,
        path: str,
        shard_size_limit: Optional[int] = None,
        bufsize: Optional[int] = None,
        readonly: bool = True,
    ):
        is_new = not osp.exists(path)
        self.readonly = readonly
        try:
            self.conn = sqlite3.connect(
                f'file:{path}?mode={"ro" if self.readonly else "rwc"}', uri=True
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
        self.fetcher = Fetcher(self.conn, self.cursor, bufsize=bufsize)
        self.fetch_one = self.fetcher.fetch_one
        self.fetch_one_or_raise = self.fetcher.fetch_one_or_raise
        self.fetch_all = self.fetcher.fetch_all
        self.fetch_iter = self.fetcher.fetch_iter
        self.fetch_many = self.fetcher.fetch_many

        self._shard_size_limit_cached = None

        if is_new:
            sql_path = osp.join(osp.dirname(__file__), '../sql/schema.sql')
            self.cursor.executescript(barecat.util.read_file(sql_path))
            with self.no_triggers():
                self.cursor.execute(
                    "INSERT INTO dirs (path, uid, gid, mtime_ns) VALUES ('', ?, ?, ?)",
                    (os.getuid(), os.getgid(), datetime_to_ns(datetime.now())),
                )

        if not self.readonly:
            self.cursor.execute('PRAGMA recursive_triggers = ON')
            self.cursor.execute('PRAGMA foreign_keys = ON')
            self._triggers_enabled = True
            self._foreign_keys_enabled = True
            if shard_size_limit is not None:
                self.shard_size_limit = shard_size_limit

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
            return self.fetch_one_or_raise(
                """
                SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns 
                FROM files WHERE path=?
                """,
                (path,),
                rowcls=BarecatFileInfo,
            )
        except LookupError:
            raise FileNotFoundBarecatError(path)

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
            return self.fetch_one_or_raise(
                """
                SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
                    mode, uid, gid, mtime_ns
                FROM dirs WHERE path=?
                """,
                (dirpath,),
                rowcls=BarecatDirInfo,
            )
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
        except LookupError:
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
        return self.fetch_one("SELECT COUNT(*) FROM dirs")[0]

    @property
    def total_size(self):
        """Total size of all files in the index, in bytes."""
        return self.fetch_one("SELECT size_tree FROM dirs WHERE path=''")[0]

    def __iter__(self):
        """Iterate over all file info objects in the index."""
        yield from self.iter_all_fileinfos(order=Order.ANY)

    def __contains__(self, path: str) -> bool:
        """Check if a file or directory exists in the index.

        Args:
            path: Path of the file or directory.

        Returns:
            True if the file or directory exists, False otherwise.
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
            order: Order in which to iterate over the files.
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
        for finfo in self.iter_all_fileinfos(order=order, bufsize=bufsize):
            yield finfo.path

    def iter_all_dirpaths(
        self, order: Order = Order.ANY, bufsize: Optional[int] = None
    ) -> Iterator[str]:
        for dinfo in self.iter_all_dirinfos(order=order, bufsize=bufsize):
            yield dinfo.path

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
            return []
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
            return []
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

    # glob paths
    def raw_glob_paths(self, pattern, order: Order = Order.ANY):
        pattern = normalize_path(pattern)
        query = """
            SELECT path FROM dirs WHERE path GLOB :pattern
            UNION ALL
            SELECT path FROM files WHERE path GLOB :pattern"""
        query += order.as_query_text()
        rows = self.fetch_all(query, dict(pattern=pattern))
        return [row['path'] for row in rows]

    def raw_iterglob_paths(self, pattern, order: Order = Order.ANY, only_files=False, bufsize=None):
        pattern = normalize_path(pattern)
        if only_files:
            query = """
                SELECT path FROM files WHERE path GLOB :pattern"""
        else:
            query = """
                SELECT path FROM dirs WHERE path GLOB :pattern
                UNION ALL
                SELECT path FROM files WHERE path GLOB :pattern"""
        query += order.as_query_text()
        rows = self.fetch_iter(query, dict(pattern=pattern), bufsize=bufsize)
        return (row['path'] for row in rows)

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
        return list(
            self.iterglob_paths(
                pattern, recursive=recursive, include_hidden=include_hidden, only_files=only_files
            )
        )

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

        if recursive and pattern == '**':
            if only_files:
                yield from self.iter_all_filepaths(bufsize=bufsize)
            else:
                yield from self.iter_all_paths(bufsize=bufsize)
            return

        parts = pattern.split('/')
        num_has_wildcard = sum(1 for p in parts if '*' in p or '?' in p)
        has_no_brackets = '[' not in pattern and ']' not in pattern
        has_no_question = '?' not in pattern

        num_asterisk = pattern.count('*')
        if (
            recursive
            and has_no_brackets
            and has_no_question
            and num_asterisk == 3
            and '*' not in pattern.replace('/**/*', '')
        ):
            yield from self.raw_iterglob_paths(
                pattern.replace('/**/*', '/*'), bufsize=bufsize, only_files=only_files
            )
            return

        if (
            recursive
            and has_no_brackets
            and has_no_question
            and num_asterisk == 2
            and pattern.endswith('/**')
        ):
            if not only_files and self.isdir(pattern[:-3]):
                yield pattern[:-3]
            yield from self.raw_iterglob_paths(pattern[:-1], bufsize=bufsize, only_files=only_files)
            return

        regex_pattern = glob_to_regex(pattern, recursive=recursive, include_hidden=include_hidden)
        if (not recursive or '**' not in pattern) and num_has_wildcard == 1 and has_no_brackets:
            parts = pattern.split('/')
            i_has_wildcard = next(i for i, p in enumerate(parts) if '*' in p or '?' in p)
            prefix = '/'.join(parts[:i_has_wildcard])
            wildcard_is_in_last_part = i_has_wildcard == len(parts) - 1
            if wildcard_is_in_last_part:
                info_generator = (
                    self.iter_direct_fileinfos(prefix) if only_files else self.iterdir_infos(prefix)
                )
                for info in info_generator:
                    if re.match(regex_pattern, info.path):
                        yield info.path
            else:
                suffix = '/'.join(parts[i_has_wildcard + 1 :])
                further_subdirs_wanted = len(parts) > i_has_wildcard + 2
                for subdirinfo in self.iter_subdir_dirinfos(prefix):
                    if (
                        further_subdirs_wanted and subdirinfo.num_subdirs == 0
                    ) or subdirinfo.num_entries == 0:
                        continue
                    candidate = subdirinfo.path + '/' + suffix
                    if re.match(regex_pattern, candidate) and (
                        (self.exists(candidate) and not only_files) or self.isfile(candidate)
                    ):
                        yield candidate
            return

        for candidate in self.raw_iterglob_paths(pattern, only_files=only_files, bufsize=bufsize):
            if re.match(regex_pattern, candidate):
                yield candidate

    ## glob infos
    def raw_iterglob_infos(self, pattern, only_files=False, bufsize=None):
        pattern = normalize_path(pattern)
        yield from self.fetch_iter(
            """
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns 
            FROM files WHERE path GLOB :pattern
            """,
            dict(pattern=pattern),
            bufsize=bufsize,
            rowcls=BarecatFileInfo,
        )
        if only_files:
            return
        yield from self.fetch_iter(
            """
            SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
                   mode, uid, gid, mtime_ns 
            FROM dirs WHERE path GLOB :pattern
            """,
            dict(pattern=pattern),
            bufsize=bufsize,
            rowcls=BarecatDirInfo,
        )

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
        if recursive and pattern == '**':
            if only_files:
                yield from self.iter_all_fileinfos(bufsize=bufsize)
            else:
                yield from self.iter_all_infos(bufsize=bufsize)
            return

        parts = pattern.split('/')
        num_has_wildcard = sum(1 for p in parts if '*' in p or '?' in p)
        has_no_brackets = '[' not in pattern and ']' not in pattern
        has_no_question = '?' not in pattern

        num_asterisk = pattern.count('*')
        if (
            recursive
            and has_no_brackets
            and has_no_question
            and num_asterisk == 3
            and '*' not in pattern.replace('/**/*', '')
        ):
            yield from self.raw_iterglob_infos(
                pattern.replace('/**/*', '/*'), bufsize=bufsize, only_files=only_files
            )
            return

        if (
            recursive
            and has_no_brackets
            and has_no_question
            and num_asterisk == 2
            and pattern.endswith('/**')
        ):
            if not only_files and self.isdir(pattern[:-3]):
                yield pattern[:-3]
            yield from self.raw_iterglob_infos(pattern[:-1], bufsize=bufsize, only_files=only_files)
            return

        regex_pattern = glob_to_regex(pattern, recursive=recursive, include_hidden=include_hidden)
        if (not recursive or '**' not in pattern) and num_has_wildcard == 1 and has_no_brackets:
            parts = pattern.split('/')
            i_has_wildcard = next(i for i, p in enumerate(parts) if '*' in p or '?' in p)
            prefix = '/'.join(parts[:i_has_wildcard])
            wildcard_is_in_last_part = i_has_wildcard == len(parts) - 1
            if wildcard_is_in_last_part:
                info_generator = (
                    self.iter_direct_fileinfos(prefix) if only_files else self.iterdir_infos(prefix)
                )
                for info in info_generator:
                    if re.match(regex_pattern, info.path):
                        yield info
            else:
                suffix = '/'.join(parts[i_has_wildcard + 1 :])
                further_subdirs_wanted = len(parts) > i_has_wildcard + 2
                for subdirinfo in self.iter_subdir_dirinfos(prefix):
                    if (
                        further_subdirs_wanted and subdirinfo.num_subdirs == 0
                    ) or subdirinfo.num_entries == 0:
                        continue
                    candidate_path = subdirinfo.path + '/' + suffix
                    if re.match(regex_pattern, candidate_path):
                        try:
                            yield (
                                self.lookup_file(candidate_path)
                                if only_files
                                else self.lookup(candidate_path)
                            )
                        except LookupError:
                            pass
            return

        for info in self.raw_iterglob_infos(pattern, only_files=only_files, bufsize=bufsize):
            if re.match(regex_pattern, info):
                yield info

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
                dinfo.path,
                [osp.basename(d.path) for d in subdirs],
                [osp.basename(f.path) for f in files],
            )

    ######################
    def reverse_lookup(self, shard: int, offset: int) -> BarecatFileInfo:
        """Look up a file by its shard and offset.

        Args:
            shard: Shard number.
            offset: Offset within the shard.

        Returns:
            The file info object.

        Raises:
            FileNotFoundBarecatError: If the file is not found.
        """

        try:
            return self.fetch_one_or_raise(
                'SELECT * FROM files WHERE shard=:shard AND offset=:offset',
                dict(shard=shard, offset=offset),
                rowcls=BarecatFileInfo,
            )
        except LookupError:
            raise FileNotFoundBarecatError(f'File with shard {shard} and offset {offset} not found')

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

        result = self.fetch_one(
            """
            SELECT coalesce(MAX(offset + size), 0) as end FROM files WHERE shard=:shard
            """,
            dict(shard=shard),
        )
        if result is None:
            return 0
        return result[0]

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
    def shard_size_limit(self, value: int):
        """Set the maximum allowed shard size, in bytes. Upon reaching this limit, a new shard is
        created.

        Args:
            value: The new shard size limit.
        """
        if self.readonly:
            raise ValueError('Cannot set shard size limit on a read-only index')
        if isinstance(value, str):
            value = barecat.util.parse_size(value)

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
            raise FileExistsBarecatError(finfo.path) from e

    def move_file(self, path: int, new_shard: int, new_offset: int):
        path = normalize_path(path)
        self.cursor.execute(
            """
            UPDATE files
            SET shard = :shard, offset = :offset
            WHERE path = :path""",
            dict(shard=new_shard, offset=new_offset, path=path),
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
            raise FileExistsBarecatError(dinfo.path) from e

    def rename(self, old: Union[BarecatEntryInfo, str], new: str):
        """Rename a file or directory in the index.

        Args:
            old: Path of the file or directory or the file or directory info object.
            new: New path.

        Raises:
            FileNotFoundBarecatError: If the file or directory is not found.
            FileExistsBarecatError: If the new path already exists.
        """
        if isinstance(old, BarecatFileInfo) or (isinstance(old, str) and self.isfile(old)):
            self.rename_file(old, new)
        elif isinstance(old, BarecatDirInfo) or (isinstance(old, str) and self.isdir(old)):
            self.rename_dir(old, new)
        else:
            raise FileNotFoundBarecatError(old)

    def rename_file(self, old: Union[BarecatFileInfo, str], new: str):
        """Rename a file in the index.

        Args:
            old: Path of the file or the file info object.
            new: New path.

        Raises:
            FileNotFoundBarecatError: If the file is not found.
            FileExistsBarecatError: If the new path already exists.
        """
        old_path = self._as_path(old)
        new_path = normalize_path(new)
        if self.exists(new_path):
            raise FileExistsBarecatError(new_path)

        try:
            self.cursor.execute(
                """
                UPDATE files SET path=:new_path WHERE path=:old_path
                """,
                dict(old_path=old_path, new_path=new_path),
            )
        except sqlite3.IntegrityError:
            raise FileExistsBarecatError(new_path)

    def rename_dir(self, old: Union[BarecatDirInfo, str], new: str):
        """Rename a directory in the index.

        Args:
            old: Path of the directory or the directory info object.
            new: New path.

        Raises:
            FileNotFoundBarecatError: If the directory is not found.
            FileExistsBarecatError: If the new path already exists.
        """

        old_path = self._as_path(old)
        new_path = normalize_path(new)
        if old_path == new_path:
            return
        if old_path == '':
            raise BarecatError('Cannot rename the root directory')

        if self.exists(new_path):
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
        if dinfo.num_files != 0 or dinfo.num_subdirs != 0:
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
        self.cursor.execute(
            r"""
            CREATE TEMPORARY TABLE temp_dir_stats (
                path TEXT PRIMARY KEY,
                num_files INTEGER DEFAULT 0,
                num_subdirs INTEGER DEFAULT 0,
                size_tree INTEGER DEFAULT 0,
                num_files_tree INTEGER DEFAULT 0)
        """
        )

        self.cursor.execute(
            r"""
            INSERT INTO temp_dir_stats (path, num_files, num_subdirs, size_tree, num_files_tree)
            SELECT
                dirs.path,
                -- Calculate the number of files in this directory
                (SELECT COUNT(*)
                 FROM files
                 WHERE files.parent = dirs.path) AS num_files,
            
                -- Calculate the number of subdirectories in this directory
                (SELECT COUNT(*)
                 FROM dirs AS subdirs
                 WHERE subdirs.parent = dirs.path) AS num_subdirs,
            
                -- Calculate the size_tree and num_files_tree using aggregation
                coalesce(SUM(files.size), 0) AS size_tree,
                COUNT(files.path) AS num_files_tree
            FROM dirs LEFT JOIN files ON files.path GLOB
                replace(replace(replace(dirs.path, '[', '[[]'), '?', '[?]'), '*', '[*]') || '/*'
                OR dirs.path = ''
            GROUP BY dirs.path
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
            print('Integrity check failed: \n' + str_result)
            is_good = False
        foreign_keys_check_result = self.fetch_all('PRAGMA foreign_key_check')
        if foreign_keys_check_result:
            str_result = str([dict(**x) for x in integrity_check_result])
            print('Foreign key check failed: \n' + str_result)
            is_good = False

        return is_good

    def merge_from_other_barecat(self, source_index_path: str, ignore_duplicates: bool = False):
        """Adds the files and directories from another Barecat index to this one.

        Typically used during symlink-based merging. That is, the shards in the source Barecat
        are assumed to be simply be placed next to each other, instead of being merged with the
        existing shards in this index.
        For merging the shards themselves, more complex logic is needed, and that method is
        in the Barecat class.

        Args:
            source_index_path: Path to the source Barecat index.
            ignore_duplicates: Whether to ignore duplicate files and directories.

        Raises:
            sqlite3.DatabaseError: If an error occurs during the operation.

        """

        with self.no_triggers():
            self.cursor.execute(f"ATTACH DATABASE 'file:{source_index_path}?mode=ro' AS sourcedb")

            # Duplicate dirs are allowed, they will be merged and updated
            self.cursor.execute(
                """
                INSERT INTO dirs (
                    path, num_subdirs, num_files, size_tree, num_files_tree,
                    mode, uid, gid, mtime_ns)
                SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
                    mode, uid, gid, mtime_ns
                FROM sourcedb.dirs WHERE true
                ON CONFLICT (dirs.path) DO UPDATE SET
                    num_subdirs = num_subdirs + excluded.num_subdirs,
                    num_files = num_files + excluded.num_files,
                    size_tree = size_tree + excluded.size_tree,
                    num_files_tree = num_files_tree + excluded.num_files_tree,
                    mode = coalesce(
                        dirs.mode | excluded.mode,
                        coalesce(dirs.mode, 0) | excluded.mode,
                        dirs.mode | coalesce(excluded.mode, 0)),
                    uid = coalesce(excluded.uid, dirs.uid),
                    gid = coalesce(excluded.gid, dirs.gid),
                    mtime_ns = coalesce(
                        max(dirs.mtime_ns, excluded.mtime_ns),
                        max(coalesce(dirs.mtime_ns, 0), excluded.mtime_ns),
                        max(dirs.mtime_ns, coalesce(excluded.mtime_ns, 0)))
                """
            )
            new_shard_number = self.num_used_shards
            maybe_ignore = 'OR IGNORE' if ignore_duplicates else ''
            self.cursor.execute(
                f"""
                INSERT {maybe_ignore} INTO files (
                    path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns)
                SELECT path, shard + ?, offset, size, crc32c, mode, uid, gid, mtime_ns
                FROM sourcedb.files
                """,
                (new_shard_number,),
            )
            self.conn.commit()
            self.cursor.execute("DETACH DATABASE sourcedb")

            if ignore_duplicates:
                self.update_treestats()
                self.conn.commit()

    def update_treestats(self):
        print('Creating temporary tables for treestats')
        self.cursor.execute(
            r"""
            CREATE TEMPORARY TABLE tmp_treestats AS
                SELECT 
                    dirs.path,
                    coalesce(SUM(files.size), 0) AS size_tree,
                    COUNT(files.path) AS num_files_tree
                FROM dirs
                LEFT JOIN files ON files.path GLOB
                    replace(replace(replace(dirs.path, '[', '[[]'), '?', '[?]'), '*', '[*]') || '/*'
                    OR dirs.path = ''
                GROUP BY dirs.path
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
        self.cursor.execute(
            r"""
            UPDATE dirs
            SET
                num_subdirs = COALESCE(sc.num_subdirs, 0),
                size_tree = COALESCE(ts.size_tree, 0),
                num_files_tree = COALESCE(ts.num_files_tree, 0)
            FROM tmp_file_counts fc
            LEFT JOIN tmp_subdir_counts sc ON sc.path = fc.path
            LEFT JOIN tmp_treestats ts ON ts.path = fc.path
            WHERE dirs.path = fc.path;
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
        """Context manager to temporarily disable triggers."""
        prev_setting = self._triggers_enabled
        if not prev_setting:
            yield
            return
        try:
            self._triggers_enabled = False
            yield
        finally:
            self._triggers_enabled = prev_setting

    @property
    def _foreign_keys_enabled(self):
        return self.fetch_one("PRAGMA foreign_keys")[0] == 1

    @_foreign_keys_enabled.setter
    def _foreign_keys_enabled(self, value):
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

    def close(self):
        """Close the index."""
        self.cursor.close()
        if not self.readonly:
            self.conn.commit()
            self.conn.execute('PRAGMA optimize')
        self.conn.close()

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


class Fetcher:
    def __init__(self, conn, cursor=None, bufsize=None, row_factory=sqlite3.Row):
        self.conn = conn
        self.cursor = conn.cursor() if cursor is None else cursor
        self.bufsize = bufsize if bufsize is not None else self.cursor.arraysize
        self.row_factory = row_factory

    def fetch_iter(self, query, params=(), cursor=None, bufsize=None, rowcls=None):
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
