import bz2
import os
import os.path as osp
import shutil
import stat
from collections.abc import Callable, Iterator, MutableMapping
from typing import Any, Optional, TYPE_CHECKING, Union

import crc32c as crc32c_lib

import barecat.progbar
import barecat.util
from barecat.core.sharder import Sharder
from barecat.defrag import BarecatDefragger
from barecat.exceptions import (
    FileExistsBarecatError,
    FileNotFoundBarecatError,
    IsADirectoryBarecatError,
)
from barecat.util import copyfileobj, raise_if_readonly, raise_if_readonly_or_append_only

if TYPE_CHECKING:
    from barecat import BarecatDirInfo, BarecatFileInfo, BarecatEntryInfo, FileSection, Index
else:
    from barecat.common import BarecatDirInfo, BarecatFileInfo, BarecatEntryInfo, FileSection
    from barecat.core.index import Index, normalize_path


class Barecat(MutableMapping[str, Any]):
    """Object for reading or writing a Barecat archive.

    A Barecat archive consists of several (large) shard files, each containing the data of multiple
    small files, and an SQLite index database that maps file paths to the corresponding shard,
    offset and size within the shard, as well as metadata such as modification time and checksum.

    The ``Barecat`` object provides two main interfaces:

    1. A dict-like interface, where keys are file paths and values are the file contents. The \
         contents can be raw bytes, or automatically decoded based on the file extension, if \
         ``auto_codec`` is set to ``True`` or codecs have been registered via \
         :meth:`register_codec`.
    2. A filesystem-like interface consisting of methods such as :meth:`open`, :meth:`exists`, \
        :meth:`listdir`, :meth:`walk`, :meth:`glob`, etc., modeled after Python's ``os`` module.

    Args:
        path: Path to the Barecat archive, without the -sqlite-index or -shard-XXXXX suffixes.
        shard_size_limit: Maximum size of each shard file. If None, the shard size is unlimited.
        readonly: If True, the Barecat archive is opened in read-only mode.
        overwrite: If True, the Barecat archive is first deleted if it already exists.
        auto_codec: If True, automatically encode/decode files based on their extension.
        exist_ok: If True, do not raise an error if the Barecat archive already exists.
        append_only: If True, only allow appending to the Barecat archive.
        threadsafe: If True, the Barecat archive is opened in thread-safe mode, where each thread
            or process will hold its own database connection and file handles for the shards.
        allow_writing_symlinked_shard: If True, allow writing to a shard file that is a symlink.
            Setting it to False is recommended, since changing the contents of a symlinked shard
            will bring the original index database out of sync with the actual shard contents.
    """

    def __init__(
        self,
        path: str,
        shard_size_limit: Optional[int] = None,
        readonly: bool = True,
        overwrite: bool = False,
        auto_codec: bool = False,
        exist_ok: bool = True,
        append_only: bool = False,
        threadsafe: bool = False,
        allow_writing_symlinked_shard: bool = False,
    ):
        if threadsafe and not readonly:
            raise ValueError('Threadsafe mode is only supported for readonly Barecat.')

        if not readonly and barecat.util.exists(path):
            if not exist_ok:
                raise FileExistsError(path)
            if overwrite:
                print(f'Overwriting existing Barecat at {path}')
                barecat.util.remove(path)

        self.path = path
        self.readonly = readonly
        self.append_only = append_only
        self.auto_codec = auto_codec
        self.threadsafe = threadsafe
        self.allow_writing_symlinked_shard = allow_writing_symlinked_shard

        # Index
        self._index = None
        if threadsafe:
            import multiprocessing_utils

            self.local = multiprocessing_utils.local()
        else:
            self.local = None

        if not readonly and shard_size_limit is not None:
            self.shard_size_limit = shard_size_limit

        # Shards
        self.sharder = Sharder(
            path,
            shard_size_limit=self.shard_size_limit,
            append_only=append_only,
            readonly=readonly,
            threadsafe=threadsafe,
            allow_writing_symlinked_shard=allow_writing_symlinked_shard,
        )

        self.codecs = {}
        if auto_codec:
            import barecat.codecs as bcc

            self.register_codec(['.jpg', '.jpeg'], bcc.encode_jpeg, bcc.decode_jpeg)
            self.register_codec(['.msgpack'], bcc.encode_msgpack_np, bcc.decode_msgpack_np)
            self.register_codec(['.npy'], bcc.encode_npy, bcc.decode_npy)
            self.register_codec(['.npz'], bcc.encode_npz, bcc.decode_npz)
            self.bz_compressor = bz2.BZ2Compressor(9)
            self.register_codec(
                ['.bz2'], self.bz_compressor.compress, bz2.decompress, nonfinal=True
            )

    ## Dict-like API: keys are filepaths, values are the file contents (bytes or decoded objects)
    def __getitem__(self, path: str) -> Union[bytes, Any]:
        """Get the contents of a file in the Barecat archive.

        Args:
            path: Path to the file within the archive.

        Returns:
            The contents of the file. Either raw bytes, or decoded based on the file extension, if
            ``auto_codec`` was True in the constructor, or
            if codecs have been registered for the file extension via ``register_codec``.

        Raises:
            KeyError: If a file with this path does not exist in the archive.

        Examples:

            >>> bc = Barecat('test.barecat', readonly=False)
            >>> bc['file.txt'] = b'Hello, world!'
            >>> bc['file.txt']
            b'Hello, world!'
        """

        # Typically used in training loop
        path = normalize_path(path)
        row = self.index.fetch_one(
            "SELECT shard, offset, size, crc32c FROM files WHERE path=?", (path,)
        )
        if row is None:
            raise KeyError(path)
        raw_data = self.sharder.read_from_address(
            row['shard'], row['offset'], row['size'], row['crc32c']
        )
        return self.decode(path, raw_data)

    def get(self, path: str, default: Any = None) -> Union[bytes, Any]:
        """Get the contents of a file in the Barecat archive, with a default value if the file does
        not exist.

        Args:
            path: Path to the file within the archive.
            default: Default value to return if the file does not exist.

        Returns:
            The contents of the file (possibly decoded), or the default value if the file does not
            exist.
        """
        try:
            return self[path]
        except KeyError:
            return default

    def items(self) -> Iterator[tuple[str, Union[bytes, Any]]]:
        """Iterate over all files in the archive, yielding (path, content) pairs.

        Returns:
            Iterator over (path, content) pairs.
        """
        for finfo in self.index.iter_all_fileinfos():
            data = self.read(finfo)
            yield finfo.path, self.decode(finfo.path, data)

    def keys(self) -> Iterator[str]:
        """Iterate over all file paths in the archive.

        Returns:
            Iterator over file paths.
        """
        return self.files()

    def values(self) -> Iterator[Union[bytes, Any]]:
        """Iterate over all file contents in the archive.

        Returns:
            Iterator over file contents, possibly decoded based on the file extension.
        """
        for key, value in self.items():
            yield value

    def __contains__(self, path: str) -> bool:
        """Check if a file with the given path exists in the archive.

        Directories are ignored in this check.

        Args:
            path: Path to the file within the archive.

        Returns:
            True if the file exists, False otherwise.
        """
        return self.index.isfile(path)

    def __len__(self) -> int:
        """Get the number of files in the archive.

        Returns:
            Number of files in the archive.
        """
        return self.index.num_files

    def __iter__(self) -> Iterator[str]:
        """Iterate over all file paths in the archive.

        Returns:
            Iterator over file paths.
        """
        return self.files()

    def __setitem__(self, path: str, content: Union[bytes, Any]):
        """Add a file to the Barecat archive.

        Args:
            path: Path to the file within the archive.
            content: Contents of the file. Either raw bytes, or an object to be encoded based on the
                file extension, if ``auto_codec`` was True in the constructor, or if codecs have
                been registered for the file extension via :meth:`register_codec`.

        Raises:
            ValueError: If the archive is read-only.
            FileExistsBarecatError: If a file or directory with the given path already exists in the
                archive.

        Examples:

            >>> bc = Barecat('test.barecat', readonly=False)
            >>> bc['file.txt'] = b'Hello, world!'
            >>> bc['file.txt']
            b'Hello, world!'

        """

        self.add(path, data=self.encode(path, content))

    def setdefault(self, key: str, default: Any = None, /):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def __delitem__(self, path: str):
        """Remove a file from the Barecat archive.

        Args:
            path: Path to the file within the archive.

        Raises:
            KeyError: If a file with this path does not exist in the archive.

        Examples:

            >>> bc = Barecat('test.barecat', readonly=False)
            >>> bc['file.txt'] = b'Hello, world!'
            >>> bc['file.txt']
            b'Hello, world!'
            >>> del bc['file.txt']
            >>> bc['file.txt']
            Traceback (most recent call last):
            ...
            KeyError: 'file.txt'

        """
        try:
            self.remove(path)
        except FileNotFoundBarecatError:
            raise KeyError(path)

    # Filesystem-like API
    # READING
    def open(self, item: Union[BarecatFileInfo, str], mode='r') -> FileSection:
        """Open a file in the archive, as a file-like object.

        Args:
            item: Either a BarecatFileInfo object, or a path to a file within the archive.
            mode: Mode to open the file in, for now only 'r' is supported.

        Returns:
            File-like object representing the file.

        Raises:
            ValueError: If the mode is not 'r'.
            FileNotFoundBarecatError: If a file with this path does not exist in the archive.

        Examples:

            >>> bc = Barecat('test.barecat', readonly=False)
            >>> bc['file.txt'] = b'Hello, world!'
            >>> with bc.open('file.txt') as f:
            ...     f.seek(8)
            ...     print(f.read())
            b'world!'

        """
        finfo = self.index._as_fileinfo(item)
        return self.sharder.open_from_address(finfo.shard, finfo.offset, finfo.size, mode)

    def exists(self, path: str) -> bool:
        """Check if a file or directory exists in the archive.

        Args:
            path: Path to the file or directory within the archive.

        Returns:
            True if and only if a file or directory exists with the given path.
        """
        return self.index.exists(path)

    def isfile(self, path):
        """Check if a file exists in the archive.

        Args:
            path: Path to the file within the archive.

        Returns:
            True if and only if a file exists with the given path.
        """
        return self.index.isfile(path)

    def isdir(self, path):
        """Check if a directory exists in the archive.

        Args:
            path: Path to the directory within the archive.

        Returns:
            True if and only if a directory exists with the given path.
        """
        return self.index.isdir(path)

    def listdir(self, path: str) -> list[str]:
        """List all files and directories in a directory.

        Args:
            path: Path to the directory within the archive.

        Returns:
            List of all files and directories contained in the directory ``path``.
        """
        return self.index.listdir_names(path)

    def walk(self, path: str) -> Iterator[tuple[str, list[str], list[str]]]:
        """Recursively list all files and directories in the tree starting from a directory.

        This is analogous to Python's ``os.walk``.

        Args:
            path: Path to the directory within the archive.

        Returns:
            Iterator over (dirpath, dirnames, filenames) tuples, where ``dirpath`` is the path to
            the directory, ``dirnames`` is a list of all subdirectory names, and ``filenames`` is
            a list of all filenames in the directory.

        Examples:

            >>> bc = Barecat('test.barecat', readonly=False)
            >>> bc['dir/file.txt'] = b'Hello, world!'
            >>> bc['dir/subdir/file2.txt'] = b'Hello, world2!'
            >>> for dirpath, dirnames, filenames in bc.walk('dir'):
            ...     print(dirpath, dirnames, filenames)
            dir ['subdir'] ['file.txt']
            dir/subdir [] ['file2.txt']

        """
        return self.index.walk_names(path)

    def scandir(self, path: str) -> Iterator[BarecatEntryInfo]:
        """Iterate over all files and sub directories, as BarecatFileInfo or BarecatDirInfo
        objects.

        Args:
            path: Path to the directory within the archive.

        Returns:
            An iterator over BarecatFileInfo or BarecatDirInfo objects.
        """
        return self.index.iterdir_infos(path)

    def glob(
        self, pattern: str, recursive: bool = False, include_hidden: bool = False
    ) -> list[str]:
        """Find all files and directories matching a Unix-like glob pattern.

        This function is equivalent to Python's :py:func:`glob.glob`.

        Args:
            pattern: Unix-like glob pattern to match.
            recursive: If True, search recursively, with ``'/**/'`` matching any number of
                directories.
            include_hidden: If True, include hidden files and directories (starting with ``"."``).

        Returns:
            List of all file and directory paths matching the pattern.

        Examples:
            >>> bc = Barecat('test.barecat', readonly=False)
            >>> bc['dir/file.txt'] = b'Hello, world!'
            >>> bc['dir/subdir/file2.txt'] = b'Hello, world2!'
            >>> bc.glob('dir/**/*.txt', recursive=True)
            ['dir/file.txt', 'dir/subdir/file2.txt']
        """
        return self.index.glob_paths(pattern, recursive, include_hidden)

    def globfiles(
        self, pattern: str, recursive: bool = False, include_hidden: bool = False
    ) -> list[str]:
        """Find all files matching a Unix-like glob pattern.

        Like ``glob``, but only returns files, not directories.

        Args:
            pattern: Unix-like glob pattern to match.
            recursive: If True, search recursively, with ``'/**/'`` matching any number of
                directories.
            include_hidden: If True, include hidden files (starting with ``"."``).

        Returns:
            List of all file paths matching the pattern.
        """
        return self.index.glob_paths(pattern, recursive, include_hidden, only_files=True)

    def iglob(
        self, pattern: str, recursive: bool = False, include_hidden: bool = False
    ) -> Iterator[str]:
        """Iterate over all files and directories matching a Unix-like glob pattern.

        This function is equivalent to Python's :py:func:`glob.iglob`.

        Args:
            pattern: Unix-like glob pattern to match.
            recursive: If True, search recursively, with ``'/**/'`` matching any number of
                directories.
            include_hidden: If True, include hidden files and directories (starting with ``'.'``).

        Returns:
            Iterator over all file and directory paths matching the pattern.
        """
        return self.index.iterglob_paths(pattern, recursive, include_hidden)

    def iglobfiles(
        self, pattern: str, recursive: bool = False, include_hidden: bool = False
    ) -> Iterator[str]:
        """Iterate over all files matching a Unix-like glob pattern.

        Like ``iglob``, but only returns files, not directories.

        Args:
            pattern: Unix-like glob pattern to match.
            recursive: If True, search recursively, with ``'/**/'`` matching any number of
                directories.
            include_hidden: If True, include hidden files (starting with ``"."``).

        Returns:
            Iterator over all file paths matching the pattern.
        """
        return self.index.iterglob_paths(pattern, recursive, include_hidden, only_files=True)

    def files(self) -> Iterator[str]:
        """Iterate over all file paths in the archive.

        Returns:
            Iterator over file paths.
        """
        return self.index.iter_all_filepaths()

    def dirs(self) -> Iterator[str]:
        """Iterate over all directory paths in the archive.

        Returns:
            Iterator over directory paths.
        """
        return self.index.iter_all_dirpaths()

    @property
    def num_files(self) -> int:
        """The number of files in the archive."""
        return self.index.num_files

    @property
    def num_dirs(self) -> int:
        """The number of directories in the archive."""
        return self.index.num_dirs

    @property
    def total_size(self) -> int:
        """The total size of all files in the archive, in bytes."""
        return self.index.total_size

    def readinto(self, item: Union[BarecatFileInfo, str], buffer, offset=0) -> int:
        """Read a file into a buffer, starting from an offset within the file.

        Read until either the buffer is full, or the end of the file is reached.

        Args:
            item: Either a BarecatFileInfo object, or a path to a file within the archive.
            buffer: Destination buffer to read the file into.
            offset: Offset within the file to start reading from.

        Returns:
            Number of bytes read.
        """

        # Used in fuse mount
        if isinstance(item, BarecatFileInfo):
            shard, offset_in_shard, size_in_shard, exp_crc32c = (
                item.shard,
                item.offset,
                item.size,
                item.crc32c,
            )
        else:
            path = normalize_path(item)
            row = self.index.fetch_one(
                "SELECT shard, offset, size, crc32c FROM files WHERE path=?", (path,)
            )
            if row is None:
                raise FileNotFoundBarecatError(path)
            shard, offset_in_shard, size_in_shard, exp_crc32c = row

        offset = max(0, min(offset, size_in_shard))
        size_to_read = min(len(buffer), size_in_shard - offset)

        if size_to_read != size_in_shard:
            exp_crc32c = None

        return self.sharder.readinto_from_address(
            shard, offset_in_shard + offset, buffer[:size_to_read], exp_crc32c
        )

    def read(self, item: Union[BarecatFileInfo, str], offset: int = 0, size: int = -1) -> bytes:
        """Read a file from the archive, starting from an offset and reading a specific number of
        bytes.

        Args:
            item: Either a BarecatFileInfo object, or a path to a file within the archive.
            offset: Offset within the file to start reading from.
            size: Number of bytes to read. If -1, read until the end of the file.

        Returns:
            The contents of the file, as bytes.

        Raises:
            ValueError: If the CRC32C checksum of the read data does not match the expected value.
            FileNotFoundBarecatError: If a file with this path does not exist in the archive.
        """
        finfo = self.index._as_fileinfo(item)
        with self.open(finfo, 'rb') as f:
            f.seek(offset)
            data = f.read(size)
        if offset == 0 and (size == -1 or size == finfo.size) and finfo.crc32c is not None:
            crc32c = crc32c_lib.crc32c(data)
            if crc32c != finfo.crc32c:
                raise ValueError(
                    f"CRC32C mismatch for {finfo.path}. Expected {finfo.crc32c}, got {crc32c}"
                )
        return data

    # WRITING
    @raise_if_readonly
    def add_by_path(
        self, filesys_path: str, store_path: Optional[str] = None, dir_exist_ok: bool = False
    ):
        """Add a file or directory from the filesystem to the archive.

        Args:
            filesys_path: Path to the file or directory on the filesystem.
            store_path: Path to store the file or directory in the archive. If None, the same path
                is used as ``filesys_path``.
            dir_exist_ok: If True, do not raise an error when adding a directory and that
                directory already exists in the archive (as a directory).

        Raises:
            ValueError: If the file is larger than the shard size limit.
            FileExistsBarecatError: If a file or directory with the same path already exists in the
                archive, unless ``dir_exist_ok`` is True and the item is a directory.

        Examples:
            >>> bc = Barecat('test.barecat', readonly=False)
            >>> bc.add_by_path('file.txt')
            >>> bc.add_by_path('dir', store_path='dir2')
        """

        if store_path is None:
            store_path = filesys_path

        statresult = os.stat(filesys_path)
        if stat.S_ISDIR(statresult.st_mode):
            finfo = BarecatDirInfo(path=store_path)
            finfo.fill_from_statresult(statresult)
            self.index.add_dir(finfo, exist_ok=dir_exist_ok)
            return

        finfo = BarecatFileInfo(path=store_path)
        finfo.fill_from_statresult(statresult)
        with open(filesys_path, 'rb') as in_file:
            self.add(finfo, fileobj=in_file)

    @raise_if_readonly
    def add(
        self,
        item: Union[BarecatEntryInfo | str],
        *,
        data: Optional[bytes] = None,
        fileobj=None,
        bufsize: int = shutil.COPY_BUFSIZE,
        dir_exist_ok: bool = False,
    ):
        """Add a file or directory to the archive.

        Args:
            item: BarecatFileInfo or BarecatDirInfo object to add or a target path for a file.
            data: File content. If None, the data is read from the file object.
            fileobj: File-like object to read the data from.
            bufsize: Buffer size to use when reading from the file object.
            dir_exist_ok: If True, do not raise an error when adding a directory and that
                directory already exists in the archive (as a directory).

        Raises:
            ValueError: If the file is larger than the shard size limit.
            FileExistsBarecatError: If a file or directory with the same path already exists in the
                archive, unless ``dir_exist_ok`` is True and the item is a directory.

        Examples:
            >>> bc = Barecat('test.barecat', readonly=False)
            >>> bc.add(BarecatFileInfo(path='file.txt', mode=0o666), data=b'Hello, world!')
            >>> bc.add(BarecatDirInfo(path='dir', mode=0o777))
        """
        if isinstance(item, BarecatDirInfo):
            self.index.add_dir(item, exist_ok=dir_exist_ok)
            return

        finfo = BarecatFileInfo(path=item) if isinstance(item, str) else item
        finfo.shard, finfo.offset, finfo.size, finfo.crc32c = self.sharder.add(
            size=finfo.size, data=data, fileobj=fileobj, bufsize=bufsize
        )

        try:
            self.index.add_file(finfo)
        except FileExistsBarecatError:
            # If the file already exists, we need to truncate the shard file back
            shard_file = self.sharder.shard_files[finfo.shard]
            with open(shard_file.name, 'r+b') as f:
                f.truncate(finfo.offset)
            raise

    # DELETION
    @raise_if_readonly_or_append_only
    def remove(self, item: Union[BarecatFileInfo, str]):
        """Remove (delete) a file from the archive.

        Technically, the data is not erased from the shard file at this point, only the
        corresponding row in the index database is removed.
        An exception is when the file is the last file in the shard, in which case the shard file
        is truncated to the end of the file.

        Args:
            item: Either a BarecatFileInfo object, or a path to a file within the archive.

        Raises:
            FileNotFoundBarecatError: If a file with this path does not exist in the archive.
            IsADirectoryBarecatError: If the path refers to a directory, not a file.
        """
        try:
            finfo = self.index._as_fileinfo(item)
        except FileNotFoundBarecatError:
            if self.isdir(item):
                raise IsADirectoryBarecatError(item)
            raise

        # If this is the last file in the shard, we can just truncate the shard file
        end = finfo.offset + finfo.size
        if (
            end >= self.sharder.shard_files[finfo.shard].tell()
            and end >= osp.getsize(self.sharder.shard_files[finfo.shard].name)
            and end == self.index.logical_shard_end(finfo.shard)
        ):
            with open(self.sharder.shard_files[finfo.shard].name, 'r+b') as f:
                f.truncate(finfo.offset)
        self.index.remove_file(finfo)

    @raise_if_readonly_or_append_only
    def rmdir(self, item: Union[BarecatDirInfo, str]):
        """Remove (delete) an empty directory from the archive.

        Args:
            item: Either a BarecatDirInfo object, or a path to a directory within the archive.

        Raises:
            FileNotFoundBarecatError: If a directory with this path does not exist in the archive.
            DirectoryNotEmptyBarecatError: If the directory is not empty.
        """
        self.index.remove_empty_dir(item)

    @raise_if_readonly_or_append_only
    def remove_recursively(self, item: Union[BarecatDirInfo, str]):
        """Remove (delete) a directory and all its contents recursively from the archive.

        Technically, file contents are not erased from the shard file at this point, only the
        corresponding rows in the index database are removed.

        Args:
            item: Either a BarecatDirInfo object, or a path to a directory within the archive.

        Raises:
            FileNotFoundBarecatError: If a directory with this path does not exist in the archive.
        """
        self.index.remove_recursively(item)

    # RENAMING
    @raise_if_readonly_or_append_only
    def rename(self, old_path: str, new_path: str):
        """Rename a file or directory in the archive.

        Args:
            old_path: Path to the file or directory to rename.
            new_path: New path for the file or directory.

        Raises:
            FileNotFoundBarecatError: If a file or directory with the old path does not exist.
            FileExistsBarecatError: If a file or directory with the new path already exists.
        """
        self.index.rename(old_path, new_path)

    @property
    def total_physical_size_seek(self) -> int:
        """Total size of all shard files, as determined by seeking to the end of the shard files.

        This is more up-to-date than :meth:`total_physical_size_stat`, but may be slower.

        Returns:
            Total size of all shard files, in bytes.
        """
        return self.sharder.total_physical_size_seek

    @property
    def total_physical_size_stat(self) -> int:
        """Total size of all shard files, as determined by the file system's `stat` response.

        This is faster than :meth:`total_physical_size_seek`, but may be less up-to-date.

        Returns:
            Total size of all shard files, in bytes.
        """
        return self.sharder.total_physical_size_stat

    @property
    def total_logical_size(self) -> int:
        """Total size of all files in the archive, as determined by the index database.

        Returns:
            Total size of all files in the archive, in bytes.
        """
        return self.index.total_size

    # MERGING
    @raise_if_readonly
    def merge_from_other_barecat(self, source_path: str, ignore_duplicates: bool = False):
        """Merge the contents of another Barecat archive into this one.

        Args:
            source_path: Path to the other Barecat archive.
            ignore_duplicates: If True, do not raise an error when a file with the same path already
                exists in the archive.

        Raises:
            ValueError: If the shard size limit is set and a file in the source archive is larger
                than the shard size limit.
        """
        out_shard_number = len(self.sharder.shard_files) - 1
        out_shard = self.sharder.shard_files[-1]
        out_shard_offset = out_shard.tell()

        source_index_path = f'{source_path}-sqlite-index'
        self.index.cursor.execute(f"ATTACH DATABASE 'file:{source_index_path}?mode=ro' AS sourcedb")

        if self.shard_size_limit is not None:
            in_max_size = self.index.fetch_one("SELECT MAX(size) FROM sourcedb.files")[0]
            if in_max_size > self.shard_size_limit:
                self.index.cursor.execute("DETACH DATABASE sourcedb")
                raise ValueError('Files in the source archive are larger than the shard size')

        with self.no_triggers():
            # Upsert all directories
            self.index.cursor.execute(
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

            in_shard_number = 0
            in_shard_path = f'{source_path}-shard-{in_shard_number:05d}'
            in_shard = open(in_shard_path, 'rb')
            in_shard_offset = 0
            in_shard_end = self.index.fetch_one(
                """
                SELECT MAX(offset + size) FROM sourcedb.files WHERE shard=?
                """,
                (in_shard_number,),
            )[0]

            while True:
                if self.shard_size_limit is not None:
                    out_shard_space_left = self.shard_size_limit - out_shard_offset
                    # check how much of the in_shard we can put in the current out_shard
                    fetched = self.index.fetch_one(
                        """
                        SELECT MAX(offset + size) - :in_shard_offset AS max_offset_size_adjusted
                        FROM sourcedb.files
                        WHERE offset + size <= :in_shard_offset + :out_shard_space_left
                        AND shard = :in_shard_number""",
                        dict(
                            in_shard_offset=in_shard_offset,
                            out_shard_space_left=out_shard_space_left,
                            in_shard_number=in_shard_number,
                        ),
                    )
                    if fetched is None:
                        # No file of the current in_shard fits in the current out_shard, must start a
                        # new one
                        self.sharder.start_new_shard()
                        out_shard_number += 1
                        out_shard_offset = 0
                        continue

                    max_copiable_amount = fetched[0]
                else:
                    max_copiable_amount = None

                # now we need to update the index, but we need to update the offset and shard
                # of the files that we copied
                maybe_ignore = 'OR IGNORE' if ignore_duplicates else ''
                self.index.cursor.execute(
                    f"""
                    INSERT {maybe_ignore} INTO files (
                        path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns)
                    SELECT path, :out_shard_number, offset - :in_shard_offset + :out_shard_offset,
                        size, crc32c, mode, uid, gid, mtime_ns 
                    FROM sourcedb.files
                    WHERE offset >= :in_shard_offset AND shard = :in_shard_number"""
                    + (
                        """
                    AND offset + size <= :in_shard_offset + :max_copiable_amount
                    """
                        if max_copiable_amount is not None
                        else ""
                    ),
                    dict(
                        out_shard_number=out_shard_number,
                        in_shard_offset=in_shard_offset,
                        out_shard_offset=out_shard_offset,
                        in_shard_number=in_shard_number,
                        max_copiable_amount=max_copiable_amount,
                    ),
                )
                copyfileobj(in_shard, out_shard, max_copiable_amount)
                out_shard_offset = out_shard.tell()
                in_shard_offset = in_shard.tell()
                if in_shard_offset == in_shard_end:
                    # we finished this in_shard, move to the next one
                    in_shard.close()
                    in_shard_number += 1
                    in_shard_path = f'{source_path}-shard-{in_shard_number:05d}'
                    try:
                        in_shard = open(in_shard_path, 'rb')
                    except FileNotFoundError:
                        # done with all in_shards of this source
                        break
                    in_shard_offset = 0
                    in_shard_end = self.index.fetch_one(
                        """
                        SELECT MAX(offset + size) FROM sourcedb.files WHERE shard=?
                        """,
                        (in_shard_number,),
                    )[0]

            in_shard.close()
            self.index.conn.commit()
            self.index.cursor.execute("DETACH DATABASE sourcedb")

            if ignore_duplicates:
                self.update_treestats()
                self.conn.commit()

    @property
    def shard_size_limit(self) -> int:
        """Maximum size of each shard file."""
        return self.index.shard_size_limit

    @shard_size_limit.setter
    def shard_size_limit(self, value: int):
        """Set the maximum size of each shard file."""
        self.index.shard_size_limit = value

    def logical_shard_end(self, shard_number: int) -> int:
        """Logical end of a shard, in bytes, that is the position after the last byte of the last
        file contained in the shard.

        Args:
            shard_number: Shard number, index starting from 0.

        Returns:
            Logical end of the shard, in bytes.
        """
        return self.index.logical_shard_end(shard_number)

    def physical_shard_end(self, shard_number):
        """Physical end of a shard, in bytes, that is the end seek position of the shard file.

        Args:
            shard_number: Shard number, index starting from 0.

        Returns:
            Physical end of the shard, in bytes.
        """

        return self.sharder.physical_shard_end(shard_number)

    def raise_if_readonly(self, message):
        if self.readonly:
            raise ValueError(message)

    def raise_if_append_only(self, message):
        if self.append_only:
            raise ValueError(message)

    # THREADSAFE
    @property
    def index(self) -> Index:
        """Index object to manipulate the metadata database of the Barecat archive."""
        if not self.local:
            if self._index is None:
                self._index = Index(f'{self.path}-sqlite-index', readonly=self.readonly)
            return self._index
        try:
            return self.local.index
        except AttributeError:
            self.local.index = Index(f'{self.path}-sqlite-index', readonly=self.readonly)
            return self.local.index

    # CONSISTENCY CHECKS
    def check_crc32c(self, item: Union[BarecatFileInfo, str]):
        """Check the CRC32C checksum of a file in the archive.

        Args:
            item: Either a BarecatFileInfo object, or a path to a file within the archive.

        Returns:
            True if the CRC32C checksum of the file matches the expected value or no checksum is
            stored in the database.

        Raises:
            LookupError: If a file with this path does not exist in the archive.
        """

        finfo = self.index._as_fileinfo(item)
        with self.open(finfo, 'rb') as f:
            crc32c = barecat.util.fileobj_crc32c_until_end(f)
        if finfo.crc32c is not None and crc32c != finfo.crc32c:
            print(f"CRC32C mismatch for {finfo.path}. Expected {finfo.crc32c}, got {crc32c}")
            return False
        return True

    def verify_integrity(self, quick=False):
        """Verify the integrity of the Barecat archive.

        This includes checking the CRC32C checksums of all files, and checking the integrity of the
        index database.

        Args:
            quick: If True, only check the CRC32C checksums of the last file of the archive.

        Returns:
            True if no problems were found, False otherwise.
        """

        is_good = True
        if quick:
            try:
                if not self.check_crc32c(self.index.get_last_file()):
                    is_good = False
            except LookupError:
                pass  # no files
        else:
            n_printed = 0
            for fi in barecat.progbar.progressbar(
                self.index.iter_all_fileinfos(), total=self.num_files
            ):
                if not self.check_crc32c(fi):
                    is_good = False
                    if n_printed >= 10:
                        print('...')
                        break
                    n_printed += 1

        if not self.index.verify_integrity():
            is_good = False
        return is_good

    # CODECS
    def register_codec(
        self,
        exts: list[str],
        encoder: Callable[[Any], bytes],
        decoder: Callable[[bytes], Any],
        nonfinal: bool = False,
    ):
        """Register an encoder and decoder for one or more file extensions.

        This allows automatic encoding and decoding (serialization/deserialization) of files based
        on their extension, used in the dictionary interface, e.g., :meth:`__getitem__`,
        :meth:`__setitem__` and :meth:`items` methods.

        If ``auto_codec`` was True in the constructor, then the codecs are already
        registered by default for the following extensions:

        - ``'.msgpack'``
        - ``'.jpg'``, ``'.jpeg'``
        - ``'.pkl'``
        - ``'.npy'``
        - ``'.npz'``


        Args:
            exts: List of file extensions to register the codec for.
            encoder: Function to encode data into bytes.
            decoder: Function to decode bytes into data.
            nonfinal: If True, other codecs are allowed to be applied afterwards in a nested
                manner. This is useful for, e.g., compression codecs.

        Examples:
            Simple text encoding:

            >>> bc = Barecat('test.barecat', readonly=False)
            >>> def encode(data):
            ...     return data.encode('utf-8')
            >>> def decode(data):
            ...     return data.decode('utf-8')
            >>> bc.register_codec(['.txt'], encode, decode)

            Or using a codec from a library:

            >>> import cv2
            >>> bc = Barecat('test.barecat', readonly=False)
            >>> def encode_png(data):
            ...     return cv2.imencode('.png', data)[1].tobytes()
            >>> def decode_png(data):
            ...     return cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_UNCHANGED)
            >>> bc.register_codec(['.png'], encode_png, decode_png)

            Or using a compression library:

            >>> import zlib
            >>> bc = Barecat('test.barecat', readonly=False)
            >>> def encode_zlib(data):
            ...     return zlib.compress(data)
            >>> def decode_zlib(data):
            ...     return zlib.decompress(data)
            >>> bc.register_codec(['.gz'], encode_zlib, decode_zlib, nonfinal=True)

            Or pickling:

            >>> import pickle
            >>> bc = Barecat('test.barecat', readonly=False)
            >>> bc.register_codec(['.pkl'], pickle.dumps, pickle.loads)
        """

        for ext in exts:
            self.codecs[ext] = (encoder, decoder, nonfinal)

    def encode(self, path, data):
        noext, ext = osp.splitext(path)
        try:
            encoder, decoder, nonfinal = self.codecs[ext.lower()]
        except KeyError:
            return data
        else:
            if nonfinal:
                data = self.encode(noext, data)
            return encoder(data)

    def decode(self, path, data):
        noext, ext = osp.splitext(path)
        try:
            encoder, decoder, nonfinal = self.codecs[ext.lower()]
            data = decoder(data)
            if nonfinal:
                data = self.decode(noext, data)
            return data
        except KeyError:
            return data

    # PICKLING
    def __reduce__(self):
        if not self.readonly:
            raise ValueError('Cannot pickle a non-readonly Barecat')
        return self.__class__, (
            self.path,
            None,
            True,
            False,
            self.auto_codec,
            True,
            False,
            self.threadsafe,
        )

    def truncate_all_to_logical_size(self):
        logical_shard_ends = [
            self.index.logical_shard_end(i) for i in range(len(self.sharder.shard_files))
        ]
        self.sharder.truncate_all_to_logical_size(logical_shard_ends)

    # DEFRAG
    def defrag(self, quick=False):
        """Defragment the Barecat archive.

        Args:
            quick: Perform a faster, but less thorough defragmentation.
        """
        defragger = BarecatDefragger(self)
        if quick:
            return defragger.defrag_quick()
        else:
            return defragger.defrag()

    def close(self):
        """Close the Barecat archive."""

        self.index.close()
        self.sharder.close()

    def __enter__(self):
        """Enter a context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit a context manager."""
        self.close()
