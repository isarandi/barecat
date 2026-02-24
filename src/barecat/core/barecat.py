import io
import logging
import os
import os.path as osp
import shutil
import stat
import warnings
from collections.abc import Callable, Iterator, MutableMapping
from contextlib import AbstractContextManager
from typing import Any, Optional, TYPE_CHECKING, Union

from ..io import codecs as barecat_codecs
from ..util import progbar as barecat_progbar
from ..util import misc as barecat_util
import crc32c as crc32c_lib
from ..core.sharder import Sharder
from ..io.fileobj import BarecatFileObjectHelper
from ..maintenance.merge import BarecatMergeHelper
from ..maintenance.defrag import BarecatDefragger
from ..exceptions import (
    FileExistsBarecatError,
    FileNotFoundBarecatError,
    IsADirectoryBarecatError,
)
from ..util.misc import raise_if_readonly, raise_if_readonly_or_append_only
from ..util.threading import ThreadLocalStorage
from ..io.copyfile import accumulate_crc32c

if TYPE_CHECKING:
    from barecat import (
        BarecatDirInfo,
        BarecatFileInfo,
        BarecatEntryInfo,
        Index,
        BarecatFileObject,
    )
    from .paths import resolve_index_path
else:
    from .types import (
        BarecatDirInfo,
        BarecatFileInfo,
        BarecatEntryInfo,
    )
    from ..io.fileobj import (
        BarecatFileObject,
    )
    from .index import Index, normalize_path
    from .paths import resolve_index_path

logger = logging.getLogger(__name__)


class Barecat(MutableMapping[str, Any], AbstractContextManager):
    """Object for reading or writing a Barecat archive.

    A Barecat archive consists of several (large) shard files, each containing the data of multiple
    small files, and an SQLite index database that maps file paths to the corresponding shard,
    offset and size within the shard, as well as metadata such as modification time and checksum.

    The ``Barecat`` object provides two main interfaces:

    1. A dict-like interface, where keys are file paths and values are the file contents as bytes.
       For automatic encoding/decoding based on file extension, wrap with :class:`DecodedView`.
    2. A filesystem-like interface consisting of methods such as :meth:`open`, :meth:`exists`, \
        :meth:`listdir`, :meth:`walk`, :meth:`glob`, etc., modeled after Python's ``os`` module.

    Args:
        path: Path to the Barecat archive (e.g., 'archive.barecat'). Shards are stored
            alongside as 'archive.barecat-shard-XXXXX'.
        shard_size_limit: Maximum size of each shard file in bytes (int) or as a string like '1G'.
            If None, the shard size is unlimited.
        readonly: If True, the Barecat archive is opened in read-only mode.
        overwrite: If True, the Barecat archive is first deleted if it already exists.
        auto_codec: **Deprecated.** Use :class:`DecodedView` instead. Will be removed in 1.0.
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
        path: Union[str, os.PathLike],
        shard_size_limit: Union[int, str, None] = None,
        readonly: bool = True,
        overwrite: bool = False,
        auto_codec: bool = False,
        exist_ok: bool = True,
        append_only: bool = False,
        threadsafe: bool = False,
        allow_writing_symlinked_shard: bool = False,
        wal: bool = False,
        readonly_is_immutable: bool = False,
    ):
        path = os.fspath(path)
        if threadsafe and not readonly:
            raise ValueError('Threadsafe mode is only supported for readonly Barecat.')

        if not readonly and barecat_util.exists(path):
            if not exist_ok:
                raise FileExistsError(path)
            if overwrite:
                logger.info('Overwriting existing Barecat at %s', path)
                barecat_util.remove(path)

        if readonly and not barecat_util.exists(path):
            raise FileNotFoundError(path)

        self.path = path
        self.readonly = readonly
        self.append_only = append_only
        self.auto_codec = auto_codec
        self.threadsafe = threadsafe
        self.allow_writing_symlinked_shard = allow_writing_symlinked_shard
        self.readonly_is_immutable = readonly_is_immutable
        self.wal = wal

        # Index
        self._index_storage = ThreadLocalStorage(threadsafe)

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

        self.codec_registry = barecat_codecs.CodecRegistry(auto_codec=auto_codec)
        self._merge_helper = BarecatMergeHelper(self)
        self._fileobj_helper = BarecatFileObjectHelper(self)

        if auto_codec:
            warnings.warn(
                'auto_codec is deprecated and will be removed in version 1.0. '
                "Use DecodedView instead: dec = DecodedView(bc); dec['file.json'] = data",
                DeprecationWarning,
                stacklevel=2,
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
            'SELECT shard, offset, size, crc32c FROM files WHERE path=?', (path,)
        )
        if row is None:
            raise KeyError(path)
        raw_data = self.sharder.read_from_address(
            row['shard'], row['offset'], row['size'], row['crc32c']
        )
        return self.codec_registry.decode(path, raw_data)

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
            yield finfo.path, self.codec_registry.decode(finfo.path, data)

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

        self.add(path, data=self.codec_registry.encode(path, content))

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
        except (FileNotFoundBarecatError, IsADirectoryBarecatError):
            raise KeyError(path)

    # Filesystem-like API
    def open(
        self,
        item: Union[BarecatFileInfo, str],
        mode: str = 'r',
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> Union[BarecatFileObject, io.TextIOWrapper]:
        """Open a file in the archive, as a file-like object.

        Args:
            item: Either a BarecatFileInfo object, or a path to a file within the archive.
            mode: Mode to open the file in.
                'r'/'rt'/'rb' - read only, file must exist
                'r+'/'r+b' - read/write, file must exist
                'w'/'wt'/'wb' - write, truncate if exists, create if not
                'w+'/'w+b' - read/write, truncate if exists, create if not
                'x'/'xb' - exclusive create, fail if exists
                'x+'/'x+b' - exclusive create read/write, fail if exists
                'a'/'ab' - append, create if not exists
                'a+'/'a+b' - append read/write, create if not exists
            encoding: Text encoding (only for text mode, default 'utf-8').
            errors: Error handling for encoding (only for text mode).
            newline: Newline handling (only for text mode).

        Returns:
            File-like object representing the file. Returns TextIOWrapper for text
            mode, BarecatFileObject for binary mode.

        Raises:
            FileNotFoundBarecatError: If file doesn't exist and mode requires it.
            FileExistsBarecatError: If file exists and mode is exclusive create.
        """
        return self._fileobj_helper.open(item, mode, encoding, errors, newline)

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

        This is analogous to Python's :py:func:`os.walk`.

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
        """Iterate over all immediate files and subdirectories of the given directory, as :class:`barecat.BarecatEntryInfo` objects.

        Args:
            path: Path to the directory within the archive.

        Returns:
            An iterator over members of the directory, as :class:`barecat.BarecatEntryInfo` objects.
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
                'SELECT shard, offset, size, crc32c FROM files WHERE path=?', (path,)
            )
            if row is None:
                raise FileNotFoundBarecatError(path)
            shard, offset_in_shard, size_in_shard, exp_crc32c = row

        offset = max(0, min(offset, size_in_shard))
        size_to_read = min(len(buffer), size_in_shard - offset)

        if size_to_read != size_in_shard:
            exp_crc32c = None

        return self.sharder.readinto_from_address(
            shard, offset_in_shard + offset, memoryview(buffer)[:size_to_read], exp_crc32c
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
                    f'CRC32C mismatch for {finfo.path}. Expected {finfo.crc32c}, got {crc32c}'
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
        item: Union[BarecatEntryInfo, str],
        *,
        data: Optional[bytes] = None,
        fileobj=None,
        bufsize: int = shutil.COPY_BUFSIZE,
        dir_exist_ok: bool = False,
        file_exist_ok: bool = False,
    ):
        """Add a file or directory to the archive.

        Parent directories are automatically created if they don't exist (like ``mkdir -p``).

        Args:
            item: BarecatFileInfo or BarecatDirInfo object to add or a target path for a file.
            data: File content. If None, the data is read from the file object.
            fileobj: File-like object to read the data from.
            bufsize: Buffer size to use when reading from the file object.
            dir_exist_ok: If True, do not raise an error when adding a directory and that
                directory already exists in the archive (as a directory).
            file_exist_ok: If True, skip adding a file if a file with the same path already
                exists in the archive. This is useful for merge operations.

        Raises:
            ValueError: If the file is larger than the shard size limit.
            FileExistsBarecatError: If a file or directory with the same path already exists in the
                archive, unless ``dir_exist_ok`` is True and the item is a directory, or
                ``file_exist_ok`` is True and the item is a file.
            NotADirectoryBarecatError: If a parent path exists as a file.

        Examples:
            >>> bc = Barecat('test.barecat', readonly=False)
            >>> bc.add(BarecatFileInfo(path='file.txt', mode=0o666), data=b'Hello, world!')
            >>> bc.add(BarecatDirInfo(path='dir', mode=0o777))
        """
        if isinstance(item, BarecatDirInfo):
            self.index.add_dir(item, exist_ok=dir_exist_ok)
            return

        finfo = BarecatFileInfo(path=item) if isinstance(item, str) else item

        # Check if file exists before writing data (to avoid wasted writes)
        if file_exist_ok and finfo.path in self:
            return

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

    @raise_if_readonly_or_append_only
    def update_file(
        self,
        old_item: Union[BarecatFileInfo, str],
        new_item: Union[BarecatFileInfo, str, None] = None,
        *,
        data: Optional[bytes] = None,
        fileobj=None,
        size: Optional[int] = None,
        bufsize: int = shutil.COPY_BUFSIZE,
    ):
        """Update an existing file's data and/or metadata, reusing space if new data fits.

        If new data is smaller or equal to old data, writes in-place at the
        existing location, avoiding fragmentation. If new data is larger,
        writes at a new location (old location becomes a gap).

        Args:
            old_item: The existing file to update. Either a BarecatFileInfo (if user
                      already looked it up) or a path string.
            new_item: Optional new metadata. If BarecatFileInfo, its metadata fields
                      (path, mtime_ns, mode, uid, gid) are the new values. If string,
                      it's the new path (rename). If None, only data is updated.
            data: New file content. Either data or fileobj must be provided.
            fileobj: File-like object to read new data from.
            size: Size of new data. Required for fileobj if writing in-place.
            bufsize: Buffer size for reading from fileobj.

        Raises:
            FileNotFoundBarecatError: If file does not exist.
            ValueError: If neither data nor fileobj provided, or both provided.
        """

        old_info = self.index._as_fileinfo(old_item)

        # Extract new metadata from new_item
        if isinstance(new_item, BarecatFileInfo):
            new_mtime_ns = new_item.mtime_ns
            new_mode = new_item.mode
            new_uid = new_item.uid
            new_gid = new_item.gid
        else:
            new_mtime_ns = None
            new_mode = None
            new_uid = None
            new_gid = None

        if data is not None:
            size = len(data)

        if size is not None and size <= old_info.size:
            # Fits in existing space - write in-place
            shard, offset, size, crc32c = self.sharder.add(
                shard=old_info.shard,
                offset=old_info.offset,
                size=size,
                data=data,
                fileobj=fileobj,
                bufsize=bufsize,
                raise_if_cannot_fit=True,
            )
            self.index.update_file(
                old_info.path,
                new_size=size,
                new_crc32c=crc32c,
                new_mtime_ns=new_mtime_ns,
                new_mode=new_mode,
                new_uid=new_uid,
                new_gid=new_gid,
            )
        else:
            # Doesn't fit or unknown size - allocate new space, then update index
            shard, offset, size, crc32c = self.sharder.add(
                data=data, fileobj=fileobj, size=size, bufsize=bufsize
            )
            self.index.update_file(
                old_info.path,
                new_shard=shard,
                new_offset=offset,
                new_size=size,
                new_crc32c=crc32c,
                new_mtime_ns=new_mtime_ns,
                new_mode=new_mode,
                new_uid=new_uid,
                new_gid=new_gid,
            )

    # DIRECTORY CREATION
    @raise_if_readonly
    def mkdir(self, path: str, mode: int = 0o755, exist_ok: bool = False):
        """Create a directory in the archive.

        Parent directories are automatically created if they don't exist (like ``mkdir -p``).

        Args:
            path: Path of the directory to create.
            mode: Permission mode for the directory (default: 0o755).
            exist_ok: If True, do not raise an error if the directory already exists.

        Raises:
            FileExistsBarecatError: If a file or directory with this path already exists,
                unless exist_ok is True and it's a directory.
            NotADirectoryBarecatError: If the path or a parent exists as a file.
        """
        self.add(BarecatDirInfo(path=path, mode=mode), dir_exist_ok=exist_ok)

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
    def rmtree(self, item: Union[BarecatDirInfo, str]):
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
    def merge_from_other_barecat(
        self,
        source_path: str,
        ignore_duplicates: bool = False,
        prefix: str = '',
        pattern: str = None,
        filter_rules: list = None,
    ):
        """Merge the contents of another Barecat archive into this one.

        Args:
            source_path: Path to the other Barecat archive.
            ignore_duplicates: If True, do not raise an error when a file with the same path already
                exists in the archive.
            prefix: Path prefix to prepend to all paths (default: '', no prefix).
            pattern: Glob pattern to filter files (uses optimized iterglob_infos).
            filter_rules: Rsync-style include/exclude rules as list of ('+'/'-', pattern) tuples.

        Raises:
            ValueError: If the shard size limit is set and a file in the source archive is larger
                than the shard size limit.
        """
        self._merge_helper.merge_from_other_barecat(
            source_path, ignore_duplicates, prefix, pattern, filter_rules
        )

    @property
    def shard_size_limit(self) -> int:
        """Maximum size of each shard file."""
        return self.index.shard_size_limit

    @shard_size_limit.setter
    def shard_size_limit(self, value: Union[int, str]):
        """Set the maximum size of each shard file.

        Args:
            value: Size in bytes (int) or as a string like '1G', '500M', '100K'.
        """
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
    def _make_index(self):
        return Index(
            resolve_index_path(self.path),
            readonly=self.readonly,
            wal=self.wal,
            readonly_is_immutable=self.readonly_is_immutable,
        )

    @property
    def index(self) -> Index:
        """Index object to manipulate the metadata database of the Barecat archive."""
        return self._index_storage.get(self._make_index)

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
            crc32c = accumulate_crc32c(f)
        if finfo.crc32c is not None and crc32c != finfo.crc32c:
            logger.warning('CRC32C mismatch for %s. Expected %s, got %s', finfo.path, finfo.crc32c, crc32c)
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
            for fi in barecat_progbar.progressbar(
                self.index.iter_all_fileinfos(), total=self.num_files
            ):
                if not self.check_crc32c(fi):
                    is_good = False
                    if n_printed >= 10:
                        logger.warning('... (further mismatches suppressed)')
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

        **Data formats:**
        - ``.json`` — dict/list (stdlib json)
        - ``.pkl``, ``.pickle`` — any object (pickle)
        - ``.npy`` — numpy array
        - ``.npz`` — dict of numpy arrays
        - ``.msgpack`` — any object (requires msgpack-numpy)

        **Image formats** (uses cv2 > PIL > imageio, whichever is available):
        - ``.jpg``, ``.jpeg``, ``.png``, ``.bmp``, ``.gif``
        - ``.tiff``, ``.tif``, ``.webp``, ``.exr``

        **Compression** (stackable with other codecs):
        - ``.gz``, ``.gzip`` — gzip
        - ``.xz``, ``.lzma`` — lzma
        - ``.bz2`` — bzip2


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
        warnings.warn(
            'register_codec is deprecated and will be removed in version 1.0. '
            'Use DecodedView instead: dec = DecodedView(bc); dec.register_codec(...)',
            DeprecationWarning,
            stacklevel=2,
        )
        self.codec_registry.register_codec(exts, encoder, decoder, nonfinal)

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
            self.index.logical_shard_end(i) for i in range(self.sharder.num_shards)
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
        self._index_storage.close()
        self.sharder.close()

    def __repr__(self):
        mode = 'readonly' if self.readonly else 'read-write'
        try:
            n = self.num_files
            return f"Barecat('{self.path}', {mode}, {n} files)"
        except Exception:
            return f"Barecat('{self.path}', {mode})"

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit a context manager."""
        self.close()
