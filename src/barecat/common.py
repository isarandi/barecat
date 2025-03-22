import io
import os
from datetime import datetime
from enum import Flag, auto
from typing import Union, TYPE_CHECKING, Optional
from barecat.util import datetime_to_ns, normalize_path, ns_to_datetime

if TYPE_CHECKING:
    from barecat import BarecatEntryInfo

SHARD_SIZE_UNLIMITED = (1 << 63) - 1  #: An extremely large integer, representing unlimited size


class BarecatEntryInfo:
    """
    Base class for file and directory information classes.

    The two subclasses are :class:`barecat.BarecatFileInfo` and :class:`barecat.BarecatDirInfo`.

    Args:
        path: path to the file or directory
        mode: file mode, i.e. permissions
        uid: user ID
        gid: group ID
        mtime_ns: last modification time in nanoseconds since the Unix epoch
    """

    __slots__ = ('_path', 'mode', 'uid', 'gid', 'mtime_ns')

    def __init__(
        self,
        path: Optional[str] = None,
        mode: Optional[int] = None,
        uid: Optional[int] = None,
        gid: Optional[int] = None,
        mtime_ns: Optional[Union[int, datetime]] = None,
    ):
        self._path = normalize_path(path)
        self.mode = mode
        """File mode, i.e., permissions."""

        self.uid = uid
        """User ID."""

        self.gid = gid
        """Group ID."""

        self.mtime_ns = mtime_ns
        """Last modification time in nanoseconds since the Unix epoch."""

        if isinstance(self.mtime_ns, datetime):
            self.mtime_ns = datetime_to_ns(self.mtime_ns)

    @property
    def path(self):
        """Path to the file or directory. The path is normalized on assignment."""
        return self._path

    @path.setter
    def path(self, value):
        self._path = normalize_path(value)

    @property
    def mtime_dt(self) -> Optional[datetime]:
        """Last modification time as a datetime object."""
        return ns_to_datetime(self.mtime_ns) if self.mtime_ns else None

    @mtime_dt.setter
    def mtime_dt(self, dt: datetime):
        self.mtime_ns = datetime_to_ns(dt)

    def update_mtime(self):
        """Update the last modification time to the current time."""
        self.mtime_dt = datetime.now()

    def fill_from_statresult(self, s: os.stat_result):
        """Fills the metadata information from a stat result, obtained from the file system.

        Args:
            s: stat result object to fill the metadata from
        """
        self.mode = s.st_mode
        self.uid = s.st_uid
        self.gid = s.st_gid
        self.mtime_ns = s.st_mtime_ns

    @classmethod
    def row_factory(cls, cursor, row):
        """Factory method for creating instances from SQLite query results.

        Args:
            cursor: SQLite cursor object
            row: row from the query result
        """

        # Raw construction without any of that property business or validation, just for speed
        instance = cls.__new__(cls)
        for field, value in zip(cursor.description, row):
            fieldname = field[0]
            if fieldname == 'path':
                instance._path = value
            else:
                object.__setattr__(instance, fieldname, value)
        return instance


class BarecatFileInfo(BarecatEntryInfo):
    """
    Describes file information such as path, location in the shards and metadata.

    This class is used both when retrieving existing file information and when adding new files.

    Args:
        path: path to the file inside the archive
        mode: file mode, i.e., permissions
        uid: user ID
        gid: group ID
        mtime_ns: last modification time in nanoseconds since the Unix epoch
        shard: shard number
        offset: offset within the shard in bytes
        size: size of the file in bytes
        crc32c: CRC32C checksum of the file contents
    """

    __slots__ = ('shard', 'offset', 'size', 'crc32c')

    def __init__(
        self,
        path: Optional[str] = None,
        mode: Optional[int] = None,
        uid: Optional[int] = None,
        gid: Optional[int] = None,
        mtime_ns: Optional[Union[int, datetime]] = None,
        shard: Optional[int] = None,
        offset: Optional[int] = None,
        size: Optional[int] = None,
        crc32c: Optional[int] = None,
    ):
        super().__init__(path, mode, uid, gid, mtime_ns)
        self.shard = shard
        """Shard number where the file is located."""

        self.offset = offset
        """Offset within the shard in bytes."""

        self.size = size
        """Size of the file in bytes."""

        self.crc32c = crc32c
        """CRC32C checksum of the file contents."""

    def asdict(self) -> dict:
        """Returns a dictionary representation of the file information.

        Returns:
            Dictionary with keys 'path', 'shard', 'offset', 'size', 'crc32c', 'mode', 'uid',
                'gid', 'mtime_ns'
        """
        return dict(
            path=self.path,
            shard=self.shard,
            offset=self.offset,
            size=self.size,
            crc32c=self.crc32c,
            mode=self.mode,
            uid=self.uid,
            gid=self.gid,
            mtime_ns=self.mtime_ns,
        )

    def fill_from_statresult(self, s: os.stat_result):
        """Fills the file metadata information from a stat result, obtained from the file system.

        Args:
            s: stat result object to fill the metadata from
        """
        super().fill_from_statresult(s)
        self.size = s.st_size

    @property
    def end(self) -> int:
        """End position of the file in the shard."""
        return self.offset + self.size


class BarecatDirInfo(BarecatEntryInfo):
    """
    Describes directory information such as path, metadata and statistics.

    This class is used both when retrieving existing directory information and when adding new
    directories.

    Args:
        path: path to the directory inside the archive
        mode: directory mode, i.e., permissions
        uid: user ID
        gid: group ID
        mtime_ns: last modification time in nanoseconds since the Unix epoch
        num_subdirs: number of subdirectories in the directory
        num_files: number of files in the directory
        size_tree: total size of the directory contents in bytes
        num_files_tree: total number of files in the directory and its subdirectories
    """

    __slots__ = ('num_subdirs', 'num_files', 'size_tree', 'num_files_tree')

    def __init__(
        self,
        path: Optional[str] = None,
        mode: Optional[int] = None,
        uid: Optional[int] = None,
        gid: Optional[int] = None,
        mtime_ns: Optional[Union[int, datetime]] = None,
        num_subdirs: Optional[bool] = None,
        num_files: Optional[int] = None,
        size_tree: Optional[int] = None,
        num_files_tree: Optional[int] = None,
    ):
        super().__init__(path, mode, uid, gid, mtime_ns)
        self.num_subdirs = num_subdirs
        """Number of immediate subdirectories in the directory."""

        self.num_files = num_files
        """Number of immediate files in the directory."""

        self.size_tree = size_tree
        """Total size of the directory's contents (recursively) in bytes."""

        self.num_files_tree = num_files_tree
        """Total number of files in the directory and its subdirectories, recursively."""

    def asdict(self) -> dict:
        """Returns a dictionary representation of the directory information.

        Returns:
            Dictionary with keys 'path', 'num_subdirs', 'num_files', 'size_tree', 'num_files_tree',
                'mode', 'uid', 'gid', 'mtime_ns'
        """
        return dict(
            path=self.path,
            num_subdirs=self.num_subdirs,
            num_files=self.num_files,
            size_tree=self.size_tree,
            num_files_tree=self.num_files_tree,
            mode=self.mode,
            uid=self.uid,
            gid=self.gid,
            mtime_ns=self.mtime_ns,
        )

    @property
    def num_entries(self) -> int:
        """Total number of entries in the directory, including subdirectories and files."""
        return self.num_subdirs + self.num_files

    def fill_from_statresult(self, s: os.stat_result):
        """Fills the directory metadata information from a stat result, from the file system.

        Args:
            s: stat result object to fill the metadata from
        """
        super().fill_from_statresult(s)
        self.num_subdirs = s.st_nlink - 2


class Order(Flag):
    """Ordering specification for file and directory listings.

    The ordering can be by address (shard and offset), path, or random. The order can be ascending
    or descending. The default order is ANY, which is the order in which SQLite yields rows.
    """

    ANY = auto()
    """Default order, as returned by SQLite"""

    RANDOM = auto()
    """Randomized order"""

    ADDRESS = auto()
    """Order by shard and offset position"""

    PATH = auto()
    """Alphabetical order by path"""

    DESC = auto()
    """Descending order"""

    def as_query_text(self) -> str:
        """Returns the SQL ORDER BY clause corresponding to the ordering specification."""

        if self & Order.ADDRESS and self & Order.DESC:
            return ' ORDER BY shard DESC, offset DESC'
        elif self & Order.ADDRESS:
            return ' ORDER BY shard, offset'
        elif self & Order.PATH and self & Order.DESC:
            return ' ORDER BY path DESC'
        elif self & Order.PATH:
            return ' ORDER BY path'
        elif self & Order.RANDOM:
            return ' ORDER BY RANDOM()'
        return ''


class FileSection(io.IOBase):
    """File-like object representing a section of a file.

    Args:
        file: file-like object to read from or write to
        start: start position of the section in the file
        size: size of the section
        readonly: whether the section should be read-only
    """

    def __init__(self, file: io.RawIOBase, start: int, size: int, readonly: bool = True):
        self.file = file
        self.start = start
        self.end = start + size
        self.position = start
        self.readonly = readonly

    def read(self, size: int = -1) -> bytes:
        """Read a from the section, starting from the current position.

        Args:
            size: number of bytes to read, or -1 to read until the end of the section

        Returns:
            Bytes read from the section.
        """
        if size == -1:
            size = self.end - self.position

        size = min(size, self.end - self.position)
        self.file.seek(self.position)
        data = self.file.read(size)
        self.position += len(data)
        return data

    def readinto(self, buffer: Union[bytearray, memoryview]) -> int:
        """Read bytes into a buffer from the section, starting from the current position.

        Will read up to the length of the buffer or until the end of the section.

        Args:
            buffer: destination buffer to read into

        Returns:
            Number of bytes read into the buffer.
        """
        size = min(len(buffer), self.end - self.position)
        if size == 0:
            return 0

        self.file.seek(self.position)
        num_read = self.file.readinto(buffer[:size])
        self.position += num_read
        return num_read

    def readall(self) -> bytes:
        """Read all remaining bytes from the section.

        Returns:
            Bytes read from the section.
        """

        return self.read()

    def readable(self):
        """Always returns True, since the section is always readable."""
        return True

    def writable(self):
        return not self.readonly

    def write(self, data: Union[bytes, bytearray, memoryview]) -> int:
        """Write data to the section, starting from the current position.

        Args:
            data: data to write to the section

        Returns:
            Number of bytes written to the section.

        Raises:
            PermissionError: if the section is read-only
            EOFError: if the write would go past the end of the section
        """

        if self.readonly:
            raise PermissionError('Cannot write to a read-only file section')

        if self.position + len(data) > self.end:
            raise EOFError('Cannot write past the end of the section')

        self.file.seek(self.position)
        n_written = self.file.write(data)
        self.position += n_written
        return n_written

    def readline(self, size: int = -1) -> bytes:
        size = min(size, self.end - self.position)
        if size == -1:
            size = self.end - self.position

        self.file.seek(self.position)
        data = self.file.readline(size)

        self.position += len(data)
        return data

    def tell(self):
        return self.position - self.start

    def seek(self, offset, whence=0):
        if whence == io.SEEK_SET:
            new_position = self.start + offset
        elif whence == io.SEEK_CUR:
            new_position = self.position + offset
        elif whence == io.SEEK_END:
            new_position = self.end + offset
        else:
            raise ValueError(f"Invalid value for whence: {whence}")

        if new_position < self.start or new_position > self.end:
            raise EOFError("Seek position out of bounds")

        self.position = new_position
        return self.position - self.start

    def close(self):
        """Close the file section, this is a no-op, since the real shard file is not closed."""
        pass

    @property
    def size(self) -> int:
        """Size of the section in bytes."""
        return self.end - self.start

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
