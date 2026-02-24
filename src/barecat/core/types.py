import os
from datetime import datetime
from enum import Flag, auto
from typing import Union, Optional
from ..util.misc import datetime_to_ns, ns_to_datetime
from ..core.paths import normalize_path

SHARD_SIZE_UNLIMITED = (1 << 63) - 1  #: An extremely large integer, representing unlimited size

# Schema version constants - must match sql/schema.sql
SCHEMA_VERSION_MAJOR = 0  #: Major version - breaking changes require code upgrade
SCHEMA_VERSION_MINOR = 3  #: Minor version - backwards compatible additions


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

    def isfile(self) -> bool:
        """True if this is a file entry."""
        return False

    def isdir(self) -> bool:
        """True if this is a directory entry."""
        return False

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

    def isfile(self) -> bool:
        return True

    def __repr__(self):
        return (
            f"BarecatFileInfo('{self.path}', {self.size} bytes, shard={self.shard})"
        )


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
        num_subdirs: Optional[int] = None,
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

    def isdir(self) -> bool:
        return True

    def __repr__(self):
        parts = [f"'{self.path}'"]
        if self.num_files_tree is not None:
            parts.append(f'{self.num_files_tree} files')
        if self.size_tree is not None:
            parts.append(f'{_format_size_short(self.size_tree)}')
        return f"BarecatDirInfo({', '.join(parts)})"


def _format_size_short(size):
    """Format a byte size as a short human-readable string."""
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if abs(size) < 1024:
            if unit == 'B':
                return f'{size} {unit}'
            return f'{size:.1f} {unit}'
        size /= 1024
    return f'{size:.1f} PB'


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
