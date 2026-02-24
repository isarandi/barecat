"""File object classes for Barecat archives."""

from __future__ import annotations

import io
import os
import os.path as osp
import tempfile
from typing import TYPE_CHECKING, Callable, Optional, Union

from ..exceptions import FileExistsBarecatError, FileNotFoundBarecatError
from ..io.copyfile import accumulate_crc32c, write_zeroes

if TYPE_CHECKING:
    from ..core.barecat import Barecat
    from ..core.types import BarecatFileInfo


# =============================================================================
# High-level helper (main entry point for bc.open())
# =============================================================================


class BarecatFileObjectHelper:
    """Manages file object lifecycle: open, write, close, reintegrate."""

    def __init__(self, bc: Barecat):
        self.bc = bc

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
        text_mode = 'b' not in mode
        binary_mode = mode.replace('t', '') if 'b' in mode else mode.replace('t', '') + 'b'
        fileobj = self._open(item, binary_mode)
        if text_mode:
            return io.TextIOWrapper(
                fileobj, encoding=encoding or 'utf-8', errors=errors, newline=newline
            )
        return fileobj

    def _open(
        self,
        item: Union[BarecatFileInfo, str],
        mode: str = 'rb',
    ) -> BarecatFileObject:
        """Open a file in binary mode. See open() for text mode support."""
        index = self.bc.index
        sharder = self.bc.sharder

        # Parse mode (strip 'b' for flag checks)
        base_mode = mode.replace('b', '')
        readonly = base_mode == 'r'
        creating = base_mode in ('w', 'w+', 'x', 'x+', 'a', 'a+')
        truncating = base_mode in ('w', 'w+')
        exclusive = base_mode in ('x', 'x+')

        try:
            finfo = index._as_fileinfo(item)
            path = finfo.path
        except FileNotFoundBarecatError:
            finfo = None
            path = item

        file_exists = finfo is not None

        # Check mode vs existence
        if exclusive and file_exists:
            raise FileExistsBarecatError(path)

        if not creating and not file_exists:
            raise FileNotFoundBarecatError(path)

        # Check readonly archive vs write mode
        if not readonly and self.bc.readonly:
            raise ValueError(
                f"Cannot open file '{path}' for writing: archive is read-only"
            )

        # Read-only mode
        if readonly:
            return sharder.open_from_address(finfo.shard, finfo.offset, finfo.size, 'rb')

        # Write modes
        def on_close(fileobj: BarecatReadWriteFileObject):
            self._reintegrate_fileobj(path, fileobj, is_new=not file_exists)

        if file_exists and not truncating:
            # Open existing file for read/write or append
            return sharder.open_from_address(finfo.shard, finfo.offset, finfo.size, mode, on_close)
        else:
            # New file or truncating existing - use a pure-spillover file object
            return BarecatReadWriteFileObject(
                shard_file=sharder.last_shard_file,
                offset=0,
                size=0,
                on_close_callback=on_close,
                mode=mode,
            )

    def _reintegrate_fileobj(
        self, path: str, fileobj: BarecatReadWriteFileObject, is_new: bool = False
    ):
        """Reintegrate a modified file object back into the archive."""
        from ..core.types import BarecatFileInfo

        index = self.bc.index
        sharder = self.bc.sharder

        if not fileobj.dirty and not is_new:
            return

        if fileobj.size == 0:
            # Empty file
            if is_new:
                # Create empty file entry
                finfo = BarecatFileInfo(path=path, shard=0, offset=0, size=0, crc32c=0)
                index.add_file(finfo)
            elif fileobj.original_size > 0:
                # Truncated to empty
                index.update_file(path, new_size=0, new_crc32c=0)
        elif is_new or fileobj.size > fileobj.original_size:
            # New file or file grew - write to new location
            fileobj.seek(0)
            shard, offset, size, crc32c = sharder.add(size=fileobj.size, fileobj=fileobj)
            if is_new:
                finfo = BarecatFileInfo(
                    path=path, shard=shard, offset=offset, size=size, crc32c=crc32c
                )
                index.add_file(finfo)
            else:
                index.update_file(path, shard, offset, size, crc32c)
        elif fileobj.dirty:
            # File shrank or modified in place
            fileobj.seek(0)
            crc32c = accumulate_crc32c(fileobj)
            index.update_file(path, new_size=fileobj.size, new_crc32c=crc32c)


# =============================================================================
# File object classes (returned to users)
# =============================================================================


class BarecatFileObject(io.IOBase):
    """Base class for Barecat file-like objects."""

    def __init__(self):
        super().__init__()


class BarecatReadOnlyFileObject(BarecatFileObject):
    """File-like object representing a section of a file.

    Args:
        shard_file: the shard file handle
        start: start position of the section in the file
        size: size of the section
    """

    def __init__(self, shard_file, start: int, size: int):
        super().__init__()
        self.shard_file = shard_file
        self.start = start
        self.end = start + size
        self.position = start

    def read(self, size: int = -1) -> bytes:
        """Read a from the section, starting from the current position.

        Args:
            size: number of bytes to read, or -1 to read until the end of the section

        Returns:
            Bytes read from the section.
        """
        # Handle position past end (e.g., after seeking past EOF)
        remaining = self.end - self.position
        if remaining <= 0:
            return b''

        if size == -1:
            size = remaining
        else:
            size = min(size, remaining)

        self.shard_file.seek(self.position)
        data = self.shard_file.read(size)
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
        remaining = self.end - self.position
        if remaining <= 0:
            return 0

        size = min(len(buffer), remaining)
        self.shard_file.seek(self.position)
        num_read = self.shard_file.readinto(buffer[:size])
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

    def seekable(self):
        """Always returns True, since the section is always seekable."""
        return True

    def sendfile(self, out_fd, offset=0, count=None):
        """Send bytes from the section to a file descriptor.

        Args:
            out_fd: destination file descriptor
            offset: offset within the section to start sending from
            count: number of bytes to send, or None to send until the end of the section
        """

        if offset < 0 or offset > self.size:
            raise ValueError('Offset out of bounds')
        if count is None:
            count = self.size - offset
        else:
            count = min(count, self.size - offset)

        n = os.sendfile(out_fd, self.shard_file.fileno(), self.start + offset, count)
        self.position = self.start + offset + n
        return n

    def writable(self):
        """Always returns False, since the section is read-only."""
        return False

    def write(self, data):
        raise io.UnsupportedOperation('not writable')

    def truncate(self, size=None):
        raise io.UnsupportedOperation('not writable')

    def readline(self, size: int = -1) -> bytes:
        remaining = self.end - self.position
        if remaining <= 0:
            return b''

        if size == -1:
            size = remaining
        else:
            size = min(size, remaining)

        self.shard_file.seek(self.position)
        data = self.shard_file.readline(size)

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
            raise ValueError(f'Invalid value for whence: {whence}')

        if new_position < self.start:
            raise ValueError('Negative seek position')

        # Allow seeking past the end (like Python file objects).
        # read() will return empty bytes when position >= end.
        self.position = new_position
        return self.position - self.start

    def close(self):
        """Mark as closed. The underlying shard file is not closed (we don't own it)."""
        super().close()

    @property
    def size(self) -> int:
        """Size of the section in bytes."""
        return self.end - self.start

    def __len__(self):
        return self.size

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class BarecatReadWriteFileObject(BarecatFileObject):
    """
    File-like object for writing to a barecat file with spillover support.

    - Writes within original bounds go directly to shard
    - Writes beyond original bounds go to spillover (temp file)

    Args:
        shard_file: file-like object representing the shard where the file lives
        offset: offset of the file within the shard
        size: original size of the file
        on_close_callback: callback function to be called on close, arg has to be self
        mode: file mode string
    """

    def __init__(
        self,
        shard_file: io.FileIO,
        offset: int,
        size: int,
        on_close_callback: Callable[['BarecatReadWriteFileObject'], None],
        mode: str = 'r+b',
    ):
        super().__init__()
        self.shard_file = shard_file

        self.original_size = size
        self.size = size
        self.offset = offset
        self.position = 0  # Relative to start of file

        self.spillover = None  # Created lazily
        self.dirty = False
        self._closed = False

        self.mode = mode
        base_mode = mode.replace('b', '')
        self.append_mode = 'a' in base_mode
        self.can_read = '+' in base_mode or base_mode.startswith('r')

        self.on_close_callback = on_close_callback

        if self.append_mode:
            self.seek(0, io.SEEK_END)

    def read(self, size: int = -1) -> bytes:
        if not self.can_read:
            raise io.UnsupportedOperation('not readable')

        if size == -1:
            size = self.size - self.position

        size = min(size, self.size - self.position)

        if size == 0:
            return b''

        # Read from shard (original region)
        shard_read_size = min(size, max(0, self.original_size - self.position))
        shard_data = b''
        if shard_read_size > 0:
            self.shard_file.seek(self.offset + self.position)
            shard_data = self.shard_file.read(shard_read_size)
            if len(shard_data) < shard_read_size:
                raise EOFError('Unexpected end of shard file during read')

        spillover_read_size = size - shard_read_size

        # Read from spillover (beyond original region)
        spillover_data = b''
        if spillover_read_size > 0 and self.spillover is not None:
            self.spillover.seek(self.position + shard_read_size - self.original_size)
            spillover_data = self.spillover.read(spillover_read_size)
            if len(spillover_data) < spillover_read_size:
                raise EOFError('Unexpected end of spillover file during read')
        self.position += size

        if spillover_read_size == 0:
            return shard_data

        if shard_read_size == 0:
            return spillover_data

        return shard_data + spillover_data

    def write(self, data: Union[bytes, bytearray, memoryview]) -> int:
        if not data:
            return 0

        if self.append_mode:
            self.seek(0, io.SEEK_END)

        self.dirty = True
        bytes_written = 0
        data = memoryview(data)

        # Write to shard (within original bounds)
        if self.position < self.original_size:
            shard_write_size = min(len(data), self.original_size - self.position)
            self.shard_file.seek(self.offset + self.position)
            n = self.shard_file.write(data[:shard_write_size])
            if n < shard_write_size:
                raise EOFError('Unexpected end of shard file during write')

            self.position += n
            bytes_written += n
            data = data[n:]

        # Write to spillover (beyond original bounds)
        if len(data) > 0:
            self._ensure_spillover()
            spillover_offset = self.position - self.original_size
            self.spillover.seek(spillover_offset)
            n = self.spillover.write(data)
            self.position += n
            bytes_written += n

        self.size = max(self.size, self.position)
        return bytes_written

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        if whence == io.SEEK_SET:
            new_position = offset
        elif whence == io.SEEK_CUR:
            new_position = self.position + offset
        elif whence == io.SEEK_END:
            new_position = self.size + offset
        else:
            raise ValueError(f'Invalid value for whence: {whence}')

        if new_position < 0:
            raise ValueError('Negative seek position')

        self.position = new_position
        return self.position

    def truncate(self, size: int = None) -> int:
        if size is None:
            size = self.position

        if size == self.size:
            return self.size

        self.dirty = True

        new_size_in_shard = min(size, self.original_size)
        if self.size < new_size_in_shard:
            self.shard_file.seek(self.offset + self.size)
            write_zeroes(self.shard_file, new_size_in_shard - self.size)

        if size > self.original_size:
            self._ensure_spillover()
            spillover_target = size - self.original_size
            self.spillover.seek(0, os.SEEK_END)
            current_spillover_size = self.spillover.tell()

            if spillover_target > current_spillover_size:
                # BytesIO/SpooledTemporaryFile truncate doesn't extend with zeros
                write_zeroes(self.spillover, spillover_target - current_spillover_size)
            else:
                self.spillover.truncate(spillover_target)
        elif self.spillover is not None:
            self.spillover.truncate(0)

        self.size = size
        return self.size

    def readable(self):
        return self.can_read

    def writable(self):
        return True

    def seekable(self):
        return True

    def flush(self):
        self.shard_file.flush()
        if self.spillover is not None:
            self.spillover.flush()

    def close(self):
        if self._closed:
            return

        try:
            # Enable reading for reintegration even if opened in write-only mode
            self.can_read = True
            self.on_close_callback(self)
        finally:
            if self.spillover is not None:
                self.spillover.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __len__(self):
        return self.size

    def _ensure_spillover(self):
        """Create spillover file if not exists."""
        if self.spillover is None:
            self.spillover = tempfile.SpooledTemporaryFile(
                max_size=50 * 1024 * 1024,
                mode='w+b',
                dir=osp.dirname(self.shard_file.name),
            )
