"""
Fast file copy utilities with kernel-space zero-copy when available.

Methods tried in order of preference:
1. splice (Linux, kernel-space, zero-copy, requires one pipe end)
2. copy_file_range (Linux 4.5+, kernel-space, zero-copy, file-to-file)
3. sendfile (kernel-space, file-to-file or file-to-socket)
4. Buffered user-space copy

For CRC32c variants, must use buffered copy since data needs to pass through user space.
"""

import ctypes
import io
import os
import stat

import crc32c as crc32c_lib

__all__ = [
    'copy',
    'copy_crc32c',
    'accumulate_crc32c',
    'write_zeroes',
]

# Kernel syscall availability (checked once at import)
_HAS_SPLICE = hasattr(os, 'splice')
_HAS_SENDFILE = hasattr(os, 'sendfile')

# Linux fallocate(2) mode flags
# See: man 2 fallocate, /usr/include/linux/falloc.h
FALLOC_FL_ZERO_RANGE = 0x10  # Zero-fill and allocate space without writing data

# Default buffer size for user-space copies (64 KB)
_DEFAULT_BUFSIZE = 64 * 1024


# =============================================================================
# Main public API
# =============================================================================


def copy(src, dst, size=None, src_offset=None, dst_offset=None, bufsize=_DEFAULT_BUFSIZE):
    """
    Copy bytes between file objects using the fastest available method.

    Automatically handles:
    - Same-file with overlap (memmove-style)
    - Same-file non-overlapping (kernel copy if available)
    - Different files (kernel copy or buffered)
    - Pipes (splice)
    - Unknown size (loops until EOF)

    Returns:
        Number of bytes copied
    """
    ctx = _CopyContext(src, dst, size, src_offset, dst_offset, bufsize)

    # Handle pipes with splice
    if ctx.has_fd and (ctx.src_is_pipe or ctx.dst_is_pipe) and _HAS_SPLICE:
        try:
            return _copy_splice(ctx)
        except OSError:
            pass

    # Unknown size - copy until EOF
    if ctx.size is None:
        return _copy_buffered_eof(ctx)

    if ctx.size == 0:
        return 0

    # Same-file with overlap - memmove style
    if ctx.has_overlap:
        return _copy_same_file_overlap(ctx)

    # Try kernel copy methods
    if ctx.has_fd and not ctx.src_is_pipe and not ctx.dst_is_pipe:
        if _HAS_COPY_FILE_RANGE:
            try:
                return _copy_file_range_loop(ctx)
            except OSError:
                pass

        if ctx.dst_is_socket and _HAS_SENDFILE:
            try:
                return _copy_sendfile(ctx)
            except OSError:
                pass

    # Buffered fallback
    return _copy_buffered(ctx)


def copy_crc32c(src, dst, size=None, src_offset=None, dst_offset=None,
                bufsize=_DEFAULT_BUFSIZE, initial=0):
    """
    Copy bytes and compute CRC32c in a single pass (when possible).

    For forward-overlapping same-file copies, uses two passes:
    copy back-to-front, then scan front-to-back for CRC.

    Returns:
        Tuple of (bytes_copied, crc32c)
    """
    ctx = _CopyContext(src, dst, size, src_offset, dst_offset, bufsize)

    # Unknown size - copy until EOF with CRC
    if ctx.size is None:
        return _copy_buffered_eof(ctx, compute_crc=True, initial_crc=initial)

    if ctx.size == 0:
        return 0, initial

    # Forward overlap - two pass (copy back-to-front, then CRC scan)
    if ctx.has_overlap and ctx.dst_offset > ctx.src_offset:
        bytes_copied = _copy_same_file_overlap(ctx)
        crc = accumulate_crc32c(dst, size=bytes_copied, offset=ctx.dst_offset,
                                 bufsize=bufsize, initial=initial)
        return bytes_copied, crc

    # Single pass - copy with CRC
    return _copy_buffered(ctx, compute_crc=True, initial_crc=initial)


def accumulate_crc32c(fileobj, size=None, offset=None, bufsize=_DEFAULT_BUFSIZE, initial=0):
    """Compute CRC32c of file contents (read-only scan)."""
    if offset is not None:
        fileobj.seek(offset)

    crc = initial

    if size is None:
        while chunk := fileobj.read(bufsize):
            crc = crc32c_lib.crc32c(chunk, crc)
    else:
        bytes_read = 0
        while bytes_read < size:
            chunk_size = min(bufsize, size - bytes_read)
            data = fileobj.read(chunk_size)
            if not data:
                break
            crc = crc32c_lib.crc32c(data, crc)
            bytes_read += len(data)

    return crc


def write_zeroes(file, n, bufsize=_DEFAULT_BUFSIZE):
    """Write n zero bytes. Tries fallocate first, falls back to buffered."""
    if n <= 0:
        return 0

    # Try fallocate with ZERO_RANGE (Linux only, fast)
    try:
        fd = file.fileno()
        file.flush()
        offset = file.tell()
        os.fallocate(fd, FALLOC_FL_ZERO_RANGE, offset, n)
        file.seek(offset + n)
        return n
    except (OSError, AttributeError, io.UnsupportedOperation):
        pass

    # Fallback: write actual zeroes
    n_written = 0
    if n >= bufsize:
        zeroes = bytearray(bufsize)
        while n >= bufsize:
            n_written += file.write(zeroes)
            n -= bufsize
    if n > 0:
        n_written += file.write(bytearray(n))
    return n_written


# =============================================================================
# Copy context - shared setup logic
# =============================================================================

from enum import Enum, auto


class _FdType(Enum):
    FILE = auto()
    PIPE = auto()
    SOCKET = auto()
    NONE = auto()  # No fd (e.g., BytesIO)


def _get_fd_type(fd):
    """Determine file descriptor type with single fstat call."""
    try:
        mode = os.fstat(fd).st_mode
        if stat.S_ISFIFO(mode):
            return _FdType.PIPE
        if stat.S_ISSOCK(mode):
            return _FdType.SOCKET
        return _FdType.FILE
    except (OSError, AttributeError):
        return _FdType.FILE


def _is_seekable(f):
    """Safely check if file object is seekable (handles broken seekable() methods)."""
    try:
        return f.seekable()
    except (AttributeError, OSError, io.UnsupportedOperation):
        return False


class _CopyContext:
    """Encapsulates copy setup: file descriptors, offsets, overlap detection."""

    def __init__(self, src, dst, size, src_offset, dst_offset, bufsize):
        self.src = src
        self.dst = dst
        self.bufsize = bufsize

        # Detect file descriptors and types
        try:
            self.src_fd = src.fileno()
            self.dst_fd = dst.fileno()
            self.has_fd = True
            self.src_type = _get_fd_type(self.src_fd)
            self.dst_type = _get_fd_type(self.dst_fd)
        except (io.UnsupportedOperation, AttributeError):
            self.src_fd = None
            self.dst_fd = None
            self.has_fd = False
            self.src_type = _FdType.NONE
            self.dst_type = _FdType.NONE

        # Check seekability (must do before other operations that may fail)
        self.src_seekable = _is_seekable(src)
        self.dst_seekable = _is_seekable(dst)

        # Detect same file (for overlap handling)
        if src is dst:
            self.same_file = True
        elif self.src_type == _FdType.FILE and self.dst_type == _FdType.FILE:
            self.same_file = _same_file(self.src_fd, self.dst_fd)
        else:
            self.same_file = False

        # Determine size if not provided
        if size is None and self.src_seekable:
            try:
                current = src.tell()
                end = src.seek(0, os.SEEK_END)
                src.seek(current)
                size = end - current
            except (io.UnsupportedOperation, OSError):
                pass
        self.size = size

        # Track position-based vs offset-based semantics
        # Position-based (offset=None): position should advance after I/O
        # Offset-based (explicit offset): position stays unchanged
        self.src_position_based = src_offset is None
        self.dst_position_based = dst_offset is None

        # Resolve offsets (only for seekable streams)
        if src_offset is None and self.src_seekable and size is not None:
            if self.has_fd:
                _sync_position(src)
            src_offset = src.tell()
        if dst_offset is None and self.dst_seekable and size is not None:
            if self.has_fd:
                _sync_position(dst)
            dst_offset = dst.tell()
        self.src_offset = src_offset
        self.dst_offset = dst_offset

        # Detect overlap for same-file copies
        self.has_overlap = False
        if self.same_file and size is not None and src_offset is not None and dst_offset is not None:
            src_end = src_offset + size
            self.has_overlap = not ((dst_offset + size <= src_offset) or (dst_offset >= src_end))

    @property
    def src_is_pipe(self):
        return self.src_type == _FdType.PIPE

    @property
    def dst_is_pipe(self):
        return self.dst_type == _FdType.PIPE

    @property
    def dst_is_socket(self):
        return self.dst_type == _FdType.SOCKET


# =============================================================================
# Copy implementations
# =============================================================================


def _copy_buffered(ctx, compute_crc=False, initial_crc=0):
    """Buffered copy with optional CRC. Handles same-file backward overlap."""
    seek_inside_loop = ctx.has_overlap

    # Save positions for offset-based calls (to restore after)
    src_orig_pos = ctx.src.tell() if not ctx.src_position_based and ctx.src_seekable else None
    dst_orig_pos = ctx.dst.tell() if not ctx.dst_position_based and ctx.dst_seekable else None

    if not seek_inside_loop:
        if ctx.src_offset is not None:
            ctx.src.seek(ctx.src_offset)
        if ctx.dst_offset is not None:
            ctx.dst.seek(ctx.dst_offset)

    crc = initial_crc
    bytes_copied = 0

    while bytes_copied < ctx.size:
        if seek_inside_loop:
            ctx.src.seek(ctx.src_offset + bytes_copied)

        chunk_size = min(ctx.bufsize, ctx.size - bytes_copied)
        data = ctx.src.read(chunk_size)
        if not data:
            break

        if compute_crc:
            crc = crc32c_lib.crc32c(data, crc)

        if seek_inside_loop:
            ctx.dst.seek(ctx.dst_offset + bytes_copied)

        ctx.dst.write(data)
        bytes_copied += len(data)

    # Restore positions for offset-based calls, advance for position-based
    if src_orig_pos is not None:
        ctx.src.seek(src_orig_pos)
    if dst_orig_pos is not None:
        ctx.dst.seek(dst_orig_pos)

    return (bytes_copied, crc) if compute_crc else bytes_copied


def _copy_buffered_eof(ctx, compute_crc=False, initial_crc=0):
    """Buffered copy until EOF."""
    if ctx.dst_offset is not None:
        ctx.dst.seek(ctx.dst_offset)

    crc = initial_crc
    bytes_copied = 0

    while True:
        data = ctx.src.read(ctx.bufsize)
        if not data:
            break
        if compute_crc:
            crc = crc32c_lib.crc32c(data, crc)
        ctx.dst.write(data)
        bytes_copied += len(data)

    return (bytes_copied, crc) if compute_crc else bytes_copied


def _copy_same_file_overlap(ctx):
    """Copy within same file with overlap (memmove-style)."""
    bytes_copied = 0

    if ctx.dst_offset > ctx.src_offset:
        # Forward shift - copy back-to-front
        while bytes_copied < ctx.size:
            remaining = ctx.size - bytes_copied
            chunk_size = min(ctx.bufsize, remaining)
            chunk_offset = remaining - chunk_size

            ctx.src.seek(ctx.src_offset + chunk_offset)
            data = ctx.src.read(chunk_size)
            if not data:
                break

            ctx.src.seek(ctx.dst_offset + chunk_offset)
            ctx.src.write(data)
            bytes_copied += len(data)
    else:
        # Backward shift - copy front-to-back
        while bytes_copied < ctx.size:
            ctx.src.seek(ctx.src_offset + bytes_copied)
            chunk_size = min(ctx.bufsize, ctx.size - bytes_copied)
            data = ctx.src.read(chunk_size)
            if not data:
                break

            ctx.src.seek(ctx.dst_offset + bytes_copied)
            ctx.src.write(data)
            bytes_copied += len(data)

    return bytes_copied


def _copy_file_range_loop(ctx):
    """Copy using copy_file_range (kernel-space, zero-copy)."""
    ctx.src.flush()
    ctx.dst.flush()

    bytes_copied = 0
    while bytes_copied < ctx.size:
        n = _copy_file_range(
            ctx.src_fd, ctx.dst_fd, ctx.size - bytes_copied,
            ctx.src_offset + bytes_copied if ctx.src_offset is not None else None,
            ctx.dst_offset + bytes_copied if ctx.dst_offset is not None else None,
        )
        if n == 0:
            break
        bytes_copied += n

    # Uphold position-based contract: advance position by bytes copied
    if ctx.src_position_based and ctx.src_seekable:
        ctx.src.seek(ctx.src_offset + bytes_copied)
    if ctx.dst_position_based and ctx.dst_seekable:
        ctx.dst.seek(ctx.dst_offset + bytes_copied)

    return bytes_copied


def _copy_sendfile(ctx):
    """Copy using sendfile (kernel-space, dst must be socket)."""
    ctx.src.flush()
    ctx.dst.flush()

    if ctx.dst_offset is not None:
        ctx.dst.seek(ctx.dst_offset)

    bytes_copied = 0
    while bytes_copied < ctx.size:
        cur_src = ctx.src_offset + bytes_copied if ctx.src_offset is not None else None
        n = os.sendfile(ctx.dst_fd, ctx.src_fd, cur_src, ctx.size - bytes_copied)
        if n == 0:
            break
        bytes_copied += n

    # Uphold position-based contract: advance position by bytes copied
    if ctx.src_position_based and ctx.src_seekable:
        ctx.src.seek(ctx.src_offset + bytes_copied)
    if ctx.dst_position_based and ctx.dst_seekable:
        ctx.dst.seek(ctx.dst_offset + bytes_copied)

    return bytes_copied


def _copy_splice(ctx):
    """Copy using splice (kernel-space, requires one pipe end)."""
    # Splice uses position-based I/O at kernel level (offsets=None)
    # Seek to requested offset first, then kernel will advance from there
    if not ctx.src_is_pipe:
        ctx.src.flush()
        if ctx.src_offset is not None:
            ctx.src.seek(ctx.src_offset)
    if not ctx.dst_is_pipe:
        ctx.dst.flush()
        if ctx.dst_offset is not None:
            ctx.dst.seek(ctx.dst_offset)

    bytes_copied = 0
    chunk = ctx.size if ctx.size is not None else ctx.bufsize

    while True:
        remaining = (ctx.size - bytes_copied) if ctx.size is not None else ctx.bufsize
        if remaining <= 0:
            break
        n = os.splice(ctx.src_fd, ctx.dst_fd, min(chunk, remaining), None, None)
        if n == 0:
            break
        bytes_copied += n

    # Kernel moved fd position; sync Python's view with it
    if not ctx.src_is_pipe:
        _sync_position(ctx.src)
    if not ctx.dst_is_pipe:
        _sync_position(ctx.dst)

    return bytes_copied


# =============================================================================
# File descriptor utilities
# =============================================================================

_copy_file_range = None

if hasattr(os, 'copy_file_range'):
    _copy_file_range = os.copy_file_range
else:
    try:
        _libc = ctypes.CDLL('libc.so.6', use_errno=True)
        _cfr = _libc.copy_file_range
        _cfr.argtypes = [
            ctypes.c_int, ctypes.POINTER(ctypes.c_longlong),
            ctypes.c_int, ctypes.POINTER(ctypes.c_longlong),
            ctypes.c_size_t, ctypes.c_uint,
        ]
        _cfr.restype = ctypes.c_ssize_t

        def _copy_file_range(src_fd, dst_fd, count, offset_src=None, offset_dst=None):
            off_in = ctypes.c_longlong(offset_src) if offset_src is not None else None
            off_out = ctypes.c_longlong(offset_dst) if offset_dst is not None else None
            result = _cfr(
                src_fd,
                ctypes.byref(off_in) if off_in is not None else None,
                dst_fd,
                ctypes.byref(off_out) if off_out is not None else None,
                count, 0,
            )
            if result < 0:
                errno = ctypes.get_errno()
                raise OSError(errno, os.strerror(errno))
            return result
    except (OSError, AttributeError):
        pass

_HAS_COPY_FILE_RANGE = _copy_file_range is not None


def _same_file(fd1, fd2):
    if fd1 == fd2:
        return True
    try:
        s1 = os.fstat(fd1)
        s2 = os.fstat(fd2)
        return s1.st_ino == s2.st_ino and s1.st_dev == s2.st_dev
    except (OSError, AttributeError):
        return False


def _sync_position(f):
    """Sync fd position to match Python's logical position."""
    f.flush()
    pos = f.tell()
    f.seek(0, os.SEEK_END)
    f.seek(pos)

