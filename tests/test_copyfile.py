"""Tests for barecat.io.copyfile module.

Tests cover:
- Position-based vs offset-based semantics
- All file types (regular files, pipes)
- Optimal code path selection (kernel vs buffered)
- Same-file copies with overlap detection
- Edge cases (empty, large, exact buffer boundaries)
"""

import io
import os
import tempfile
import threading
from unittest import mock

import pytest

from barecat.io import copyfile


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def temp_files():
    """Create temporary source and destination files."""
    with tempfile.NamedTemporaryFile(delete=False) as src:
        src.write(b'hello world 12345')
        src_path = src.name
    with tempfile.NamedTemporaryFile(delete=False) as dst:
        dst_path = dst.name

    yield src_path, dst_path

    os.unlink(src_path)
    os.unlink(dst_path)


@pytest.fixture
def large_temp_file():
    """Create a larger temp file for buffer boundary tests."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        # Write 1MB of data
        data = b'x' * (1024 * 1024)
        f.write(data)
        path = f.name
    yield path, len(data)
    os.unlink(path)


# =============================================================================
# Position-based vs Offset-based Semantics
# =============================================================================


class TestPositionSemantics:
    """Test that position-based and offset-based semantics are honored."""

    def test_position_based_advances_both_positions(self, temp_files):
        """copy(src, dst, size) with no offsets should advance both file positions."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            assert src.tell() == 0
            assert dst.tell() == 0

            n = copyfile.copy(src, dst, 11)  # "hello world"

            assert n == 11
            assert src.tell() == 11, "Source position should advance"
            assert dst.tell() == 11, "Dest position should advance"

    def test_position_based_from_middle(self, temp_files):
        """Position-based copy from middle of file."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            src.seek(6)  # Start at "world"
            dst.seek(0)

            n = copyfile.copy(src, dst, 5)  # "world"

            assert n == 5
            assert src.tell() == 11
            assert dst.tell() == 5

        with open(dst_path, 'rb') as f:
            assert f.read(5) == b'world'

    def test_offset_based_src_only_no_position_change(self, temp_files):
        """Explicit src_offset should not change source position."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            src.seek(3)  # Set to arbitrary position
            dst.seek(0)

            n = copyfile.copy(src, dst, 5, src_offset=6)  # Read "world" from offset 6

            assert n == 5
            assert src.tell() == 3, "Source position should NOT change with explicit offset"
            assert dst.tell() == 5, "Dest position should advance (no dst_offset given)"

        with open(dst_path, 'rb') as f:
            assert f.read(5) == b'world'

    def test_offset_based_dst_only_no_position_change(self, temp_files):
        """Explicit dst_offset should not change dest position."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            src.seek(0)
            dst.seek(3)  # Set to arbitrary position

            n = copyfile.copy(src, dst, 5, dst_offset=10)  # Write at offset 10

            assert n == 5
            assert src.tell() == 5, "Source position should advance (no src_offset given)"
            assert dst.tell() == 3, "Dest position should NOT change with explicit offset"

        with open(dst_path, 'rb') as f:
            f.seek(10)
            assert f.read(5) == b'hello'

    def test_offset_based_both_no_position_change(self, temp_files):
        """Both explicit offsets should not change either position."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            src.seek(2)
            dst.seek(3)

            n = copyfile.copy(src, dst, 5, src_offset=6, dst_offset=0)

            assert n == 5
            assert src.tell() == 2, "Source position should NOT change"
            assert dst.tell() == 3, "Dest position should NOT change"

        with open(dst_path, 'rb') as f:
            assert f.read(5) == b'world'

    def test_size_none_copies_until_eof(self, temp_files):
        """size=None should copy until EOF and advance positions."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            n = copyfile.copy(src, dst, None)

            assert n == 17  # "hello world 12345"
            assert src.tell() == 17
            assert dst.tell() == 17

    def test_size_none_from_middle(self, temp_files):
        """size=None from middle should copy remaining bytes."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            src.seek(12)  # Start at "12345"

            n = copyfile.copy(src, dst, None)

            assert n == 5
            assert src.tell() == 17
            assert dst.tell() == 5


class TestPositionSemanticsCrc32c:
    """Test position semantics for copy_crc32c."""

    def test_position_based_advances_positions(self, temp_files):
        """copy_crc32c with no offsets should advance positions."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            n, crc = copyfile.copy_crc32c(src, dst, 11)

            assert n == 11
            assert src.tell() == 11
            assert dst.tell() == 11
            assert crc != 0  # Should have computed a CRC

    def test_offset_based_no_position_change(self, temp_files):
        """copy_crc32c with explicit offsets should not change positions."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            src.seek(2)
            dst.seek(3)

            n, crc = copyfile.copy_crc32c(src, dst, 5, src_offset=0, dst_offset=0)

            assert n == 5
            assert src.tell() == 2
            assert dst.tell() == 3


class TestAccumulateCrc32c:
    """Test accumulate_crc32c position semantics."""

    def test_position_based_advances(self, temp_files):
        """accumulate_crc32c without offset advances position."""
        src_path, _ = temp_files

        with open(src_path, 'rb') as f:
            crc = copyfile.accumulate_crc32c(f, size=5)
            assert f.tell() == 5

    def test_offset_based_seeks_and_advances(self, temp_files):
        """accumulate_crc32c with offset seeks first, then advances."""
        src_path, _ = temp_files

        with open(src_path, 'rb') as f:
            f.seek(100)  # Arbitrary position
            crc = copyfile.accumulate_crc32c(f, size=5, offset=6)
            # After seeking to 6 and reading 5, position is 11
            assert f.tell() == 11


# =============================================================================
# Code Path Selection (Kernel vs Buffered)
# =============================================================================


class TestCodePathSelection:
    """Test that optimal code paths are selected."""

    def test_file_to_file_uses_copy_file_range(self, temp_files):
        """Regular file to file should try copy_file_range."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            with mock.patch.object(
                copyfile, '_copy_file_range_loop', wraps=copyfile._copy_file_range_loop
            ) as mock_cfr:
                # Only test if copy_file_range is available
                if copyfile._copy_file_range is not None:
                    copyfile.copy(src, dst, 10)
                    mock_cfr.assert_called_once()

    def test_pipe_to_file_uses_splice(self, temp_files):
        """Pipe to file should use splice if available."""
        _, dst_path = temp_files

        if not copyfile._HAS_SPLICE:
            pytest.skip("splice not available")

        r_fd, w_fd = os.pipe()
        try:
            # Write to pipe in background
            def writer():
                os.write(w_fd, b'pipe data here')
                os.close(w_fd)

            t = threading.Thread(target=writer)
            t.start()

            with os.fdopen(r_fd, 'rb') as pipe_in, open(dst_path, 'r+b') as dst:
                with mock.patch.object(
                    copyfile, '_copy_splice', wraps=copyfile._copy_splice
                ) as mock_splice:
                    n = copyfile.copy(pipe_in, dst, 14)
                    assert n == 14
                    # Splice should have been attempted
                    mock_splice.assert_called()

            t.join()
        except:
            os.close(r_fd)
            os.close(w_fd)
            raise

    def test_file_to_pipe_uses_splice(self, temp_files):
        """File to pipe should use splice if available."""
        src_path, _ = temp_files

        if not copyfile._HAS_SPLICE:
            pytest.skip("splice not available")

        r_fd, w_fd = os.pipe()
        try:
            # Read from pipe in background
            result = []

            def reader():
                data = os.read(r_fd, 1024)
                result.append(data)
                os.close(r_fd)

            t = threading.Thread(target=reader)
            t.start()

            with open(src_path, 'rb') as src, os.fdopen(w_fd, 'wb') as pipe_out:
                with mock.patch.object(
                    copyfile, '_copy_splice', wraps=copyfile._copy_splice
                ) as mock_splice:
                    n = copyfile.copy(src, pipe_out, 10)
                    assert n == 10
                    mock_splice.assert_called()

            t.join()
            assert result[0] == b'hello worl'
        except:
            try:
                os.close(r_fd)
            except:
                pass
            try:
                os.close(w_fd)
            except:
                pass
            raise

    def test_bytesio_uses_buffered(self):
        """BytesIO (no fd) should use buffered copy."""
        src = io.BytesIO(b'hello world')
        dst = io.BytesIO()

        with mock.patch.object(
            copyfile, '_copy_buffered', wraps=copyfile._copy_buffered
        ) as mock_buf:
            n = copyfile.copy(src, dst, 11)
            assert n == 11
            mock_buf.assert_called()

        assert dst.getvalue() == b'hello world'

    def test_crc32c_always_uses_buffered(self, temp_files):
        """copy_crc32c must use buffered (data must pass through userspace)."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            # copy_crc32c should NOT use kernel copy methods
            with mock.patch.object(
                copyfile, '_copy_file_range_loop'
            ) as mock_kernel:
                n, crc = copyfile.copy_crc32c(src, dst, 10)
                assert n == 10
                mock_kernel.assert_not_called()


# =============================================================================
# Same-file Copies
# =============================================================================


class TestSameFileCopy:
    """Test same-file copy with overlap detection."""

    def test_same_file_no_overlap_backward(self):
        """Same file, dst before src (no overlap) should work."""
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as f:
            f.write(b'AAAAA' + b'hello')  # 5 bytes padding, then "hello"
            path = f.name

        try:
            with open(path, 'r+b') as f:
                # Copy "hello" from offset 5 to offset 0
                n = copyfile.copy(f, f, 5, src_offset=5, dst_offset=0)
                assert n == 5

            with open(path, 'rb') as f:
                assert f.read(5) == b'hello'
        finally:
            os.unlink(path)

    def test_same_file_no_overlap_forward(self):
        """Same file, dst after src end (no overlap) should work."""
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as f:
            f.write(b'hello' + b'\x00' * 10)
            path = f.name

        try:
            with open(path, 'r+b') as f:
                # Copy "hello" from offset 0 to offset 10 (no overlap)
                n = copyfile.copy(f, f, 5, src_offset=0, dst_offset=10)
                assert n == 5

            with open(path, 'rb') as f:
                f.seek(10)
                assert f.read(5) == b'hello'
        finally:
            os.unlink(path)

    def test_same_file_backward_overlap(self):
        """Same file backward shift with overlap should work (memmove-style)."""
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as f:
            f.write(b'XXXhello')  # "hello" at offset 3
            path = f.name

        try:
            with open(path, 'r+b') as f:
                # Shift "hello" from offset 3 to offset 0 (overlaps by 2 bytes)
                n = copyfile.copy(f, f, 5, src_offset=3, dst_offset=0)
                assert n == 5

            with open(path, 'rb') as f:
                assert f.read(5) == b'hello'
        finally:
            os.unlink(path)

    def test_same_file_forward_overlap_works(self):
        """Same file forward overlap uses back-to-front copy (memmove-style)."""
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as f:
            f.write(b'hello world')
            path = f.name

        try:
            with open(path, 'r+b') as f:
                # Copy "hello" from 0 to 2 - forward overlap, handled by back-to-front
                n = copyfile.copy(f, f, 5, src_offset=0, dst_offset=2)
                assert n == 5

            with open(path, 'rb') as f:
                # Result: "hehello rld" (original "hello" shifted right by 2)
                assert f.read() == b'hehelloorld'
        finally:
            os.unlink(path)

    def test_same_file_detected_via_inode(self):
        """Same file should be detected even with different file objects."""
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as f:
            f.write(b'hello world')
            path = f.name

        try:
            # Open same file twice - should still detect as same file via inode
            with open(path, 'r+b') as f1, open(path, 'r+b') as f2:
                # Forward overlap handled correctly via back-to-front copy
                n = copyfile.copy(f1, f2, 5, src_offset=0, dst_offset=2)
                assert n == 5

            with open(path, 'rb') as f:
                assert f.read() == b'hehelloorld'
        finally:
            os.unlink(path)

    def test_same_file_size_none_calculates_remaining(self):
        """Same-file copy with size=None calculates remaining bytes."""
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as f:
            f.write(b'hello world')
            path = f.name

        try:
            with open(path, 'r+b') as f:
                f.seek(6)  # Position at "world"
                # size=None calculates remaining (5 bytes), copies to offset 0
                n = copyfile.copy(f, f, None, dst_offset=0)
                assert n == 5

            with open(path, 'rb') as f:
                assert f.read(5) == b'world'
        finally:
            os.unlink(path)


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_size_copy(self, temp_files):
        """Copying zero bytes should work and not change positions."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            src.seek(5)
            dst.seek(3)

            n = copyfile.copy(src, dst, 0)

            assert n == 0
            assert src.tell() == 5  # Unchanged
            assert dst.tell() == 3  # Unchanged

    def test_empty_file(self):
        """Copying from empty file should return 0."""
        with tempfile.NamedTemporaryFile(delete=False) as src:
            src_path = src.name
        with tempfile.NamedTemporaryFile(delete=False) as dst:
            dst_path = dst.name

        try:
            with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
                n = copyfile.copy(src, dst, None)
                assert n == 0
        finally:
            os.unlink(src_path)
            os.unlink(dst_path)

    def test_exact_buffer_boundary(self, large_temp_file):
        """Test copy at exact buffer size boundary."""
        path, size = large_temp_file
        bufsize = 64 * 1024  # Default bufsize

        with tempfile.NamedTemporaryFile(delete=False) as dst:
            dst_path = dst.name

        try:
            with open(path, 'rb') as src, open(dst_path, 'r+b') as dst:
                # Copy exactly one buffer
                n = copyfile.copy(src, dst, bufsize)
                assert n == bufsize
                assert src.tell() == bufsize
                assert dst.tell() == bufsize
        finally:
            os.unlink(dst_path)

    def test_one_byte_less_than_buffer(self, large_temp_file):
        """Test copy at buffer size - 1."""
        path, size = large_temp_file
        bufsize = 64 * 1024

        with tempfile.NamedTemporaryFile(delete=False) as dst:
            dst_path = dst.name

        try:
            with open(path, 'rb') as src, open(dst_path, 'r+b') as dst:
                n = copyfile.copy(src, dst, bufsize - 1)
                assert n == bufsize - 1
        finally:
            os.unlink(dst_path)

    def test_one_byte_more_than_buffer(self, large_temp_file):
        """Test copy at buffer size + 1."""
        path, size = large_temp_file
        bufsize = 64 * 1024

        with tempfile.NamedTemporaryFile(delete=False) as dst:
            dst_path = dst.name

        try:
            with open(path, 'rb') as src, open(dst_path, 'r+b') as dst:
                n = copyfile.copy(src, dst, bufsize + 1)
                assert n == bufsize + 1
        finally:
            os.unlink(dst_path)

    def test_single_byte(self, temp_files):
        """Copy single byte."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            n = copyfile.copy(src, dst, 1)
            assert n == 1
            assert src.tell() == 1
            assert dst.tell() == 1

        with open(dst_path, 'rb') as f:
            assert f.read(1) == b'h'

    def test_read_past_eof(self, temp_files):
        """Requesting more bytes than available should copy what's there."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            src.seek(12)  # Only 5 bytes left ("12345")
            n = copyfile.copy(src, dst, 100)  # Request 100
            assert n == 5  # Only get 5

    def test_write_zeroes(self):
        """Test write_zeroes function."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        try:
            with open(path, 'r+b') as f:
                copyfile.write_zeroes(f, 1000)

            with open(path, 'rb') as f:
                data = f.read()
                assert len(data) == 1000
                assert data == b'\x00' * 1000
        finally:
            os.unlink(path)

    def test_write_zeroes_large(self):
        """Test write_zeroes with size larger than buffer."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        try:
            with open(path, 'r+b') as f:
                copyfile.write_zeroes(f, 100_000)

            assert os.path.getsize(path) == 100_000
        finally:
            os.unlink(path)


class TestCrc32cCorrectness:
    """Test CRC32c computation correctness."""

    def test_crc32c_matches_direct_computation(self, temp_files):
        """copy_crc32c should produce same CRC as direct computation."""
        import crc32c as crc32c_lib

        src_path, dst_path = temp_files

        # Compute expected CRC directly
        with open(src_path, 'rb') as f:
            data = f.read(10)
            expected_crc = crc32c_lib.crc32c(data)

        # Compute via copy_crc32c
        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            n, actual_crc = copyfile.copy_crc32c(src, dst, 10)

        assert actual_crc == expected_crc

    def test_accumulate_crc32c_matches(self, temp_files):
        """accumulate_crc32c should match direct computation."""
        import crc32c as crc32c_lib

        src_path, _ = temp_files

        with open(src_path, 'rb') as f:
            data = f.read()
            expected = crc32c_lib.crc32c(data)

        with open(src_path, 'rb') as f:
            actual = copyfile.accumulate_crc32c(f)

        assert actual == expected

    def test_crc32c_initial_value(self, temp_files):
        """Test CRC32c with non-zero initial value."""
        import crc32c as crc32c_lib

        src_path, dst_path = temp_files

        with open(src_path, 'rb') as f:
            data = f.read(10)
            expected = crc32c_lib.crc32c(data, 12345)

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            n, actual = copyfile.copy_crc32c(src, dst, 10, initial=12345)

        assert actual == expected


class TestNonSeekableStreams:
    """Test with non-seekable streams like BytesIO, tar streams, etc."""

    def test_bytesio_position_based(self):
        """BytesIO should work with position-based semantics."""
        src = io.BytesIO(b'hello world 12345')
        dst = io.BytesIO()

        n = copyfile.copy(src, dst, 11)

        assert n == 11
        assert src.tell() == 11, "BytesIO src position should advance"
        assert dst.tell() == 11, "BytesIO dst position should advance"
        assert dst.getvalue() == b'hello world'

    def test_bytesio_offset_based(self):
        """BytesIO with explicit offsets should not change positions."""
        src = io.BytesIO(b'hello world 12345')
        dst = io.BytesIO(b'\x00' * 20)

        src.seek(3)
        dst.seek(5)

        n = copyfile.copy(src, dst, 5, src_offset=6, dst_offset=10)

        assert n == 5
        assert src.tell() == 3, "BytesIO src position should NOT change with offset"
        assert dst.tell() == 5, "BytesIO dst position should NOT change with offset"
        assert dst.getvalue()[10:15] == b'world'

    def test_bytesio_size_none(self):
        """BytesIO with size=None should copy until EOF."""
        src = io.BytesIO(b'hello world')
        dst = io.BytesIO()

        src.seek(6)
        n = copyfile.copy(src, dst, None)

        assert n == 5
        assert src.tell() == 11
        assert dst.getvalue() == b'world'

    def test_bytesio_uses_buffered_path(self):
        """BytesIO should use buffered copy (no fd)."""
        src = io.BytesIO(b'hello world')
        dst = io.BytesIO()

        with mock.patch.object(copyfile, '_copy_buffered', wraps=copyfile._copy_buffered) as m:
            copyfile.copy(src, dst, 11)
            m.assert_called_once()

    def test_tar_stream_like_non_seekable(self):
        """Test with a non-seekable stream wrapper."""
        class NonSeekableWrapper:
            """Wrapper that makes a file non-seekable (like tar ExFileObject)."""
            def __init__(self, data):
                self._data = data
                self._pos = 0

            def read(self, n=-1):
                if n == -1:
                    result = self._data[self._pos:]
                    self._pos = len(self._data)
                else:
                    result = self._data[self._pos:self._pos + n]
                    self._pos += len(result)
                return result

            def seekable(self):
                return False

            def tell(self):
                return self._pos

        src = NonSeekableWrapper(b'hello world')
        dst = io.BytesIO()

        n = copyfile.copy(src, dst, 11)

        assert n == 11
        assert dst.getvalue() == b'hello world'

    def test_tar_stream_size_none(self):
        """Non-seekable stream with size=None should copy until EOF."""
        class NonSeekableWrapper:
            def __init__(self, data):
                self._data = data
                self._pos = 0

            def read(self, n=-1):
                if n == -1:
                    result = self._data[self._pos:]
                    self._pos = len(self._data)
                else:
                    result = self._data[self._pos:self._pos + n]
                    self._pos += len(result)
                return result

            def seekable(self):
                return False

            def fileno(self):
                raise io.UnsupportedOperation("no fileno")

        src = NonSeekableWrapper(b'hello world')
        dst = io.BytesIO()

        n = copyfile.copy(src, dst, None)

        assert n == 11
        assert dst.getvalue() == b'hello world'


class TestSocketCopy:
    """Test copy to/from sockets."""

    def test_file_to_socket_data_integrity(self, temp_files):
        """File to socket should copy data correctly."""
        import socket

        src_path, _ = temp_files

        server_sock, client_sock = socket.socketpair()
        try:
            received = []

            def receiver():
                data = server_sock.recv(1024)
                received.append(data)

            t = threading.Thread(target=receiver)
            t.start()

            with open(src_path, 'rb') as src:
                with client_sock.makefile('wb') as dst:
                    n = copyfile.copy(src, dst, 10)
                    assert n == 10

            t.join(timeout=2)
            assert received[0] == b'hello worl'
        finally:
            server_sock.close()
            client_sock.close()

    def test_socket_to_file_data_integrity(self, temp_files):
        """Socket to file should copy data correctly."""
        import socket

        _, dst_path = temp_files

        server_sock, client_sock = socket.socketpair()
        try:
            # Send data through socket
            client_sock.sendall(b'socket data!')

            with server_sock.makefile('rb') as src, open(dst_path, 'wb') as dst:
                n = copyfile.copy(src, dst, 12)
                assert n == 12

            with open(dst_path, 'rb') as f:
                assert f.read() == b'socket data!'
        finally:
            server_sock.close()
            client_sock.close()


class TestCodePathVerification:
    """Verify correct code paths are taken AND succeed for different scenarios."""

    def test_copy_file_range_actually_works(self, temp_files):
        """Verify copy_file_range actually copies data (not just attempted)."""
        src_path, dst_path = temp_files

        if not copyfile._HAS_COPY_FILE_RANGE:
            pytest.skip("copy_file_range not available")

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            used_kernel = [False]

            orig_cfr = copyfile._copy_file_range_loop

            def track_cfr(ctx):
                result = orig_cfr(ctx)
                used_kernel[0] = True
                return result

            # Block buffered fallback to ensure kernel method is used
            def fail_buffered(ctx, **kw):
                raise AssertionError("Should not fall back to buffered")

            with mock.patch.object(copyfile, '_copy_file_range_loop', track_cfr):
                with mock.patch.object(copyfile, '_copy_buffered', fail_buffered):
                    n = copyfile.copy(src, dst, 10)

            assert used_kernel[0], "copy_file_range should have been used"
            assert n == 10

        # Verify data integrity
        with open(dst_path, 'rb') as f:
            assert f.read(10) == b'hello worl'

    def test_splice_actually_works_pipe_to_file(self, temp_files):
        """Verify splice actually works for pipe→file."""
        if not copyfile._HAS_SPLICE:
            pytest.skip("splice not available")

        _, dst_path = temp_files

        r_fd, w_fd = os.pipe()
        try:
            os.write(w_fd, b'splice test data')
            os.close(w_fd)
            w_fd = None

            with os.fdopen(r_fd, 'rb') as pipe_in, open(dst_path, 'r+b') as dst:
                r_fd = None

                used_splice = [False]
                orig_splice = copyfile._copy_splice

                def track_splice(ctx):
                    result = orig_splice(ctx)
                    used_splice[0] = True
                    return result

                with mock.patch.object(copyfile, '_copy_splice', track_splice):
                    n = copyfile.copy(pipe_in, dst, 16)

                assert used_splice[0], "splice should have been used"
                assert n == 16

            with open(dst_path, 'rb') as f:
                assert f.read(16) == b'splice test data'
        finally:
            if r_fd is not None:
                os.close(r_fd)
            if w_fd is not None:
                os.close(w_fd)

    def test_splice_actually_works_file_to_pipe(self, temp_files):
        """Verify splice actually works for file→pipe."""
        if not copyfile._HAS_SPLICE:
            pytest.skip("splice not available")

        src_path, _ = temp_files

        r_fd, w_fd = os.pipe()
        try:
            received = []

            def reader():
                data = os.read(r_fd, 1024)
                received.append(data)

            t = threading.Thread(target=reader)
            t.start()

            with open(src_path, 'rb') as src, os.fdopen(w_fd, 'wb') as pipe_out:
                w_fd = None

                used_splice = [False]
                orig_splice = copyfile._copy_splice

                def track_splice(ctx):
                    result = orig_splice(ctx)
                    used_splice[0] = True
                    return result

                with mock.patch.object(copyfile, '_copy_splice', track_splice):
                    n = copyfile.copy(src, pipe_out, 10)

                assert used_splice[0], "splice should have been used"
                assert n == 10

            t.join(timeout=2)
            os.close(r_fd)
            r_fd = None
            assert received[0] == b'hello worl'
        finally:
            if r_fd is not None:
                os.close(r_fd)
            if w_fd is not None:
                os.close(w_fd)

    def test_buffered_fallback_when_no_fd(self):
        """BytesIO (no fd) must use buffered and actually work."""
        src = io.BytesIO(b'buffered copy test')
        dst = io.BytesIO()

        used_buffered = [False]
        orig_buf = copyfile._copy_buffered

        def track_buf(ctx, **kw):
            result = orig_buf(ctx, **kw)
            used_buffered[0] = True
            return result

        # Ensure kernel methods would fail if called
        def fail_kernel(ctx):
            raise AssertionError("Should not try kernel copy for BytesIO")

        with mock.patch.object(copyfile, '_copy_file_range_loop', fail_kernel):
            with mock.patch.object(copyfile, '_copy_buffered', track_buf):
                n = copyfile.copy(src, dst, 18)

        assert used_buffered[0], "buffered copy should have been used"
        assert n == 18
        assert dst.getvalue() == b'buffered copy test'

    def test_crc32c_never_uses_kernel_copy(self, temp_files):
        """copy_crc32c must use buffered (data must pass through userspace)."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            call_log = []

            def track_cfr(ctx):
                call_log.append('copy_file_range')
                raise AssertionError("copy_file_range should not be called for CRC")

            def track_splice(ctx):
                call_log.append('splice')
                raise AssertionError("splice should not be called for CRC")

            with mock.patch.object(copyfile, '_copy_file_range_loop', track_cfr):
                with mock.patch.object(copyfile, '_copy_splice', track_splice):
                    n, crc = copyfile.copy_crc32c(src, dst, 10)

            assert n == 10
            assert crc != 0
            assert 'copy_file_range' not in call_log
            assert 'splice' not in call_log

    def test_same_file_uses_overlap_handler(self):
        """Same-file with overlap should use _copy_same_file_overlap."""
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as f:
            f.write(b'hello world')
            path = f.name

        try:
            with open(path, 'r+b') as f:
                with mock.patch.object(
                    copyfile, '_copy_same_file_overlap',
                    wraps=copyfile._copy_same_file_overlap
                ) as m:
                    # Backward overlap
                    copyfile.copy(f, f, 5, src_offset=5, dst_offset=2)
                    m.assert_called_once()
        finally:
            os.unlink(path)

    def test_bytesio_uses_buffered_not_kernel(self):
        """BytesIO should use buffered, not kernel methods."""
        src = io.BytesIO(b'hello world')
        dst = io.BytesIO()

        call_log = []

        def track_cfr(ctx):
            call_log.append('copy_file_range')
            raise AssertionError("Should not use copy_file_range for BytesIO")

        def track_buf(ctx, **kw):
            call_log.append('buffered')
            return copyfile._copy_buffered.__wrapped__(ctx, **kw) if hasattr(
                copyfile._copy_buffered, '__wrapped__') else 11

        with mock.patch.object(copyfile, '_copy_file_range_loop', track_cfr):
            copyfile.copy(src, dst, 11)

        # Should not have tried kernel copy
        assert 'copy_file_range' not in call_log

    def test_sendfile_actually_works_file_to_socket(self, temp_files):
        """Verify sendfile is used and works for file→socket."""
        import socket

        if not copyfile._HAS_SENDFILE:
            pytest.skip("sendfile not available")

        src_path, _ = temp_files

        server_sock, client_sock = socket.socketpair()
        try:
            received = []

            def reader():
                data = client_sock.recv(1024)
                received.append(data)

            t = threading.Thread(target=reader)
            t.start()

            with open(src_path, 'rb') as src:
                used_sendfile = [False]
                orig_sendfile = copyfile._copy_sendfile

                def track_sendfile(ctx):
                    result = orig_sendfile(ctx)
                    used_sendfile[0] = True
                    return result

                # Block fallback to ensure sendfile is used
                def fail_buffered(ctx, **kw):
                    raise AssertionError("Should not fall back to buffered")

                with mock.patch.object(copyfile, '_copy_sendfile', track_sendfile):
                    with mock.patch.object(copyfile, '_copy_buffered', fail_buffered):
                        n = copyfile.copy(src, server_sock.makefile('wb', buffering=0), 10)

                assert used_sendfile[0], "sendfile should have been used"
                assert n == 10

            server_sock.close()
            t.join(timeout=2)
            assert received[0] == b'hello worl'
        finally:
            client_sock.close()


class TestMixedPositionOffsetSemantics:
    """Test mixed position/offset combinations thoroughly."""

    def test_src_position_dst_offset(self, temp_files):
        """src position-based, dst offset-based."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            src.seek(0)
            dst.seek(5)  # Will be ignored due to explicit dst_offset

            n = copyfile.copy(src, dst, 5, dst_offset=10)

            assert n == 5
            assert src.tell() == 5, "src should advance (position-based)"
            assert dst.tell() == 5, "dst should NOT advance (offset-based)"

    def test_src_offset_dst_position(self, temp_files):
        """src offset-based, dst position-based."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            src.seek(3)
            dst.seek(0)

            n = copyfile.copy(src, dst, 5, src_offset=6)

            assert n == 5
            assert src.tell() == 3, "src should NOT advance (offset-based)"
            assert dst.tell() == 5, "dst should advance (position-based)"

    def test_position_position_from_middle(self, temp_files):
        """Both position-based from middle of files."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            src.seek(6)
            dst.seek(2)

            n = copyfile.copy(src, dst, 5)

            assert n == 5
            assert src.tell() == 11
            assert dst.tell() == 7

    def test_offset_offset_positions_unchanged(self, temp_files):
        """Both offset-based, positions should not change."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            src.seek(1)
            dst.seek(2)

            n = copyfile.copy(src, dst, 5, src_offset=0, dst_offset=0)

            assert n == 5
            assert src.tell() == 1, "src position unchanged"
            assert dst.tell() == 2, "dst position unchanged"


class TestDataIntegrity:
    """Test that data is copied correctly."""

    def test_data_integrity_small(self, temp_files):
        """Verify copied data matches source."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'wb') as dst:
            copyfile.copy(src, dst, None)

        with open(src_path, 'rb') as src, open(dst_path, 'rb') as dst:
            assert src.read() == dst.read()

    def test_data_integrity_large(self, large_temp_file):
        """Verify large file copy integrity."""
        src_path, size = large_temp_file

        with tempfile.NamedTemporaryFile(delete=False) as dst:
            dst_path = dst.name

        try:
            with open(src_path, 'rb') as src, open(dst_path, 'wb') as dst:
                n = copyfile.copy(src, dst, None)
                assert n == size

            # Verify content
            with open(src_path, 'rb') as src, open(dst_path, 'rb') as dst:
                assert src.read() == dst.read()
        finally:
            os.unlink(dst_path)

    def test_data_integrity_with_offset(self, temp_files):
        """Verify data integrity when using offsets."""
        src_path, dst_path = temp_files

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:
            copyfile.copy(src, dst, 5, src_offset=6, dst_offset=0)  # "world"

        with open(dst_path, 'rb') as f:
            assert f.read(5) == b'world'

    def test_data_integrity_same_file_shift(self):
        """Verify data integrity in same-file backward shift."""
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as f:
            original = b'XXXXXhello world'
            f.write(original)
            path = f.name

        try:
            with open(path, 'r+b') as f:
                copyfile.copy(f, f, 11, src_offset=5, dst_offset=0)

            with open(path, 'rb') as f:
                assert f.read(11) == b'hello world'
        finally:
            os.unlink(path)
