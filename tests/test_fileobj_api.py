"""Tests for Barecat file object API."""
import io
import tempfile
import os.path as osp

import pytest

from barecat import Barecat
from barecat.exceptions import FileNotFoundBarecatError, FileExistsBarecatError


class TestOpenRead:
    """Test open() for reading."""

    def test_open_read_basic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                with bc.open('file.txt', 'rb') as f:
                    assert f.read() == b'hello world'

    def test_open_read_partial(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                with bc.open('file.txt', 'rb') as f:
                    assert f.read(5) == b'hello'
                    assert f.read(6) == b' world'

    def test_open_read_missing_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['other.txt'] = b'data'

            with Barecat(path, readonly=True) as bc:
                with pytest.raises(FileNotFoundBarecatError):
                    bc.open('nonexistent.txt', 'rb')


class TestOpenWrite:
    """Test open() for writing."""

    def test_open_write_new_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                with bc.open('file.txt', 'wb') as f:
                    f.write(b'hello')

                assert bc['file.txt'] == b'hello'

    def test_open_write_overwrites(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'original'

                with bc.open('file.txt', 'wb') as f:
                    f.write(b'new')

                assert bc['file.txt'] == b'new'

    def test_open_exclusive_new_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                with bc.open('file.txt', 'xb') as f:
                    f.write(b'exclusive')

                assert bc['file.txt'] == b'exclusive'

    def test_open_exclusive_existing_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'existing'

                with pytest.raises(FileExistsBarecatError):
                    bc.open('file.txt', 'xb')


class TestOpenAppend:
    """Test open() for appending."""

    def test_open_append_existing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello'

                with bc.open('file.txt', 'ab') as f:
                    f.write(b' world')

                assert bc['file.txt'] == b'hello world'

    def test_open_append_new(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                with bc.open('new.txt', 'ab') as f:
                    f.write(b'appended')

                assert bc['new.txt'] == b'appended'


class TestOpenReadWrite:
    """Test open() for read/write modes."""

    def test_open_rplus_read_then_write(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

                with bc.open('file.txt', 'r+b') as f:
                    # Read first part
                    data = f.read(5)
                    assert data == b'hello'
                    # Overwrite rest
                    f.write(b' universe')

                assert bc['file.txt'] == b'hello universe'

    def test_open_wplus_write_then_read(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                with bc.open('file.txt', 'w+b') as f:
                    f.write(b'hello')
                    f.seek(0)
                    assert f.read() == b'hello'


class TestFileObjectSeekTell:
    """Test seek and tell operations on file objects."""

    def test_seek_from_start(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                with bc.open('file.txt', 'rb') as f:
                    f.seek(6)
                    assert f.read() == b'world'

    def test_seek_from_current(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                with bc.open('file.txt', 'rb') as f:
                    f.read(3)
                    f.seek(3, io.SEEK_CUR)
                    assert f.read() == b'world'

    def test_seek_from_end(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                with bc.open('file.txt', 'rb') as f:
                    f.seek(-5, io.SEEK_END)
                    assert f.read() == b'world'

    def test_tell(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                with bc.open('file.txt', 'rb') as f:
                    assert f.tell() == 0
                    f.read(5)
                    assert f.tell() == 5
                    f.seek(0)
                    assert f.tell() == 0


class TestReadinto:
    """Test readinto() method.

    Note: bc.readinto() requires memoryview when buffer is larger than file,
    as bytearray slicing creates a copy. Use memoryview for reliable behavior.
    """

    def test_readinto_basic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                # Use memoryview for reliable behavior
                buffer = bytearray(11)
                n = bc.readinto('file.txt', memoryview(buffer))
                assert n == 11
                assert bytes(buffer) == b'hello world'

    def test_readinto_with_offset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                buffer = bytearray(5)
                n = bc.readinto('file.txt', memoryview(buffer), offset=6)
                assert n == 5
                assert bytes(buffer) == b'world'

    def test_readinto_partial_buffer(self):
        """Buffer smaller than file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                buffer = bytearray(5)
                n = bc.readinto('file.txt', memoryview(buffer))
                assert n == 5
                assert bytes(buffer) == b'hello'

    def test_readinto_larger_buffer(self):
        """Buffer larger than file - requires memoryview."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello'

            with Barecat(path, readonly=True) as bc:
                buffer = bytearray(100)
                n = bc.readinto('file.txt', memoryview(buffer))
                assert n == 5
                assert bytes(buffer[:5]) == b'hello'


class TestReadMethod:
    """Test read() method on Barecat."""

    def test_read_basic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                assert bc.read('file.txt') == b'hello world'

    def test_read_with_offset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                assert bc.read('file.txt', offset=6) == b'world'

    def test_read_with_size(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                assert bc.read('file.txt', size=5) == b'hello'

    def test_read_with_offset_and_size(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                assert bc.read('file.txt', offset=3, size=5) == b'lo wo'


class TestFileObjectReadinto:
    """Test readinto on file objects."""

    def test_fileobj_readinto(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

            with Barecat(path, readonly=True) as bc:
                with bc.open('file.txt', 'rb') as f:
                    buffer = bytearray(5)
                    n = f.readinto(memoryview(buffer))
                    assert n == 5
                    assert bytes(buffer) == b'hello'

                    n = f.readinto(memoryview(buffer))
                    assert n == 5
                    assert bytes(buffer) == b' worl'

    def test_fileobj_readinto_at_eof(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hi'

            with Barecat(path, readonly=True) as bc:
                with bc.open('file.txt', 'rb') as f:
                    buffer = bytearray(10)
                    n = f.readinto(buffer)
                    assert n == 2
                    n = f.readinto(buffer)
                    assert n == 0


class TestFileObjectReadline:
    """Test readline on file objects."""

    def test_readline_basic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'line1\nline2\nline3'

            with Barecat(path, readonly=True) as bc:
                with bc.open('file.txt', 'rb') as f:
                    assert f.readline() == b'line1\n'
                    assert f.readline() == b'line2\n'
                    assert f.readline() == b'line3'

    def test_readline_with_limit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world\n'

            with Barecat(path, readonly=True) as bc:
                with bc.open('file.txt', 'rb') as f:
                    assert f.readline(5) == b'hello'


class TestFileObjectTruncate:
    """Test truncate on writable file objects."""

    def test_truncate_shrink(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

                with bc.open('file.txt', 'r+b') as f:
                    f.truncate(5)

                assert bc['file.txt'] == b'hello'

    def test_truncate_grow(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hi'

                with bc.open('file.txt', 'r+b') as f:
                    f.truncate(5)

                assert bc['file.txt'] == b'hi\x00\x00\x00'

    def test_truncate_at_position(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello world'

                with bc.open('file.txt', 'r+b') as f:
                    f.seek(5)
                    f.truncate()  # Truncate at current position

                assert bc['file.txt'] == b'hello'


class TestEmptyFile:
    """Test handling of empty files."""

    def test_write_empty_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                with bc.open('empty.txt', 'wb') as f:
                    pass  # Write nothing

                assert bc['empty.txt'] == b''

    def test_read_empty_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['empty.txt'] = b''

            with Barecat(path, readonly=True) as bc:
                with bc.open('empty.txt', 'rb') as f:
                    assert f.read() == b''

    def test_truncate_to_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'

                with bc.open('file.txt', 'r+b') as f:
                    f.truncate(0)

                assert bc['file.txt'] == b''


class TestFileObjectProperties:
    """Test file object properties and methods."""

    def test_readable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'test'

            with Barecat(path, readonly=True) as bc:
                with bc.open('file.txt', 'rb') as f:
                    assert f.readable() is True

    def test_writable_readonly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'test'

            with Barecat(path, readonly=True) as bc:
                with bc.open('file.txt', 'rb') as f:
                    assert f.writable() is False

    def test_writable_write_mode(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                with bc.open('file.txt', 'wb') as f:
                    assert f.writable() is True

    def test_len(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello'

            with Barecat(path, readonly=True) as bc:
                with bc.open('file.txt', 'rb') as f:
                    assert len(f) == 5


