"""Tests for Barecat dict-like API."""
import tempfile
import os.path as osp

import pytest

from barecat import Barecat


class TestGetSetDel:
    """Test __getitem__, __setitem__, __delitem__."""

    def test_setitem_getitem_basic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'hello'
                assert bc['file.txt'] == b'hello'

    def test_setitem_existing_raises(self):
        """setitem on existing file raises FileExistsBarecatError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'first'
                with pytest.raises(Exception):  # FileExistsBarecatError
                    bc['file.txt'] = b'second'

    def test_overwrite_via_delete_and_set(self):
        """To overwrite, delete first then set."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'first'
                del bc['file.txt']
                bc['file.txt'] = b'second'
                assert bc['file.txt'] == b'second'

    def test_getitem_missing_raises_keyerror(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                with pytest.raises(KeyError):
                    _ = bc['nonexistent.txt']

    def test_delitem(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                del bc['file.txt']
                with pytest.raises(KeyError):
                    _ = bc['file.txt']

    def test_delitem_missing_raises_keyerror(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                with pytest.raises(KeyError):
                    del bc['nonexistent.txt']


class TestContains:
    """Test __contains__ (in operator)."""

    def test_contains_existing_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                assert 'file.txt' in bc

    def test_contains_missing_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                assert 'nonexistent.txt' not in bc

    def test_contains_directory_is_false(self):
        """Directories are not 'in' the barecat (only files)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir/file.txt'] = b'content'
                # dir exists as directory but not as file
                assert 'dir' not in bc
                assert 'dir/file.txt' in bc


class TestLen:
    """Test __len__."""

    def test_len_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                assert len(bc) == 0

    def test_len_with_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file1.txt'] = b'a'
                bc['file2.txt'] = b'b'
                bc['dir/file3.txt'] = b'c'
                assert len(bc) == 3

    def test_len_after_delete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file1.txt'] = b'a'
                bc['file2.txt'] = b'b'
                del bc['file1.txt']
                assert len(bc) == 1


class TestIter:
    """Test __iter__."""

    def test_iter_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                assert list(bc) == []

    def test_iter_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['b.txt'] = b'b'
                bc['a.txt'] = b'a'
                bc['dir/c.txt'] = b'c'
                paths = list(bc)
                assert set(paths) == {'a.txt', 'b.txt', 'dir/c.txt'}


class TestGet:
    """Test get() method."""

    def test_get_existing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                assert bc.get('file.txt') == b'content'

    def test_get_missing_returns_none(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                assert bc.get('nonexistent.txt') is None

    def test_get_missing_with_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                assert bc.get('nonexistent.txt', b'default') == b'default'

    def test_get_existing_ignores_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                assert bc.get('file.txt', b'default') == b'content'


class TestSetdefault:
    """Test setdefault() method."""

    def test_setdefault_missing_sets_value(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                result = bc.setdefault('file.txt', b'default')
                assert result == b'default'
                assert bc['file.txt'] == b'default'

    def test_setdefault_existing_returns_existing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'existing'
                result = bc.setdefault('file.txt', b'default')
                assert result == b'existing'
                assert bc['file.txt'] == b'existing'

    def test_setdefault_empty_bytes_default(self):
        """setdefault with no default uses empty bytes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                result = bc.setdefault('file.txt', b'')
                assert result == b''
                assert bc['file.txt'] == b''


class TestItemsKeysValues:
    """Test items(), keys(), values()."""

    def test_keys_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                assert list(bc.keys()) == []

    def test_keys_with_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'a'
                bc['b.txt'] = b'b'
                keys = set(bc.keys())
                assert keys == {'a.txt', 'b.txt'}

    def test_values_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                assert list(bc.values()) == []

    def test_values_with_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'aaa'
                bc['b.txt'] = b'bbb'
                values = set(bc.values())
                assert values == {b'aaa', b'bbb'}

    def test_items_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                assert list(bc.items()) == []

    def test_items_with_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'aaa'
                bc['b.txt'] = b'bbb'
                items = dict(bc.items())
                assert items == {'a.txt': b'aaa', 'b.txt': b'bbb'}


class TestReadonlyMode:
    """Test that write operations fail in readonly mode."""

    def test_setitem_readonly_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'

            with Barecat(path, readonly=True) as bc:
                with pytest.raises(Exception):  # ReadOnlyBarecatError
                    bc['new.txt'] = b'data'

    def test_delitem_readonly_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'

            with Barecat(path, readonly=True) as bc:
                with pytest.raises(Exception):  # ReadOnlyBarecatError
                    del bc['file.txt']

    def test_setdefault_readonly_existing_ok(self):
        """setdefault on existing key doesn't write, so should be ok."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'

            with Barecat(path, readonly=True) as bc:
                # This should work since file exists
                result = bc.setdefault('file.txt', b'default')
                assert result == b'content'

    def test_setdefault_readonly_missing_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'

            with Barecat(path, readonly=True) as bc:
                with pytest.raises(Exception):  # ReadOnlyBarecatError
                    bc.setdefault('new.txt', b'default')


class TestAppendOnlyMode:
    """Test append_only mode restrictions."""

    def test_append_only_allows_new_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False, append_only=True) as bc:
                bc['file.txt'] = b'content'
                assert bc['file.txt'] == b'content'

    def test_append_only_prevents_overwrite(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'original'

            with Barecat(path, readonly=False, append_only=True) as bc:
                with pytest.raises(Exception):  # AppendOnlyBarecatError
                    bc['file.txt'] = b'overwrite'

    def test_append_only_prevents_delete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'

            with Barecat(path, readonly=False, append_only=True) as bc:
                with pytest.raises(Exception):  # AppendOnlyBarecatError
                    del bc['file.txt']
