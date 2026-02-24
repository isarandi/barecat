"""Tests for Index class features."""

import tempfile
import os.path as osp

import pytest

from barecat import Barecat
from barecat import BarecatDirInfo, Order


class TestIsFileIsDir:
    """Test isfile(), isdir(), exists()."""

    def test_isfile_existing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                assert bc.index.isfile('file.txt') is True

    def test_isfile_nonexistent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['other.txt'] = b'content'
                assert bc.index.isfile('missing.txt') is False

    def test_isfile_directory_returns_false(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir/file.txt'] = b'content'
                assert bc.index.isfile('dir') is False

    def test_isdir_existing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir/file.txt'] = b'content'
                assert bc.index.isdir('dir') is True

    def test_isdir_nonexistent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                assert bc.index.isdir('missing') is False

    def test_isdir_file_returns_false(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                assert bc.index.isdir('file.txt') is False

    def test_isdir_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                assert bc.index.isdir('') is True

    def test_exists_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                assert bc.index.exists('file.txt') is True

    def test_exists_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir/file.txt'] = b'content'
                assert bc.index.exists('dir') is True

    def test_exists_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                assert bc.index.exists('missing') is False


class TestListdir:
    """Test listdir_names() and listdir_infos()."""

    def test_listdir_names_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'a'
                bc['b.txt'] = b'b'
                bc['dir/c.txt'] = b'c'

                names = bc.index.listdir_names('')
                assert set(names) == {'a.txt', 'b.txt', 'dir'}

    def test_listdir_names_subdir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir/file1.txt'] = b'1'
                bc['dir/file2.txt'] = b'2'
                bc['dir/sub/file3.txt'] = b'3'

                names = bc.index.listdir_names('dir')
                assert set(names) == {'file1.txt', 'file2.txt', 'sub'}

    def test_listdir_infos(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir/file.txt'] = b'content'
                bc['dir/sub/nested.txt'] = b'nested'

                infos = bc.index.listdir_infos('dir')
                assert len(infos) == 2

                paths = {info.path for info in infos}
                assert paths == {'dir/file.txt', 'dir/sub'}

    def test_listdir_empty_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc.index.add_dir(BarecatDirInfo(path='emptydir'))
                names = bc.index.listdir_names('emptydir')
                assert names == []


class TestIterdir:
    """Test iterdir_names() and iterdir_infos()."""

    def test_iterdir_names(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'a'
                bc['b.txt'] = b'b'

                names = list(bc.index.iterdir_names(''))
                assert set(names) == {'a.txt', 'b.txt'}

    def test_iterdir_infos(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                bc['dir/nested.txt'] = b'nested'

                infos = list(bc.index.iterdir_infos(''))
                assert len(infos) == 2


class TestGlobPaths:
    """Test glob_paths() and iterglob_paths()."""

    def test_glob_star(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file1.txt'] = b'1'
                bc['file2.txt'] = b'2'
                bc['other.py'] = b'3'

                paths = bc.index.glob_paths('*.txt')
                assert set(paths) == {'file1.txt', 'file2.txt'}

    def test_glob_question_mark(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['f1.txt'] = b'1'
                bc['f2.txt'] = b'2'
                bc['f10.txt'] = b'10'

                paths = bc.index.glob_paths('f?.txt')
                assert set(paths) == {'f1.txt', 'f2.txt'}

    def test_glob_subdirectory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['src/a.py'] = b'a'
                bc['src/b.py'] = b'b'
                bc['tests/c.py'] = b'c'

                paths = bc.index.glob_paths('src/*.py')
                assert set(paths) == {'src/a.py', 'src/b.py'}

    def test_glob_recursive(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'a'
                bc['dir/b.txt'] = b'b'
                bc['dir/sub/c.txt'] = b'c'

                # **/*.txt matches files in subdirectories
                paths = bc.index.glob_paths('**/*.txt', recursive=True)
                # Root level files may or may not be included based on glob implementation
                assert 'dir/b.txt' in paths
                assert 'dir/sub/c.txt' in paths

    def test_glob_recursive_all(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'a'
                bc['dir/b.txt'] = b'b'
                bc['dir/sub/c.txt'] = b'c'

                # ** should match all paths
                paths = bc.index.glob_paths('**', recursive=True)
                assert 'a.txt' in paths
                assert 'dir/b.txt' in paths
                assert 'dir/sub/c.txt' in paths

    def test_glob_only_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir/file.txt'] = b'content'
                bc['dir2/nested/file.txt'] = b'nested'

                # Without only_files, should include dirs
                paths = bc.index.glob_paths('*')
                assert 'dir' in paths
                assert 'dir2' in paths

                # With only_files, should exclude dirs
                paths = bc.index.glob_paths('*', only_files=True)
                assert 'dir' not in paths
                assert 'dir2' not in paths

    def test_iterglob_paths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'a'
                bc['b.txt'] = b'b'

                paths = list(bc.index.iterglob_paths('*.txt'))
                assert set(paths) == {'a.txt', 'b.txt'}


class TestIterAllFileinfos:
    """Test iter_all_fileinfos() and iter_all_dirinfos()."""

    def test_iter_all_fileinfos(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'aaa'
                bc['dir/b.txt'] = b'bbb'

                infos = list(bc.index.iter_all_fileinfos())
                assert len(infos) == 2

                paths = {info.path for info in infos}
                assert paths == {'a.txt', 'dir/b.txt'}

    def test_iter_all_fileinfos_with_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['b.txt'] = b'b'
                bc['a.txt'] = b'a'

                infos = list(bc.index.iter_all_fileinfos(order=Order.PATH))
                paths = [info.path for info in infos]
                assert paths == ['a.txt', 'b.txt']

    def test_iter_all_dirinfos(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir1/file.txt'] = b'1'
                bc['dir2/file.txt'] = b'2'

                infos = list(bc.index.iter_all_dirinfos())
                paths = {info.path for info in infos}
                # Should include root '' and the two dirs
                assert '' in paths
                assert 'dir1' in paths
                assert 'dir2' in paths


class TestIterAllPaths:
    """Test iter_all_filepaths() and iter_all_paths()."""

    def test_iter_all_filepaths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'a'
                bc['dir/b.txt'] = b'b'

                paths = list(bc.index.iter_all_filepaths())
                assert set(paths) == {'a.txt', 'dir/b.txt'}

    def test_iter_all_paths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                bc['dir/nested.txt'] = b'nested'

                paths = list(bc.index.iter_all_paths())
                # Should include files and dirs (including root)
                assert 'file.txt' in paths
                assert 'dir/nested.txt' in paths
                assert 'dir' in paths
                assert '' in paths


class TestWalk:
    """Test walk_names() and walk_infos()."""

    def test_walk_names(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'a'
                bc['dir/b.txt'] = b'b'
                bc['dir/sub/c.txt'] = b'c'

                results = list(bc.index.walk_names(''))
                # Should be (dirpath, dirnames, filenames) tuples
                assert len(results) == 3

                root_entry = next(r for r in results if r[0] == '.')
                assert 'a.txt' in root_entry[2]
                assert 'dir' in root_entry[1]

    def test_walk_infos(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                bc['dir/nested.txt'] = b'nested'

                results = list(bc.index.walk_infos(''))
                assert len(results) == 2

                # Each result should be (BarecatDirInfo, list[BarecatDirInfo], list[BarecatFileInfo])
                for dirinfo, subdirs, files in results:
                    assert isinstance(dirinfo, BarecatDirInfo)


class TestLookupFile:
    """Test lookup_file() and lookup_dir()."""

    def test_lookup_file_existing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'

                finfo = bc.index.lookup_file('file.txt')
                assert finfo.path == 'file.txt'
                assert finfo.size == 7

    def test_lookup_file_missing_raises(self):
        from barecat.exceptions import FileNotFoundBarecatError

        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['other.txt'] = b'content'

                with pytest.raises(FileNotFoundBarecatError):
                    bc.index.lookup_file('missing.txt')

    def test_lookup_dir_existing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir/file.txt'] = b'content'

                dinfo = bc.index.lookup_dir('dir')
                assert dinfo.path == 'dir'
                assert dinfo.num_files == 1

    def test_lookup_dir_missing_raises(self):
        from barecat.exceptions import FileNotFoundBarecatError

        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'

                with pytest.raises(FileNotFoundBarecatError):
                    bc.index.lookup_dir('missing')


class TestTotalSize:
    """Test total_size property."""

    def test_total_size(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'12345'  # 5 bytes
                bc['b.txt'] = b'123'  # 3 bytes

                assert bc.index.total_size == 8


class TestNumFiles:
    """Test num_files property."""

    def test_num_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'a'
                bc['b.txt'] = b'b'
                bc['dir/c.txt'] = b'c'

                assert bc.index.num_files == 3


class TestPathNormalization:
    """Test that paths are normalized."""

    def test_leading_slash_stripped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['/file.txt'] = b'content'
                assert bc.index.isfile('file.txt') is True
                assert bc.index.isfile('/file.txt') is True

    def test_trailing_slash_stripped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir/file.txt'] = b'content'
                assert bc.index.isdir('dir/') is True

    def test_double_slash_normalized(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir//file.txt'] = b'content'
                assert bc.index.isfile('dir/file.txt') is True


class TestOrder:
    """Test Order enum functionality."""

    def test_order_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['c.txt'] = b'c'
                bc['a.txt'] = b'a'
                bc['b.txt'] = b'b'

                _paths = list(bc.index.iterglob_paths('*', only_files=True))

                # With ORDER.PATH
                ordered = [info.path for info in bc.index.iter_all_fileinfos(order=Order.PATH)]
                assert ordered == ['a.txt', 'b.txt', 'c.txt']

    def test_order_path_desc(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'a'
                bc['b.txt'] = b'b'
                bc['c.txt'] = b'c'

                ordered = [
                    info.path
                    for info in bc.index.iter_all_fileinfos(order=Order.PATH | Order.DESC)
                ]
                assert ordered == ['c.txt', 'b.txt', 'a.txt']


class TestDirectFileInfoLookup:
    """Test list_direct_fileinfos() and iter_direct_fileinfos()."""

    def test_list_direct_fileinfos(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['a.txt'] = b'a'
                bc['dir/b.txt'] = b'b'
                bc['dir/c.txt'] = b'c'

                infos = bc.index.list_direct_fileinfos('dir')
                paths = {info.path for info in infos}
                assert paths == {'dir/b.txt', 'dir/c.txt'}

    def test_iter_direct_fileinfos(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir/a.txt'] = b'a'
                bc['dir/b.txt'] = b'b'

                infos = list(bc.index.iter_direct_fileinfos('dir'))
                assert len(infos) == 2


class TestSubdirInfoLookup:
    """Test list_subdir_dirinfos() and iter_subdir_dirinfos()."""

    def test_list_subdir_dirinfos(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir/sub1/file.txt'] = b'1'
                bc['dir/sub2/file.txt'] = b'2'

                infos = bc.index.list_subdir_dirinfos('dir')
                paths = {info.path for info in infos}
                assert paths == {'dir/sub1', 'dir/sub2'}

    def test_iter_subdir_dirinfos(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['dir/sub1/file.txt'] = b'1'
                bc['dir/sub2/file.txt'] = b'2'

                infos = list(bc.index.iter_subdir_dirinfos('dir'))
                assert len(infos) == 2


class TestVerifyIntegrity:
    """Test verify_integrity()."""

    def test_verify_integrity_passes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'
                bc['dir/nested.txt'] = b'nested'

            with Barecat(path, readonly=True) as bc:
                result = bc.index.verify_integrity()
                assert result is True
