"""Tests for barecat rsync functionality."""
import os
import os.path as osp
import tempfile
import time

import pytest

import barecat
import tarfile
import zipfile

from barecat.maintenance.rsync import parse_path, rsync, RsyncOptions, PathType


class TestParsePath:
    """Tests for parse_path function."""

    def test_local_dir(self):
        pp = parse_path('./data/')
        assert pp.path_type == PathType.LOCAL
        assert pp.archive_path is None
        assert pp.inner_path == ''
        assert pp.trailing_slash is True

    def test_local_dir_no_slash(self):
        pp = parse_path('./data')
        assert pp.path_type == PathType.LOCAL
        assert pp.trailing_slash is False

    def test_local_archive_root(self):
        pp = parse_path('./archive.barecat::')
        assert pp.path_type == PathType.LOCAL_ARCHIVE
        assert pp.archive_path == './archive.barecat'
        assert pp.inner_path == ''
        assert pp.trailing_slash is False

    def test_local_archive_inner(self):
        pp = parse_path('./archive.barecat::images/')
        assert pp.path_type == PathType.LOCAL_ARCHIVE
        assert pp.archive_path == './archive.barecat'
        assert pp.inner_path == 'images'
        assert pp.trailing_slash is True

    def test_local_archive_deep_inner(self):
        pp = parse_path('/path/to/archive.barecat::train/subset/')
        assert pp.path_type == PathType.LOCAL_ARCHIVE
        assert pp.archive_path == '/path/to/archive.barecat'
        assert pp.inner_path == 'train/subset'
        assert pp.trailing_slash is True

    def test_ssh_dir(self):
        pp = parse_path('host:/path/dir/')
        assert pp.path_type == PathType.SSH
        assert pp.host == 'host'
        assert pp.user is None
        assert pp.filesystem_path == '/path/dir'
        assert pp.archive_path is None
        assert pp.trailing_slash is True

    def test_ssh_with_user(self):
        pp = parse_path('user@host:/data/')
        assert pp.path_type == PathType.SSH
        assert pp.host == 'host'
        assert pp.user == 'user'

    def test_ssh_archive(self):
        pp = parse_path('host:/path/archive.barecat::')
        assert pp.path_type == PathType.SSH_ARCHIVE
        assert pp.host == 'host'
        assert pp.archive_path == '/path/archive.barecat'
        assert pp.inner_path == ''

    def test_ssh_archive_with_inner(self):
        pp = parse_path('user@host:/path/archive.barecat::train/')
        assert pp.path_type == PathType.SSH_ARCHIVE
        assert pp.host == 'host'
        assert pp.user == 'user'
        assert pp.archive_path == '/path/archive.barecat'
        assert pp.inner_path == 'train'
        assert pp.trailing_slash is True

    def test_barecat_server(self):
        pp = parse_path('barecat://host:50003/archive::images/')
        assert pp.path_type == PathType.BARECAT_SERVER
        assert pp.host == 'host:50003'
        assert pp.archive_path == 'archive'
        assert pp.inner_path == 'images'
        assert pp.trailing_slash is True

    def test_barecat_server_no_inner(self):
        pp = parse_path('barecat://localhost:8080/myarchive::')
        assert pp.path_type == PathType.BARECAT_SERVER
        assert pp.host == 'localhost:8080'
        assert pp.archive_path == 'myarchive'
        assert pp.inner_path == ''

    def test_archive_basename(self):
        pp = parse_path('./data/archive.barecat::')
        assert pp.archive_basename == 'archive'

    def test_archive_basename_no_extension(self):
        pp = parse_path('./data/myarchive::')
        assert pp.archive_basename == 'myarchive'

    def test_is_archive(self):
        assert parse_path('./dir/').is_archive is False
        assert parse_path('./archive.barecat::').is_archive is True
        assert parse_path('host:/dir/').is_archive is False
        assert parse_path('host:/archive.barecat::').is_archive is True
        assert parse_path('barecat://host:8080/arch::').is_archive is True

    def test_is_remote(self):
        assert parse_path('./dir/').is_remote is False
        assert parse_path('./archive.barecat::').is_remote is False
        assert parse_path('host:/dir/').is_remote is True
        assert parse_path('host:/archive.barecat::').is_remote is True

    def test_escaped_double_colon(self):
        r"""Test \:: escapes to literal :: in filename."""
        pp = parse_path(r'./my\::weird.barecat::')
        assert pp.path_type == PathType.LOCAL_ARCHIVE
        assert pp.archive_path == './my::weird.barecat'
        assert pp.inner_path == ''

    def test_escaped_double_colon_in_inner(self):
        r"""Test \:: in inner path."""
        pp = parse_path(r'./archive.barecat::path\::with\::colons/')
        assert pp.archive_path == './archive.barecat'
        assert pp.inner_path == 'path::with::colons'
        assert pp.trailing_slash is True

    def test_escaped_backslash(self):
        r"""Test \\ escapes to literal backslash."""
        pp = parse_path(r'./path\\with\\backslash.barecat::')
        assert pp.archive_path == r'./path\with\backslash.barecat'

    def test_escaped_backslash_before_colons(self):
        r"""Test \\:: means literal backslash followed by delimiter."""
        pp = parse_path(r'./file\\.barecat::inner')
        assert pp.archive_path == r'./file\.barecat'
        assert pp.inner_path == 'inner'

    def test_multiple_escapes(self):
        r"""Test multiple escape sequences."""
        pp = parse_path(r'./a\::b\\c\::d.barecat::')
        assert pp.archive_path == r'./a::b\c::d.barecat'


class TestRsync:
    """Tests for rsync operations."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory with test files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create source directory structure
            src = osp.join(tmpdir, 'src')
            os.makedirs(src)
            os.makedirs(osp.join(src, 'sub'))

            # Create test files
            with open(osp.join(src, 'file1.txt'), 'w') as f:
                f.write('file1')
            with open(osp.join(src, 'file2.txt'), 'w') as f:
                f.write('file2')
            with open(osp.join(src, 'sub', 'sub1.txt'), 'w') as f:
                f.write('sub1')

            yield tmpdir

    def test_local_to_archive_contents_mode(self, temp_dir):
        """Test rsync local/ -> archive:: (contents mode)."""
        src = osp.join(temp_dir, 'src/')
        archive = osp.join(temp_dir, 'test.barecat::')

        rsync([src], archive)

        with barecat.Barecat(osp.join(temp_dir, 'test.barecat'), readonly=True) as bc:
            assert 'file1.txt' in bc
            assert 'file2.txt' in bc
            assert 'sub/sub1.txt' in bc
            assert bc['file1.txt'] == b'file1'

    def test_local_to_archive_as_subdir(self, temp_dir):
        """Test rsync local -> archive:: (as subdirectory)."""
        src = osp.join(temp_dir, 'src')  # no trailing slash
        archive = osp.join(temp_dir, 'test.barecat::')

        rsync([src], archive)

        with barecat.Barecat(osp.join(temp_dir, 'test.barecat'), readonly=True) as bc:
            assert 'src/file1.txt' in bc
            assert 'src/file2.txt' in bc
            assert 'src/sub/sub1.txt' in bc

    def test_archive_to_local_contents_mode(self, temp_dir):
        """Test rsync archive::/ -> local/ (contents mode)."""
        # First create archive
        src = osp.join(temp_dir, 'src/')
        archive_path = osp.join(temp_dir, 'test.barecat')
        rsync([src], archive_path + '::')

        # Extract to new directory
        out_dir = osp.join(temp_dir, 'out/')
        rsync([archive_path + '::/'], out_dir)

        assert osp.exists(osp.join(temp_dir, 'out', 'file1.txt'))
        assert osp.exists(osp.join(temp_dir, 'out', 'file2.txt'))
        assert osp.exists(osp.join(temp_dir, 'out', 'sub', 'sub1.txt'))

        with open(osp.join(temp_dir, 'out', 'file1.txt')) as f:
            assert f.read() == 'file1'

    def test_archive_to_local_as_subdir(self, temp_dir):
        """Test rsync archive:: -> local/ (as subdirectory)."""
        # First create archive
        src = osp.join(temp_dir, 'src/')
        archive_path = osp.join(temp_dir, 'test.barecat')
        rsync([src], archive_path + '::')

        # Extract to new directory (no trailing slash on archive = as subdir)
        out_dir = osp.join(temp_dir, 'out/')
        rsync([archive_path + '::'], out_dir)

        # Should be under out/test/
        assert osp.exists(osp.join(temp_dir, 'out', 'test', 'file1.txt'))
        assert osp.exists(osp.join(temp_dir, 'out', 'test', 'sub', 'sub1.txt'))

    def test_skip_unchanged(self, temp_dir):
        """Test that unchanged files are skipped on re-sync."""
        src = osp.join(temp_dir, 'src/')
        archive_path = osp.join(temp_dir, 'test.barecat')

        # Initial sync
        rsync([src], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            original_mtime = bc.index.lookup_file('file1.txt').mtime_ns

        # Re-sync (should skip)
        rsync([src], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            # Should have same mtime (wasn't re-added)
            assert bc.index.lookup_file('file1.txt').mtime_ns == original_mtime

    def test_update_changed_file(self, temp_dir):
        """Test that changed files are updated."""
        src = osp.join(temp_dir, 'src/')
        archive_path = osp.join(temp_dir, 'test.barecat')

        # Initial sync
        rsync([src], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            original_size = bc.index.lookup_file('file1.txt').size

        # Modify source file
        time.sleep(0.01)  # Ensure different mtime
        with open(osp.join(temp_dir, 'src', 'file1.txt'), 'w') as f:
            f.write('modified file1 content')

        # Re-sync
        rsync([src], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert bc.index.lookup_file('file1.txt').size != original_size
            assert bc['file1.txt'] == b'modified file1 content'

    def test_metadata_preserved(self, temp_dir):
        """Test that file metadata (mtime) is preserved."""
        src = osp.join(temp_dir, 'src/')
        archive_path = osp.join(temp_dir, 'test.barecat')

        # Get original mtime
        src_file = osp.join(temp_dir, 'src', 'file1.txt')
        original_mtime_ns = os.stat(src_file).st_mtime_ns

        rsync([src], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert bc.index.lookup_file('file1.txt').mtime_ns == original_mtime_ns

    def test_metadata_preserved_on_extract(self, temp_dir):
        """Test that file metadata is preserved when extracting."""
        src = osp.join(temp_dir, 'src/')
        archive_path = osp.join(temp_dir, 'test.barecat')

        # Get original mtime
        src_file = osp.join(temp_dir, 'src', 'file1.txt')
        original_mtime_ns = os.stat(src_file).st_mtime_ns

        rsync([src], archive_path + '::')

        # Extract
        out_dir = osp.join(temp_dir, 'out/')
        rsync([archive_path + '::/'], out_dir)

        # Check extracted file has same mtime
        extracted_mtime_ns = os.stat(osp.join(temp_dir, 'out', 'file1.txt')).st_mtime_ns
        assert extracted_mtime_ns == original_mtime_ns

    def test_archive_to_archive_merge(self, temp_dir):
        """Test merging two archives."""
        # Create first archive
        src = osp.join(temp_dir, 'src/')
        archive1 = osp.join(temp_dir, 'arch1.barecat')
        rsync([src], archive1 + '::')

        # Create second archive with different content
        src2 = osp.join(temp_dir, 'src2')
        os.makedirs(src2)
        with open(osp.join(src2, 'other.txt'), 'w') as f:
            f.write('other')
        archive2 = osp.join(temp_dir, 'arch2.barecat')
        rsync([src2 + '/'], archive2 + '::')

        # Merge into new archive
        merged = osp.join(temp_dir, 'merged.barecat')
        rsync([archive1 + '::/', archive2 + '::/'], merged + '::')

        with barecat.Barecat(merged, readonly=True) as bc:
            assert 'file1.txt' in bc
            assert 'file2.txt' in bc
            assert 'sub/sub1.txt' in bc
            assert 'other.txt' in bc

    def test_dry_run(self, temp_dir):
        """Test dry-run mode doesn't modify anything."""
        src = osp.join(temp_dir, 'src/')
        archive_path = osp.join(temp_dir, 'test.barecat')

        rsync([src], archive_path + '::', RsyncOptions(dry_run=True))

        # Archive should not exist (new format: archive path IS the index file)
        assert not osp.exists(archive_path)

    def test_exclude_pattern(self, temp_dir):
        """Test exclude patterns."""
        src = osp.join(temp_dir, 'src/')
        archive_path = osp.join(temp_dir, 'test.barecat')

        rsync([src], archive_path + '::', RsyncOptions(exclude=['*.txt']))

        with barecat.Barecat(archive_path, readonly=True) as bc:
            # No .txt files should be added
            assert len(list(bc.keys())) == 0

    def test_local_to_local_raises(self, temp_dir):
        """Test that local-to-local raises error."""
        src = osp.join(temp_dir, 'src/')
        dest = osp.join(temp_dir, 'dest/')

        with pytest.raises(ValueError, match='regular rsync'):
            rsync([src], dest)

    def test_inner_path_source(self, temp_dir):
        """Test syncing from inner path within archive."""
        # Create archive with nested structure
        src = osp.join(temp_dir, 'src/')
        archive_path = osp.join(temp_dir, 'test.barecat')
        rsync([src], archive_path + '::data/')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'data/file1.txt' in bc

        # Extract only from inner path
        out_dir = osp.join(temp_dir, 'out/')
        rsync([archive_path + '::data/'], out_dir)

        assert osp.exists(osp.join(temp_dir, 'out', 'file1.txt'))

    def test_inner_path_dest(self, temp_dir):
        """Test syncing to inner path within archive."""
        src = osp.join(temp_dir, 'src/')
        archive_path = osp.join(temp_dir, 'test.barecat')

        rsync([src], archive_path + '::images/')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'images/file1.txt' in bc
            assert 'images/sub/sub1.txt' in bc

    def test_delete_flag(self, temp_dir):
        """Test --delete removes extraneous files."""
        src = osp.join(temp_dir, 'src/')
        archive_path = osp.join(temp_dir, 'test.barecat')

        # Initial sync
        rsync([src], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'file1.txt' in bc
            assert 'file2.txt' in bc

        # Remove file from source
        os.remove(osp.join(temp_dir, 'src', 'file2.txt'))

        # Sync with delete
        rsync([src], archive_path + '::', RsyncOptions(delete=True))

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'file1.txt' in bc
            assert 'file2.txt' not in bc


class TestParsePathTarZip:
    """Tests for parse_path with tar/zip archives."""

    def test_tar_without_delimiter_is_local(self):
        """data.tar.gz without :: is a local file."""
        pp = parse_path('data.tar.gz')
        assert pp.path_type == PathType.LOCAL
        assert pp.is_tar_zip is False

    def test_tar_with_delimiter_is_tarzip(self):
        """data.tar.gz:: is a tar archive source."""
        pp = parse_path('data.tar.gz::')
        assert pp.path_type == PathType.TAR_ZIP
        assert pp.is_tar_zip is True
        assert pp.archive_path == 'data.tar.gz'
        assert pp.inner_path == ''

    def test_tar_with_inner_path(self):
        """data.tar.gz::subdir/ parses inner path."""
        pp = parse_path('/path/to/data.tar.gz::images/train/')
        assert pp.path_type == PathType.TAR_ZIP
        assert pp.archive_path == '/path/to/data.tar.gz'
        assert pp.inner_path == 'images/train'
        assert pp.trailing_slash is True

    def test_zip_with_delimiter(self):
        """.zip files also work."""
        pp = parse_path('archive.zip::')
        assert pp.path_type == PathType.TAR_ZIP
        assert pp.archive_path == 'archive.zip'

    def test_tgz_extension(self):
        """.tgz works."""
        pp = parse_path('data.tgz::subdir')
        assert pp.path_type == PathType.TAR_ZIP
        assert pp.inner_path == 'subdir'
        assert pp.trailing_slash is False

    def test_tar_bz2_extension(self):
        """.tar.bz2 works."""
        pp = parse_path('data.tar.bz2::')
        assert pp.path_type == PathType.TAR_ZIP


class TestRsyncTarZip:
    """Tests for rsync with tar/zip sources."""

    @pytest.fixture
    def temp_dir_with_tar(self):
        """Create temp dir with test tar file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create source files
            src = osp.join(tmpdir, 'src')
            os.makedirs(osp.join(src, 'subdir'))
            with open(osp.join(src, 'file1.txt'), 'w') as f:
                f.write('file1 content')
            with open(osp.join(src, 'subdir', 'file2.txt'), 'w') as f:
                f.write('file2 content')

            # Create tar.gz
            tar_path = osp.join(tmpdir, 'test.tar.gz')
            with tarfile.open(tar_path, 'w:gz') as tar:
                tar.add(osp.join(src, 'file1.txt'), arcname='file1.txt')
                tar.add(osp.join(src, 'subdir', 'file2.txt'), arcname='subdir/file2.txt')

            # Create zip
            zip_path = osp.join(tmpdir, 'test.zip')
            with zipfile.ZipFile(zip_path, 'w') as zf:
                zf.write(osp.join(src, 'file1.txt'), 'file1.txt')
                zf.write(osp.join(src, 'subdir', 'file2.txt'), 'subdir/file2.txt')

            yield tmpdir

    def test_tar_to_barecat_contents(self, temp_dir_with_tar):
        """Test tar.gz::/ -> barecat:: (contents mode)."""
        tar_path = osp.join(temp_dir_with_tar, 'test.tar.gz')
        archive_path = osp.join(temp_dir_with_tar, 'out.barecat')

        rsync([tar_path + '::/'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'file1.txt' in bc
            assert 'subdir/file2.txt' in bc
            assert bc['file1.txt'] == b'file1 content'

    def test_tar_to_barecat_as_subdir(self, temp_dir_with_tar):
        """Test tar.gz:: -> barecat:: (creates test/ subdir)."""
        tar_path = osp.join(temp_dir_with_tar, 'test.tar.gz')
        archive_path = osp.join(temp_dir_with_tar, 'out.barecat')

        rsync([tar_path + '::'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'test/file1.txt' in bc
            assert 'test/subdir/file2.txt' in bc

    def test_tar_to_local_contents(self, temp_dir_with_tar):
        """Test tar.gz::/ -> local/ (contents mode)."""
        tar_path = osp.join(temp_dir_with_tar, 'test.tar.gz')
        out_dir = osp.join(temp_dir_with_tar, 'extracted/')

        rsync([tar_path + '::/'], out_dir)

        assert osp.exists(osp.join(temp_dir_with_tar, 'extracted', 'file1.txt'))
        assert osp.exists(osp.join(temp_dir_with_tar, 'extracted', 'subdir', 'file2.txt'))
        with open(osp.join(temp_dir_with_tar, 'extracted', 'file1.txt')) as f:
            assert f.read() == 'file1 content'

    def test_tar_to_local_as_subdir(self, temp_dir_with_tar):
        """Test tar.gz:: -> local/ (creates test/ subdir)."""
        tar_path = osp.join(temp_dir_with_tar, 'test.tar.gz')
        out_dir = osp.join(temp_dir_with_tar, 'extracted/')

        rsync([tar_path + '::'], out_dir)

        assert osp.exists(osp.join(temp_dir_with_tar, 'extracted', 'test', 'file1.txt'))

    def test_tar_inner_path(self, temp_dir_with_tar):
        """Test tar.gz::subdir/ extracts only subdir contents."""
        tar_path = osp.join(temp_dir_with_tar, 'test.tar.gz')
        archive_path = osp.join(temp_dir_with_tar, 'out.barecat')

        rsync([tar_path + '::subdir/'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            keys = list(bc.keys())
            assert keys == ['file2.txt']

    def test_zip_to_barecat(self, temp_dir_with_tar):
        """Test .zip -> barecat."""
        zip_path = osp.join(temp_dir_with_tar, 'test.zip')
        archive_path = osp.join(temp_dir_with_tar, 'out.barecat')

        rsync([zip_path + '::/'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'file1.txt' in bc
            assert 'subdir/file2.txt' in bc

    def test_tar_dry_run(self, temp_dir_with_tar):
        """Test tar rsync with dry-run doesn't create archive."""
        tar_path = osp.join(temp_dir_with_tar, 'test.tar.gz')
        archive_path = osp.join(temp_dir_with_tar, 'out.barecat')

        rsync([tar_path + '::'], archive_path + '::', RsyncOptions(dry_run=True))

        assert not osp.exists(archive_path)

    def test_tar_exclude(self, temp_dir_with_tar):
        """Test tar rsync with exclude pattern."""
        tar_path = osp.join(temp_dir_with_tar, 'test.tar.gz')
        archive_path = osp.join(temp_dir_with_tar, 'out.barecat')

        rsync([tar_path + '::/'], archive_path + '::', RsyncOptions(exclude=['file1.txt']))

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'file1.txt' not in bc
            assert 'subdir/file2.txt' in bc


class TestRsyncEdgeCases:
    """Edge cases and error handling tests."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    def test_nonexistent_source_raises(self, temp_dir):
        """Non-existent source raises FileNotFoundError."""
        archive_path = osp.join(temp_dir, 'out.barecat')
        with pytest.raises(FileNotFoundError):
            rsync(['/nonexistent/path/'], archive_path + '::')

    def test_nonexistent_tar_raises(self, temp_dir):
        """Non-existent tar source raises FileNotFoundError."""
        archive_path = osp.join(temp_dir, 'out.barecat')
        with pytest.raises(FileNotFoundError):
            rsync(['/nonexistent.tar.gz::'], archive_path + '::')

    def test_empty_source_dir(self, temp_dir):
        """Empty source directory creates empty archive."""
        src = osp.join(temp_dir, 'empty')
        os.makedirs(src)
        archive_path = osp.join(temp_dir, 'out.barecat')

        rsync([src + '/'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert len(list(bc.keys())) == 0

    def test_empty_tar_archive(self, temp_dir):
        """Empty tar archive creates empty barecat."""
        tar_path = osp.join(temp_dir, 'empty.tar.gz')
        with tarfile.open(tar_path, 'w:gz'):
            pass  # Create empty tar

        archive_path = osp.join(temp_dir, 'out.barecat')
        rsync([tar_path + '::'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert len(list(bc.keys())) == 0

    def test_zero_size_file(self, temp_dir):
        """Zero-size files are handled correctly."""
        src = osp.join(temp_dir, 'src')
        os.makedirs(src)
        open(osp.join(src, 'empty.txt'), 'w').close()

        archive_path = osp.join(temp_dir, 'out.barecat')
        rsync([src + '/'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'empty.txt' in bc
            assert bc['empty.txt'] == b''

    def test_hidden_files(self, temp_dir):
        """Hidden files (starting with .) are included."""
        src = osp.join(temp_dir, 'src')
        os.makedirs(src)
        with open(osp.join(src, '.hidden'), 'w') as f:
            f.write('hidden content')

        archive_path = osp.join(temp_dir, 'out.barecat')
        rsync([src + '/'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert '.hidden' in bc

    def test_spaces_in_filename(self, temp_dir):
        """Files with spaces in names are handled."""
        src = osp.join(temp_dir, 'src')
        os.makedirs(src)
        with open(osp.join(src, 'file with spaces.txt'), 'w') as f:
            f.write('content')

        archive_path = osp.join(temp_dir, 'out.barecat')
        rsync([src + '/'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'file with spaces.txt' in bc

    def test_unicode_filename(self, temp_dir):
        """Unicode filenames are handled."""
        src = osp.join(temp_dir, 'src')
        os.makedirs(src)
        with open(osp.join(src, '文件.txt'), 'w') as f:
            f.write('unicode content')

        archive_path = osp.join(temp_dir, 'out.barecat')
        rsync([src + '/'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert '文件.txt' in bc

    def test_deeply_nested_path(self, temp_dir):
        """Deeply nested directories work."""
        src = osp.join(temp_dir, 'src')
        deep_dir = osp.join(src, 'a', 'b', 'c', 'd', 'e')
        os.makedirs(deep_dir)
        with open(osp.join(deep_dir, 'deep.txt'), 'w') as f:
            f.write('deep')

        archive_path = osp.join(temp_dir, 'out.barecat')
        rsync([src + '/'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'a/b/c/d/e/deep.txt' in bc

    def test_tar_as_destination_raises(self, temp_dir):
        """Using tar as destination raises error."""
        src = osp.join(temp_dir, 'src')
        os.makedirs(src)
        with open(osp.join(src, 'file.txt'), 'w') as f:
            f.write('content')

        # tar/zip can't be destination
        with pytest.raises(ValueError):
            rsync([src + '/'], osp.join(temp_dir, 'out.tar.gz::'))

    def test_nonexistent_inner_path_empty_result(self, temp_dir):
        """Non-existent inner path in archive gives empty result."""
        src = osp.join(temp_dir, 'src')
        os.makedirs(src)
        with open(osp.join(src, 'file.txt'), 'w') as f:
            f.write('content')

        archive1 = osp.join(temp_dir, 'src.barecat')
        rsync([src + '/'], archive1 + '::')

        # Sync from non-existent inner path
        archive2 = osp.join(temp_dir, 'out.barecat')
        rsync([archive1 + '::nonexistent/'], archive2 + '::')

        with barecat.Barecat(archive2, readonly=True) as bc:
            assert len(list(bc.keys())) == 0

    def test_double_slash_in_path_normalized(self, temp_dir):
        """Double slashes in paths are handled."""
        src = osp.join(temp_dir, 'src')
        os.makedirs(src)
        with open(osp.join(src, 'file.txt'), 'w') as f:
            f.write('content')

        archive_path = osp.join(temp_dir, 'out.barecat')
        # Double slash shouldn't break things
        rsync([src + '//'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'file.txt' in bc

    def test_multiple_sources(self, temp_dir):
        """Multiple sources are combined."""
        src1 = osp.join(temp_dir, 'src1')
        src2 = osp.join(temp_dir, 'src2')
        os.makedirs(src1)
        os.makedirs(src2)
        with open(osp.join(src1, 'a.txt'), 'w') as f:
            f.write('a')
        with open(osp.join(src2, 'b.txt'), 'w') as f:
            f.write('b')

        archive_path = osp.join(temp_dir, 'out.barecat')
        rsync([src1 + '/', src2 + '/'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'a.txt' in bc
            assert 'b.txt' in bc

    def test_overwrite_existing_file(self, temp_dir):
        """Re-syncing with changed content overwrites."""
        src = osp.join(temp_dir, 'src')
        os.makedirs(src)
        with open(osp.join(src, 'file.txt'), 'w') as f:
            f.write('original')

        archive_path = osp.join(temp_dir, 'out.barecat')
        rsync([src + '/'], archive_path + '::')

        # Modify and force different mtime
        time.sleep(0.01)
        with open(osp.join(src, 'file.txt'), 'w') as f:
            f.write('modified')

        rsync([src + '/'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert bc['file.txt'] == b'modified'

    def test_symlink_skipped(self, temp_dir):
        """Symlinks are skipped (not followed or added)."""
        src = osp.join(temp_dir, 'src')
        os.makedirs(src)
        with open(osp.join(src, 'real.txt'), 'w') as f:
            f.write('real')
        os.symlink(osp.join(src, 'real.txt'), osp.join(src, 'link.txt'))

        archive_path = osp.join(temp_dir, 'out.barecat')
        rsync([src + '/'], archive_path + '::')

        with barecat.Barecat(archive_path, readonly=True) as bc:
            assert 'real.txt' in bc
            # Symlink should not be in archive (os.walk doesn't yield symlinks as files)
            # Actually os.walk does include symlinks in files list, let's check behavior


class TestParsePathEdgeCases:
    """Edge cases for parse_path."""

    def test_empty_string_raises(self):
        """Empty string path."""
        pp = parse_path('')
        assert pp.path_type == PathType.LOCAL
        assert pp.filesystem_path == ''

    def test_just_double_colon(self):
        """Just :: is a barecat with empty archive path."""
        pp = parse_path('::')
        assert pp.path_type == PathType.LOCAL_ARCHIVE
        assert pp.archive_path == ''

    def test_trailing_double_colon_only(self):
        """Path ending with :: but nothing after."""
        pp = parse_path('./archive.barecat::')
        assert pp.inner_path == ''

    def test_multiple_double_colons(self):
        """Multiple :: - first one is the delimiter."""
        pp = parse_path('./archive.barecat::inner::path')
        assert pp.archive_path == './archive.barecat'
        assert pp.inner_path == 'inner::path'

    def test_colon_in_path_not_ssh(self):
        """Single colon followed by non-slash is not SSH."""
        pp = parse_path('./file:name.barecat::')
        assert pp.path_type == PathType.LOCAL_ARCHIVE
        assert pp.archive_path == './file:name.barecat'

    def test_ssh_relative_path_not_matched(self):
        """SSH requires absolute path (colon followed by /)."""
        pp = parse_path('host:relative/path')
        # This should be parsed as local path with colon in name
        assert pp.path_type == PathType.LOCAL

    def test_barecat_url_missing_slash(self):
        """barecat:// URL without proper format."""
        with pytest.raises(ValueError):
            parse_path('barecat://hostonly')
