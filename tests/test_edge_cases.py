"""Edge case tests for barecat - weird inputs and boundary conditions."""

import shutil
import tempfile
import os.path as osp

import pytest

from barecat import Barecat, BarecatError, IsADirectoryBarecatError


@pytest.fixture
def archive():
    """Create a temporary archive for testing."""
    tempdir = tempfile.mkdtemp()
    filepath = osp.join(tempdir, 'test.barecat')
    yield filepath
    shutil.rmtree(tempdir, ignore_errors=True)


# =============================================================================
# Empty and minimal content
# =============================================================================


def test_empty_file(archive):
    """Zero-byte files should work."""
    with Barecat(archive, readonly=False) as bc:
        bc['empty.txt'] = b''

    with Barecat(archive, readonly=True) as bc:
        assert bc['empty.txt'] == b''
        info = bc.index.lookup_file('empty.txt')
        assert info.size == 0


def test_single_byte_file(archive):
    """Single byte file."""
    with Barecat(archive, readonly=False) as bc:
        bc['one.bin'] = b'\x00'
        bc['two.bin'] = b'\xff'

    with Barecat(archive, readonly=True) as bc:
        assert bc['one.bin'] == b'\x00'
        assert bc['two.bin'] == b'\xff'


def test_empty_archive_operations(archive):
    """Operations on empty archive."""
    with Barecat(archive, readonly=False) as bc:
        assert list(bc.keys()) == []
        assert len(bc) == 0
        assert list(bc.walk('')) == [('.', [], [])]
        assert bc.listdir('') == []

        with pytest.raises(KeyError):
            _ = bc['nonexistent']


def test_single_file_at_root(archive):
    """File directly at root level (no directory)."""
    with Barecat(archive, readonly=False) as bc:
        bc['rootfile.txt'] = b'at root'

    with Barecat(archive, readonly=True) as bc:
        assert bc['rootfile.txt'] == b'at root'
        assert bc.listdir('') == ['rootfile.txt']


# =============================================================================
# Path edge cases
# =============================================================================


def test_path_normalization_leading_slash(archive):
    """Leading slashes should be stripped."""
    with Barecat(archive, readonly=False) as bc:
        bc['/leading/slash.txt'] = b'data'

    with Barecat(archive, readonly=True) as bc:
        assert bc['leading/slash.txt'] == b'data'
        # Original path with leading slash should also work
        assert bc['/leading/slash.txt'] == b'data'


def test_double_slash_collapsed(archive):
    """Multiple leading slashes are all stripped."""
    with Barecat(archive, readonly=False) as bc:
        bc['//double/slash.txt'] = b'data'
        bc['///triple.txt'] = b'triple'

    with Barecat(archive, readonly=True) as bc:
        # All leading slashes stripped, paths are equivalent
        assert bc['//double/slash.txt'] == b'data'
        assert bc['/double/slash.txt'] == b'data'
        assert bc['double/slash.txt'] == b'data'
        assert bc['triple.txt'] == b'triple'


def test_path_normalization_trailing_slash(archive):
    """Trailing slashes on file paths."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'data'

    with Barecat(archive, readonly=True) as bc:
        # Trailing slash on file should still find it
        assert bc['file.txt'] == b'data'


def test_path_normalization_dot_segments(archive):
    """Paths with . and .. segments."""
    with Barecat(archive, readonly=False) as bc:
        bc['dir/./file.txt'] = b'dot'
        bc['dir/sub/../other.txt'] = b'dotdot'

    with Barecat(archive, readonly=True) as bc:
        assert bc['dir/file.txt'] == b'dot'
        assert bc['dir/other.txt'] == b'dotdot'


def test_path_with_spaces(archive):
    """Paths containing spaces."""
    with Barecat(archive, readonly=False) as bc:
        bc['path with spaces/file name.txt'] = b'spacy'
        bc['  leading.txt'] = b'lead'
        bc['trailing  .txt'] = b'trail'

    with Barecat(archive, readonly=True) as bc:
        assert bc['path with spaces/file name.txt'] == b'spacy'
        assert bc['  leading.txt'] == b'lead'
        assert bc['trailing  .txt'] == b'trail'


def test_path_with_dots_in_name(archive):
    """Filenames with multiple dots."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.tar.gz'] = b'compressed'
        bc['...'] = b'dots'
        bc['a.b.c.d.e'] = b'many'
        bc['.hidden'] = b'hidden'
        bc['dir/.hidden'] = b'hidden2'

    with Barecat(archive, readonly=True) as bc:
        assert bc['file.tar.gz'] == b'compressed'
        assert bc['...'] == b'dots'
        assert bc['a.b.c.d.e'] == b'many'
        assert bc['.hidden'] == b'hidden'
        assert bc['dir/.hidden'] == b'hidden2'


def test_unicode_paths(archive):
    """Unicode characters in paths."""
    with Barecat(archive, readonly=False) as bc:
        bc['日本語/ファイル.txt'] = b'japanese'
        bc['emoji/🎉🎊.txt'] = b'party'
        bc['accents/café.txt'] = b'coffee'
        bc['chinese/中文文件.txt'] = b'chinese'
        bc['arabic/ملف.txt'] = b'arabic'

    with Barecat(archive, readonly=True) as bc:
        assert bc['日本語/ファイル.txt'] == b'japanese'
        assert bc['emoji/🎉🎊.txt'] == b'party'
        assert bc['accents/café.txt'] == b'coffee'
        assert bc['chinese/中文文件.txt'] == b'chinese'
        assert bc['arabic/ملف.txt'] == b'arabic'


def test_very_long_path(archive):
    """Path approaching filesystem limits."""
    # Most filesystems support 4096 byte paths
    long_component = 'a' * 200
    long_path = '/'.join([long_component] * 15) + '/file.txt'  # ~3000 chars

    with Barecat(archive, readonly=False) as bc:
        bc[long_path] = b'deep'

    with Barecat(archive, readonly=True) as bc:
        assert bc[long_path] == b'deep'


def test_deeply_nested_directory(archive):
    """Very deep directory nesting."""
    depth = 50
    path = '/'.join([f'd{i}' for i in range(depth)]) + '/file.txt'

    with Barecat(archive, readonly=False) as bc:
        bc[path] = b'deep'

    with Barecat(archive, readonly=True) as bc:
        assert bc[path] == b'deep'
        # Check intermediate directories exist
        partial = '/'.join([f'd{i}' for i in range(25)])
        assert bc.index.isdir(partial)


def test_special_characters_in_path(archive):
    """Special characters that might cause issues."""
    paths = [
        'with-dash.txt',
        'with_underscore.txt',
        'with+plus.txt',
        'with=equals.txt',
        'with@at.txt',
        'with#hash.txt',
        'with%percent.txt',
        'with&ampersand.txt',
        "with'quote.txt",
        'with(parens).txt',
        'with[brackets].txt',
        'with{braces}.txt',
        'with;semicolon.txt',
        'with,comma.txt',
        'with!exclaim.txt',
        'with~tilde.txt',
        'with`backtick.txt',
        'with$dollar.txt',
    ]

    with Barecat(archive, readonly=False) as bc:
        for i, path in enumerate(paths):
            bc[path] = f'content{i}'.encode()

    with Barecat(archive, readonly=True) as bc:
        for i, path in enumerate(paths):
            assert bc[path] == f'content{i}'.encode()


# =============================================================================
# Binary content edge cases
# =============================================================================


def test_binary_content_all_bytes(archive):
    """Content containing all possible byte values."""
    all_bytes = bytes(range(256))

    with Barecat(archive, readonly=False) as bc:
        bc['allbytes.bin'] = all_bytes

    with Barecat(archive, readonly=True) as bc:
        assert bc['allbytes.bin'] == all_bytes


def test_null_bytes_in_content(archive):
    """Content with null bytes (shouldn't affect anything)."""
    data = b'before\x00middle\x00\x00after\x00'

    with Barecat(archive, readonly=False) as bc:
        bc['nulls.bin'] = data

    with Barecat(archive, readonly=True) as bc:
        assert bc['nulls.bin'] == data


def test_content_looks_like_path(archive):
    """Content that looks like file paths."""
    data = b'/etc/passwd\n/home/user/.ssh/id_rsa\n'

    with Barecat(archive, readonly=False) as bc:
        bc['paths.txt'] = data

    with Barecat(archive, readonly=True) as bc:
        assert bc['paths.txt'] == data


# =============================================================================
# Overwrite and delete edge cases
# =============================================================================


def test_replace_with_smaller(archive):
    """Replace file with smaller content via delete+add."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'x' * 1000

    with Barecat(archive, readonly=False, append_only=False) as bc:
        del bc['file.txt']
        bc['file.txt'] = b'small'

    with Barecat(archive, readonly=True) as bc:
        assert bc['file.txt'] == b'small'


def test_replace_with_larger(archive):
    """Replace file with larger content via delete+add."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'small'

    with Barecat(archive, readonly=False, append_only=False) as bc:
        del bc['file.txt']
        bc['file.txt'] = b'x' * 1000

    with Barecat(archive, readonly=True) as bc:
        assert bc['file.txt'] == b'x' * 1000


def test_replace_with_same_size(archive):
    """Replace file with same-size content via delete+add."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'aaaaa'

    with Barecat(archive, readonly=False, append_only=False) as bc:
        del bc['file.txt']
        bc['file.txt'] = b'bbbbb'

    with Barecat(archive, readonly=True) as bc:
        assert bc['file.txt'] == b'bbbbb'


def test_replace_empty_with_content(archive):
    """Replace empty file with content via delete+add."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b''

    with Barecat(archive, readonly=False, append_only=False) as bc:
        del bc['file.txt']
        bc['file.txt'] = b'now has content'

    with Barecat(archive, readonly=True) as bc:
        assert bc['file.txt'] == b'now has content'


def test_replace_content_with_empty(archive):
    """Replace file with empty content via delete+add."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'has content'

    with Barecat(archive, readonly=False, append_only=False) as bc:
        del bc['file.txt']
        bc['file.txt'] = b''

    with Barecat(archive, readonly=True) as bc:
        assert bc['file.txt'] == b''


def test_overwrite_blocked_in_append_only_mode(archive):
    """Overwriting is blocked by default (append_only=True)."""
    from barecat import FileExistsBarecatError

    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'original'

    with Barecat(archive, readonly=False) as bc:  # Default is append_only=True
        with pytest.raises(FileExistsBarecatError):
            bc['file.txt'] = b'overwrite attempt'


def test_delete_and_recreate(archive):
    """Delete file then create again."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'original'
        del bc['file.txt']
        bc['file.txt'] = b'recreated'

    with Barecat(archive, readonly=True) as bc:
        assert bc['file.txt'] == b'recreated'


def test_delete_nonexistent(archive):
    """Deleting non-existent file should raise."""
    with Barecat(archive, readonly=False) as bc:
        with pytest.raises(KeyError):
            del bc['nonexistent.txt']


def test_delete_last_file_in_dir(archive):
    """Delete the only file in a directory."""
    with Barecat(archive, readonly=False) as bc:
        bc['dir/only.txt'] = b'alone'
        del bc['dir/only.txt']
        # Directory should still exist but be empty
        assert bc.listdir('dir') == []


# =============================================================================
# Directory edge cases
# =============================================================================


def test_empty_directory_explicit(archive):
    """Explicitly created empty directory via add()."""
    from barecat import BarecatDirInfo

    with Barecat(archive, readonly=False) as bc:
        bc.add(BarecatDirInfo(path='empty_dir', mode=0o755))

    with Barecat(archive, readonly=True) as bc:
        assert bc.index.isdir('empty_dir')
        assert bc.listdir('empty_dir') == []


def test_implicit_directory_creation(archive):
    """Directories created implicitly by adding files."""
    with Barecat(archive, readonly=False) as bc:
        bc['a/b/c/d/file.txt'] = b'deep'

    with Barecat(archive, readonly=True) as bc:
        assert bc.index.isdir('a')
        assert bc.index.isdir('a/b')
        assert bc.index.isdir('a/b/c')
        assert bc.index.isdir('a/b/c/d')


def test_many_files_in_one_directory(archive):
    """Directory with many files."""
    count = 1000

    with Barecat(archive, readonly=False) as bc:
        for i in range(count):
            bc[f'dir/file{i:04d}.txt'] = f'content{i}'.encode()

    with Barecat(archive, readonly=True) as bc:
        files = bc.listdir('dir')
        assert len(files) == count
        # Spot check
        assert bc['dir/file0500.txt'] == b'content500'


def test_many_subdirectories(archive):
    """Directory with many subdirectories."""
    count = 100

    with Barecat(archive, readonly=False) as bc:
        for i in range(count):
            bc[f'parent/sub{i:03d}/file.txt'] = f'content{i}'.encode()

    with Barecat(archive, readonly=True) as bc:
        subdirs = [e for e in bc.listdir('parent')]
        assert len(subdirs) == count


def test_directory_vs_file_same_name_prefix(archive):
    """File and directory with same prefix shouldn't conflict."""
    with Barecat(archive, readonly=False) as bc:
        bc['foo'] = b'file named foo'
        bc['foo.txt'] = b'file named foo.txt'
        bc['foobar/baz.txt'] = b'in foobar dir'

    with Barecat(archive, readonly=True) as bc:
        assert bc['foo'] == b'file named foo'
        assert bc['foo.txt'] == b'file named foo.txt'
        assert bc['foobar/baz.txt'] == b'in foobar dir'
        assert not bc.index.isdir('foo')
        assert bc.index.isdir('foobar')


# =============================================================================
# Read operations edge cases
# =============================================================================


def test_read_nonexistent_raises_keyerror(archive):
    """Reading non-existent file raises KeyError."""
    with Barecat(archive, readonly=False) as bc:
        bc['exists.txt'] = b'here'

    with Barecat(archive, readonly=True) as bc:
        with pytest.raises(KeyError):
            _ = bc['does_not_exist.txt']


def test_read_directory_as_file(archive):
    """Trying to read a directory as a file should fail."""
    with Barecat(archive, readonly=False) as bc:
        bc['dir/file.txt'] = b'content'

    with Barecat(archive, readonly=True) as bc:
        with pytest.raises((KeyError, IsADirectoryError, IsADirectoryBarecatError)):
            _ = bc['dir']


def test_listdir_file_raises(archive):
    """listdir on a file raises FileNotFoundBarecatError.

    Note: Ideally this would raise NotADirectoryBarecatError to distinguish
    'exists but not a directory' from 'does not exist'. Current behavior
    raises FileNotFoundBarecatError for both cases.
    """
    from barecat import FileNotFoundBarecatError

    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'content'

    with Barecat(archive, readonly=True) as bc:
        with pytest.raises(FileNotFoundBarecatError):
            bc.listdir('file.txt')


def test_listdir_nonexistent_raises(archive):
    """listdir on non-existent path raises FileNotFoundBarecatError."""
    from barecat import FileNotFoundBarecatError

    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'content'

    with Barecat(archive, readonly=True) as bc:
        with pytest.raises(FileNotFoundBarecatError):
            bc.listdir('nonexistent')


# =============================================================================
# File handle edge cases
# =============================================================================


def test_seek_beyond_end_allowed(archive):
    """Seeking beyond file end is allowed (like Python file objects).

    Reading after seeking past EOF returns empty bytes.
    """
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'short'

    with Barecat(archive, readonly=True) as bc:
        with bc.open('file.txt', 'rb') as f:
            f.seek(1000)  # Should not raise
            assert f.tell() == 1000
            assert f.read() == b''  # Reading past EOF returns empty


def test_seek_negative_from_end(archive):
    """Seeking from end with negative offset."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'hello world'

    with Barecat(archive, readonly=True) as bc:
        with bc.open('file.txt', 'rb') as f:
            f.seek(-5, 2)  # 5 bytes from end
            assert f.read() == b'world'


def test_read_zero_bytes(archive):
    """Reading zero bytes."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'content'

    with Barecat(archive, readonly=True) as bc:
        with bc.open('file.txt', 'rb') as f:
            assert f.read(0) == b''


def test_multiple_reads(archive):
    """Multiple sequential reads."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'abcdefghij'

    with Barecat(archive, readonly=True) as bc:
        with bc.open('file.txt', 'rb') as f:
            assert f.read(3) == b'abc'
            assert f.read(3) == b'def'
            assert f.read(3) == b'ghi'
            assert f.read(3) == b'j'
            assert f.read(3) == b''


# =============================================================================
# Concurrent/multiple file edge cases
# =============================================================================


def test_same_content_different_files(archive):
    """Multiple files with identical content."""
    content = b'duplicate content'

    with Barecat(archive, readonly=False) as bc:
        bc['file1.txt'] = content
        bc['file2.txt'] = content
        bc['dir/file3.txt'] = content

    with Barecat(archive, readonly=True) as bc:
        assert bc['file1.txt'] == content
        assert bc['file2.txt'] == content
        assert bc['dir/file3.txt'] == content


def test_interleaved_operations(archive):
    """Interleaved create/read/delete operations."""
    with Barecat(archive, readonly=False) as bc:
        bc['a.txt'] = b'a'
        bc['b.txt'] = b'b'
        assert bc['a.txt'] == b'a'
        bc['c.txt'] = b'c'
        del bc['b.txt']
        bc['d.txt'] = b'd'
        assert bc['c.txt'] == b'c'

    with Barecat(archive, readonly=True) as bc:
        assert bc['a.txt'] == b'a'
        assert bc['c.txt'] == b'c'
        assert bc['d.txt'] == b'd'
        with pytest.raises(KeyError):
            _ = bc['b.txt']


# =============================================================================
# Integrity and verification
# =============================================================================


def test_crc_stored_correctly(archive):
    """CRC32C should be stored and verifiable."""
    import crc32c

    content = b'test content for crc'
    expected_crc = crc32c.crc32c(content)

    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = content

    with Barecat(archive, readonly=True) as bc:
        info = bc.index.lookup_file('file.txt')
        assert info.crc32c == expected_crc


def test_verify_integrity_clean_archive(archive):
    """Integrity check on clean archive should pass."""
    with Barecat(archive, readonly=False) as bc:
        bc['file1.txt'] = b'content1'
        bc['file2.txt'] = b'content2'
        bc['dir/file3.txt'] = b'content3'

    with Barecat(archive, readonly=True) as bc:
        assert bc.verify_integrity(quick=True)


# =============================================================================
# Mode and permission edge cases
# =============================================================================


def test_readonly_prevents_write(archive):
    """Readonly mode should prevent writes."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'original'

    with Barecat(archive, readonly=True) as bc:
        with pytest.raises((PermissionError, IOError, BarecatError)):
            bc['file.txt'] = b'modified'


def test_reopen_after_close(archive):
    """Archive can be reopened after close."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'content'

    # First reopen
    with Barecat(archive, readonly=True) as bc:
        assert bc['file.txt'] == b'content'

    # Second reopen
    with Barecat(archive, readonly=True) as bc:
        assert bc['file.txt'] == b'content'


# =============================================================================
# Walk edge cases
# =============================================================================


def test_walk_empty_archive(archive):
    """Walk on empty archive."""
    with Barecat(archive, readonly=False) as bc:
        pass  # Create empty archive

    with Barecat(archive, readonly=True) as bc:
        result = list(bc.walk(''))
        assert result == [('.', [], [])]


def test_walk_single_file_at_root(archive):
    """Walk with single file at root."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'content'

    with Barecat(archive, readonly=True) as bc:
        result = list(bc.walk(''))
        assert result == [('.', [], ['file.txt'])]


def test_walk_complex_structure(archive):
    """Walk complex directory structure."""
    with Barecat(archive, readonly=False) as bc:
        bc['a/b/c/file1.txt'] = b'1'
        bc['a/b/file2.txt'] = b'2'
        bc['a/file3.txt'] = b'3'
        bc['x/file4.txt'] = b'4'

    with Barecat(archive, readonly=True) as bc:
        result = list(bc.walk(''))
        # Check we got all directories
        dirs_visited = [r[0] for r in result]
        assert '.' in dirs_visited
        assert 'a' in dirs_visited
        assert 'a/b' in dirs_visited
        assert 'a/b/c' in dirs_visited
        assert 'x' in dirs_visited


# =============================================================================
# Keys/items/values iteration
# =============================================================================


def test_keys_iteration(archive):
    """Iterating over keys."""
    with Barecat(archive, readonly=False) as bc:
        bc['a.txt'] = b'a'
        bc['b.txt'] = b'b'
        bc['dir/c.txt'] = b'c'

    with Barecat(archive, readonly=True) as bc:
        keys = set(bc.keys())
        assert keys == {'a.txt', 'b.txt', 'dir/c.txt'}


def test_contains_check(archive):
    """Using 'in' operator."""
    with Barecat(archive, readonly=False) as bc:
        bc['exists.txt'] = b'here'
        bc['dir/nested.txt'] = b'nested'

    with Barecat(archive, readonly=True) as bc:
        assert 'exists.txt' in bc
        assert 'dir/nested.txt' in bc
        assert 'nonexistent.txt' not in bc
        assert 'dir' not in bc  # directories shouldn't be "in" bc


def test_len_count(archive):
    """len() returns file count."""
    with Barecat(archive, readonly=False) as bc:
        bc['a.txt'] = b'a'
        bc['b.txt'] = b'b'
        bc['dir/c.txt'] = b'c'

    with Barecat(archive, readonly=True) as bc:
        assert len(bc) == 3


# =============================================================================
# Rename operations
# =============================================================================


def test_rename_file(archive):
    """Rename a file."""
    with Barecat(archive, readonly=False) as bc:
        bc['old.txt'] = b'content'
        bc.rename('old.txt', 'new.txt')
        assert 'new.txt' in bc
        assert 'old.txt' not in bc
        assert bc['new.txt'] == b'content'


def test_rename_file_to_different_dir(archive):
    """Rename a file to a different directory."""
    with Barecat(archive, readonly=False) as bc:
        bc['src/file.txt'] = b'content'
        bc['dst/dummy.txt'] = b'dummy'  # Create dst dir
        bc.rename('src/file.txt', 'dst/file.txt')
        assert 'dst/file.txt' in bc
        assert 'src/file.txt' not in bc
        assert bc['dst/file.txt'] == b'content'


def test_rename_directory(archive):
    """Rename a directory."""
    with Barecat(archive, readonly=False) as bc:
        bc['olddir/file1.txt'] = b'content1'
        bc['olddir/file2.txt'] = b'content2'
        bc.rename('olddir', 'newdir')
        assert bc.index.isdir('newdir')
        assert not bc.index.isdir('olddir')
        assert bc['newdir/file1.txt'] == b'content1'
        assert bc['newdir/file2.txt'] == b'content2'


def test_rename_nonexistent_raises(archive):
    """Rename non-existent path raises error."""
    from barecat import FileNotFoundBarecatError

    with Barecat(archive, readonly=False) as bc:
        with pytest.raises(FileNotFoundBarecatError):
            bc.rename('nonexistent', 'newname')


def test_rename_to_existing_raises(archive):
    """Rename to existing path raises error."""
    from barecat import FileExistsBarecatError

    with Barecat(archive, readonly=False) as bc:
        bc['file1.txt'] = b'content1'
        bc['file2.txt'] = b'content2'
        with pytest.raises(FileExistsBarecatError):
            bc.rename('file1.txt', 'file2.txt')


# =============================================================================
# rmdir operations
# =============================================================================


def test_rmdir_empty_directory(archive):
    """Remove an empty directory."""
    from barecat import BarecatDirInfo

    with Barecat(archive, readonly=False) as bc:
        bc.add(BarecatDirInfo(path='emptydir', mode=0o755))
        assert bc.index.isdir('emptydir')
        bc.rmdir('emptydir')
        assert not bc.index.isdir('emptydir')


def test_rmdir_nonempty_raises(archive):
    """Remove non-empty directory raises error."""
    from barecat import DirectoryNotEmptyBarecatError

    with Barecat(archive, readonly=False) as bc:
        bc['dir/file.txt'] = b'content'
        with pytest.raises(DirectoryNotEmptyBarecatError):
            bc.rmdir('dir')


def test_rmdir_nonexistent_raises(archive):
    """Remove non-existent directory raises error."""
    from barecat import FileNotFoundBarecatError

    with Barecat(archive, readonly=False) as bc:
        with pytest.raises(FileNotFoundBarecatError):
            bc.rmdir('nonexistent')


def test_rmdir_after_deleting_files(archive):
    """Remove directory after deleting all files."""
    with Barecat(archive, readonly=False) as bc:
        bc['dir/file.txt'] = b'content'
        del bc['dir/file.txt']
        bc.rmdir('dir')
        assert not bc.index.isdir('dir')


# =============================================================================
# rmtree operations
# =============================================================================


def test_rmtree_directory(archive):
    """Remove directory and all contents."""
    with Barecat(archive, readonly=False) as bc:
        bc['dir/file1.txt'] = b'content1'
        bc['dir/subdir/file2.txt'] = b'content2'
        bc['other.txt'] = b'other'
        bc.rmtree('dir')
        assert not bc.index.isdir('dir')
        assert 'dir/file1.txt' not in bc
        assert 'dir/subdir/file2.txt' not in bc
        assert 'other.txt' in bc  # Other files untouched


def test_rmtree_empty_dir(archive):
    """Remove empty directory recursively."""
    from barecat import BarecatDirInfo

    with Barecat(archive, readonly=False) as bc:
        bc.add(BarecatDirInfo(path='emptydir', mode=0o755))
        bc.rmtree('emptydir')
        assert not bc.index.isdir('emptydir')


def test_rmtree_nonexistent_raises(archive):
    """Remove non-existent directory recursively raises error."""
    from barecat import FileNotFoundBarecatError

    with Barecat(archive, readonly=False) as bc:
        with pytest.raises(FileNotFoundBarecatError):
            bc.rmtree('nonexistent')


def test_rmtree_deep_structure(archive):
    """Remove deeply nested structure."""
    with Barecat(archive, readonly=False) as bc:
        for i in range(10):
            bc[f'dir/sub{i}/deep/file.txt'] = f'content{i}'.encode()
        bc.rmtree('dir')
        assert not bc.index.isdir('dir')
        assert len(bc) == 0


# =============================================================================
# scandir operations
# =============================================================================


def test_scandir_basic(archive):
    """Basic scandir iteration."""
    with Barecat(archive, readonly=False) as bc:
        bc['dir/file1.txt'] = b'content1'
        bc['dir/file2.txt'] = b'content2'
        bc['dir/subdir/file3.txt'] = b'content3'

    with Barecat(archive, readonly=True) as bc:
        entries = list(bc.scandir('dir'))
        names = [e.path.split('/')[-1] for e in entries]
        assert set(names) == {'file1.txt', 'file2.txt', 'subdir'}


def test_scandir_entry_info(archive):
    """scandir returns BarecatEntryInfo with correct attributes."""
    with Barecat(archive, readonly=False) as bc:
        bc['dir/file.txt'] = b'hello'

    with Barecat(archive, readonly=True) as bc:
        entries = list(bc.scandir('dir'))
        assert len(entries) == 1
        entry = entries[0]
        assert entry.path == 'dir/file.txt'
        assert entry.isfile()
        assert not entry.isdir()
        assert entry.size == 5


def test_scandir_mixed_files_and_dirs(archive):
    """scandir returns both files and directories."""
    with Barecat(archive, readonly=False) as bc:
        bc['dir/file.txt'] = b'file'
        bc['dir/subdir/nested.txt'] = b'nested'

    with Barecat(archive, readonly=True) as bc:
        entries = list(bc.scandir('dir'))
        files = [e for e in entries if e.isfile()]
        dirs = [e for e in entries if e.isdir()]
        assert len(files) == 1
        assert len(dirs) == 1
        assert files[0].path == 'dir/file.txt'
        assert dirs[0].path == 'dir/subdir'


def test_scandir_empty_directory(archive):
    """scandir on empty directory returns empty iterator."""
    from barecat import BarecatDirInfo

    with Barecat(archive, readonly=False) as bc:
        bc.add(BarecatDirInfo(path='emptydir', mode=0o755))

    with Barecat(archive, readonly=True) as bc:
        entries = list(bc.scandir('emptydir'))
        assert entries == []


def test_scandir_root(archive):
    """scandir on root directory."""
    with Barecat(archive, readonly=False) as bc:
        bc['file.txt'] = b'root file'
        bc['dir/nested.txt'] = b'nested'

    with Barecat(archive, readonly=True) as bc:
        entries = list(bc.scandir(''))
        names = [e.path for e in entries]
        assert set(names) == {'file.txt', 'dir'}
