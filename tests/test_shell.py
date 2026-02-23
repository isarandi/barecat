"""Tests for barecat shell module."""
import os
import shutil
import tempfile

import pytest

import barecat
from barecat.cli.shell import BarecatShell


@pytest.fixture
def archive_with_shell():
    """Create a test archive and shell."""
    tmpdir = tempfile.mkdtemp()
    archive_path = os.path.join(tmpdir, 'test')
    extract_dir = os.path.join(tmpdir, 'extracted')
    os.makedirs(extract_dir)

    # Create test archive
    with barecat.Barecat(archive_path, readonly=False) as bc:
        bc['file1.txt'] = b'hello world'
        bc['dir1/file2.txt'] = b'nested file content'
        bc['dir1/subdir/file3.txt'] = b'deeply nested'
        bc['dir2/another.txt'] = b'another file'

    shell = BarecatShell(archive_path)
    shell.local_cwd = extract_dir

    yield shell, archive_path, extract_dir, tmpdir

    shell.close()
    shutil.rmtree(tmpdir)


class TestNavigation:
    """Test navigation commands."""

    def test_pwd_root(self, archive_with_shell):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('pwd')
        # Should print "/" - just verify no error

    def test_cd_and_pwd(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('cd dir1')
        shell.onecmd('pwd')
        captured = capsys.readouterr()
        assert '/dir1' in captured.out

    def test_cd_subdir(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('cd dir1/subdir')
        shell.onecmd('pwd')
        captured = capsys.readouterr()
        assert '/dir1/subdir' in captured.out

    def test_cd_back_to_root(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('cd dir1')
        shell.onecmd('cd /')
        shell.onecmd('pwd')
        captured = capsys.readouterr()
        assert captured.out.strip() == '/'


class TestListing:
    """Test listing commands."""

    def test_ls_root(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('ls')
        captured = capsys.readouterr()
        assert 'dir1/' in captured.out
        assert 'dir2/' in captured.out
        assert 'file1.txt' in captured.out

    def test_ls_long(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('ls -l')
        captured = capsys.readouterr()
        # Long format should show total, permissions, sizes
        assert 'total' in captured.out
        assert 'dir1' in captured.out
        assert 'file1.txt' in captured.out

    def test_ls_subdir(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('ls dir1')
        captured = capsys.readouterr()
        assert 'file2.txt' in captured.out
        assert 'subdir/' in captured.out

    def test_tree(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('tree')
        captured = capsys.readouterr()
        # tree command doesn't add trailing slashes (like real /usr/bin/tree)
        assert 'dir1' in captured.out
        assert 'file2.txt' in captured.out
        assert 'subdir' in captured.out
        assert 'file3.txt' in captured.out


class TestFileOperations:
    """Test file inspection commands."""

    def test_cat(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('cat file1.txt')
        captured = capsys.readouterr()
        assert 'hello world' in captured.out

    def test_cat_nested(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('cat dir1/file2.txt')
        captured = capsys.readouterr()
        assert 'nested file content' in captured.out

    def test_stat_file(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('stat file1.txt')
        captured = capsys.readouterr()
        assert 'File:' in captured.out
        assert 'Size:' in captured.out
        assert 'Shard:' in captured.out

    def test_stat_dir(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('stat dir1')
        captured = capsys.readouterr()
        assert 'Dir:' in captured.out
        assert 'Files:' in captured.out

    def test_head(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('head -n 5 file1.txt')
        captured = capsys.readouterr()
        assert 'hello' in captured.out


class TestFind:
    """Test find command (like /usr/bin/find)."""

    def test_find_by_name(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd("find -name '*.txt'")
        captured = capsys.readouterr()
        assert 'dir1/file2.txt' in captured.out
        assert 'dir1/subdir/file3.txt' in captured.out
        assert 'file1.txt' in captured.out

    def test_find_from_path(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd("find dir1 -name '*.txt'")
        captured = capsys.readouterr()
        assert 'file2.txt' in captured.out
        assert 'file3.txt' in captured.out
        assert 'file1.txt' not in captured.out


class TestInfo:
    """Test info commands."""

    def test_info(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('info')
        captured = capsys.readouterr()
        assert 'Archive:' in captured.out
        assert 'Files:' in captured.out
        assert '4' in captured.out  # 4 files

    def test_du(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('du dir1')
        captured = capsys.readouterr()
        # Should show size and path
        assert 'dir1' in captured.out


class TestLocalNavigation:
    """Test local filesystem navigation."""

    def test_lpwd(self, archive_with_shell, capsys):
        shell, _, extract_dir, _ = archive_with_shell
        shell.onecmd('lpwd')
        captured = capsys.readouterr()
        assert extract_dir in captured.out

    def test_lcd(self, archive_with_shell, capsys):
        shell, _, _, tmpdir = archive_with_shell
        shell.onecmd(f'lcd {tmpdir}')
        shell.onecmd('lpwd')
        captured = capsys.readouterr()
        assert tmpdir in captured.out

    def test_lls(self, archive_with_shell, capsys):
        shell, _, extract_dir, _ = archive_with_shell
        # Create a local file
        with open(os.path.join(extract_dir, 'local.txt'), 'w') as f:
            f.write('test')
        shell.onecmd('lls')
        captured = capsys.readouterr()
        assert 'local.txt' in captured.out


class TestExtract:
    """Test file extraction commands."""

    def test_get_single_file(self, archive_with_shell):
        shell, _, extract_dir, _ = archive_with_shell
        shell.onecmd('get file1.txt')
        assert os.path.exists(os.path.join(extract_dir, 'file1.txt'))
        with open(os.path.join(extract_dir, 'file1.txt'), 'rb') as f:
            assert f.read() == b'hello world'

    def test_get_with_dest(self, archive_with_shell):
        shell, _, extract_dir, _ = archive_with_shell
        shell.onecmd('get file1.txt output.txt')
        assert os.path.exists(os.path.join(extract_dir, 'output.txt'))

    def test_get_directory(self, archive_with_shell):
        shell, _, extract_dir, _ = archive_with_shell
        shell.onecmd('get dir1 extracted_dir1')
        extracted = os.path.join(extract_dir, 'extracted_dir1')
        assert os.path.exists(os.path.join(extracted, 'file2.txt'))
        assert os.path.exists(os.path.join(extracted, 'subdir', 'file3.txt'))

    def test_mget(self, archive_with_shell):
        shell, _, extract_dir, _ = archive_with_shell
        out_dir = os.path.join(extract_dir, 'txtfiles')
        os.makedirs(out_dir)
        shell.onecmd(f'mget **/*.txt {out_dir}')
        # Should extract all .txt files
        assert os.path.exists(os.path.join(out_dir, 'file2.txt'))
        assert os.path.exists(os.path.join(out_dir, 'file3.txt'))


class TestShellEscape:
    """Test shell escape command."""

    def test_shell_escape(self, archive_with_shell):
        shell, _, extract_dir, _ = archive_with_shell
        # Create a file to list
        test_file = os.path.join(extract_dir, 'test.txt')
        with open(test_file, 'w') as f:
            f.write('test')
        # Shell escape runs in subprocess, output not captured by pytest
        # Just verify it doesn't error
        shell.onecmd('!ls')
        # Verify command ran in correct directory by checking side effect
        shell.onecmd(f'!touch {extract_dir}/touched.txt')
        assert os.path.exists(os.path.join(extract_dir, 'touched.txt'))


class TestWriteOperations:
    """Test write operations (put, rm, mv)."""

    def test_put(self):
        tmpdir = tempfile.mkdtemp()
        try:
            archive_path = os.path.join(tmpdir, 'test')
            local_dir = os.path.join(tmpdir, 'local')
            os.makedirs(local_dir)

            # Create test archive
            with barecat.Barecat(archive_path, readonly=False) as bc:
                bc['existing.txt'] = b'existing content'

            # Create local file to put
            with open(os.path.join(local_dir, 'newfile.txt'), 'wb') as f:
                f.write(b'new content')

            # Run shell command and close to commit
            with BarecatShell(archive_path, readonly=False) as shell:
                shell.local_cwd = local_dir
                shell.onecmd('put newfile.txt')

            # Verify file was added
            with barecat.Barecat(archive_path) as bc:
                assert bc['newfile.txt'] == b'new content'
        finally:
            shutil.rmtree(tmpdir)

    def test_put_with_dest(self):
        tmpdir = tempfile.mkdtemp()
        try:
            archive_path = os.path.join(tmpdir, 'test')
            local_dir = os.path.join(tmpdir, 'local')
            os.makedirs(local_dir)

            with barecat.Barecat(archive_path, readonly=False) as bc:
                bc['existing.txt'] = b'existing content'

            with open(os.path.join(local_dir, 'newfile.txt'), 'wb') as f:
                f.write(b'new content')

            with BarecatShell(archive_path, readonly=False) as shell:
                shell.local_cwd = local_dir
                shell.onecmd('put newfile.txt subdir/renamed.txt')

            with barecat.Barecat(archive_path) as bc:
                assert bc['subdir/renamed.txt'] == b'new content'
        finally:
            shutil.rmtree(tmpdir)

    def test_rm(self):
        tmpdir = tempfile.mkdtemp()
        try:
            archive_path = os.path.join(tmpdir, 'test')

            with barecat.Barecat(archive_path, readonly=False) as bc:
                bc['existing.txt'] = b'existing content'

            with BarecatShell(archive_path, readonly=False) as shell:
                shell.onecmd('rm existing.txt')

            with barecat.Barecat(archive_path) as bc:
                assert 'existing.txt' not in bc
        finally:
            shutil.rmtree(tmpdir)

    def test_mv(self):
        tmpdir = tempfile.mkdtemp()
        try:
            archive_path = os.path.join(tmpdir, 'test')

            with barecat.Barecat(archive_path, readonly=False) as bc:
                bc['existing.txt'] = b'existing content'

            with BarecatShell(archive_path, readonly=False) as shell:
                shell.onecmd('mv existing.txt renamed.txt')

            with barecat.Barecat(archive_path) as bc:
                assert 'existing.txt' not in bc
                assert bc['renamed.txt'] == b'existing content'
        finally:
            shutil.rmtree(tmpdir)

    def test_readonly_rejects_write(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('put nonexistent.txt')
        captured = capsys.readouterr()
        assert 'read-only' in captured.out


class TestDotCommands:
    """Test dot-command aliases."""

    def test_dot_quit(self, archive_with_shell):
        shell, _, _, _ = archive_with_shell
        result = shell.onecmd('.quit')
        assert result is True

    def test_dot_ls(self, archive_with_shell, capsys):
        shell, _, _, _ = archive_with_shell
        shell.onecmd('.ls')
        captured = capsys.readouterr()
        assert 'dir1/' in captured.out
