"""Tests verifying barecat glob matches Python glob.glob behavior.

This module creates identical directory structures in both the filesystem
and a barecat archive, then compares glob results to ensure consistency.
"""
import glob
import os
import os.path as osp
import tempfile

import pytest

from barecat import Barecat


def create_test_structure(base_path, create_file_func):
    """Create a comprehensive test directory structure.

    Args:
        base_path: Base path for relative paths
        create_file_func: Function(relative_path) to create a file
    """
    files = [
        # Root level files
        'a.txt',
        'b.txt',
        'c.py',
        'readme.md',

        # Single char names for ? wildcard
        'x.txt',
        'y.txt',
        'z.txt',

        # Bracket pattern targets [abc]
        'a.log',
        'b.log',
        'c.log',
        'd.log',  # Should NOT match [abc].log

        # Range pattern targets [a-c]
        'file_a.dat',
        'file_b.dat',
        'file_c.dat',
        'file_d.dat',  # Should NOT match file_[a-c].dat

        # Negation pattern targets [!abc]
        '1.tmp',
        '2.tmp',
        'a.tmp',  # Should NOT match [!a].tmp

        # Hidden files (start with .)
        '.hidden',
        '.config',
        '.gitignore',

        # Simple subdirectory
        'dir/file1.txt',
        'dir/file2.txt',
        'dir/file.py',

        # Nested subdirectory
        'dir/sub/nested.txt',
        'dir/sub/nested.py',

        # Deep nesting
        'dir/sub/deep/very/deep.txt',
        'dir/sub/deep/very/deep.py',

        # Another top-level dir
        'src/main.py',
        'src/util.py',
        'src/lib/helper.py',
        'src/lib/core.py',

        # Directory with hidden files
        'config/.env',
        'config/.secret',
        'config/settings.json',

        # Multiple extensions
        'data/file.tar.gz',
        'data/file.txt.bak',
        'data/archive.zip',

        # Numeric names
        'logs/1.log',
        'logs/2.log',
        'logs/10.log',
        'logs/100.log',

        # Mixed patterns
        'test/test_a.py',
        'test/test_b.py',
        'test/test_c.py',
        'test/helper.py',  # Doesn't match test_*.py

        # Same filename at different depths
        'file.txt',
        'dir/file.txt',
        'dir/sub/file.txt',
        'dir/sub/deep/file.txt',

        # Names with multiple dots
        'docs/api.v1.md',
        'docs/api.v2.md',
        'docs/guide.md',

        # Single letter directories
        'a/file.txt',
        'b/file.txt',
        'c/nested/file.txt',
    ]

    for f in files:
        create_file_func(f)


def filesystem_create_file(base_path, rel_path):
    """Create a file in the filesystem."""
    full_path = osp.join(base_path, rel_path)
    os.makedirs(osp.dirname(full_path), exist_ok=True)
    with open(full_path, 'w') as f:
        f.write('content')


def normalize_glob_results(paths, base_path=''):
    """Normalize glob results for comparison.

    - Remove base path prefix
    - Sort for deterministic comparison
    - Convert to set
    """
    if base_path:
        base_path = base_path.rstrip('/') + '/'
        paths = [p[len(base_path):] if p.startswith(base_path) else p for p in paths]
    return set(paths)


class TestGlobBasicPatterns:
    """Test basic glob patterns (* and ?)."""

    @pytest.fixture
    def both_structures(self):
        """Create identical structures in filesystem and barecat."""
        with tempfile.TemporaryDirectory() as fs_dir:
            bc_path = osp.join(fs_dir, 'test.barecat')
            fs_base = osp.join(fs_dir, 'files')
            os.makedirs(fs_base)

            # Create in filesystem
            create_test_structure(fs_base, lambda p: filesystem_create_file(fs_base, p))

            # Create in barecat
            with Barecat(bc_path, readonly=False) as bc:
                create_test_structure(bc_path, lambda p: bc.__setitem__(p, b'content'))

                yield fs_base, bc

    def test_star_txt_root(self, both_structures):
        """*.txt at root level."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/*.txt'), fs_base)
        bc_result = set(bc.index.glob_paths('*.txt'))

        assert bc_result == py_result

    def test_star_py_root(self, both_structures):
        """*.py at root level."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/*.py'), fs_base)
        bc_result = set(bc.index.glob_paths('*.py'))

        assert bc_result == py_result

    def test_star_all_root(self, both_structures):
        """* at root level (files and dirs)."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/*'), fs_base)
        bc_result = set(bc.index.glob_paths('*'))

        assert bc_result == py_result

    def test_question_mark_txt(self, both_structures):
        """?.txt - single char before .txt."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/?.txt'), fs_base)
        bc_result = set(bc.index.glob_paths('?.txt'))

        assert bc_result == py_result

    def test_question_mark_log(self, both_structures):
        """?.log - single char before .log."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/?.log'), fs_base)
        bc_result = set(bc.index.glob_paths('?.log'))

        assert bc_result == py_result

    def test_star_in_subdir(self, both_structures):
        """dir/*.txt - star in subdirectory."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/dir/*.txt'), fs_base)
        bc_result = set(bc.index.glob_paths('dir/*.txt'))

        assert bc_result == py_result

    def test_star_star_extension(self, both_structures):
        """src/*/*.py - star directory, star file."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/src/*/*.py'), fs_base)
        bc_result = set(bc.index.glob_paths('src/*/*.py'))

        assert bc_result == py_result

    def test_multiple_question_marks(self, both_structures):
        """logs/??.log - two char names."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/logs/??.log'), fs_base)
        bc_result = set(bc.index.glob_paths('logs/??.log'))

        assert bc_result == py_result


class TestGlobBracketPatterns:
    """Test bracket patterns [abc], [a-z], [!abc]."""

    @pytest.fixture
    def both_structures(self):
        """Create identical structures in filesystem and barecat."""
        with tempfile.TemporaryDirectory() as fs_dir:
            bc_path = osp.join(fs_dir, 'test.barecat')
            fs_base = osp.join(fs_dir, 'files')
            os.makedirs(fs_base)

            create_test_structure(fs_base, lambda p: filesystem_create_file(fs_base, p))

            with Barecat(bc_path, readonly=False) as bc:
                create_test_structure(bc_path, lambda p: bc.__setitem__(p, b'content'))

                yield fs_base, bc

    def test_bracket_set(self, both_structures):
        """[abc].log - character set."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/[abc].log'), fs_base)
        bc_result = set(bc.index.glob_paths('[abc].log'))

        assert bc_result == py_result
        # Verify d.log is NOT included
        assert 'd.log' not in bc_result

    def test_bracket_range(self, both_structures):
        """file_[a-c].dat - character range."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/file_[a-c].dat'), fs_base)
        bc_result = set(bc.index.glob_paths('file_[a-c].dat'))

        assert bc_result == py_result
        # Verify file_d.dat is NOT included
        assert 'file_d.dat' not in bc_result

    def test_bracket_negation(self, both_structures):
        """[!a].tmp - negation pattern."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/[!a].tmp'), fs_base)
        bc_result = set(bc.index.glob_paths('[!a].tmp'))

        assert bc_result == py_result
        # Verify a.tmp is NOT included
        assert 'a.tmp' not in bc_result

    def test_bracket_in_subdir(self, both_structures):
        """test/test_[abc].py - bracket in subdir."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/test/test_[abc].py'), fs_base)
        bc_result = set(bc.index.glob_paths('test/test_[abc].py'))

        assert bc_result == py_result

    def test_bracket_dir_name(self, both_structures):
        """[ab]/file.txt - bracket in directory name."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/[ab]/file.txt'), fs_base)
        bc_result = set(bc.index.glob_paths('[ab]/file.txt'))

        assert bc_result == py_result


class TestGlobRecursive:
    """Test recursive glob patterns with **."""

    @pytest.fixture
    def both_structures(self):
        """Create identical structures in filesystem and barecat."""
        with tempfile.TemporaryDirectory() as fs_dir:
            bc_path = osp.join(fs_dir, 'test.barecat')
            fs_base = osp.join(fs_dir, 'files')
            os.makedirs(fs_base)

            create_test_structure(fs_base, lambda p: filesystem_create_file(fs_base, p))

            with Barecat(bc_path, readonly=False) as bc:
                create_test_structure(bc_path, lambda p: bc.__setitem__(p, b'content'))

                yield fs_base, bc

    def test_doublestar_txt(self, both_structures):
        """**/*.txt - all txt files recursively (including root)."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(
            glob.glob(f'{fs_base}/**/*.txt', recursive=True), fs_base
        )
        bc_result = set(bc.index.glob_paths('**/*.txt', recursive=True))

        assert bc_result == py_result

    def test_doublestar_py(self, both_structures):
        """**/*.py - all py files recursively."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(
            glob.glob(f'{fs_base}/**/*.py', recursive=True), fs_base
        )
        bc_result = set(bc.index.glob_paths('**/*.py', recursive=True))

        assert bc_result == py_result

    def test_doublestar_all(self, both_structures):
        """** - all files and dirs recursively."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(
            glob.glob(f'{fs_base}/**', recursive=True), fs_base
        )
        bc_result = set(bc.index.glob_paths('**', recursive=True))

        assert bc_result == py_result

    def test_doublestar_specific_file(self, both_structures):
        """**/file.txt - specific filename at any depth."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(
            glob.glob(f'{fs_base}/**/file.txt', recursive=True), fs_base
        )
        bc_result = set(bc.index.glob_paths('**/file.txt', recursive=True))

        assert bc_result == py_result

    def test_dir_doublestar_txt(self, both_structures):
        """dir/**/*.txt - recursive within specific dir."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(
            glob.glob(f'{fs_base}/dir/**/*.txt', recursive=True), fs_base
        )
        bc_result = set(bc.index.glob_paths('dir/**/*.txt', recursive=True))

        assert bc_result == py_result

    def test_dir_doublestar_all(self, both_structures):
        """dir/** - everything under dir."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(
            glob.glob(f'{fs_base}/dir/**', recursive=True), fs_base
        )
        bc_result = set(bc.index.glob_paths('dir/**', recursive=True))

        assert bc_result == py_result

    def test_doublestar_deep_file(self, both_structures):
        """**/deep.txt - deeply nested file."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(
            glob.glob(f'{fs_base}/**/deep.txt', recursive=True), fs_base
        )
        bc_result = set(bc.index.glob_paths('**/deep.txt', recursive=True))

        assert bc_result == py_result


class TestGlobHiddenFiles:
    """Test glob behavior with hidden files (starting with .)."""

    @pytest.fixture
    def both_structures(self):
        """Create identical structures in filesystem and barecat."""
        with tempfile.TemporaryDirectory() as fs_dir:
            bc_path = osp.join(fs_dir, 'test.barecat')
            fs_base = osp.join(fs_dir, 'files')
            os.makedirs(fs_base)

            create_test_structure(fs_base, lambda p: filesystem_create_file(fs_base, p))

            with Barecat(bc_path, readonly=False) as bc:
                create_test_structure(bc_path, lambda p: bc.__setitem__(p, b'content'))

                yield fs_base, bc

    def test_star_excludes_hidden(self, both_structures):
        """* should NOT match hidden files by default."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/*'), fs_base)
        bc_result = set(bc.index.glob_paths('*'))

        assert bc_result == py_result
        # Hidden files should NOT be included
        assert '.hidden' not in bc_result
        assert '.config' not in bc_result

    def test_dot_star_matches_hidden(self, both_structures):
        """.* should match hidden files."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/.*'), fs_base)
        bc_result = set(bc.index.glob_paths('.*'))

        assert bc_result == py_result
        assert '.hidden' in bc_result

    def test_hidden_in_subdir(self, both_structures):
        """config/.* - hidden files in subdir."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/config/.*'), fs_base)
        bc_result = set(bc.index.glob_paths('config/.*'))

        assert bc_result == py_result

    def test_recursive_excludes_hidden(self, both_structures):
        """**/*.txt should not descend into hidden dirs by default."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(
            glob.glob(f'{fs_base}/**/*.txt', recursive=True), fs_base
        )
        bc_result = set(bc.index.glob_paths('**/*.txt', recursive=True))

        assert bc_result == py_result

    def test_include_hidden_flag(self, both_structures):
        """Test include_hidden=True flag."""
        fs_base, bc = both_structures

        # barecat-specific: include_hidden parameter
        bc_default = set(bc.index.glob_paths('*'))
        bc_with_hidden = set(bc.index.glob_paths('*', include_hidden=True))

        # With include_hidden, should have more results
        assert len(bc_with_hidden) >= len(bc_default)
        assert '.hidden' in bc_with_hidden


class TestGlobEdgeCases:
    """Test edge cases and unusual patterns."""

    @pytest.fixture
    def both_structures(self):
        """Create identical structures in filesystem and barecat."""
        with tempfile.TemporaryDirectory() as fs_dir:
            bc_path = osp.join(fs_dir, 'test.barecat')
            fs_base = osp.join(fs_dir, 'files')
            os.makedirs(fs_base)

            create_test_structure(fs_base, lambda p: filesystem_create_file(fs_base, p))

            with Barecat(bc_path, readonly=False) as bc:
                create_test_structure(bc_path, lambda p: bc.__setitem__(p, b'content'))

                yield fs_base, bc

    def test_no_matches(self, both_structures):
        """Pattern that matches nothing."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/*.xyz'), fs_base)
        bc_result = set(bc.index.glob_paths('*.xyz'))

        assert bc_result == py_result == set()

    def test_exact_path(self, both_structures):
        """Exact path (no wildcards)."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/a.txt'), fs_base)
        bc_result = set(bc.index.glob_paths('a.txt'))

        assert bc_result == py_result

    def test_exact_path_nested(self, both_structures):
        """Exact nested path."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/dir/sub/nested.txt'), fs_base)
        bc_result = set(bc.index.glob_paths('dir/sub/nested.txt'))

        assert bc_result == py_result

    def test_multiple_extensions(self, both_structures):
        """*.tar.gz - multiple dots in extension."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/data/*.tar.gz'), fs_base)
        bc_result = set(bc.index.glob_paths('data/*.tar.gz'))

        assert bc_result == py_result

    def test_star_dot_star(self, both_structures):
        """*.*.* - multiple wildcards with dots."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/docs/*.*.*'), fs_base)
        bc_result = set(bc.index.glob_paths('docs/*.*.*'))

        assert bc_result == py_result

    def test_question_star_combo(self, both_structures):
        """?/*.txt - single char dir, then txt files."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/?/*.txt'), fs_base)
        bc_result = set(bc.index.glob_paths('?/*.txt'))

        assert bc_result == py_result

    def test_star_question_combo(self, both_structures):
        """logs/?.log - star dir, question file."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/logs/?.log'), fs_base)
        bc_result = set(bc.index.glob_paths('logs/?.log'))

        assert bc_result == py_result

    def test_multiple_star_segments(self, both_structures):
        """*/*/*.py - multiple star segments."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/*/*/*'), fs_base)
        bc_result = set(bc.index.glob_paths('*/*/*'))

        assert bc_result == py_result

    def test_bracket_range_numeric(self, both_structures):
        """logs/[0-9].log - numeric range."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(glob.glob(f'{fs_base}/logs/[0-9].log'), fs_base)
        bc_result = set(bc.index.glob_paths('logs/[0-9].log'))

        assert bc_result == py_result


class TestGlobOnlyFiles:
    """Test only_files parameter (barecat-specific)."""

    @pytest.fixture
    def both_structures(self):
        """Create identical structures in filesystem and barecat."""
        with tempfile.TemporaryDirectory() as fs_dir:
            bc_path = osp.join(fs_dir, 'test.barecat')
            fs_base = osp.join(fs_dir, 'files')
            os.makedirs(fs_base)

            create_test_structure(fs_base, lambda p: filesystem_create_file(fs_base, p))

            with Barecat(bc_path, readonly=False) as bc:
                create_test_structure(bc_path, lambda p: bc.__setitem__(p, b'content'))

                yield fs_base, bc

    def test_star_only_files(self, both_structures):
        """* with only_files=True excludes directories."""
        fs_base, bc = both_structures

        bc_all = set(bc.index.glob_paths('*'))
        bc_files_only = set(bc.index.glob_paths('*', only_files=True))

        # Files only should be subset
        assert bc_files_only <= bc_all
        # Should not contain directory names
        assert 'dir' not in bc_files_only
        assert 'src' not in bc_files_only
        # Should contain files
        assert 'a.txt' in bc_files_only

    def test_recursive_only_files(self, both_structures):
        """** with only_files=True."""
        fs_base, bc = both_structures

        bc_all = set(bc.index.glob_paths('**', recursive=True))
        bc_files_only = set(bc.index.glob_paths('**', recursive=True, only_files=True))

        # Should not contain any directory paths
        assert 'dir' not in bc_files_only
        assert 'dir/sub' not in bc_files_only
        # Should contain file paths
        assert 'a.txt' in bc_files_only
        assert 'dir/file1.txt' in bc_files_only


class TestGlobEmptyResults:
    """Test patterns that should return empty results."""

    def test_empty_archive(self):
        """Glob on empty archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                assert bc.index.glob_paths('*') == []
                # Barecat always has root directory '', so ** matches it
                assert bc.index.glob_paths('**', recursive=True) == ['']
                assert bc.index.glob_paths('*.txt') == []

    def test_nonexistent_subdir(self):
        """Glob in nonexistent subdirectory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = osp.join(tmpdir, 'test.barecat')
            with Barecat(path, readonly=False) as bc:
                bc['file.txt'] = b'content'

                assert bc.index.glob_paths('nonexistent/*.txt') == []
                assert bc.index.glob_paths('nonexistent/**', recursive=True) == []


class TestGlobSpecialPatterns:
    """Test special and complex patterns."""

    @pytest.fixture
    def both_structures(self):
        """Create identical structures in filesystem and barecat."""
        with tempfile.TemporaryDirectory() as fs_dir:
            bc_path = osp.join(fs_dir, 'test.barecat')
            fs_base = osp.join(fs_dir, 'files')
            os.makedirs(fs_base)

            create_test_structure(fs_base, lambda p: filesystem_create_file(fs_base, p))

            with Barecat(bc_path, readonly=False) as bc:
                create_test_structure(bc_path, lambda p: bc.__setitem__(p, b'content'))

                yield fs_base, bc

    def test_doublestar_slash_star(self, both_structures):
        """**/* - all files at all depths."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(
            glob.glob(f'{fs_base}/**/*', recursive=True), fs_base
        )
        bc_result = set(bc.index.glob_paths('**/*', recursive=True))

        assert bc_result == py_result

    def test_dir_doublestar_specific(self, both_structures):
        """src/**/helper.py - specific file in recursive subdir."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(
            glob.glob(f'{fs_base}/src/**/helper.py', recursive=True), fs_base
        )
        bc_result = set(bc.index.glob_paths('src/**/helper.py', recursive=True))

        assert bc_result == py_result

    def test_bracket_multiple_ranges(self, both_structures):
        """[a-c0-9].* - combined ranges."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(
            glob.glob(f'{fs_base}/[a-c0-9].*'), fs_base
        )
        bc_result = set(bc.index.glob_paths('[a-c0-9].*'))

        assert bc_result == py_result

    def test_star_md(self, both_structures):
        """**/*.md - markdown files recursively."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(
            glob.glob(f'{fs_base}/**/*.md', recursive=True), fs_base
        )
        bc_result = set(bc.index.glob_paths('**/*.md', recursive=True))

        assert bc_result == py_result

    def test_logs_star_log(self, both_structures):
        """logs/*.log - all log files."""
        fs_base, bc = both_structures

        py_result = normalize_glob_results(
            glob.glob(f'{fs_base}/logs/*.log'), fs_base
        )
        bc_result = set(bc.index.glob_paths('logs/*.log'))

        assert bc_result == py_result
