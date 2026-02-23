"""Tests for glob_to_sqlite function.

Tests that glob_to_sqlite correctly converts Python glob patterns to SQLite GLOB patterns,
handling the differences in bracket negation syntax ([!...] vs [^...]) and literal caret handling.
"""

import glob
import itertools
import os
import random
import shutil
import sqlite3
import tempfile

import pytest

from barecat.util.glob_to_regex import glob_to_sqlite


class TestGlobToSqliteBasic:
    """Basic transformation tests."""

    def test_no_brackets_unchanged(self):
        """Patterns without brackets pass through unchanged."""
        assert glob_to_sqlite('*.txt') == '*.txt'
        assert glob_to_sqlite('hello') == 'hello'
        assert glob_to_sqlite('a?b') == 'a?b'
        assert glob_to_sqlite('**/*.py') == '**/*.py'

    def test_normal_brackets_unchanged(self):
        """Normal brackets without ! or ^ pass through unchanged."""
        assert glob_to_sqlite('[abc]') == '[abc]'
        assert glob_to_sqlite('[a-z]') == '[a-z]'
        assert glob_to_sqlite('[0-9]') == '[0-9]'

    def test_negation_converted(self):
        """[!...] converted to [^...]."""
        assert glob_to_sqlite('[!a]') == '[^a]'
        assert glob_to_sqlite('[!ab]') == '[^ab]'
        assert glob_to_sqlite('[!abc]') == '[^abc]'
        assert glob_to_sqlite('[!a-z]') == '[^a-z]'

    def test_literal_bang_invalid_bracket(self):
        """[!] (invalid negation) becomes literal [!]."""
        assert glob_to_sqlite('[!]') == '[[]!]'

    def test_caret_only_becomes_literal(self):
        """[^], [^^], etc. become literal ^."""
        assert glob_to_sqlite('[^]') == '^'
        assert glob_to_sqlite('[^^]') == '^'
        assert glob_to_sqlite('[^^^]') == '^'

    def test_caret_moved_to_end(self):
        """Leading carets moved to end of bracket."""
        assert glob_to_sqlite('[^a]') == '[a^]'
        assert glob_to_sqlite('[^^a]') == '[a^^]'
        assert glob_to_sqlite('[^^^a]') == '[a^^^]'
        assert glob_to_sqlite('[^ab]') == '[ab^]'
        assert glob_to_sqlite('[^^ab]') == '[ab^^]'

    def test_caret_in_middle_unchanged(self):
        """Carets not at start stay in place."""
        assert glob_to_sqlite('[a^]') == '[a^]'
        assert glob_to_sqlite('[a^b]') == '[a^b]'
        assert glob_to_sqlite('[ab^]') == '[ab^]'

    def test_unclosed_bracket(self):
        """Unclosed brackets become literal [."""
        assert glob_to_sqlite('[abc') == '[[]abc'
        assert glob_to_sqlite('[!abc') == '[[]!abc'
        assert glob_to_sqlite('[^abc') == '[[]^abc'
        assert glob_to_sqlite('a[b') == 'a[[]b'


class TestGlobToSqliteBracketWithClosingBracket:
    """Tests for ] as first char in bracket (literal content)."""

    def test_negation_of_closing_bracket(self):
        """[!]] means 'not ]'."""
        assert glob_to_sqlite('[!]]') == '[^]]'

    def test_negation_of_closing_bracket_and_more(self):
        """[!]a] means 'not ] or a'."""
        assert glob_to_sqlite('[!]a]') == '[^]a]'
        assert glob_to_sqlite('[!]ab]') == '[^]ab]'
        assert glob_to_sqlite('[!][^]') == '[^][^]'

    def test_caret_then_closing_bracket(self):
        """[^]] is [^] followed by literal ]."""
        assert glob_to_sqlite('[^]]') == '^]'
        assert glob_to_sqlite('[^]a]') == '^a]'


class TestGlobToSqliteMixed:
    """Tests with mixed special characters."""

    def test_bang_and_caret(self):
        """Patterns with both ! and ^."""
        assert glob_to_sqlite('[!^]') == '[^^]'
        assert glob_to_sqlite('[^!]') == '[!^]'
        assert glob_to_sqlite('[!^a]') == '[^^a]'
        assert glob_to_sqlite('[^!a]') == '[!a^]'

    def test_multiple_brackets(self):
        """Multiple bracket expressions."""
        assert glob_to_sqlite('[a][b]') == '[a][b]'
        assert glob_to_sqlite('[!a][b]') == '[^a][b]'
        assert glob_to_sqlite('[a][!b]') == '[a][^b]'
        assert glob_to_sqlite('[!a][!b]') == '[^a][^b]'
        assert glob_to_sqlite('[^a][^b]') == '[a^][b^]'

    def test_brackets_with_wildcards(self):
        """Brackets combined with * and ?."""
        assert glob_to_sqlite('[!a]*') == '[^a]*'
        assert glob_to_sqlite('*[!a]') == '*[^a]'
        assert glob_to_sqlite('[^a]?') == '[a^]?'
        assert glob_to_sqlite('?[^a]?') == '?[a^]?'


class TestGlobToSqliteBackslash:
    """Tests with backslash characters."""

    def test_backslash_in_brackets(self):
        """Backslash inside brackets."""
        assert glob_to_sqlite('[\\]') == '[\\]'
        assert glob_to_sqlite('[\\a]') == '[\\a]'
        assert glob_to_sqlite('[!\\]') == '[^\\]'
        assert glob_to_sqlite('[!\\a]') == '[^\\a]'

    def test_backslash_with_caret(self):
        """Backslash and caret combinations."""
        assert glob_to_sqlite('[^\\]') == '[\\^]'
        assert glob_to_sqlite('[^\\a]') == '[\\a^]'


class TestGlobToSqliteFastPath:
    """Test the fast path optimization."""

    def test_fast_path_no_brackets(self):
        """Patterns without opening bracket return immediately."""
        # These should be identity transformations via fast path
        patterns = ['abc', '*.txt', 'a?b', '**/*', 'hello!world', 'a^b']
        for p in patterns:
            assert glob_to_sqlite(p) == p


@pytest.fixture
def test_files_and_db():
    """Create test files and SQLite database."""
    tmpdir = tempfile.mkdtemp()
    original_dir = os.getcwd()
    os.chdir(tmpdir)

    # Create a variety of test files
    files = [
        'a',
        'b',
        'c',
        'x',
        'y',
        'z',
        '0',
        '1',
        '9',
        '!',
        '@',
        '#',
        '$',
        '%',
        '&',
        '-',
        '_',
        '+',
        '[',
        ']',
        '^',
        '\\',
        '[!]',
        '[^]',
        '[]',
        '[[',
        ']]',
        '!]',
        '^]',
        '![',
        '^[',
        'ab',
        '!a',
        '^a',
        '[a',
        ']a',
        '\\a',
        'abc',
        '[!a]',
        '[^a]',
        '[ab]',
    ]

    created_files = []
    for f in files:
        try:
            open(f, 'w').close()
            created_files.append(f)
        except (OSError, IOError):
            pass

    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE t (name TEXT)')
    for f in created_files:
        conn.execute('INSERT INTO t VALUES (?)', (f,))

    yield tmpdir, conn, created_files

    os.chdir(original_dir)
    conn.close()
    shutil.rmtree(tmpdir)


class TestGlobToSqliteAgainstPythonGlob:
    """Test that converted patterns match Python glob behavior."""

    def test_designed_patterns(self, test_files_and_db):
        """Test designed edge case patterns against Python glob."""
        tmpdir, conn, files = test_files_and_db

        patterns = [
            '[!a]',
            '[!ab]',
            '[!abc]',
            '[!]]',
            '[!]a]',
            '[!]ab]',
            '[!][^]',
            '[!]',
            '[^]',
            '[^a]',
            '[^ab]',
            '[^^]',
            '[^^^]',
            '[^^a]',
            '[^]]',
            '[^]a]',
            '[!^]',
            '[^!]',
            '[!^a]',
            '[^!a]',
            '[a][b]',
            '[!a][b]',
            '[a][!b]',
            '[!a][!b]',
            '[abc',
            '[!abc',
            'a[b',
            '[\\]',
            '[!\\]',
            '[^\\]',
            '[!!]',
            '[!^!]',
            '[^!^]',
            '[a-z]',
            '[!a-z]',
        ]

        for pattern in patterns:
            py_result = sorted(glob.glob(pattern))
            sqlite_pattern = glob_to_sqlite(pattern)
            sq_result = sorted(
                [
                    r[0]
                    for r in conn.execute(
                        'SELECT name FROM t WHERE name GLOB ?', (sqlite_pattern,)
                    )
                ]
            )
            assert py_result == sq_result, (
                f'Pattern {pattern!r} -> {sqlite_pattern!r}: '
                f'Python={py_result}, SQLite={sq_result}'
            )

    def test_random_patterns(self, test_files_and_db):
        """Test random patterns against Python glob."""
        tmpdir, conn, files = test_files_and_db

        random.seed(42)
        special_chars = list('[]!^\\abcxyz')

        for _ in range(500):
            length = random.randint(1, 10)
            pattern = ''.join(random.choices(special_chars, k=length))

            py_result = sorted(glob.glob(pattern))
            sqlite_pattern = glob_to_sqlite(pattern)
            sq_result = sorted(
                [
                    r[0]
                    for r in conn.execute(
                        'SELECT name FROM t WHERE name GLOB ?', (sqlite_pattern,)
                    )
                ]
            )
            assert py_result == sq_result, (
                f'Pattern {pattern!r} -> {sqlite_pattern!r}: '
                f'Python={py_result}, SQLite={sq_result}'
            )

    def test_concatenated_patterns(self, test_files_and_db):
        """Test concatenations of base patterns."""
        tmpdir, conn, files = test_files_and_db

        base_patterns = [
            '[!]',
            '[^]',
            '[!a]',
            '[^a]',
            '[!ab]',
            '[^ab]',
            '[!]]',
            '[^]]',
            '[a]',
            '[ab]',
            'a',
            'ab',
        ]

        random.seed(123)
        concat_patterns = []
        for p1, p2 in random.sample(list(itertools.product(base_patterns, base_patterns)), 50):
            concat_patterns.append(p1 + p2)

        for pattern in concat_patterns:
            py_result = sorted(glob.glob(pattern))
            sqlite_pattern = glob_to_sqlite(pattern)
            sq_result = sorted(
                [
                    r[0]
                    for r in conn.execute(
                        'SELECT name FROM t WHERE name GLOB ?', (sqlite_pattern,)
                    )
                ]
            )
            assert py_result == sq_result, (
                f'Pattern {pattern!r} -> {sqlite_pattern!r}: '
                f'Python={py_result}, SQLite={sq_result}'
            )

    def test_patterns_with_wildcards(self, test_files_and_db):
        """Test patterns containing * and ? wildcards."""
        tmpdir, conn, files = test_files_and_db

        patterns = [
            '*',
            '?',
            '??',
            'a*',
            '*a',
            '?a',
            'a?',
            '[!a]*',
            '*[!a]',
            '[^a]*',
            '*[^a]',
            '[!a]?',
            '?[!a]',
            '[^a]?',
            '?[^a]',
            '*[!ab]*',
            '?[^ab]?',
        ]

        for pattern in patterns:
            py_result = sorted(glob.glob(pattern))
            sqlite_pattern = glob_to_sqlite(pattern)
            sq_result = sorted(
                [
                    r[0]
                    for r in conn.execute(
                        'SELECT name FROM t WHERE name GLOB ?', (sqlite_pattern,)
                    )
                ]
            )
            assert py_result == sq_result, (
                f'Pattern {pattern!r} -> {sqlite_pattern!r}: '
                f'Python={py_result}, SQLite={sq_result}'
            )
