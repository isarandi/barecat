"""Property-based tests for glob_to_regex module using Hypothesis."""

import fnmatch
import re
import sqlite3

from hypothesis import given, strategies as st, assume

from barecat.util.glob_to_regex import glob_to_regex, glob_to_sqlite, expand_doublestar


# =============================================================================
# Strategies for generating glob patterns
# =============================================================================

# Safe characters for glob patterns
safe_chars = 'abcdefghijklmnopqrstuvwxyz0123456789_-.'
# Characters that won't create hidden files when at start
safe_chars_no_dot = 'abcdefghijklmnopqrstuvwxyz0123456789_-'
glob_special = '*?'
bracket_chars = '[]!^'

# Simple glob patterns (no brackets)
simple_glob = st.text(alphabet=safe_chars + glob_special + '/', min_size=0, max_size=30)

# Patterns that are literal (no wildcards)
literal_pattern = st.text(alphabet=safe_chars, min_size=1, max_size=20)

# Path-like patterns (non-hidden: don't start with dot)
path_segment = st.text(alphabet=safe_chars_no_dot, min_size=1, max_size=10)
path_pattern = st.lists(path_segment, min_size=1, max_size=5).map('/'.join)


# =============================================================================
# Tests for glob_to_regex
# =============================================================================


class TestGlobToRegexNoCrash:
    """Ensure glob_to_regex never crashes on any input."""

    @given(st.text(min_size=0, max_size=50))
    def test_arbitrary_input_no_crash(self, pattern):
        """glob_to_regex should handle any string without crashing."""
        try:
            result = glob_to_regex(pattern)
            # Result should be a valid regex
            re.compile(result)
        except (re.error, ValueError):
            pass  # OK to reject invalid patterns

    @given(simple_glob)
    def test_simple_glob_produces_valid_regex(self, pattern):
        """Simple glob patterns should produce valid regexes."""
        result = glob_to_regex(pattern)
        compiled = re.compile(result)
        assert compiled is not None

    @given(simple_glob, st.booleans(), st.booleans())
    def test_with_options_no_crash(self, pattern, recursive, include_hidden):
        """glob_to_regex with all option combinations should not crash."""
        result = glob_to_regex(pattern, recursive=recursive, include_hidden=include_hidden)
        re.compile(result)


class TestGlobToRegexLiterals:
    """Test that literal patterns match themselves."""

    @given(literal_pattern)
    def test_literal_matches_itself(self, s):
        """A pattern with no wildcards should match exactly itself."""
        regex = glob_to_regex(s)
        assert re.fullmatch(regex, s), f'Pattern {s!r} should match itself'

    @given(literal_pattern, literal_pattern)
    def test_literal_doesnt_match_other(self, s1, s2):
        """A literal pattern should not match a different string."""
        assume(s1 != s2)
        regex = glob_to_regex(s1)
        assert not re.fullmatch(regex, s2), f'Pattern {s1!r} should not match {s2!r}'

    @given(path_pattern)
    def test_path_literal_matches_itself(self, path):
        """A literal path pattern should match itself."""
        regex = glob_to_regex(path)
        assert re.fullmatch(regex, path), f'Path {path!r} should match itself'


class TestGlobToRegexWildcards:
    """Test wildcard behavior."""

    @given(literal_pattern, st.text(alphabet=safe_chars, min_size=0, max_size=10))
    def test_star_matches_suffix(self, prefix, suffix):
        """prefix* should match prefix followed by anything."""
        regex = glob_to_regex(f'{prefix}*')
        # Should match prefix alone
        assert re.fullmatch(regex, prefix)
        # Should match prefix + any suffix
        assert re.fullmatch(regex, prefix + suffix)

    @given(literal_pattern, st.text(alphabet=safe_chars_no_dot, min_size=0, max_size=10))
    def test_star_matches_prefix(self, suffix, prefix):
        """*suffix should match anything followed by suffix."""
        # Suffix must not start with dot (hidden file), since * doesn't match hidden by default
        assume(not suffix.startswith('.'))
        regex = glob_to_regex(f'*{suffix}')
        assert re.fullmatch(regex, suffix)
        # prefix + suffix must also not be hidden
        assume(not (prefix + suffix).startswith('.'))
        assert re.fullmatch(regex, prefix + suffix)

    @given(
        st.text(alphabet='abc', min_size=1, max_size=5),
        st.text(alphabet='abc', min_size=1, max_size=5),
    )
    def test_question_mark_single_char(self, prefix, suffix):
        """? should match exactly one character."""
        regex = glob_to_regex(f'{prefix}?{suffix}')

        # Should match with any single char in middle
        assert re.fullmatch(regex, f'{prefix}X{suffix}')
        assert re.fullmatch(regex, f'{prefix}7{suffix}')

        # Should NOT match with zero or two chars
        assert not re.fullmatch(regex, f'{prefix}{suffix}')
        assert not re.fullmatch(regex, f'{prefix}XX{suffix}')

    @given(st.integers(min_value=0, max_value=5))
    def test_multiple_question_marks(self, n):
        """n question marks should match exactly n characters."""
        pattern = '?' * n
        regex = glob_to_regex(pattern)

        # Should match exactly n chars
        assert re.fullmatch(regex, 'x' * n)

        # Should not match n-1 or n+1 chars (unless n=0)
        if n > 0:
            assert not re.fullmatch(regex, 'x' * (n - 1))
        assert not re.fullmatch(regex, 'x' * (n + 1))


class TestGlobToRegexPaths:
    """Test path-related behavior."""

    @given(path_segment, path_segment)
    def test_star_doesnt_cross_slash(self, dir_name, file_name):
        """* should not match across path separators."""
        assume(dir_name != file_name)
        regex = glob_to_regex(f'{dir_name}/*')

        # Should match direct children
        assert re.fullmatch(regex, f'{dir_name}/{file_name}')

        # Should NOT match nested paths
        assert not re.fullmatch(regex, f'{dir_name}/sub/{file_name}')

    @given(path_segment, path_segment, path_segment)
    def test_doublestar_crosses_slash(self, dir_name, subdir, file_name):
        """** with recursive=True should match across path separators."""
        regex = glob_to_regex(f'{dir_name}/**', recursive=True)

        # Should match direct children
        assert re.fullmatch(regex, f'{dir_name}/{file_name}')

        # Should also match nested paths
        assert re.fullmatch(regex, f'{dir_name}/{subdir}/{file_name}')


class TestGlobToRegexHidden:
    """Test hidden file behavior."""

    @given(st.text(alphabet='abc', min_size=1, max_size=5))
    def test_star_excludes_hidden_by_default(self, name):
        """* should not match hidden files by default."""
        regex = glob_to_regex('*')

        # Should match regular files
        assert re.fullmatch(regex, name)

        # Should NOT match hidden files
        assert not re.fullmatch(regex, f'.{name}')

    @given(st.text(alphabet='abc', min_size=1, max_size=5))
    def test_star_includes_hidden_with_flag(self, name):
        """* with include_hidden=True should match hidden files."""
        regex = glob_to_regex('*', include_hidden=True)

        # Should match both regular and hidden files
        assert re.fullmatch(regex, name)
        assert re.fullmatch(regex, f'.{name}')


# =============================================================================
# Tests for glob_to_sqlite
# =============================================================================


def sqlite_glob_matches(pattern, text):
    """Check if SQLite GLOB matches the given text."""
    conn = sqlite3.connect(':memory:')
    cursor = conn.execute('SELECT ? GLOB ?', (text, pattern))
    result = cursor.fetchone()[0]
    conn.close()
    return bool(result)


class TestGlobToSqlite:
    """Test glob_to_sqlite conversion."""

    @given(st.text(alphabet=safe_chars + '*?/', min_size=0, max_size=30))
    def test_no_brackets_unchanged(self, pattern):
        """Patterns without brackets should pass through unchanged."""
        result = glob_to_sqlite(pattern)
        assert result == pattern

    @given(st.text(alphabet='abc', min_size=1, max_size=5))
    def test_negation_converted(self, chars):
        """[!abc] should become [^abc]."""
        pattern = f'[!{chars}]'
        result = glob_to_sqlite(pattern)
        assert result == f'[^{chars}]'

    @given(st.text(alphabet=safe_chars + '*?/', min_size=0, max_size=30))
    def test_produces_valid_pattern(self, pattern):
        """glob_to_sqlite should always produce a valid pattern."""
        # Should not crash
        result = glob_to_sqlite(f'[{pattern}]')
        assert isinstance(result, str)

    @given(
        st.text(alphabet='abc', min_size=1, max_size=5),
        st.text(alphabet='abc', min_size=1, max_size=5),
    )
    def test_negation_semantics(self, chars, test_char):
        """[!abc] in SQLite should match same as Python for single chars."""
        pattern = f'[!{chars}]'
        sqlite_pattern = glob_to_sqlite(pattern)

        # Python regex behavior
        regex = glob_to_regex(pattern)
        python_matches = bool(re.fullmatch(regex, test_char))

        # SQLite behavior
        sqlite_matches = sqlite_glob_matches(sqlite_pattern, test_char)

        # SQLite should match at least what Python matches
        if python_matches:
            assert (
                sqlite_matches
            ), f"Python matched {test_char!r} with {pattern!r} but SQLite didn't"

    @given(
        st.text(alphabet='abc', min_size=1, max_size=3),
        st.text(alphabet='abc', min_size=1, max_size=5),
    )
    def test_bracket_class_semantics(self, chars, test_char):
        """[abc] in SQLite should match same as Python for single chars."""
        pattern = f'[{chars}]'
        sqlite_pattern = glob_to_sqlite(pattern)

        regex = glob_to_regex(pattern)
        python_matches = bool(re.fullmatch(regex, test_char))
        sqlite_matches = sqlite_glob_matches(sqlite_pattern, test_char)

        # For character classes, should be exact match
        assert python_matches == sqlite_matches, (
            f'Mismatch for pattern={pattern!r}, char={test_char!r}: '
            f'Python={python_matches}, SQLite={sqlite_matches}'
        )

    @given(path_pattern, path_pattern)
    def test_sqlite_matches_superset(self, pattern, path):
        """SQLite GLOB should match at least what Python glob matches."""
        # Skip patterns with brackets (complex conversion)
        assume('[' not in pattern)

        sqlite_pattern = glob_to_sqlite(pattern)
        regex = glob_to_regex(pattern, include_hidden=True)

        python_matches = bool(re.fullmatch(regex, path))
        sqlite_matches = sqlite_glob_matches(sqlite_pattern, path)

        # If Python matches, SQLite must also match
        if python_matches:
            assert sqlite_matches, f"Python matched {path!r} with {pattern!r} but SQLite didn't"


# =============================================================================
# Tests for expand_doublestar
# =============================================================================


class TestExpandDoublestar:
    """Test expand_doublestar behavior."""

    @given(st.text(alphabet=safe_chars + '*?/', min_size=0, max_size=20))
    def test_no_doublestar_unchanged(self, pattern):
        """Patterns without ** should return unchanged."""
        assume('**' not in pattern)
        result = expand_doublestar(pattern)
        assert result == [pattern]

    @given(path_segment)
    def test_leading_doublestar_expands(self, suffix):
        """**/X should expand to [X, */X]."""
        result = expand_doublestar(f'**/{suffix}')
        assert suffix in result
        assert f'*/{suffix}' in result

    @given(path_segment)
    def test_trailing_doublestar_expands(self, prefix):
        """X/** should expand to [X, X/*]."""
        result = expand_doublestar(f'{prefix}/**')
        assert prefix in result
        assert f'{prefix}/*' in result

    @given(path_segment, path_segment)
    def test_middle_doublestar_expands(self, a, b):
        """a/**/b should expand to [a/b, a/*/b]."""
        result = expand_doublestar(f'{a}/**/{b}')
        assert f'{a}/{b}' in result
        assert f'{a}/*/{b}' in result

    def test_bare_doublestar(self):
        """** alone should become *."""
        result = expand_doublestar('**')
        assert result == ['*']

    @given(st.text(alphabet=safe_chars + '*?/', min_size=0, max_size=30))
    def test_always_returns_list(self, pattern):
        """expand_doublestar should always return a list."""
        result = expand_doublestar(pattern)
        assert isinstance(result, list)
        assert len(result) >= 1


# =============================================================================
# Cross-validation with fnmatch
# =============================================================================


class TestCrossValidation:
    """Cross-validate glob_to_regex against fnmatch."""

    @given(
        literal_pattern,
        st.text(alphabet=safe_chars, min_size=1, max_size=10),
    )
    def test_star_matches_same_as_fnmatch(self, base, suffix):
        """Our glob should match the same strings as fnmatch for * patterns."""
        pattern = f'{base}*'
        test_str = base + suffix

        fnmatch_result = fnmatch.fnmatch(test_str, pattern)
        regex = glob_to_regex(pattern)
        our_result = bool(re.fullmatch(regex, test_str))

        assert (
            our_result == fnmatch_result
        ), f'Mismatch for pattern={pattern!r}, string={test_str!r}'

    @given(
        st.text(alphabet='abc', min_size=1, max_size=3),
        st.text(alphabet='abc', min_size=1, max_size=3),
        st.text(alphabet='abcd', min_size=1, max_size=1),
    )
    def test_question_matches_same_as_fnmatch(self, prefix, suffix, middle):
        """Our glob should match the same strings as fnmatch for ? patterns."""
        pattern = f'{prefix}?{suffix}'
        test_str = f'{prefix}{middle}{suffix}'

        fnmatch_result = fnmatch.fnmatch(test_str, pattern)
        regex = glob_to_regex(pattern)
        our_result = bool(re.fullmatch(regex, test_str))

        assert our_result == fnmatch_result


# =============================================================================
# Regression tests (placeholder for bugs found by Hypothesis)
# =============================================================================


class TestRegressions:
    """Tests for specific bugs found by Hypothesis or users."""

    def test_empty_pattern(self):
        """Empty pattern should only match empty string."""
        regex = glob_to_regex('')
        assert re.fullmatch(regex, '')
        assert not re.fullmatch(regex, 'a')

    def test_only_star(self):
        """* alone should match any non-hidden filename."""
        regex = glob_to_regex('*')
        assert re.fullmatch(regex, 'foo')
        assert re.fullmatch(regex, 'bar.txt')
        assert not re.fullmatch(regex, '.hidden')
        assert not re.fullmatch(regex, 'foo/bar')  # No slash crossing

    def test_only_doublestar(self):
        """** alone with recursive should match anything."""
        regex = glob_to_regex('**', recursive=True)
        assert re.fullmatch(regex, 'foo')
        assert re.fullmatch(regex, 'foo/bar')
        assert re.fullmatch(regex, 'foo/bar/baz')
