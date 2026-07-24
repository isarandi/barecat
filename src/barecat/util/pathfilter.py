"""Matching rules for ``--include``/``--exclude`` filter patterns.

One dialect for the whole CLI, chosen to agree with the tools users already know
rather than with Python's ``glob``:

- ``*`` matches inside a single path component, ``**`` crosses components, ``?``
  matches one non-separator character, ``[...]`` is a character class. This is
  rsync's and git's rule.
- A pattern **without** a leading ``/`` matches at any depth: ``*.log`` matches
  both ``a.log`` and ``d/s/a.log``. rsync and git say so explicitly; ``tar`` and
  ``zip`` arrive at the same result by letting ``*`` cross ``/``. Python's
  ``glob`` is the outlier here, and matching it would mean ``--exclude '*.log'``
  quietly keeping every nested log file.
- A **leading** ``/`` anchors the pattern at the archive root, so ``/logs/*``
  matches ``logs/x`` but not ``d/logs/x``.
- A **trailing** ``/`` stands for the whole subtree: ``logs/`` means ``logs/**``.

Rules are applied first-match-wins, again like rsync: the first pattern that
matches decides, so ``--include '*.jpg' --exclude '*'`` keeps only JPEGs.
"""

import functools
import re

from .glob_to_regex import glob_to_regex, glob_to_sqlite, expand_doublestar


def normalize_filter_pattern(pattern):
    """Split a filter pattern into ``(anchored, pattern)``.

    Strips the leading ``/`` that marks anchoring and expands a trailing ``/``
    into ``/**``.
    """
    anchored = pattern.startswith('/')
    if anchored:
        pattern = pattern.lstrip('/')
    if pattern.endswith('/'):
        pattern += '**'
    return anchored, pattern


@functools.lru_cache(maxsize=512)
def compile_filter_pattern(pattern):
    """Compile a filter pattern into a regex matching whole archive-relative paths."""
    anchored, pattern = normalize_filter_pattern(pattern)
    regex = glob_to_regex(pattern, recursive=True, include_hidden=True)
    if not anchored:
        # Let any number of leading components be skipped, so the pattern matches
        # at any depth (rsync: "each path component gets a turn").
        regex = f'(?:.*/)?{regex}'
    return re.compile(regex)


def matches_filter_pattern(path, pattern):
    """Whether an archive-relative path matches a single filter pattern."""
    return compile_filter_pattern(pattern).match(path) is not None


def first_match_wins(path, rules, default=True):
    """Apply the ordered rules to a path; the first matching rule decides.

    Args:
        path: Archive-relative path to test.
        rules: Ordered ``(sign, pattern)`` tuples, sign ``'+'`` to include and
            ``'-'`` to exclude.
        default: Verdict for a path that no rule matches.
    """
    for sign, pattern in rules:
        if compile_filter_pattern(pattern).match(path):
            return sign == '+'
    return default


def rules_from_include_exclude(include, exclude):
    """Build ordered rules when only unordered include/exclude lists are known.

    Includes come first so the ``--include '*.jpg' --exclude '*'`` idiom works;
    callers that know the command-line order should pass their own rules instead.
    """
    return [('+', p) for p in include or []] + [('-', p) for p in exclude or []]


def filter_pattern_to_sqlite_globs(pattern):
    """SQLite GLOB patterns that **over**match a filter pattern.

    Used as a cheap index-side prefilter for include rules; the precise verdict
    is made afterwards by :func:`first_match_wins`. Overmatching is what keeps
    that safe, so an unanchored pattern gets a leading ``*`` (SQLite's ``*``
    crosses ``/`` and also matches the empty string, so one variant covers both
    the root-level and the nested case).
    """
    anchored, pattern = normalize_filter_pattern(pattern)
    globs = expand_doublestar(glob_to_sqlite(pattern), recursive='**' in pattern)
    if anchored:
        return globs
    return ['*' if g == '*' else f'*{g}' for g in globs]
