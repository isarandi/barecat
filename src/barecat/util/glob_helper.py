"""Glob functionality for Barecat Index.

This module provides the GlobHelper class which handles all glob-related operations
for the Index class, including path and info globbing with Python glob compatibility.
"""

import re
from typing import Iterator, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..core.index import Index
    from ..core.types import BarecatDirInfo, BarecatFileInfo, BarecatEntryInfo, Order
else:
    from ..core.types import BarecatDirInfo, BarecatFileInfo, BarecatEntryInfo, Order

from ..exceptions import FileNotFoundBarecatError
from ..util.glob_to_regex import (
    glob_to_regex,
    glob_to_sqlite,
    expand_doublestar,
    pattern_to_sql_exclude,
)
from ..core.paths import normalize_path


class GlobHelper:
    """Handles glob operations for an Index instance.

    Args:
        index: The Index instance to operate on.
    """

    def __init__(self, index: 'Index'):
        self._index = index

    # Raw glob paths (direct SQLite GLOB, no Python glob conversion)

    def raw_glob_paths(self, pattern, order: 'Order' = Order.ANY):
        pattern = normalize_path(pattern)
        query = """
            SELECT path FROM dirs WHERE path GLOB :pattern
            UNION ALL
            SELECT path FROM files WHERE path GLOB :pattern"""
        query += order.as_query_text()
        rows = self._index.fetch_all(query, dict(pattern=pattern))
        return [row['path'] for row in rows]

    def raw_iterglob_paths(
        self, pattern, order: 'Order' = Order.ANY, only_files=False, bufsize=None
    ):
        pattern = normalize_path(pattern)
        if only_files:
            query = """
                SELECT path FROM files WHERE path GLOB :pattern"""
        else:
            query = """
                SELECT path FROM dirs WHERE path GLOB :pattern
                UNION ALL
                SELECT path FROM files WHERE path GLOB :pattern"""
        query += order.as_query_text()
        rows = self._index.fetch_iter(query, dict(pattern=pattern), bufsize=bufsize)
        return (row['path'] for row in rows)

    def raw_iterglob_paths_multi(
        self, patterns, order: 'Order' = Order.ANY, only_files=False, bufsize=None
    ):
        """Like raw_iterglob_paths but with multiple patterns OR'd together."""
        patterns = [normalize_path(p) for p in patterns]
        params = {f'p{i}': p for i, p in enumerate(patterns)}
        glob_expr = ' OR '.join(f'path GLOB :p{i}' for i in range(len(patterns)))
        if only_files:
            query = f'SELECT DISTINCT path FROM files WHERE {glob_expr}'
        else:
            query = f"""
                SELECT DISTINCT path FROM (
                    SELECT path FROM dirs WHERE {glob_expr}
                    UNION ALL
                    SELECT path FROM files WHERE {glob_expr}
                )"""
        query += order.as_query_text()
        rows = self._index.fetch_iter(query, params, bufsize=bufsize)
        return (row['path'] for row in rows)

    # Python glob-compatible path globbing

    def glob_paths(
        self,
        pattern: str,
        recursive: bool = False,
        include_hidden: bool = False,
        only_files: bool = False,
    ):
        r"""Glob for paths matching a pattern.

        The glob syntax is equivalent to Python's :py:func:`glob.glob`.

        Args:
            pattern: Glob pattern.
            recursive: Whether to glob recursively. If True, the pattern can contain the ``'/**/'``
                sequence to match any number of directories.
            include_hidden: Whether to include hidden files and directories (those starting with a
                dot).
            only_files: Whether to glob only files and not directories.

        Returns:
            A list of paths.
        """
        return list(
            self.iterglob_paths(
                pattern, recursive=recursive, include_hidden=include_hidden, only_files=only_files
            )
        )

    def iterglob_paths(
        self,
        pattern: str,
        recursive: bool = False,
        include_hidden: bool = False,
        bufsize: Optional[int] = None,
        only_files: bool = False,
    ) -> Iterator[str]:
        r"""Iterate over paths matching a pattern.

        The glob syntax is equivalent to Python's :py:func:`glob.iglob`.

        Args:
            pattern: Glob pattern.
            recursive: Whether to glob recursively. If True, the pattern can contain the ``'/**/'``
                sequence to match any number of directories.
            include_hidden: Whether to include hidden files and directories (those starting with a
                dot).
            bufsize: Buffer size for fetching rows.
            only_files: Whether to glob only files and not directories.

        Returns:
            An iterator over the paths.
        """
        regex_pattern = glob_to_regex(pattern, recursive=recursive, include_hidden=include_hidden)
        try:
            for candidate in self._iterglob_paths_unfiltered(
                pattern, recursive=recursive, bufsize=bufsize, only_files=only_files
            ):
                if re.match(regex_pattern, candidate):
                    yield candidate
        except FileNotFoundBarecatError:
            return

    def _iterglob_paths_unfiltered(
        self,
        pattern: str,
        recursive: bool = False,
        bufsize: Optional[int] = None,
        only_files: bool = False,
    ) -> Iterator[str]:
        """Internal glob without hidden filtering. Use iterglob_paths instead."""

        if recursive and pattern == '**':
            if only_files:
                yield from self._index.iter_all_filepaths(bufsize=bufsize)
            else:
                yield from self._index.iter_all_paths(bufsize=bufsize)
            return

        parts = pattern.split('/')
        num_has_wildcard = sum(1 for p in parts if '*' in p or '?' in p)
        has_no_brackets = '[' not in pattern and ']' not in pattern
        has_no_question = '?' not in pattern

        num_asterisk = pattern.count('*')
        if recursive and has_no_brackets and has_no_question and num_asterisk == 3:
            # Handle **/* at start OR /**/* in middle
            temp = '/' + pattern if pattern.startswith('**/') else pattern
            simplified = temp.replace('/**/*', '/*')
            if simplified.startswith('/'):
                simplified = simplified[1:]
            if '*' not in simplified.replace('*', '', 1):
                yield from self.raw_iterglob_paths(
                    simplified, bufsize=bufsize, only_files=only_files
                )
                return

        if (
            recursive
            and has_no_brackets
            and has_no_question
            and num_asterisk == 2
            and pattern.endswith('/**')
        ):
            if self._index.isdir(pattern[:-3]):
                if not only_files:
                    yield pattern[:-3] + '/'
                yield from self.raw_iterglob_paths(
                    pattern[:-1], bufsize=bufsize, only_files=only_files
                )
            return

        # Regex for fast short-circuit before exists check (include_hidden=True to not filter here)
        regex_pattern = glob_to_regex(pattern, recursive=recursive, include_hidden=True)
        if (not recursive or '**' not in pattern) and num_has_wildcard == 1 and has_no_brackets:
            parts = pattern.split('/')
            i_has_wildcard = next(i for i, p in enumerate(parts) if '*' in p or '?' in p)
            prefix = '/'.join(parts[:i_has_wildcard])
            wildcard_is_in_last_part = i_has_wildcard == len(parts) - 1
            if wildcard_is_in_last_part:
                info_generator = (
                    self._index.iter_direct_fileinfos(prefix)
                    if only_files
                    else self._index.iterdir_infos(prefix)
                )
                for info in info_generator:
                    yield info.path
            else:
                suffix = '/'.join(parts[i_has_wildcard + 1 :])
                further_subdirs_wanted = len(parts) > i_has_wildcard + 2
                for subdirinfo in self._index.iter_subdir_dirinfos(prefix):
                    if (
                        further_subdirs_wanted and subdirinfo.num_subdirs == 0
                    ) or subdirinfo.num_entries == 0:
                        continue
                    candidate = subdirinfo.path + '/' + suffix
                    if re.match(regex_pattern, candidate) and (
                        (self._index.exists(candidate) and not only_files)
                        or self._index.isfile(candidate)
                    ):
                        yield candidate
            return

        # Convert bracket syntax and expand ** patterns for SQLite GLOB
        sqlite_pattern = glob_to_sqlite(pattern)
        sqlite_patterns = expand_doublestar(sqlite_pattern, recursive=recursive)

        yield from self.raw_iterglob_paths_multi(
            sqlite_patterns, only_files=only_files, bufsize=bufsize
        )

    # Raw glob infos (direct SQLite GLOB, no Python glob conversion)

    def raw_iterglob_infos(self, pattern, only_files=False, bufsize=None):
        pattern = normalize_path(pattern)
        yield from self._index.fetch_iter(
            """
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
            FROM files WHERE path GLOB :pattern
            """,
            dict(pattern=pattern),
            bufsize=bufsize,
            rowcls=BarecatFileInfo,
        )
        if only_files:
            return
        yield from self._index.fetch_iter(
            """
            SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
                   mode, uid, gid, mtime_ns
            FROM dirs WHERE path GLOB :pattern
            """,
            dict(pattern=pattern),
            bufsize=bufsize,
            rowcls=BarecatDirInfo,
        )

    def raw_iterglob_infos_multi(self, patterns, only_files=False, bufsize=None):
        """Like raw_iterglob_infos but with multiple patterns OR'd together."""
        patterns = [normalize_path(p) for p in patterns]
        params = {f'p{i}': p for i, p in enumerate(patterns)}
        glob_expr = ' OR '.join(f'path GLOB :p{i}' for i in range(len(patterns)))

        fquery = f"""
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
            FROM files WHERE {glob_expr}
            """
        yield from self._index.fetch_iter(fquery, params, bufsize=bufsize, rowcls=BarecatFileInfo)
        if only_files:
            return

        dquery = f"""
            SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
                   mode, uid, gid, mtime_ns
            FROM dirs WHERE {glob_expr}
            """
        yield from self._index.fetch_iter(dquery, params, bufsize=bufsize, rowcls=BarecatDirInfo)

    def raw_iterglob_infos_incl_excl(self, patterns, only_files=False, bufsize=None):
        pattern_dict = {f'pattern{i}': normalize_path(p[1]) for i, p in enumerate(patterns)}
        globexpr = f'path GLOB :pattern{0}' if patterns[0][0] else f'path NOT GLOB :pattern{0}'
        for i, p in enumerate(patterns[1:], start=1):
            globexpr += f' OR path GLOB :pattern{i}' if p[0] else f' AND path NOT GLOB :pattern{i}'

        fquery = f"""
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
            FROM files WHERE {globexpr}
            """
        yield from self._index.fetch_iter(
            fquery, pattern_dict, bufsize=bufsize, rowcls=BarecatFileInfo
        )
        if only_files:
            return

        dquery = f"""
            SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
                   mode, uid, gid, mtime_ns
            FROM dirs WHERE {globexpr}
            """
        yield from self._index.fetch_iter(
            dquery, pattern_dict, bufsize=bufsize, rowcls=BarecatDirInfo
        )

    # Python glob-compatible info globbing

    def iterglob_infos(
        self,
        pattern: str,
        recursive: bool = False,
        include_hidden: bool = False,
        bufsize: Optional[int] = None,
        only_files: bool = False,
    ) -> Iterator['BarecatEntryInfo']:
        r"""Iterate over file and directory info objects matching a pattern.

        The glob syntax is equivalent to Python's :py:func:`glob.glob`.

        Args:
            pattern: Glob pattern.
            recursive: Whether to glob recursively. If True, the pattern can contain the ``'/**/'``
                sequence to match any number of directories.
            include_hidden: Whether to include hidden files and directories (those starting with a
                dot).
            bufsize: Buffer size for fetching rows.
            only_files: Whether to glob only files and not directories.

        Returns:
            An iterator over the file and directory info objects.
        """
        regex_pattern = glob_to_regex(pattern, recursive=recursive, include_hidden=include_hidden)
        try:
            for info in self._iterglob_infos_unfiltered(
                pattern, recursive=recursive, bufsize=bufsize, only_files=only_files
            ):
                if re.match(regex_pattern, info.path):
                    yield info
        except FileNotFoundBarecatError:
            return

    def _iterglob_infos_unfiltered(
        self,
        pattern: str,
        recursive: bool = False,
        bufsize: Optional[int] = None,
        only_files: bool = False,
    ) -> Iterator['BarecatEntryInfo']:
        """Internal glob without hidden filtering. Use iterglob_infos instead."""

        if recursive and pattern == '**':
            if only_files:
                yield from self._index.iter_all_fileinfos(bufsize=bufsize)
            else:
                yield from self._index.iter_all_infos(bufsize=bufsize)
            return

        parts = pattern.split('/')
        num_has_wildcard = sum(1 for p in parts if '*' in p or '?' in p)
        has_no_brackets = '[' not in pattern and ']' not in pattern
        has_no_question = '?' not in pattern

        num_asterisk = pattern.count('*')
        if recursive and has_no_brackets and has_no_question and num_asterisk == 3:
            # Handle **/* at start OR /**/* in middle
            temp = '/' + pattern if pattern.startswith('**/') else pattern
            simplified = temp.replace('/**/*', '/*')
            if simplified.startswith('/'):
                simplified = simplified[1:]
            if '*' not in simplified.replace('*', '', 1):
                yield from self.raw_iterglob_infos(
                    simplified, bufsize=bufsize, only_files=only_files
                )
                return

        if (
            recursive
            and has_no_brackets
            and has_no_question
            and num_asterisk == 2
            and pattern.endswith('/**')
        ):
            if self._index.isdir(pattern[:-3]):
                if not only_files:
                    yield self._index.lookup_dir(pattern[:-3])
                yield from self.raw_iterglob_infos(
                    pattern[:-1], bufsize=bufsize, only_files=only_files
                )
            return

        # Regex for fast short-circuit before exists check (include_hidden=True to not filter here)
        regex_pattern = glob_to_regex(pattern, recursive=recursive, include_hidden=True)
        if (not recursive or '**' not in pattern) and num_has_wildcard == 1 and has_no_brackets:
            parts = pattern.split('/')
            i_has_wildcard = next(i for i, p in enumerate(parts) if '*' in p or '?' in p)
            prefix = '/'.join(parts[:i_has_wildcard])
            wildcard_is_in_last_part = i_has_wildcard == len(parts) - 1
            if wildcard_is_in_last_part:
                info_generator = (
                    self._index.iter_direct_fileinfos(prefix)
                    if only_files
                    else self._index.iterdir_infos(prefix)
                )
                for info in info_generator:
                    yield info
            else:
                suffix = '/'.join(parts[i_has_wildcard + 1 :])
                further_subdirs_wanted = len(parts) > i_has_wildcard + 2
                for subdirinfo in self._index.iter_subdir_dirinfos(prefix):
                    if (
                        further_subdirs_wanted and subdirinfo.num_subdirs == 0
                    ) or subdirinfo.num_entries == 0:
                        continue
                    candidate_path = subdirinfo.path + '/' + suffix
                    if re.match(regex_pattern, candidate_path):
                        try:
                            yield (
                                self._index.lookup_file(candidate_path)
                                if only_files
                                else self._index.lookup(candidate_path)
                            )
                        except LookupError:
                            pass
            return

        # Convert bracket syntax and expand ** patterns for SQLite GLOB
        sqlite_pattern = glob_to_sqlite(pattern)
        sqlite_patterns = expand_doublestar(sqlite_pattern, recursive=recursive)

        yield from self.raw_iterglob_infos_multi(
            sqlite_patterns, only_files=only_files, bufsize=bufsize
        )

    # Rsync-style filtering

    def iterglob_infos_incl_excl(
        self,
        rules: list[tuple[str, str]],
        default_include: bool = True,
        only_files: bool = False,
        bufsize: Optional[int] = None,
    ) -> Iterator['BarecatEntryInfo']:
        """Iterate over infos matching rsync-style include/exclude rules.

        Uses "first match wins" semantics: each file is tested against rules
        in order, and the first matching rule determines inclusion/exclusion.

        Args:
            rules: List of (sign, pattern) tuples. sign is '+' for include,
                   '-' for exclude. Patterns use Python glob syntax with ** support.
            default_include: If no rule matches, include (True) or exclude (False).
            only_files: Whether to return only files, not directories.
            bufsize: Buffer size for fetching rows.

        Returns:
            Iterator over matching file/directory info objects.

        Example:
            rules = [
                ('+', '**/thumbs/important.jpg'),  # include this specific file
                ('-', '**/thumbs/*'),              # exclude other thumbs
                ('+', '**/*.jpg'),                 # include all other jpgs
            ]
            # Files not matching any rule: included (default_include=True)
        """
        if not rules:
            if default_include:
                yield from (
                    self._index.iter_all_fileinfos(bufsize=bufsize)
                    if only_files
                    else self._index.iter_all_infos(bufsize=bufsize)
                )
            return

        # Build nested SQL expression for first-match-wins:
        # GLOB inc1_over OR (NOT exc1_sql AND (GLOB inc2_over OR (... OR 1)))
        sql_expr, params = self._build_filter_sql(rules, default_include)

        # Precompute regex patterns for Python filtering
        rule_regexes = [
            (sign, glob_to_regex(p, recursive='**' in p, include_hidden=True)) for sign, p in rules
        ]

        fquery = f"""
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
            FROM files WHERE {sql_expr}
            """
        for info in self._index.fetch_iter(
            fquery, params, bufsize=bufsize, rowcls=BarecatFileInfo
        ):
            if self._first_match_wins(info.path, rule_regexes, default_include):
                yield info

        if only_files:
            return

        dquery = f"""
            SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
                   mode, uid, gid, mtime_ns
            FROM dirs WHERE {sql_expr}
            """
        for info in self._index.fetch_iter(dquery, params, bufsize=bufsize, rowcls=BarecatDirInfo):
            if self._first_match_wins(info.path, rule_regexes, default_include):
                yield info

    def _build_filter_sql(
        self, rules: list[tuple[str, str]], default_include: bool
    ) -> tuple[str, dict]:
        """Build nested SQL expression for first-match-wins filtering.

        Returns (sql_expr, params) where sql_expr uses overmatching for includes
        (safe: fetches superset) and undermatching for excludes (safe: excludes subset).
        Python filter does precise matching afterward.

        Pattern: GLOB inc1 OR (NOT exc1 AND (GLOB inc2 OR (NOT exc2 AND (... OR 1))))
        """
        params = {}
        param_idx = 0

        def add_param(value: str) -> str:
            nonlocal param_idx
            name = f'p{param_idx}'
            param_idx += 1
            params[name] = value
            return name

        # Build from inside out (reverse order), starting with default
        if default_include:
            expr = '1'  # OR 1 at deepest level
        else:
            expr = '0'  # OR 0 (nothing) at deepest level

        for sign, pattern in reversed(rules):
            if sign == '+':
                # Include: overmatch with GLOB (SQLite * matches /, that's OK)
                sqlite_patterns = expand_doublestar(
                    glob_to_sqlite(pattern), recursive='**' in pattern
                )
                if len(sqlite_patterns) == 1:
                    pname = add_param(sqlite_patterns[0])
                    inc_expr = f'path GLOB :{pname}'
                else:
                    parts = []
                    for sp in sqlite_patterns:
                        pname = add_param(sp)
                        parts.append(f'path GLOB :{pname}')
                    inc_expr = '(' + ' OR '.join(parts) + ')'
                expr = f'{inc_expr} OR ({expr})'
            else:
                # Special case: -x '**' means "exclude everything else"
                # Just set expr to 0 (no additional clause needed)
                if pattern == '**':
                    expr = '0'
                    continue

                # Exclude: try SQL-optimized pattern first
                sql_excl = pattern_to_sql_exclude(pattern)
                if sql_excl is not None:
                    excl_sql, excl_params = sql_excl
                    # Rename params to avoid collision
                    for k, v in excl_params.items():
                        new_name = add_param(v)
                        excl_sql = excl_sql.replace(f':{k}', f':{new_name}')
                    expr = f'NOT ({excl_sql}) AND ({expr})'
                else:
                    # Fallback: undermatch by replacing ** with * (doesn't cross /)
                    undermatch = pattern.replace('**/', '*/').replace('/**', '/*')
                    if undermatch == pattern:
                        undermatch = pattern.replace('**', '*')
                    sqlite_pat = glob_to_sqlite(undermatch)
                    pname = add_param(sqlite_pat)
                    expr = f'NOT (path GLOB :{pname}) AND ({expr})'

        return expr, params

    @staticmethod
    def _first_match_wins(path: str, rule_regexes: list[tuple[str, str]], default: bool) -> bool:
        """Apply rsync-style first-match-wins logic."""
        for sign, regex in rule_regexes:
            if re.match(regex, path):
                return sign == '+'
        return default
