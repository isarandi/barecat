"""Merge functionality for Barecat Index and Barecat archive.

This module provides helper classes for merging Barecat archives:
- IndexMergeHelper: Handles index-only merging (for symlink mode)
- BarecatMergeHelper: Handles full merging with data copying
"""

import os
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ..core.index import Index
    from ..core.barecat import Barecat

from ..core.paths import get_ancestors, resolve_index_path
from ..core.types import BarecatFileInfo
from ..io.copyfile import copy


class IndexMergeHelper:
    """Handles index-only merging for symlink mode.

    Args:
        index: The Index instance to operate on.
    """

    def __init__(self, index: 'Index'):
        self._index = index

    def merge_from_other_barecat(
        self,
        source_index_path: str,
        ignore_duplicates: bool = False,
        prefix: str = None,
        update_treestats: bool = True,
    ):
        """Adds the files and directories from another Barecat index to this one.

        Typically used during symlink-based merging. That is, the shards in the source Barecat
        are assumed to be simply be placed next to each other, instead of being merged with the
        existing shards in this index.
        For merging the shards themselves, more complex logic is needed, and that method is
        in the Barecat class.

        Args:
            source_index_path: Path to the source Barecat index.
            ignore_duplicates: Whether to ignore duplicate files and directories.
            prefix: Optional path prefix to prepend to all paths.
            update_treestats: Whether to recompute directory tree statistics after merging.

        Raises:
            sqlite3.DatabaseError: If an error occurs during the operation.
            ValueError: If file/directory path conflicts exist between source and target.
        """
        with self._index.attached_database(source_index_path):
            self._merge_from_attached_sourcedb(ignore_duplicates, prefix, update_treestats)

    def _merge_from_attached_sourcedb(
        self, ignore_duplicates: bool, prefix: str, update_treestats: bool
    ):
        path_expr = self.check_merge_conflicts(prefix)

        with self._index.no_triggers():
            if prefix:
                # Upsert prefix directory chain with source's root stats
                root_stats = self._index.fetch_one(
                    "SELECT size_tree, num_files_tree FROM sourcedb.dirs WHERE path = ''"
                )
                if root_stats:
                    self._index.cursor.executemany(
                        """INSERT INTO dirs (path, size_tree, num_files_tree) VALUES (?, ?, ?)
                           ON CONFLICT(path) DO UPDATE SET
                               size_tree = size_tree + excluded.size_tree,
                               num_files_tree = num_files_tree + excluded.num_files_tree""",
                        [(a, root_stats[0], root_stats[1]) for a in get_ancestors(prefix)],
                    )

            # Upsert all directories (exclude root if prefix, since it's handled above)
            self._index.cursor.execute(
                f"""
                INSERT INTO dirs (
                    path, num_subdirs, num_files, size_tree, num_files_tree,
                    mode, uid, gid, mtime_ns)
                SELECT {path_expr}, num_subdirs, num_files, size_tree, num_files_tree,
                    mode, uid, gid, mtime_ns
                FROM sourcedb.dirs WHERE {"path != ''" if prefix else "true"}
                ON CONFLICT (dirs.path) DO UPDATE SET
                    num_subdirs = num_subdirs + excluded.num_subdirs,
                    num_files = num_files + excluded.num_files,
                    size_tree = size_tree + excluded.size_tree,
                    num_files_tree = num_files_tree + excluded.num_files_tree,
                    mode = coalesce(
                        dirs.mode | excluded.mode,
                        coalesce(dirs.mode, 0) | excluded.mode,
                        dirs.mode | coalesce(excluded.mode, 0)),
                    uid = coalesce(excluded.uid, dirs.uid),
                    gid = coalesce(excluded.gid, dirs.gid),
                    mtime_ns = coalesce(
                        max(dirs.mtime_ns, excluded.mtime_ns),
                        max(coalesce(dirs.mtime_ns, 0), excluded.mtime_ns),
                        max(dirs.mtime_ns, coalesce(excluded.mtime_ns, 0)))
                """
            )
            new_shard_number = self._index.num_used_shards
            maybe_ignore = 'OR IGNORE' if ignore_duplicates else ''
            self._index.cursor.execute(
                f"""
                INSERT {maybe_ignore} INTO files (
                    path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns)
                SELECT {path_expr}, shard + ?, offset, size, crc32c, mode, uid, gid, mtime_ns
                FROM sourcedb.files
                """,
                (new_shard_number,),
            )

        # Post-processing (triggers can be on)
        if prefix:
            # Update num_subdirs and num_files for prefix chain (these aren't additive)
            for ancestor in get_ancestors(prefix):
                self._index.cursor.execute(
                    """
                    UPDATE dirs SET
                        num_subdirs = (SELECT COUNT(*) FROM dirs WHERE parent = ?),
                        num_files = (SELECT COUNT(*) FROM files WHERE parent = ?)
                    WHERE path = ?
                """,
                    (ancestor, ancestor, ancestor),
                )
        elif update_treestats and ignore_duplicates:
            self._index.update_treestats()

    def check_merge_conflicts(self, prefix: str) -> str:
        """Check for file/directory conflicts with attached sourcedb.

        Must be called while sourcedb is attached.

        Args:
            prefix: Path prefix to prepend to source paths (empty string for no prefix).

        Returns:
            SQL expression for transforming source paths with prefix.

        Raises:
            ValueError: If file/directory path conflicts exist.
        """
        if prefix:
            # Escape single quotes for safe SQL string interpolation (' -> '')
            escaped = prefix.replace("'", "''")
            path_expr = f"CASE WHEN path = '' THEN '{escaped}' ELSE '{escaped}/' || path END"
            # Check prefix ancestors don't conflict with target files
            parts = prefix.split('/')
            for i in range(len(parts)):
                ancestor = '/'.join(parts[: i + 1])
                if self._index.isfile(ancestor):
                    raise ValueError(
                        f"Cannot use prefix '{prefix}': '{ancestor}' exists as a file"
                    )
        else:
            path_expr = "path"

        # Source file conflicts with target directory
        conflict = self._index.fetch_one(
            f"SELECT {path_expr} FROM sourcedb.files "
            f"WHERE {path_expr} IN (SELECT path FROM dirs) LIMIT 1"
        )
        if conflict:
            raise ValueError(f"Source file '{conflict[0]}' conflicts with target directory")

        # Source directory conflicts with target file
        conflict = self._index.fetch_one(
            f"SELECT {path_expr} FROM sourcedb.dirs "
            f"WHERE {path_expr} IN (SELECT path FROM files) LIMIT 1"
        )
        if conflict:
            raise ValueError(f"Source directory '{conflict[0]}' conflicts with target file")

        return path_expr


class BarecatMergeHelper:
    """Handles full merging with data copying.

    Args:
        barecat: The Barecat instance to operate on.
    """

    def __init__(self, barecat: 'Barecat'):
        self._bc = barecat

    def merge_from_other_barecat(
        self,
        source_path: str,
        ignore_duplicates: bool = False,
        prefix: str = '',
        pattern: str = None,
        filter_rules: list = None,
    ):
        """Merge the contents of another Barecat archive into this one.

        Args:
            source_path: Path to the other Barecat archive.
            ignore_duplicates: If True, do not raise an error when a file with the same path already
                exists in the archive.
            prefix: Path prefix to prepend to all paths (default: '', no prefix).
            pattern: Glob pattern to filter files (uses optimized iterglob_infos).
            filter_rules: Rsync-style include/exclude rules as list of ('+'/'-', pattern) tuples.

        Raises:
            ValueError: If the shard size limit is set and a file in the source archive is larger
                than the shard size limit.
        """
        if prefix is None:
            prefix = ''

        if pattern is not None or filter_rules:
            self._merge_from_other_barecat_filtered(
                source_path, ignore_duplicates, prefix, pattern, filter_rules
            )
            return

        source_index_path = resolve_index_path(source_path)
        with self._bc.index.attached_database(source_index_path):
            self._merge_from_attached_sourcedb(source_path, ignore_duplicates, prefix)

    def _merge_from_attached_sourcedb(
        self, source_path: str, ignore_duplicates: bool, prefix: str
    ):
        index = self._bc.index
        sharder = self._bc.sharder

        out_shard_number = sharder.num_shards - 1
        out_shard = sharder.last_shard_file
        out_shard.seek(0, os.SEEK_END)
        out_shard_offset = out_shard.tell()

        path_expr = index._merge_helper.check_merge_conflicts(prefix)

        if self._bc.shard_size_limit is not None:
            in_max_size = index.fetch_one("SELECT MAX(size) FROM sourcedb.files")[0]
            if in_max_size is not None and in_max_size > self._bc.shard_size_limit:
                raise ValueError('Files in the source archive are larger than the shard size')

        # === TRIGGERS ON ===

        # Step 1: Upsert target root (prefix) with source root's stats
        # Triggers propagate size_tree/num_files_tree to ancestors of prefix
        root_stats = index.fetch_one(
            "SELECT size_tree, num_files_tree, num_files, mode, uid, gid, mtime_ns "
            "FROM sourcedb.dirs WHERE path = ''"
        )
        index.cursor.execute(
            """INSERT INTO dirs (path, size_tree, num_files_tree, num_files, mode, uid, gid, mtime_ns)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(path) DO UPDATE SET
                   size_tree = size_tree + excluded.size_tree,
                   num_files_tree = num_files_tree + excluded.num_files_tree,
                   num_files = num_files + excluded.num_files,
                   mode = COALESCE(dirs.mode | excluded.mode,
                                   COALESCE(dirs.mode, 0) | excluded.mode,
                                   dirs.mode | COALESCE(excluded.mode, 0)),
                   uid = COALESCE(excluded.uid, dirs.uid),
                   gid = COALESCE(excluded.gid, dirs.gid),
                   mtime_ns = COALESCE(
                       MAX(dirs.mtime_ns, excluded.mtime_ns),
                       MAX(COALESCE(dirs.mtime_ns, 0), excluded.mtime_ns),
                       MAX(dirs.mtime_ns, COALESCE(excluded.mtime_ns, 0)))""",
            (prefix, *root_stats),
        )

        # Step 2: Insert all source dir paths (triggers handle num_subdirs)
        index.cursor.execute(
            f"INSERT OR IGNORE INTO dirs (path) SELECT {path_expr} FROM sourcedb.dirs"
        )

        # === TRIGGERS OFF ===

        with index.no_triggers():
            # Step 3: Update non-root dirs with their stats
            index.cursor.execute(
                f"""UPDATE dirs SET
                        size_tree = COALESCE(dirs.size_tree, 0) + src.size_tree,
                        num_files_tree = COALESCE(dirs.num_files_tree, 0) + src.num_files_tree,
                        num_files = COALESCE(dirs.num_files, 0) + src.num_files,
                        mode = COALESCE(dirs.mode | src.mode,
                                        COALESCE(dirs.mode, 0) | src.mode,
                                        dirs.mode | COALESCE(src.mode, 0)),
                        uid = COALESCE(src.uid, dirs.uid),
                        gid = COALESCE(src.gid, dirs.gid),
                        mtime_ns = COALESCE(
                            MAX(dirs.mtime_ns, src.mtime_ns),
                            MAX(COALESCE(dirs.mtime_ns, 0), src.mtime_ns),
                            MAX(dirs.mtime_ns, COALESCE(src.mtime_ns, 0)))
                    FROM (
                        SELECT {path_expr} AS path, size_tree, num_files_tree, num_files,
                               mode, uid, gid, mtime_ns
                        FROM sourcedb.dirs WHERE path != ''
                    ) src
                    WHERE dirs.path = src.path"""
            )

            in_shard_number = 0
            in_shard_path = f'{source_path}-shard-{in_shard_number:05d}'
            in_shard = open(in_shard_path, 'rb')
            in_shard_offset = 0
            in_shard_end = index.fetch_one(
                "SELECT COALESCE(MAX(offset + size), 0) FROM sourcedb.files WHERE shard=?",
                (in_shard_number,),
            )[0]

            while True:
                if in_shard_offset == in_shard_end:
                    # we finished this in_shard, move to the next one
                    in_shard.close()
                    in_shard_number += 1
                    in_shard_path = f'{source_path}-shard-{in_shard_number:05d}'
                    try:
                        in_shard = open(in_shard_path, 'rb')
                    except FileNotFoundError:
                        # done with all in_shards of this source
                        break
                    in_shard_offset = 0
                    in_shard_end = index.fetch_one(
                        "SELECT COALESCE(MAX(offset + size), 0) FROM sourcedb.files WHERE shard=?",
                        (in_shard_number,),
                    )[0]
                    continue

                if self._bc.shard_size_limit is not None:
                    out_shard_space_left = self._bc.shard_size_limit - out_shard_offset
                    # check how much of the in_shard we can put in the current out_shard
                    fetched = index.fetch_one(
                        """
                        SELECT MAX(offset + size) - :in_shard_offset AS max_offset_size_adjusted
                        FROM sourcedb.files
                        WHERE offset >= :in_shard_offset
                        AND offset + size <= :in_shard_offset + :out_shard_space_left
                        AND shard = :in_shard_number""",
                        dict(
                            in_shard_offset=in_shard_offset,
                            out_shard_space_left=out_shard_space_left,
                            in_shard_number=in_shard_number,
                        ),
                    )
                    if fetched is None or fetched[0] is None:
                        # No file of the current in_shard fits in the current out_shard,
                        # must start a new one
                        sharder.start_new_shard()
                        out_shard = sharder.last_shard_file
                        out_shard_number += 1
                        out_shard_offset = 0
                        continue

                    max_copiable_amount = fetched[0]
                else:
                    max_copiable_amount = None

                # now we need to update the index, but we need to update the offset and shard
                # of the files that we copied
                maybe_ignore = 'OR IGNORE' if ignore_duplicates else ''
                index.cursor.execute(
                    f"""
                    INSERT {maybe_ignore} INTO files (
                        path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns)
                    SELECT {path_expr}, :out_shard_number, offset + :out_minus_in_shard_offset,
                        size, crc32c, mode, uid, gid, mtime_ns
                    FROM sourcedb.files
                    WHERE offset >= :in_shard_offset AND shard = :in_shard_number"""
                    + (
                        """
                    AND offset + size <= :in_shard_offset + :max_copiable_amount
                    """
                        if max_copiable_amount is not None
                        else ""
                    ),
                    dict(
                        out_shard_number=out_shard_number,
                        in_shard_offset=in_shard_offset,
                        out_minus_in_shard_offset=out_shard_offset - in_shard_offset,
                        in_shard_number=in_shard_number,
                        max_copiable_amount=max_copiable_amount,
                    ),
                )
                copy(in_shard, out_shard, max_copiable_amount)
                out_shard_offset = out_shard.tell()
                in_shard_offset = in_shard.tell()

        # Step 4: If ignore_duplicates, recompute stats for affected dirs
        # (some files may have been skipped, making source's stats too high)
        if ignore_duplicates:
            with index.no_triggers():
                # Recompute num_files (direct count)
                index.cursor.execute(
                    f"""UPDATE dirs SET
                            num_files = (SELECT COUNT(*) FROM files WHERE parent = dirs.path)
                        WHERE path IN (SELECT {path_expr} FROM sourcedb.dirs)"""
                )
                # Recompute size_tree and num_files_tree using CTE for affected branches
                index.cursor.execute(
                    f"""WITH RECURSIVE
                        affected_ancestors AS (
                            -- Start from parents of source files (with prefix applied)
                            SELECT CASE WHEN parent = '' THEN :prefix
                                        WHEN :prefix = '' THEN parent
                                        ELSE :prefix || '/' || parent END AS path
                            FROM sourcedb.files
                            UNION
                            -- Walk up to root
                            SELECT rtrim(rtrim(path, replace(path, '/', '')), '/')
                            FROM affected_ancestors WHERE path != ''
                        ),
                        file_ancestors AS (
                            -- Expand each file to its ancestors (only within affected paths)
                            SELECT parent AS ancestor, size FROM files
                            WHERE parent IN (SELECT path FROM affected_ancestors)
                            UNION ALL
                            SELECT rtrim(rtrim(ancestor, replace(ancestor, '/', '')), '/'), size
                            FROM file_ancestors
                            WHERE ancestor != '' AND ancestor IN (SELECT path FROM affected_ancestors)
                        ),
                        recomputed AS (
                            SELECT ancestor AS path,
                                   SUM(size) AS size_tree,
                                   COUNT(*) AS num_files_tree
                            FROM file_ancestors
                            GROUP BY ancestor
                        )
                    UPDATE dirs SET
                        size_tree = COALESCE(recomputed.size_tree, 0),
                        num_files_tree = COALESCE(recomputed.num_files_tree, 0)
                    FROM recomputed
                    WHERE dirs.path = recomputed.path""",
                    {'prefix': prefix},
                )

    def _merge_from_other_barecat_filtered(
        self,
        source_path: str,
        ignore_duplicates: bool,
        prefix: str,
        pattern: Union[str, None],
        filter_rules: Union[list, None],
    ):
        """Merge with filtering using hybrid SQL/Python approach.

        1. Filter files in Python (iterglob_infos or iterglob_infos_incl_excl)
        2. Sort by physical order
        3. Python computes dest placements (handles shard boundaries)
        4. Detect contiguous blocks for efficient copying
        5. Copy blocks, bulk insert files
        6. update_dirs + update_treestats
        """
        import barecat

        index = self._bc.index
        sharder = self._bc.sharder
        shard_size_limit = self._bc.shard_size_limit

        # Get current dest position
        dst_shard = sharder.num_shards - 1
        dst_file = sharder.last_shard_file
        dst_file.seek(0, os.SEEK_END)
        dst_offset = dst_file.tell()

        with barecat.Barecat(source_path, readonly=True) as source:
            # Step 1: Get filtered file infos
            if pattern is not None:
                file_infos = list(source.index.iterglob_infos(
                    pattern, recursive=True, include_hidden=True, only_files=True
                ))
            else:
                file_infos = list(source.index.iterglob_infos_incl_excl(
                    filter_rules, only_files=True
                ))

            if not file_infos:
                return

            # Step 2: Sort by physical order for efficient sequential reads
            file_infos.sort(key=lambda f: (f.shard, f.offset))

            # Step 3: Compute destination placements and detect contiguous blocks
            # A block is contiguous if files are adjacent in both src and dst
            placements = []  # (fi, dst_shard, dst_offset)
            blocks = []  # (src_shard, src_offset, dst_shard, dst_offset, size)

            block_src_shard = file_infos[0].shard
            block_src_offset = file_infos[0].offset
            block_dst_shard = dst_shard
            block_dst_offset = dst_offset
            block_size = 0

            for fi in file_infos:
                # Check if file fits in current dest shard
                if shard_size_limit is not None and dst_offset + fi.size > shard_size_limit:
                    # Flush current block before starting new shard
                    if block_size > 0:
                        blocks.append((
                            block_src_shard, block_src_offset,
                            block_dst_shard, block_dst_offset, block_size
                        ))
                    # Start new shard
                    sharder.start_new_shard()
                    dst_shard += 1
                    dst_offset = 0
                    dst_file = sharder.last_shard_file
                    # Start new block
                    block_src_shard = fi.shard
                    block_src_offset = fi.offset
                    block_dst_shard = dst_shard
                    block_dst_offset = dst_offset
                    block_size = 0

                # Check if this file continues the current block
                expected_src_offset = block_src_offset + block_size
                is_contiguous = (
                    fi.shard == block_src_shard and
                    fi.offset == expected_src_offset
                )

                if not is_contiguous and block_size > 0:
                    # Flush current block
                    blocks.append((
                        block_src_shard, block_src_offset,
                        block_dst_shard, block_dst_offset, block_size
                    ))
                    # Start new block
                    block_src_shard = fi.shard
                    block_src_offset = fi.offset
                    block_dst_shard = dst_shard
                    block_dst_offset = dst_offset
                    block_size = 0

                # Record placement
                placements.append((fi, dst_shard, dst_offset))
                block_size += fi.size
                dst_offset += fi.size

            # Flush final block
            if block_size > 0:
                blocks.append((
                    block_src_shard, block_src_offset,
                    block_dst_shard, block_dst_offset, block_size
                ))

            # Step 4: Copy blocks
            src_shard_files = {}  # cache open shard files
            try:
                for src_shard, src_offset, dst_shard_num, dst_off, size in blocks:
                    # Open source shard if needed
                    if src_shard not in src_shard_files:
                        src_shard_path = f'{source_path}-shard-{src_shard:05d}'
                        src_shard_files[src_shard] = open(src_shard_path, 'rb')

                    src_file = src_shard_files[src_shard]
                    src_file.seek(src_offset)

                    # Get dest shard file
                    dst_file = sharder.shard_files[dst_shard_num]
                    dst_file.seek(dst_off)

                    copy(src_file, dst_file, size)
            finally:
                for f in src_shard_files.values():
                    f.close()

        # Step 5: Bulk insert files (outside source context)
        with index.no_triggers():
            maybe_ignore = 'OR IGNORE' if ignore_duplicates else ''

            # Prepare file data for insert
            file_rows = []
            for fi, new_shard, new_offset in placements:
                if prefix:
                    new_path = f'{prefix}/{fi.path}' if fi.path else prefix
                else:
                    new_path = fi.path
                file_rows.append((
                    new_path, new_shard, new_offset, fi.size, fi.crc32c,
                    fi.mode, fi.uid, fi.gid, fi.mtime_ns
                ))

            index.cursor.executemany(
                f"""INSERT {maybe_ignore} INTO files
                    (path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                file_rows,
            )

        # Step 6: Create ancestor directories and update stats
        index.update_dirs()
        index.update_treestats()


