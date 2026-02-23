from __future__ import annotations

import bisect
import dataclasses
import os
import time
from typing import TYPE_CHECKING

from ..core.index import Order
from ..util.progbar import progressbar
from ..io import copyfile as barecat_copyfile

if TYPE_CHECKING:
    from ..core.barecat import Barecat


class BarecatDefragger:
    def __init__(self, bc: Barecat):
        self.bc = bc
        self.index = bc.index
        self.shard_size_limit = bc.shard_size_limit
        self.readonly = bc.readonly
        self.shard_files = bc.sharder.shard_files

    def defrag(self):
        if self.readonly:
            raise ValueError('Cannot defrag a read-only Barecat')

        new_shard = 0
        new_offset = 0

        old_total = self.bc.total_physical_size_seek

        try:
            for i in range(len(self.shard_files)):
                self.bc.sharder.reopen_shard(i, 'r+b')

            file_iter = self.index.iter_all_fileinfos(order=Order.ADDRESS)
            for fi in progressbar(file_iter, total=self.index.num_files, desc='Defragging'):
                if (self.shard_size_limit is not None and new_offset + fi.size >
                        self.shard_size_limit):
                    self.shard_files[new_shard].truncate(new_offset)
                    self.bc.sharder.reopen_shard(new_shard, 'rb')
                    new_shard += 1
                    new_offset = 0

                if not (new_shard == fi.shard and new_offset == fi.offset):
                    barecat_copyfile.copy(
                        self.shard_files[fi.shard], self.shard_files[new_shard],
                        fi.size, src_offset=fi.offset, dst_offset=new_offset)
                    self.index.update_file(fi.path, new_shard, new_offset)

                new_offset += fi.size

            # Truncate the last shard to its real size (the others are truncated already)
            self.shard_files[new_shard].truncate(new_offset)
            # Close and delete all shards after the last one
            for i in range(new_shard + 1, len(self.shard_files)):
                self.shard_files[i].close()
                os.remove(self.shard_files[i].name)
            del self.shard_files[new_shard + 1:]

            new_total = self.bc.total_physical_size_seek
            return old_total - new_total
        finally:
            self.bc.sharder.reopen_shards()

    def defrag_smart(self):
        """Defrag by copying contiguous chunks of files instead of individual files.

        This is more efficient than defrag() because it:
        - Copies entire contiguous ranges in one operation
        - Uses fewer syscalls and SQL statements
        - Can leverage kernel-level zero-copy for non-overlapping copies
        """
        if self.readonly:
            raise ValueError('Cannot defrag a read-only Barecat')

        new_shard = 0
        new_offset = 0
        src_shard = 0
        src_offset = 0

        old_total = self.bc.total_physical_size_seek

        try:
            for i in range(len(self.shard_files)):
                self.bc.sharder.reopen_shard(i, 'r+b')

            total_files = self.index.num_files
            files_processed = 0

            while files_processed < total_files:
                available_space = (
                    self.shard_size_limit - new_offset
                    if self.shard_size_limit is not None
                    else float('inf')
                )

                chunk = self._find_next_chunk(src_shard, src_offset, available_space)

                if chunk is None:
                    # No files fit in remaining space, move to next shard
                    if new_offset > 0 and self.shard_size_limit is not None:
                        self.shard_files[new_shard].truncate(new_offset)
                        self.bc.sharder.reopen_shard(new_shard, 'rb')
                        new_shard += 1
                        new_offset = 0
                        continue
                    else:
                        # No more files at all
                        break

                # Copy chunk if it needs to move
                if not (new_shard == chunk.shard and new_offset == chunk.start_offset):
                    barecat_copyfile.copy(
                        self.shard_files[chunk.shard],
                        self.shard_files[new_shard],
                        chunk.total_size,
                        src_offset=chunk.start_offset,
                        dst_offset=new_offset,
                    )

                    # Batch update all files in chunk
                    offset_delta = new_offset - chunk.start_offset
                    self.index.cursor.execute(
                        """
                        UPDATE files
                        SET shard = ?, offset = offset + ?
                        WHERE shard = ? AND offset >= ? AND offset < ?
                        """,
                        (
                            new_shard,
                            offset_delta,
                            chunk.shard,
                            chunk.start_offset,
                            chunk.end_offset,
                        ),
                    )

                new_offset += chunk.total_size
                files_processed += chunk.file_count

                # Advance source position past this chunk
                src_shard = chunk.shard
                src_offset = chunk.end_offset

            # Truncate the last shard
            self.shard_files[new_shard].truncate(new_offset)

            # Close and delete unused shards
            for i in range(new_shard + 1, len(self.shard_files)):
                self.shard_files[i].close()
                os.remove(self.shard_files[i].name)
            del self.shard_files[new_shard + 1:]

            new_total = self.bc.total_physical_size_seek
            return old_total - new_total
        finally:
            self.bc.sharder.reopen_shards()

    def _find_next_chunk(self, min_shard, min_offset, max_size):
        """Find the next contiguous chunk of files that fits within max_size.

        Args:
            min_shard: Minimum shard to consider.
            min_offset: Minimum offset within min_shard.
            max_size: Maximum total size of the chunk.

        Returns:
            ChunkInfo or None if no more files.
        """
        # Use recursive CTE to walk contiguous files - stops at first gap
        # Much more efficient than window functions for large archives
        result = self.index.fetch_one(
            """
            WITH RECURSIVE
            first_file AS (
                SELECT shard, offset, size, offset + size AS end_offset
                FROM files
                WHERE shard > :min_shard OR (shard = :min_shard AND offset >= :min_offset)
                ORDER BY shard, offset
                LIMIT 1
            ),
            chunk_files AS (
                -- Base case: first file (only if it fits)
                SELECT shard, offset, size, end_offset, offset AS chunk_start
                FROM first_file
                WHERE size <= :max_size

                UNION ALL

                -- Recursive: next contiguous file in same shard that still fits
                SELECT f.shard, f.offset, f.size, f.offset + f.size, c.chunk_start
                FROM chunk_files c
                JOIN files f ON f.shard = c.shard AND f.offset = c.end_offset
                WHERE f.offset + f.size <= c.chunk_start + :max_size
            )
            SELECT
                MIN(shard) AS shard,
                MIN(offset) AS start_offset,
                MAX(end_offset) AS end_offset,
                MAX(end_offset) - MIN(offset) AS total_size,
                COUNT(*) AS file_count
            FROM chunk_files
            """,
            dict(min_shard=min_shard, min_offset=min_offset, max_size=max_size),
            rowcls=ChunkInfo,
        )
        # If no files fit (file_count is 0 or None), return None
        if result is None or result.file_count == 0:
            return None
        return result

    def defrag_quick(self, time_max_seconds=5, max_skip_normal=2, max_skip_outlier=10):
        """Quick defrag: move tail files to fill earlier gaps.

        Args:
            time_max_seconds: Time limit for the operation.
            max_skip_normal: Max normal-sized stuck files to skip (these suggest gaps exhausted).
            max_skip_outlier: Max outlier-large stuck files to skip (these are just too big).
        """
        if self.readonly:
            raise ValueError('Cannot defrag a read-only Barecat')

        start_time = time.monotonic()
        gaps = self.get_gaps()
        old_total = self.bc.total_physical_size_seek
        outlier_threshold = self._get_outlier_threshold()

        try:
            for i in range(len(self.shard_files)):
                self.bc.sharder.reopen_shard(i, 'r+b')

            skipped = []
            normal_skipped = 0
            outlier_skipped = 0

            for fi in self.index.iter_all_fileinfos(order=Order.ADDRESS | Order.DESC):
                if time.monotonic() - start_time > time_max_seconds:
                    break

                old_shard, old_offset = fi.shard, fi.offset
                moved = self.move_to_earlier_gap(fi, gaps)
                if moved:
                    self._insert_gap_sorted(gaps, FragmentGap(old_shard, old_offset, fi.size))
                else:
                    skipped.append(fi)
                    if fi.size >= outlier_threshold:
                        outlier_skipped += 1
                        if outlier_skipped > max_skip_outlier:
                            break
                    else:
                        normal_skipped += 1
                        if normal_skipped > max_skip_normal:
                            break

            if skipped:
                # Compact all files from earliest skipped position onward
                earliest = min(skipped, key=lambda f: (f.shard, f.offset))
                tail_files = self.index.fetch_all("""
                    SELECT path, shard, offset, size
                    FROM files
                    WHERE shard > ? OR (shard = ? AND offset >= ?)
                    ORDER BY shard, offset
                """, (earliest.shard, earliest.shard, earliest.offset), rowcls=TailFile)
                self._compact_stuck_tail(tail_files, gaps)

            self.bc.truncate_all_to_logical_size()
        finally:
            self.bc.sharder.reopen_shards()

        return old_total - self.bc.total_physical_size_seek

    def needs_defrag(self):
        # check if total size of shards is larger than the sum of the sizes of the files in index
        # the getsize() function may not be fully up to date but this is only a heuristic anyway.
        return self.bc.total_physical_size_seek > self.bc.total_logical_size

    def get_defrag_info(self):
        return self.bc.total_physical_size_seek, self.bc.total_logical_size

    def get_gap_stats(self):
        """Get statistics about gaps in the archive.

        Returns:
            dict with keys:
                - total_gap_size: Total freeable space in bytes
                - num_gaps: Number of gaps
                - gap_sizes: List of individual gap sizes
                - physical_size: Total physical size of shards
                - logical_size: Total logical size of files
                - fragmentation_ratio: physical_size / logical_size (1.0 = no fragmentation)
                - gaps_by_shard: Dict mapping shard index to list of (offset, size) tuples
        """
        gaps = self.get_gaps(include_end_of_shard=False)
        physical_size = self.bc.total_physical_size_seek
        logical_size = self.bc.total_logical_size

        gap_sizes = [g.size for g in gaps]
        total_gap_size = sum(gap_sizes)

        gaps_by_shard = {}
        for g in gaps:
            if g.shard not in gaps_by_shard:
                gaps_by_shard[g.shard] = []
            gaps_by_shard[g.shard].append((g.offset, g.size))

        return {
            'total_gap_size': total_gap_size,
            'num_gaps': len(gaps),
            'gap_sizes': gap_sizes,
            'physical_size': physical_size,
            'logical_size': logical_size,
            'fragmentation_ratio': physical_size / logical_size if logical_size > 0 else 1.0,
            'gaps_by_shard': gaps_by_shard,
        }

    def get_gaps(self, include_end_of_shard=True):
        """Get list of gaps (unused space) in the archive.

        Args:
            include_end_of_shard: If True, include gaps from last file to shard_size_limit.
                                  If False, only include gaps between files.
        """
        if include_end_of_shard:
            # Include gaps to shard_size_limit (for defrag operations)
            gaps = self.index.fetch_all("""
                WITH x AS (
                    SELECT config.value_int AS shard_size_limit
                    FROM config
                    WHERE config.key = 'shard_size_limit'
                ),
                first_gaps AS (
                    SELECT
                        f.shard,
                        0 AS offset,
                        MIN(f.offset) AS size
                    FROM files f
                    GROUP BY f.shard
                ),
                nonfirst_gaps AS (
                    SELECT
                        f.shard,
                        (f.offset + f.size) AS offset,
                        coalesce(
                            lead(f.offset, 1) OVER (PARTITION BY f.shard ORDER BY f.offset),
                            x.shard_size_limit
                        ) - (f.offset + f.size) AS size
                    FROM files f, x
                ),
                all_gaps AS (SELECT * FROM first_gaps UNION ALL SELECT * FROM nonfirst_gaps)
                SELECT shard, offset, size
                FROM all_gaps
                WHERE size > 0
                ORDER BY shard, offset
            """, rowcls=FragmentGap)

            empty_shard_gaps = [
                FragmentGap(shard, 0, self.shard_size_limit)
                for shard in range(len(self.shard_files))
                if self.bc.index.logical_shard_end(shard) == 0]
            gaps.extend(empty_shard_gaps)
        else:
            # Only gaps between files (for stats)
            gaps = self.index.fetch_all("""
                WITH first_gaps AS (
                    SELECT
                        f.shard,
                        0 AS offset,
                        MIN(f.offset) AS size
                    FROM files f
                    GROUP BY f.shard
                ),
                nonfirst_gaps AS (
                    SELECT
                        f.shard,
                        (f.offset + f.size) AS offset,
                        lead(f.offset, 1) OVER (PARTITION BY f.shard ORDER BY f.offset)
                            - (f.offset + f.size) AS size
                    FROM files f
                ),
                all_gaps AS (SELECT * FROM first_gaps UNION ALL SELECT * FROM nonfirst_gaps)
                SELECT shard, offset, size
                FROM all_gaps
                WHERE size > 0
                ORDER BY shard, offset
            """, rowcls=FragmentGap)

        gaps.sort(key=lambda gap: (gap.shard, gap.offset))
        return gaps

    def move_to_earlier_gap(self, fi, gaps):
        for i_gap, gap in enumerate(gaps):
            if gap.shard > fi.shard or (gap.shard == fi.shard and gap.offset >= fi.offset):
                # reached the gap that is after the file, no move is possible
                return False
            if gap.size >= fi.size:
                barecat_copyfile.copy(
                    self.shard_files[fi.shard], self.shard_files[gap.shard],
                    fi.size, src_offset=fi.offset, dst_offset=gap.offset)

                self.index.update_file(fi.path, gap.shard, gap.offset)
                gap.size -= fi.size
                gap.offset += fi.size
                if gap.size == 0:
                    # even though we are changing the list while in a for loop that is iterating
                    # over it, this is safe because we are immediately returning in this iteration.
                    del gaps[i_gap]
                return True
        return False

    def _get_outlier_threshold(self):
        """Get file size threshold above which a file is considered an outlier (95th percentile)."""
        result = self.index.fetch_one("""
            SELECT size FROM files
            ORDER BY size
            LIMIT 1 OFFSET (SELECT CAST(COUNT(*) * 0.95 AS INTEGER) FROM files)
        """)
        return result[0] if result else float('inf')

    def _insert_gap_sorted(self, gaps, new_gap):
        """Insert gap into sorted list, merging with adjacent gaps."""
        i = bisect.bisect_left(gaps, new_gap)

        # Try merge with previous gap
        if i > 0:
            prev = gaps[i - 1]
            if prev.shard == new_gap.shard and prev.offset + prev.size == new_gap.offset:
                prev.size += new_gap.size
                # Also try merge with next
                if i < len(gaps):
                    next_gap = gaps[i]
                    if next_gap.shard == prev.shard and prev.offset + prev.size == next_gap.offset:
                        prev.size += next_gap.size
                        del gaps[i]
                return

        # Try merge with next gap
        if i < len(gaps):
            next_gap = gaps[i]
            if next_gap.shard == new_gap.shard and new_gap.offset + new_gap.size == next_gap.offset:
                next_gap.offset = new_gap.offset
                next_gap.size += new_gap.size
                return

        # No merge, just insert
        gaps.insert(i, new_gap)

    def _compact_stuck_tail(self, stuck_files, gaps):
        """Shift stuck files backward to close gaps immediately before them."""
        # Process from earliest to latest so shifts don't invalidate later positions
        stuck_files.sort(key=lambda f: (f.shard, f.offset))

        for fi in stuck_files:
            # Find gap immediately before this file in the same shard
            for i, gap in enumerate(gaps):
                if gap.shard == fi.shard and gap.offset + gap.size == fi.offset:
                    # Shift file backward to close the gap
                    new_offset = gap.offset
                    barecat_copyfile.copy(
                        self.shard_files[fi.shard], self.shard_files[fi.shard],
                        fi.size, src_offset=fi.offset, dst_offset=new_offset)
                    self.index.update_file(fi.path, fi.shard, new_offset)
                    # Gap moves to after the file
                    gap.offset = new_offset + fi.size
                    break
                if gap.shard > fi.shard or (gap.shard == fi.shard and gap.offset > fi.offset):
                    break


@dataclasses.dataclass
class FragmentGap:
    shard: int
    offset: int
    size: int

    def __lt__(self, other):
        return (self.shard, self.offset) < (other.shard, other.offset)

    @classmethod
    def row_factory(cls, cursor, row):
        field_names = [d[0] for d in cursor.description]
        return cls(**dict(zip(field_names, row)))


@dataclasses.dataclass
class TailFile:
    path: str
    shard: int
    offset: int
    size: int

    @classmethod
    def row_factory(cls, cursor, row):
        field_names = [d[0] for d in cursor.description]
        return cls(**dict(zip(field_names, row)))


@dataclasses.dataclass
class ChunkInfo:
    """Info about a contiguous chunk of files."""
    shard: int
    start_offset: int
    end_offset: int
    total_size: int
    file_count: int

    @classmethod
    def row_factory(cls, cursor, row):
        field_names = [d[0] for d in cursor.description]
        return cls(**dict(zip(field_names, row)))
