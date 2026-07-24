"""Reshard a barecat archive to a different shard size limit."""

from __future__ import annotations

import logging
import os
import os.path as osp
import uuid
from typing import TYPE_CHECKING, Optional

from ..core.index import Order
from ..core.sharder import Sharder
from ..exceptions import FileTooLargeBarecatError
from ..util.progbar import progressbar
from ..io import copyfile as barecat_copyfile

if TYPE_CHECKING:
    from ..core.barecat import Barecat

logger = logging.getLogger(__name__)


def reshard(bc: Barecat, target_shard_size_limit: int):
    """Reshard the archive to a new shard size limit.

    Optimized to avoid copying data that stays in place:
    - Shard0 is kept in place, files at the start that fit are not copied
    - Overflow goes to temp shard files
    - Old shards are truncated/deleted as they're fully read (progressive storage reclaim)
    - Temp shards are renamed to final names at the end

    Warning:
        Resharding rewrites shard data in place — do not interrupt it (kill/power
        loss can leave the archive needing manual recovery). For irreplaceable data,
        run on a copy of the archive. A clean Python-level error (e.g. out of disk
        space) is recovered automatically where safe; see the log output in that case.

    Args:
        bc: The barecat archive to reshard (must be opened read-write).
        target_shard_size_limit: The new shard size limit.

    Raises:
        FileTooLargeBarecatError: If any file exceeds the target shard size limit.
        ValueError: If the archive is read-only.
    """
    if bc.readonly:
        raise ValueError('Cannot reshard a read-only Barecat')

    # Validate all files fit in new limit
    max_file_size = bc.index.fetch_one('SELECT MAX(size) FROM files')[0] or 0
    if max_file_size > target_shard_size_limit:
        raise FileTooLargeBarecatError(max_file_size, target_shard_size_limit)

    # Temp path for overflow shards (shard1, shard2, ...)
    temp_suffix = f'_{uuid.uuid4().hex[:12]}'
    temp_path = f'{bc.path}{temp_suffix}'

    old_shard_files = bc.sharder.shard_files

    # Reopen shard0 for read+write
    bc.sharder.reopen_shard(0, 'r+b')
    shard0_file = old_shard_files[0]

    # Temp sharder for overflow (created lazily when needed)
    temp_sharder: Optional[Sharder] = None

    new_shard = 0
    new_offset = 0
    prev_source_shard = 0
    shard0_final_size = 0

    try:
        file_iter = bc.index.iter_all_fileinfos(order=Order.ADDRESS)
        for fi in progressbar(file_iter, total=bc.index.num_files, desc='Resharding'):
            # When we move to a new source shard, handle the old one
            if fi.shard > prev_source_shard:
                for s in range(prev_source_shard, fi.shard):
                    if s == 0:
                        # Truncate shard0 to its new size
                        shard0_file.truncate(shard0_final_size)
                        shard0_file.flush()
                    else:
                        # Delete old shards beyond 0
                        old_shard_files[s].truncate(0)
                prev_source_shard = fi.shard

            # Check if we need to move to a new destination shard
            if new_offset + fi.size > target_shard_size_limit:
                if new_shard == 0:
                    shard0_final_size = new_offset
                new_shard += 1
                new_offset = 0

                # Create temp sharder lazily
                if temp_sharder is None:
                    temp_sharder = Sharder(
                        temp_path,
                        shard_size_limit=target_shard_size_limit,
                        readonly=False,
                        append_only=False,
                    )

            # Write to destination
            if new_shard == 0:
                # Writing to shard0 (in place)
                if fi.shard == 0 and fi.offset == new_offset:
                    # File is already in the right place - no copy needed!
                    pass
                else:
                    # Copy to shard0
                    barecat_copyfile.copy(
                        old_shard_files[fi.shard],
                        shard0_file,
                        fi.size,
                        src_offset=fi.offset,
                        dst_offset=new_offset,
                    )
                    bc.index.update_file(fi.path, 0, new_offset)
                shard0_final_size = new_offset + fi.size
            else:
                # Writing to temp shard (new_shard - 1 because temp shards start at 0)
                temp_shard_idx, temp_offset, _, _ = temp_sharder.add(
                    size=fi.size,
                    fileobj=bc.sharder.open_from_address(fi.shard, fi.offset, fi.size),
                )
                # Map temp shard index to final shard number (temp0 -> shard1, etc.)
                bc.index.update_file(fi.path, temp_shard_idx + 1, temp_offset)

            new_offset += fi.size

        # Handle remaining old shards
        for s in range(prev_source_shard, len(old_shard_files)):
            if s == 0:
                shard0_file.truncate(shard0_final_size)
            else:
                old_shard_files[s].truncate(0)

        # Close and delete old shards (except shard0 which we keep)
        for i, f in enumerate(old_shard_files):
            if i == 0:
                continue
            name = f.name
            f.close()
            os.remove(name)

        # Rename temp shards to final names (temp0 -> shard1, temp1 -> shard2, etc.)
        if temp_sharder is not None:
            temp_sharder.close()
            for i in range(temp_sharder.num_shards):
                temp_name = temp_sharder.get_shard_path(i)
                final_name = bc.sharder.get_shard_path(i + 1)
                os.rename(temp_name, final_name)

        # Update shard size limit in index
        bc.shard_size_limit = target_shard_size_limit

        # Reopen sharder with new shards
        bc.sharder._shard_files = bc.sharder.open_shard_files()

    except BaseException as original_error:
        _recover_after_failed_reshard(bc, temp_sharder, original_error)
        raise
    finally:
        if temp_sharder is not None:
            temp_sharder.close()


def _recover_after_failed_reshard(bc, temp_sharder: Optional[Sharder], original_error):
    """Best-effort recovery after a clean mid-reshard error (e.g. out of disk space).

    Index rows written so far already carry final shard numbers while the data may
    still sit in temp-named files — and old shard files with those same final names
    may still exist, so reads could return silently wrong bytes. Two steps, both
    collision-safe:

    1. Rename each completed temp shard to its final name only where that name is
       free (old shard absent or already truncated to zero). Temps whose final name
       still holds live old data are left in place, with explicit instructions logged.
    2. Commit the index: reshard moves bytes before updating rows, so the pending
       rows describe where the data actually is — rolling them back (the normal
       exception behavior) would instead detach the index from the already-rewritten
       shard0.

    Recovery-path errors are logged, never raised, so the original error propagates.
    """
    try:
        leftover = []
        if temp_sharder is not None:
            temp_sharder.close()
            for i in range(temp_sharder.num_shards):
                temp_name = temp_sharder.get_shard_path(i)
                final_name = bc.sharder.get_shard_path(i + 1)
                if not osp.exists(temp_name):
                    continue
                if not osp.exists(final_name) or osp.getsize(final_name) == 0:
                    os.replace(temp_name, final_name)
                else:
                    leftover.append((temp_name, final_name))
        bc.index.conn.commit()
        if leftover:
            logger.error(
                'Reshard failed mid-way (%s). Completed portions were recovered, but '
                'the following temp shards could not be moved because their final '
                'names still hold not-yet-consumed old data. To finish recovery, '
                'verify each destination is no longer needed, then move: %s',
                original_error,
                '; '.join(f'{t} -> {f}' for t, f in leftover),
            )
        else:
            logger.error(
                'Reshard failed mid-way (%s); completed portions were recovered and '
                'the index was committed to match the data on disk. Re-run reshard '
                'to continue, and run verify to check the archive.',
                original_error,
            )
    except BaseException as recovery_error:
        logger.error(
            'Recovery after failed reshard itself failed: %r (original error: %r). '
            'Temp shard files are left in place.',
            recovery_error,
            original_error,
        )
