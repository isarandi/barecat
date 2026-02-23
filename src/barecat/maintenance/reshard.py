"""Reshard a barecat archive to a different shard size limit."""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING, Optional

from ..core.index import Order
from ..core.sharder import Sharder
from ..exceptions import FileTooLargeBarecatError
from ..util.progbar import progressbar
from ..io import copyfile as barecat_copyfile

if TYPE_CHECKING:
    from ..core.barecat import Barecat


def reshard(bc: Barecat, target_shard_size_limit: int):
    """Reshard the archive to a new shard size limit.

    Optimized to avoid copying data that stays in place:
    - Shard0 is kept in place, files at the start that fit are not copied
    - Overflow goes to temp shard files
    - Old shards are truncated/deleted as they're fully read (progressive storage reclaim)
    - Temp shards are renamed to final names at the end

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
        bc.sharder._shard_files_storage._local.value = None
        bc.sharder._shard_files_storage._local.value = bc.sharder.open_shard_files()

    finally:
        if temp_sharder is not None:
            temp_sharder.close()
