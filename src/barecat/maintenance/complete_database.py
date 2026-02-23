#!/usr/bin/env python3
"""
Upgrade a simple barecat index (files only) to the full schema.

Creates a new proper barecat index and copies data from the simple one.

Usage:
    python barecat_upgrade_index.py /path/to/archive.barecat
"""

import os
import shutil
import sys
import barecat


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <barecat-index>")
        print(f"\nExample: {sys.argv[0]} /data/my_archive.barecat")
        sys.exit(1)

    db_path = sys.argv[1]
    upgrade_index(db_path)


def upgrade_index(db_path: str):
    tmp_path = db_path + '.new'

    # Remove temp if exists from previous failed run
    if os.path.exists(tmp_path):
        os.remove(tmp_path)

    # Create fresh index with full schema
    print("Step 1: Creating new index with full schema")

    with (
        barecat.Index(tmp_path, readonly=False) as index,
        index.no_triggers(),
    ):
        print("Step 2: Copying files from simple index...")
        index.cursor.execute(f"ATTACH DATABASE 'file:{db_path}?mode=ro' AS sourcedb")
        index.cursor.execute(
            """
            INSERT INTO files (path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns)
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
            FROM sourcedb.files
            """
        )
        index.conn.commit()
        file_count = index.cursor.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        print(f"  Copied {file_count} files")

        print("Step 3: Populating dirs table...")
        index.update_dirs()
        index.conn.commit()
        index.cursor.execute("DETACH DATABASE sourcedb")

        dir_count = index.cursor.execute("SELECT COUNT(*) FROM dirs").fetchone()[0]
        print(f"  Created {dir_count} directories")

        print("Step 4: Calculating tree statistics...")
        index.update_treestats()
        index.conn.commit()

        total_size = index.cursor.execute("SELECT SUM(size) FROM files").fetchone()[0] or 0

        print(f"\nDone!")
        print(f"  Files: {file_count}")
        print(f"  Directories: {dir_count}")
        print(f"  Total size: {total_size:,} bytes ({total_size / (1024**3):.2f} GB)")

    # Replace old with new
    os.replace(db_path, db_path + '.bak')
    os.replace(tmp_path, db_path)
    print(f"\nReplaced {db_path}")


if __name__ == "__main__":
    main()
