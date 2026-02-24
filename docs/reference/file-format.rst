File Format Specification
=========================

This document describes the barecat file format.

Overview
--------

A barecat archive consists of:

1. **Shard files** - Binary files containing concatenated file data
2. **Index file** - SQLite database with metadata

File Naming
-----------

Given a base path ``myarchive.barecat``:

.. code-block:: text

   myarchive.barecat                 # SQLite index database
   myarchive.barecat-shard-00000     # First data shard
   myarchive.barecat-shard-00001     # Second data shard (if needed)
   myarchive.barecat-shard-00002     # ...

Shard numbers are zero-padded to 5 digits.

Shard Files
-----------

Shard files are simple binary files containing file data concatenated
sequentially:

.. code-block:: text

   +----------------+----------------+----------------+-----+
   | File 1 data    | File 2 data    | File 3 data    | ... |
   +----------------+----------------+----------------+-----+
   ^                ^                ^
   offset=0         offset=1000      offset=2500

There is no header, footer, or delimiter between files. The index stores
the offset and size of each file.

When files are deleted, gaps may appear:

.. code-block:: text

   +----------------+--------+----------------+-----+
   | File 1 data    | [gap]  | File 3 data    | ... |
   +----------------+--------+----------------+-----+

Gaps are reclaimed by ``barecat defrag``.

SQLite Index
------------

The index is a standard SQLite 3 database with the following schema:

Tables
~~~~~~

**files** - File metadata

.. code-block:: sql

   CREATE TABLE files (
       path     TEXT NOT NULL,
       parent   TEXT GENERATED ALWAYS AS (
           rtrim(rtrim(path, replace(path, '/', '')), '/')
       ) VIRTUAL NOT NULL REFERENCES dirs(path),
       shard    INTEGER NOT NULL,
       offset   INTEGER NOT NULL,
       size     INTEGER DEFAULT 0,
       crc32c   INTEGER DEFAULT NULL,
       mode     INTEGER DEFAULT NULL,
       uid      INTEGER DEFAULT NULL,
       gid      INTEGER DEFAULT NULL,
       mtime_ns INTEGER DEFAULT NULL
   );

- ``path``: Full path within the archive (e.g., "dir/subdir/file.txt")
- ``parent``: Computed parent directory path
- ``shard``: Shard number (0, 1, 2, ...)
- ``offset``: Byte offset within the shard
- ``size``: File size in bytes
- ``crc32c``: CRC32C checksum of file contents
- ``mode``: Unix file mode (permissions)
- ``uid``, ``gid``: Owner user/group ID
- ``mtime_ns``: Modification time in nanoseconds since Unix epoch

**dirs** - Directory metadata and statistics

.. code-block:: sql

   CREATE TABLE dirs (
       path           TEXT NOT NULL,
       parent         TEXT GENERATED ALWAYS AS (
           CASE WHEN path = '' THEN NULL
           ELSE rtrim(rtrim(path, replace(path, '/', '')), '/') END
       ) VIRTUAL REFERENCES dirs(path),
       num_subdirs    INTEGER DEFAULT 0,
       num_files      INTEGER DEFAULT 0,
       num_files_tree INTEGER DEFAULT 0,
       size_tree      INTEGER DEFAULT 0,
       mode           INTEGER DEFAULT NULL,
       uid            INTEGER DEFAULT NULL,
       gid            INTEGER DEFAULT NULL,
       mtime_ns       INTEGER DEFAULT NULL
   );

- ``num_subdirs``: Immediate subdirectory count
- ``num_files``: Immediate file count
- ``num_files_tree``: Recursive file count
- ``size_tree``: Recursive total size

**config** - Archive configuration

.. code-block:: sql

   CREATE TABLE config (
       key        TEXT PRIMARY KEY,
       value_text TEXT DEFAULT NULL,
       value_int  INTEGER DEFAULT NULL
   ) WITHOUT ROWID;

Standard config entries:

- ``use_triggers``: 1 if triggers are active
- ``shard_size_limit``: Maximum shard size in bytes
- ``schema_version_major``: Schema major version (currently 0)
- ``schema_version_minor``: Schema minor version (currently 3)

Indexes
~~~~~~~

.. code-block:: sql

   CREATE UNIQUE INDEX idx_files_path ON files(path);
   CREATE UNIQUE INDEX idx_dirs_path ON dirs(path);
   CREATE INDEX idx_files_parent ON files(parent);
   CREATE INDEX idx_dirs_parent ON dirs(parent);
   CREATE INDEX idx_files_shard_offset ON files(shard, offset);

Triggers
~~~~~~~~

The database uses triggers to maintain directory statistics. When a file is
added, the parent directory's counters are automatically updated, propagating
up the tree.

Triggers can be disabled for bulk operations via the ``use_triggers`` config
flag.

Checksum
--------

CRC32C (Castagnoli) is used for file checksums:

- Polynomial: 0x1EDC6F41
- Hardware accelerated on modern CPUs
- Compatible with Google's CRC32C implementation

Data Integrity
--------------

Barecat provides:

1. **File checksums** - CRC32C for each file
2. **SQLite integrity** - ACID transactions, journaling
3. **Verification** - ``barecat verify`` checks all checksums

It does NOT provide:

- Archive-level signatures
- Encryption
- Compression (files stored as-is)

Compatibility
-------------

The format is designed for simplicity and long-term compatibility:

- **SQLite** - Universally supported, stable format
- **Shards** - Plain binary, no proprietary encoding
- **Schema versioning** - Allows forward-compatible changes

Reading a barecat archive requires:

1. SQLite library
2. Ability to read binary files
3. Understanding of this specification

No special decompression or decryption is needed.

Writing a barecat archive is also straightforward with standard SQLite using the `schema.sql` file and regular file I/O.

Example: Reading Without Library
--------------------------------

Using standard tools:

.. code-block:: bash

   # List all files
   sqlite3 myarchive.barecat "SELECT path, shard, offset, size FROM files"

   # Extract a specific file
   sqlite3 myarchive.barecat \
       "SELECT shard, offset, size FROM files WHERE path='dir/file.txt'"
   # Returns: 0|1234|5678

   # Read the data
   dd if=myarchive.barecat-shard-00000 bs=1 skip=1234 count=5678

Using Python without barecat:

.. code-block:: python

   import sqlite3

   conn = sqlite3.connect('myarchive.barecat')
   cursor = conn.execute(
       "SELECT shard, offset, size FROM files WHERE path=?",
       ('dir/file.txt',)
   )
   shard, offset, size = cursor.fetchone()

   with open(f'myarchive.barecat-shard-{shard:05d}', 'rb') as f:
       f.seek(offset)
       data = f.read(size)

Version History
---------------

**Schema 0.3** (unreleased)

- Fixed trigger bug: ``num_files`` no longer incorrectly propagated on directory move/delete
  (``num_files`` counts direct children only, not recursive)

**Schema 0.2** (v0.2.5, January 2025)

First released schema version.

- ``config`` table with schema versioning (``schema_version_major``, ``schema_version_minor``)
- ``crc32c`` column in files for checksums
- ``mode``, ``uid``, ``gid``, ``mtime_ns`` columns for Unix metadata
- ``dirs`` table with ``num_subdirs``, ``num_files``, ``num_files_tree``, ``size_tree``
- SQLite triggers for automatic stats propagation
- ``config`` table uses WITHOUT ROWID

*Internal development note:* During development, an intermediate version briefly used
WITHOUT ROWID for all tables (files, dirs, config). This was reverted before release
because rowid tables are more space-efficient for this use case. The script
``upgrade_database2.py`` exists to convert databases from this intermediate format,
but since it was never released, this script is unlikely to be needed.

**Pre-versioned format** (original, June 2023)

Never formally released. Incompatible with current barecat.

- Simple schema without ``config`` table or versioning
- ``files``: path, parent, shard, offset, size
- ``directories``: path, parent, total_size, total_file_count
- No checksums or Unix metadata

Upgrading
---------

Run ``barecat upgrade <archive>`` to upgrade an archive to the current
schema version. The upgrade process detects the source version automatically.

**Pre-versioned → 0.3**

Heavy migration that:

1. Renames old index to ``.old``
2. Creates new index with current schema
3. Copies directory and file metadata
4. Calculates CRC32C checksums for all files (uses ``--workers``)

**0.2 → 0.3**

Lightweight in-place fix:

1. Drops buggy ``del_subdir`` and ``move_subdir`` triggers
2. Recreates triggers with fixed logic
3. Rebuilds directory tree statistics to fix any corruption
4. Updates schema version

This is fast even for large archives since it doesn't touch file data.
