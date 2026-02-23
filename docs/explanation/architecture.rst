Architecture
============

Barecat stores millions of small files in a way that allows fast random access
by path. This page explains why the format exists, how the pieces fit together,
and the design choices that make it work.

The problem
-----------

Barecat is designed to address the use case of storing millions of small files
(e.g., images for deep learning training) that need to be read randomly (but are typically just written once).
Storing them as individual files has problems in practice:

- **Transfer overhead**: Copying 10 million files to a cluster (or back) is
  painfully slow. Each file is a separate metadata operation. Rsync, scp, and
  friends spend more time on bookkeeping than actual data transfer. A single
  archive file transfers at full disk/network speed.

- **File count quotas**: Compute clusters often limit how many files a user can
  have. 10 million images can exceed the quota even if the total size is small.

- **Network filesystem overhead**: Loading many small files from NFS, GPFS, or
  Lustre hammers the metadata servers. Each ``open()`` is a round-trip. Training
  jobs that load random images become bottlenecked on metadata, not data.

Traditional archives (tar, zip) solve the space problem but create a new one:
no random access. To read the millionth file in a tar, one must scan through
the first 999,999. Zip has a central directory, but it is at the end of the
file, and the file data is still scattered.

The solution: index + blobs
---------------------------

Barecat separates metadata from data:

- **SQLite index**: An SQLite database stores the path, size, location, and
  checksum of every file. SQLite gives us O(1) lookups by path (via B-tree) and we don't have to load the index fully into memory.

- **Shard files**: The actual file data is concatenated into large "shard"
  files. Each file's bytes sit at a specific offset in a specific shard.
  No headers, no padding, just raw bytes back to back. This is very easy to produce with other tools as well, including custom oneoff scripts.

To read a file::

    1. Query the index: SELECT shard, offset, size FROM files WHERE path = ?
    2. Seek to offset in the shard file
    3. Read size bytes

This is O(1) in the number of files. The index query is a B-tree lookup.
The shard read is a single seek plus a sequential read.

Why SQLite?
~~~~~~~~~~~

SQLite is an unusual choice for an archive format. Most formats use custom
binary structures. But SQLite brings compelling advantages:

- **No custom parser**: The format is self-describing. Any SQLite client can
  read it. No version skew, no parsing bugs.

- **Indexes for free**: B-tree indexes give O(log n) lookups. Adding a new
  index (say, by modification time) is one ``CREATE INDEX`` statement.

- **Transactions**: Adding files can be be atomic. A crash mid-write leaves the
  database consistent.

- **Tooling**: The index can be inspected with any SQLite browser. Run ad-hoc
  queries. Export to CSV. Debug without special tools.

The downside is size. SQLite has overhead per row (rowid, B-tree nodes, page
alignment). But compared to filesystem metadata and the flexibility gained, this is acceptable.

Why shards?
~~~~~~~~~~~

A single monolithic data file would work, but shards help in practice:

- **Disk space limits**: There might not be 10 TB free on a single filesystem.
  Shards can live on different disks or be moved independently.

- **Incremental transfer**: Moving a 10 TB archive between clusters is easier
  in 100 GB chunks. Transfer a few shards, verify, continue later. Resume
  after interruptions without starting over.

- **Combining archives without copying**: Symlink shards from multiple archives
  into one directory, name them consecutively, and create a merged index. No
  data copying needed. This is how ``barecat-merge-symlink`` works.

The default shard size limit is effectively unlimited (2^63 bytes), but it
can be set lower for the benefits above.

File naming
~~~~~~~~~~~

A Barecat archive at path ``/data/myarchive`` consists of::

    /data/myarchive-sqlite-index     # The SQLite database
    /data/myarchive-shard-00000      # First shard
    /data/myarchive-shard-00001      # Second shard (if needed)
    ...


The index schema
----------------

The SQLite database has three tables:

**files**: One row per file::

    path      TEXT PRIMARY KEY   -- Full path within archive
    parent    TEXT (generated)   -- Parent directory path
    shard     INTEGER            -- Which shard file
    offset    INTEGER            -- Byte offset within shard
    size      INTEGER            -- Size in bytes
    crc32c    INTEGER            -- CRC32C checksum (optional)
    mode      INTEGER            -- Unix permissions (optional)
    uid       INTEGER            -- Owner user ID (optional)
    gid       INTEGER            -- Owner group ID (optional)
    mtime_ns  INTEGER            -- Modification time in nanoseconds (optional)

**dirs**: One row per directory::

    path           TEXT PRIMARY KEY
    parent         TEXT (generated)
    num_subdirs    INTEGER         -- Direct subdirectory count
    num_files      INTEGER         -- Direct file count
    num_files_tree INTEGER         -- Total files in subtree
    size_tree      INTEGER         -- Total bytes in subtree
    mode, uid, gid, mtime_ns       -- Same as files

**config**: Key-value settings::

    use_triggers          -- Enable/disable trigger-based stats
    shard_size_limit      -- Maximum bytes per shard
    schema_version_major  -- Schema major version
    schema_version_minor  -- Schema minor version

The ``parent`` column is a generated column. SQLite computes it automatically
from the path by stripping the last path component. This keeps the data
normalized while still allowing fast queries by parent directory.

Trigger-based statistics
------------------------

The ``dirs`` table tracks aggregate statistics: how many files in each
directory, how many in the entire subtree, total size of the subtree.
This enables ncdu-like browsing: instantly see which parts of the dataset
occupy the most space without scanning millions of files.

Instead, SQLite triggers do the bookkeeping automatically. When inserting a
file::

    INSERT INTO files (path, shard, offset, size) VALUES ('a/b/c.txt', 0, 0, 100)

The ``add_file`` trigger fires and::

    1. Upserts directory 'a/b' with num_files += 1, size_tree += 100
    2. The upsert triggers 'add_subdir' or 'resize_dir' on 'a/b'
    3. That propagates to 'a', then to '' (root)

Each ancestor's statistics update in a single transaction. The trigger chain
is recursive: SQLite's ``PRAGMA recursive_triggers = ON`` enables this.

Why triggers?
~~~~~~~~~~~~~

Without triggers, computing the tree statistics requires scanning all files
and propagating sizes up the directory tree. On a dataset with 21 million
files, this takes about 3 minutes. With triggers, the stats are always up
to date and reading them is O(1).

Is it worth the complexity? The benefit is questionable for some workloads,
but implementing it was a fun puzzle. The downside is write overhead.
Every file insert updates multiple directory
rows (one per ancestor). For bulk imports, this can be slow.

Disabling triggers for bulk operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For bulk imports, disable triggers, insert files ignoring the treestat
columns, then rebuild stats once at the end::

    with bc.index.no_triggers():
        for path, data in items:
            bc.add(path, data=data)
    bc.index.update_treestats()
    bc.index.conn.commit()

This is often faster than letting triggers fire for each insert.

The ``no_triggers()`` context manager toggles a config flag::

    UPDATE config SET value_int = 0 WHERE key = 'use_triggers'

Each trigger checks this flag before running::

    WHEN (SELECT value_int FROM config WHERE key = 'use_triggers') = 1

This is unusual. Normally one would drop and recreate triggers. But that
requires schema modification permissions and cannot be done in a transaction
that also modifies data. The config flag approach is transactional and
requires no special permissions.

The Sharder
-----------

The ``Sharder`` class manages the shard files. It handles:

- **Opening shards on demand**: Shards are opened lazily when first accessed.

- **Rotation**: When a shard exceeds the size limit, a new shard is started.

- **Read modes**: Shards can be opened read-only, read-write, or append-only.

- **Thread safety**: In threadsafe mode, each thread gets its own file handles.

When adding a file, the Sharder:

1. Checks if the file fits in the current shard.
2. If not, starts a new shard.
3. Seeks to the write position.
4. Writes the data, computing CRC32C as it goes.
5. Returns (shard, offset, size, crc32c).

The Barecat class then inserts this info into the index.

Overwrites and deletions
~~~~~~~~~~~~~~~~~~~~~~~~

By default, writing a file that already exists raises ``FileExistsBarecatError``.
This prevents accidental overwrites and keeps the semantics simple.

When deleting a file, the index entry is removed but the bytes remain in the
shard as a "hole". Barecat does not track free space within shards or attempt
to reuse it. New files are always appended.

Why not maintain free space info? Because that is not the use case we are
designing for. The typical workflow is: build the archive once, then read
from it repeatedly. Deletions happen occasionally (a mistake was made, some
files are corrupt, etc.) but are not the primary operation. So deletion is
allowed, but you need to run ``defrag()`` afterward to reclaim space.

We have to keep scope creep in check. Barecat is not a full-fledged filesystem
with a block allocator. It is a simple archive format optimized for a specific
workflow. Adding free space tracking would mean dealing with fragmentation,
coalescing adjacent gaps, best-fit vs first-fit allocation, handling unknown
file sizes during streaming writes, and concurrent access coordination.
That complexity is not worth it for the intended use case.

Defragmentation
---------------

Over time, deletions and updates create gaps in the shards. Defragmentation
reclaims this space.

**Full defrag** rewrites the entire archive::

    for each file in address order:
        copy to the next available position
        update the index

This guarantees optimal packing but requires reading and writing every byte.
For a 1 TB archive, this takes a while.

**Quick defrag** is opportunistic. It scans files from the end backward,
moving each file to the earliest gap that precedes and fits it. This runs for a limited
time (default 5 seconds) and reclaims space incrementally. Run it periodically
during idle time.

The gap-finding query is::

    SELECT shard, offset + size AS gap_start,
           LEAD(offset) OVER (PARTITION BY shard ORDER BY offset) - (offset + size) AS gap_size
    FROM files

This uses window functions to compute the gap after each file without any
self-joins.

Thread safety
-------------

By default, Barecat is single-threaded. The SQLite connection and file handles
are shared. This is fine for sequential access.

For parallel access, open with ``threadsafe=True``::

    bc = Barecat('archive', readonly=True, threadsafe=True)

In threadsafe mode:

- Each thread gets its own SQLite connection (via thread-local storage).
- Each thread gets its own shard file handles.
- SQLite's WAL mode is optional but recommended for concurrent reads.

Note: Threadsafe mode only supports read-only access. Concurrent writes would
require coordination that the basic Barecat class does not implement.

Codecs
------

Wrap a Barecat archive with ``DecodedView`` for automatic encoding and decoding
based on file extension::

    from barecat import Barecat, DecodedView

    with Barecat('archive', readonly=False) as bc:
        dec = DecodedView(bc)
        dec['image.jpg'] = numpy_array  # Encodes as JPEG
        arr = dec['image.jpg']          # Decodes back to array

Built-in codecs:

- ``.json``: JSON (dict/list)
- ``.pkl``, ``.pickle``: Pickle (any Python object)
- ``.jpg``, ``.jpeg``: JPEG (via jpeg4py, OpenCV, Pillow, or imageio)
- ``.png``, ``.bmp``, ``.gif``, ``.tiff``, ``.webp``, ``.exr``: Images (via OpenCV, Pillow, or imageio)
- ``.npy``: NumPy arrays
- ``.npz``: NumPy archives (multiple arrays)
- ``.msgpack``: MessagePack with NumPy support
- ``.gz``, ``.bz2``, ``.xz``: Compression (stackable with other extensions)

Codecs can be stacked::

    dec['data.npy.bz2'] = array  # Saves as compressed NumPy

The ``nonfinal=True`` flag marks a codec as a compression layer that wraps
another format.

Register custom codecs with::

    dec.register_codec(['.yaml'], yaml_encode, yaml_decode)

Two APIs
--------

Barecat provides two interfaces:

**Dict-like**: Treat the archive as a dictionary::

    bc['path/to/file'] = data
    data = bc['path/to/file']
    del bc['path/to/file']
    for path in bc:
        ...

This is convenient for simple access patterns.

**Filesystem-like**: Methods mirroring Python's ``os`` module::

    bc.open('path/to/file', 'rb')
    bc.exists('path')
    bc.listdir('dir')
    bc.walk('dir')
    bc.glob('**/*.jpg')

This is familiar to anyone who has worked with files and supports more
complex operations like partial reads and iteration.

Both APIs use the same underlying index and shards.

Merging archives
----------------

Two archives can be merged in two ways:

**Data copy** (``barecat merge``): Copy shard contents into the target archive.
The data is physically combined. This is the "true" merge but requires
rewriting all the source data.

**Symlink merge** (``barecat merge --symlink``): Create symlinks to the source
shards and merge only the indexes. The source shards are renumbered in the
target index to avoid collisions. No data is copied.

Symlink merge is fast but has caveats:

- The source archive must not be modified afterward.
- The symlinks must remain valid (no moving the source).
- ``allow_writing_symlinked_shard=False`` (the default) prevents accidental
  writes that would corrupt the source.

Integrity verification
----------------------

Each file can store a CRC32C checksum. On read, the checksum is verified::

    bc.read('file')  # Raises ValueError on checksum mismatch

The ``barecat verify`` command checks all files::

    barecat verify /path/to/archive

This reads every byte of every file and compares checksums. It also runs
SQLite's ``PRAGMA integrity_check`` on the index.

For quick sanity checks, ``verify_integrity(quick=True)`` only checks the
last file in the archive. This catches truncation but not corruption in
the middle.

Performance tips
----------------

**Bulk imports**: Disable triggers during large imports::

    with bc.index.no_triggers():
        for path, data in items:
            bc.add(path, data=data)
    bc.index.update_treestats()
    bc.index.conn.commit()

**Read order**: If you know you will read files in a certain order, sorting
by (shard, offset) minimizes seeks::

    for fi in bc.index.iter_all_fileinfos(order=Order.ADDRESS):
        data = bc.read(fi)

**Memory-mapped index**: The index uses ``PRAGMA mmap_size`` to memory-map
the database file. This speeds up repeated queries significantly.


Summary
-------

Barecat's architecture is simple:

1. An SQLite database indexes file metadata.
2. Shard files store concatenated file data.
3. Triggers maintain directory statistics.
4. The Sharder handles shard rotation and I/O.
5. Codecs provide automatic serialization.
