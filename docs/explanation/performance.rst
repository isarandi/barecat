Performance Characteristics
===========================

This document explains barecat's performance characteristics and optimization
strategies.

Lookup Performance
------------------

File lookup by path is **O(1)** via SQLite index:

.. code-block:: python

   # Constant time regardless of archive size
   data = bc['path/to/file.jpg']

The SQLite B-tree index on ``files.path`` provides logarithmic lookup,
but with SQLite's page caching and memory-mapping, this approaches constant
time for typical workloads.

**Benchmark** (1M files):

- First lookup: ~1ms (cold cache)
- Subsequent lookups: ~0.1ms (warm cache)

Directory Listing
-----------------

Directory operations use the ``parent`` column index:

.. code-block:: python

   # O(k) where k = number of entries in directory
   entries = bc.listdir('some/directory')

The ``idx_files_parent`` and ``idx_dirs_parent`` indexes make this efficient.

Walking the entire archive:

.. code-block:: python

   # O(n) where n = total files
   for root, dirs, files in bc.walk(''):
       pass

Sequential vs Random Access
---------------------------

**Sequential access** (reading in physical order):

.. code-block:: python

   from barecat import Order

   # Optimal for HDDs, good for SSDs
   for f in bc.index.iter_all_fileinfos(order=Order.ADDRESS):
       data = bc[f.path]

This minimizes disk seeks by reading files in the order they're stored.

**Random access** (shuffled order):

.. code-block:: python

   import random

   paths = list(bc.index.iter_all_paths())
   random.shuffle(paths)
   for path in paths:
       data = bc[path]

Performance depends heavily on storage:

- **SSD**: Random access is nearly as fast as sequential
- **HDD**: Random access can be 100x slower than sequential

Memory Usage
------------

Barecat uses memory-mapping for large reads:

.. code-block:: text

   PRAGMA mmap_size = 30000000000  # ~30GB mmap window

This allows the OS to manage file caching efficiently.

Memory overhead per open archive:

- SQLite connection: ~1MB
- File handles: ~1KB per open shard
- Index cache: Managed by SQLite

For ``threadsafe=True``, each thread/process gets its own connections,
multiplying memory usage.

Write Performance
-----------------

**Single file writes**:

.. code-block:: python

   bc['file.txt'] = data  # Append to shard + index insert

Write performance is limited by:

1. SQLite transaction overhead
2. fsync for durability

**Bulk writes** (disable triggers for speed):

.. code-block:: python

   with bc.index.no_triggers():
       for path, data in items:
           bc[path] = data
   bc.index.rebuild_dir_stats()  # Rebuild after bulk insert

This can be 10-100x faster for large imports.

Sharding Impact
---------------

Shards affect performance in several ways:

**Too many shards** (small shard size):

- More file handles to manage
- More metadata in index
- Good for distribution

**Too few shards** (large shard size):

- Single points of failure
- Harder to move around
- Better sequential read performance

**Recommended shard sizes**:

- 10-50GB for most use cases
- 1-10GB if distribution is important
- Unlimited for small archives

Multi-Process Performance
-------------------------

With ``threadsafe=True``, each DataLoader worker gets isolated resources:

.. code-block:: python

   bc = barecat.Barecat('data.barecat', threadsafe=True)

   # Each worker has:
   # - Own SQLite connection
   # - Own file handles
   # - No locking overhead

This scales linearly with workers up to I/O saturation.

**Benchmark** (8 workers, SSD):

.. code-block:: text

   Workers  Throughput (images/sec)
   1        2,500
   2        5,000
   4        9,500
   8        15,000 (I/O limited)

Network Filesystem Considerations
---------------------------------

Barecat was designed for network filesystems (NFS, Lustre, GPFS).

**Why it helps**:

- Fewer files = less metadata overhead
- Sequential reads within shards
- SQLite index can be cached locally

**Optimization tips**:

1. Copy index to local SSD if possible:

   .. code-block:: bash

      cp archive.barecat-sqlite-index /local/tmp/
      ln -sf /network/archive.barecat-shard-* /local/tmp/
      # Use /local/tmp/archive.barecat

2. Use read-ahead for sequential access

3. Consider ``readonly_is_immutable=True`` for better caching

Comparison with Raw Files
-------------------------

Reading 1M small files (1KB each):

.. code-block:: text

   Method              Time (sec)    Notes
   Raw files (SSD)     120           1M stat + open + read
   Raw files (NFS)     3600+         Network metadata overhead
   Barecat (SSD)       30            Single index + sequential read
   Barecat (NFS)       60            Index cached, shard streaming

The improvement comes from:

1. Single SQLite lookup vs filesystem metadata
2. Sequential shard read vs many small reads
3. No directory traversal overhead

Profiling Tips
--------------

**Identify I/O bottlenecks**:

.. code-block:: python

   import cProfile
   import pstats

   with cProfile.Profile() as pr:
       for i in range(1000):
           data = bc[paths[i]]

   stats = pstats.Stats(pr)
   stats.sort_stats('cumulative')
   stats.print_stats(20)

**Monitor with iostat**:

.. code-block:: bash

   iostat -x 1  # Watch disk utilization during training

**PyTorch profiler**:

.. code-block:: python

   with torch.profiler.profile(
       activities=[torch.profiler.ProfilerActivity.CPU],
       with_stack=True,
   ) as prof:
       for batch in loader:
           pass

   print(prof.key_averages().table(sort_by="cpu_time_total"))

Optimization Checklist
----------------------

1. **Use SSDs** if possible
2. **Set threadsafe=True** for multi-worker DataLoader
3. **Use Order.ADDRESS** for sequential workloads on HDDs
4. **Disable triggers** for bulk imports
5. **Size shards appropriately** (10-50GB typical)
6. **Cache index locally** on network filesystems
7. **Profile before optimizing** - measure, don't guess
