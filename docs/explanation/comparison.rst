Comparison with Other Formats
=============================

This document compares barecat with other storage formats for large file
collections.

Quick Comparison
----------------

+----------------+-------------+-------------+-------------+-------------+
| Feature        | Barecat     | tar         | zip         | HDF5        |
+================+=============+=============+=============+=============+
| Random access  | O(1)        | O(n)        | O(n)*       | O(1)        |
+----------------+-------------+-------------+-------------+-------------+
| Append files   | Yes         | Slow**      | No          | Yes         |
+----------------+-------------+-------------+-------------+-------------+
| Delete files   | Yes         | No          | No          | Partial     |
+----------------+-------------+-------------+-------------+-------------+
| Millions files | Yes         | Slow        | Slow        | Slow        |
+----------------+-------------+-------------+-------------+-------------+
| Browse         | Yes         | Stream only | Yes         | Yes         |
+----------------+-------------+-------------+-------------+-------------+
| Simple format  | Yes         | Yes         | Yes         | No          |
+----------------+-------------+-------------+-------------+-------------+
| Compression    | External    | Yes         | Yes         | Yes         |
+----------------+-------------+-------------+-------------+-------------+
| Encryption     | No          | No          | Yes         | No          |
+----------------+-------------+-------------+-------------+-------------+

\* zip requires scanning central directory
\** tar append requires rewriting

tar
---

**tar** (tape archive) is the classic Unix archiver.

Pros:

- Universal support
- Simple streaming format
- Supports compression (gzip, bzip2, xz)

Cons:

- **No random access** - Must scan sequentially to find a file
- **No index** - File lookup is O(n)
- **No modification** - Can only append, not delete or modify

Use tar when:

- Distributing software releases
- Creating backups for sequential restore
- Maximum compatibility is needed

Use barecat instead when:

- You need random access to individual files
- You have millions of files
- You'll be reading files non-sequentially (ML training)

zip
---

**zip** is widely used for compressed archives.

Pros:

- Per-file compression
- Central directory for file listing
- Windows-native support
- Optional encryption

Cons:

- **Central directory must be read** - O(n) to load, memory overhead
- **No modification** - Updating requires rewriting
- **Slow for huge archives** - Millions of entries = slow startup

Use zip when:

- Distributing files to end users
- Need Windows compatibility
- Need per-file compression

Use barecat instead when:

- Archive has millions of files
- Need fast startup (no CD loading)
- Need to add/delete files

HDF5
----

**HDF5** is a hierarchical data format popular in scientific computing.

Pros:

- Complex data types (arrays, tables)
- Chunking and compression
- Parallel I/O support

Cons:

- **Complex format** - Many features = complexity
- **Single-file limitation** - All data in one file
- **Slow with many small items** - Designed for large arrays
- **Hard to browse** - Need special tools

Use HDF5 when:

- Storing numerical arrays
- Need compression of large blocks
- Using scientific Python stack

Use barecat instead when:

- Storing many small files (images, text)
- Need simple filesystem-like browsing
- Want to avoid HDF5 complexity

LMDB
----

**LMDB** (Lightning Memory-Mapped Database) is a key-value store.

Pros:

- Very fast reads
- Memory-mapped access
- ACID transactions

Cons:

- **Single-file** - Can grow very large
- **Key-value only** - No directory structure
- **Limited tooling** - Need LMDB utilities

Use LMDB when:

- Need maximum read speed
- Data fits in a single file
- Don't need filesystem structure

Use barecat instead when:

- Need sharding across multiple files
- Want directory/path structure
- Need to browse with standard tools

TFRecord
--------

**TFRecord** is TensorFlow's recommended data format.

Pros:

- Optimized for TensorFlow
- Supports sharding
- Compression support

Cons:

- **TensorFlow-specific** - Hard to use elsewhere
- **No random access** - Sequential reads only
- **Binary protocol buffers** - Hard to inspect

Use TFRecord when:

- Exclusively using TensorFlow
- Following TensorFlow tutorials

Use barecat instead when:

- Using PyTorch or other frameworks
- Need random access
- Want to inspect files directly

Raw Files on Filesystem
-----------------------

Storing files directly on the filesystem.

Pros:

- Maximum compatibility
- Easy to inspect
- No archive overhead

Cons:

- **Inode limits** - Filesystems limit file count
- **Metadata overhead** - Each file has filesystem metadata
- **Network FS performance** - Many small files = slow
- **Backup complexity** - Many files to track

Use raw files when:

- Small number of files (< 100k)
- Need direct tool access
- Local SSD storage

Use barecat instead when:

- Millions of files
- Network filesystem (NFS, Lustre, GPFS)
- Need to move/copy dataset as unit

WebDataset
----------

**WebDataset** stores files as tar shards with naming conventions.

Pros:

- Simple shard format
- Streaming-friendly
- Works with PyTorch

Cons:

- **No random access** - Sequential shard reading
- **Naming conventions** - Must follow patterns
- **No index** - Can't look up specific files

Use WebDataset when:

- Streaming large datasets
- Don't need random access
- Already using tar shards

Use barecat instead when:

- Need random access
- Want to look up specific files
- Need filesystem-like operations

Summary Recommendations
-----------------------

**Choose barecat when:**

- Dataset has millions of small files
- You need random access (ML training with shuffling)
- You want filesystem-like operations (listdir, walk)
- You're using network storage
- You want to browse archives easily

**Choose tar when:**

- Creating archives for distribution
- Sequential access is fine
- Maximum compatibility needed

**Choose zip when:**

- Distributing to end users
- Need Windows compatibility
- Archive is not huge (< 100k files)

**Choose HDF5 when:**

- Storing large numerical arrays
- Using scientific Python stack
- Need internal compression

**Choose raw files when:**

- Small dataset (< 100k files)
- Direct tool access needed
- Local SSD storage
