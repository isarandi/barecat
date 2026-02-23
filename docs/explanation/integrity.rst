Integrity Checking
==================

Barecat stores a CRC32C checksum for each file. This page explains when
checksums are verified, when they are not, and why.

What gets stored
----------------

When a file is added to a Barecat archive, its CRC32C checksum is computed
during the write and stored in the index::

    INSERT INTO files (path, shard, offset, size, crc32c, ...)
    VALUES ('images/001.jpg', 0, 0, 12345, 0x1A2B3C4D, ...)

The checksum covers the raw bytes written to the shard file. If codecs are
used (e.g., JPEG encoding), the checksum is of the encoded bytes, not the
original data.

When verification happens
-------------------------

Verification behavior depends on how the file is accessed.

**Dict-style access** (``bc[path]``): The entire file is read and its
checksum is verified before returning::

    data = bc['images/001.jpg']  # CRC verified

If the checksum does not match, a ``ValueError`` is raised.

**The read() method**: Verified for full-file reads. If ``offset`` or
``size`` parameters request a partial read, verification is skipped
(there is no checksum for arbitrary byte ranges)::

    data = bc.read('images/001.jpg')              # CRC verified
    data = bc.read('images/001.jpg', offset=100)  # Not verified

**File handles** (``bc.open()``): Not verified::

    with bc.open('images/001.jpg', 'rb') as f:
        data = f.read()  # Not verified

**Distributed clients**: The same rules apply. ``client[path]`` verifies;
``client.open(path).read()`` does not.

Why file handles skip verification
----------------------------------

File handles support ``seek()``. A caller might read bytes 1000-2000, then
jump to byte 5000, then read the last 100 bytes. There is no way to verify
these arbitrary ranges against a whole-file checksum without reading the
entire file first, which defeats the purpose of random access.

We considered alternatives:

**Per-block checksums**: Store a CRC for each N-byte block (e.g., 64 KB).
Verify each block as it is read. This works but adds storage overhead
(about 0.006% for 64 KB blocks), complicates the schema, and requires
protocol changes for distributed access.

**On-the-fly chunk checksums**: For distributed reads, the server could
compute a checksum of the requested byte range and send it with the data.
The client verifies after receiving. This adds CPU overhead on every read
request, which may be acceptable for network transfers but is unnecessary
for local access where TCP already provides reliable delivery.

We chose not to implement these. The threat model does not justify the
complexity for the intended use case.

What CRC is for
---------------

The checksum catches problems in the write path:

- Bugs in code: wrong offset, wrong size, data written to the wrong location
- Incomplete operations: a write that did not flush, a transaction that
  did not commit
- Distributed writer issues: race conditions, partial failures, protocol
  bugs

It also serves as an end-to-end sanity check: did the data survive the
round trip through the entire pipeline?

The checksum is not primarily for:

- **Network corruption**: TCP has its own checksums. Errors in transit are
  caught at the transport layer.

- **Storage bit rot**: Modern drives have ECC. Filesystems like ZFS and
  Btrfs have their own checksums. Barecat's CRC is not a substitute for
  storage-level integrity.

These can still happen, and the CRC will catch them if they do. But they
are not the primary motivation.

The verify command
------------------

The ``barecat verify`` command performs a full integrity check::

    barecat verify /path/to/archive

This reads every byte of every file and compares checksums. It also runs
SQLite's ``PRAGMA integrity_check`` on the index database.

For a quick sanity check, use ``--quick``::

    barecat verify --quick /path/to/archive

Quick mode only checks the last file in the archive (by address order).
This catches truncation or incomplete writes but not corruption in the
middle.

Programmatically::

    bc.verify_integrity()        # Full check
    bc.verify_integrity(quick=True)  # Quick check

Practical guidance
------------------

For ML training, the typical access pattern is::

    for path in paths:
        data = bc[path]  # Verified
        # ... process data

Every file is verified on read. No additional steps are needed.

After a multi-process ingest, run a full verification::

    barecat verify /path/to/archive

This confirms that all writes completed correctly and the index is
consistent.

If using file handles for streaming access to large files, understand that
you are in "trust mode". The data was verified on ingest; you are trusting
that it has not changed since. For most workflows this is fine.
