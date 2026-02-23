How to Verify and Repair Archives
==================================

This guide covers maintaining archive integrity.

Verifying Integrity
-------------------

Full Verification
~~~~~~~~~~~~~~~~~

Verify all file checksums:

.. code-block:: bash

   barecat verify myarchive.barecat

This reads every file and compares its CRC32C checksum against the index.
Output:

.. code-block:: text

   Verifying 1234567 files...
   OK: All files verified successfully.

Or on failure:

.. code-block:: text

   ERROR: Checksum mismatch for path/to/corrupted/file.jpg
   ERROR: 3 files failed verification.

Quick Verification
~~~~~~~~~~~~~~~~~~

Check index integrity without reading file contents:

.. code-block:: bash

   barecat verify --quick myarchive.barecat

This verifies:

- SQLite database integrity
- Index consistency (parent directories exist, etc.)
- Shard file existence and sizes

Much faster, but won't detect corrupted file contents.

Defragmentation
---------------

When files are deleted, they leave gaps in the shard files. Defragmentation
reclaims this space.

Check Fragmentation
~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   barecat du myarchive.barecat

Look at the "wasted space" or gap statistics.

Run Defrag
~~~~~~~~~~

.. code-block:: bash

   barecat defrag myarchive.barecat

This rewrites files to eliminate gaps, compacting the archive.

Quick Defrag
~~~~~~~~~~~~

For faster but less thorough defragmentation:

.. code-block:: bash

   barecat defrag --quick myarchive.barecat

Uses a best-fit algorithm that may leave some small gaps.

Resharding
----------

Change the shard size limit of an existing archive:

.. code-block:: bash

   # Consolidate into larger shards
   barecat reshard -s 50G myarchive.barecat

   # Split into smaller shards
   barecat reshard -s 1G myarchive.barecat

This reorganizes all data according to the new shard size limit.

Use cases:

- Consolidate many small shards into fewer large ones
- Split large shards for easier distribution
- Prepare archive for a filesystem with file size limits

Database Upgrade
----------------

When barecat schema changes, upgrade existing archives:

.. code-block:: bash

   barecat upgrade myarchive.barecat

With multiple workers for faster processing:

.. code-block:: bash

   barecat upgrade -j 8 myarchive.barecat

This migrates the SQLite schema while preserving all data.

Common Issues
-------------

"Database schema version X.Y is older than supported"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the upgrade:

.. code-block:: bash

   barecat upgrade myarchive.barecat

"Checksum mismatch"
~~~~~~~~~~~~~~~~~~~

A file is corrupted. Options:

1. **Restore from backup** - If you have one

2. **Delete the corrupted file**:

   .. code-block:: python

      import barecat
      with barecat.Barecat('archive.barecat', readonly=False) as bc:
          del bc['path/to/corrupted/file.jpg']

3. **Re-add from source** - If original still exists:

   .. code-block:: python

      with barecat.Barecat('archive.barecat', readonly=False) as bc:
          del bc['path/to/corrupted/file.jpg']
          bc.add_by_path('/original/path/file.jpg', store_path='path/to/corrupted/file.jpg')

"Shard file missing"
~~~~~~~~~~~~~~~~~~~~

A shard file was deleted or moved. Either:

1. Restore the shard file from backup
2. Recreate the archive from original sources

Python API
----------

.. code-block:: python

   import barecat

   # Verify
   with barecat.Barecat('archive.barecat') as bc:
       bc.verify_integrity(quick=False)

   # Defrag
   with barecat.Barecat('archive.barecat', readonly=False, append_only=False) as bc:
       bc.defrag(quick=False)

Maintenance Schedule
--------------------

Recommended practices:

1. **After bulk deletions**: Run defrag to reclaim space
2. **Periodically**: Run ``verify --quick`` to catch issues early
3. **After barecat upgrade**: Run ``barecat upgrade`` if prompted

See Also
--------

- :doc:`../reference/cli` - CLI reference
- :doc:`../explanation/architecture` - How barecat stores data
