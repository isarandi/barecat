How to Merge Archives
=====================

This guide covers combining multiple archives into one.

Basic Merge
-----------

Merge multiple barecat archives:

.. code-block:: bash

   barecat merge -o combined.barecat archive1.barecat archive2.barecat archive3.barecat

Merge with shard size limit:

.. code-block:: bash

   barecat merge -o combined.barecat -s 50G *.barecat

Merging Mixed Archive Types
---------------------------

Barecat can merge barecat, tar, and zip archives in a single command:

.. code-block:: bash

   barecat merge -o combined.barecat \
       existing.barecat \
       new_data.tar.gz \
       more_data.zip

This is useful when:

- Adding new data delivered as tar/zip to an existing barecat
- Consolidating data from multiple sources

Handling Duplicates
-------------------

By default, duplicate paths cause an error. Options:

Ignore Duplicates
~~~~~~~~~~~~~~~~~

Keep the first occurrence, skip later ones:

.. code-block:: bash

   barecat merge -o combined.barecat --ignore-duplicates archive1.barecat archive2.barecat

Append Mode
~~~~~~~~~~~

Append to an existing archive (implies ``--ignore-duplicates``):

.. code-block:: bash

   # First merge creates the archive
   barecat merge -o combined.barecat archive1.barecat

   # Later, append more data
   barecat merge -o combined.barecat -a archive2.barecat archive3.barecat

Force Overwrite
~~~~~~~~~~~~~~~

Overwrite existing output archive:

.. code-block:: bash

   barecat merge -o combined.barecat -f archive1.barecat archive2.barecat

Symlink Mode (Zero-Copy)
------------------------

For barecat-to-barecat merges, use symlinks instead of copying data:

.. code-block:: bash

   barecat merge -o combined.barecat --symlink archive1.barecat archive2.barecat

This creates:

.. code-block:: text

   combined.barecat                  # New merged SQLite index
   combined.barecat-shard-00000 -> archive1.barecat-shard-00000
   combined.barecat-shard-00001 -> archive1.barecat-shard-00001
   combined.barecat-shard-00002 -> archive2.barecat-shard-00000
   ...

Benefits:

- **Instant** - No data copying
- **Space efficient** - No duplication

Caveats:

- Original archives must remain in place
- Only works with barecat inputs (not tar/zip)
- Cannot be used with ``-a`` (append)

.. code-block:: bash

   # This will error
   barecat merge -o out.barecat --symlink data.tar.gz
   # Error: --symlink not supported with tar/zip inputs

Practical Examples
------------------

Consolidating Daily Uploads
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Initial archive
   barecat create dataset.barecat /data/day1/

   # Each day, append new data
   barecat merge -o dataset.barecat -a /data/day2/
   barecat merge -o dataset.barecat -a new_batch.tar.gz

Combining Training Splits
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   barecat merge -o full_dataset.barecat \
       train.barecat \
       val.barecat \
       test.barecat \
       -s 50G

Creating a Mirror with Symlinks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Create a combined view without copying
   barecat merge -o combined.barecat --symlink \
       /archive1/data.barecat \
       /archive2/data.barecat \
       /archive3/data.barecat

Python API
----------

.. code-block:: python

   from barecat import merge, merge_symlink

   # Regular merge
   merge(
       source_paths=['archive1.barecat', 'archive2.barecat', 'data.tar.gz'],
       target_path='combined.barecat',
       shard_size_limit=50 * 1024**3,
       ignore_duplicates=True,
   )

   # Symlink merge
   merge_symlink(
       source_paths=['archive1.barecat', 'archive2.barecat'],
       target_path='combined.barecat',
   )

Troubleshooting
---------------

"File already exists" Error
~~~~~~~~~~~~~~~~~~~~~~~~~~~

A file path exists in multiple input archives. Use ``--ignore-duplicates``:

.. code-block:: bash

   barecat merge -o out.barecat --ignore-duplicates *.barecat

"--symlink not supported with tar/zip"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Symlink mode only works with barecat inputs. For tar/zip, data must be copied:

.. code-block:: bash

   # This works
   barecat merge -o out.barecat archive1.barecat data.tar.gz

   # This doesn't
   barecat merge -o out.barecat --symlink data.tar.gz

See Also
--------

- :doc:`convert-archives` - Convert between formats
- :doc:`../reference/cli` - CLI reference for ``barecat merge``
