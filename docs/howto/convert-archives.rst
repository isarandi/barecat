How to Convert Archives
=======================

This guide covers converting between barecat and traditional archive formats
(tar, zip).

Converting tar/zip to Barecat
-----------------------------

Basic Conversion
~~~~~~~~~~~~~~~~

.. code-block:: bash

   # From tar.gz
   barecat convert dataset.tar.gz dataset.barecat

   # From zip
   barecat convert dataset.zip dataset.barecat

   # From uncompressed tar
   barecat convert dataset.tar dataset.barecat

With Shard Size Limit
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   barecat convert dataset.tar.gz dataset.barecat -s 50G

Streaming from stdin
~~~~~~~~~~~~~~~~~~~~

For very large archives or when piping from another command:

.. code-block:: bash

   # Pipe from curl
   curl -s https://example.com/data.tar.gz | \
       barecat convert --stdin tar.gz dataset.barecat

   # Pipe from decompression
   zstd -d -c data.tar.zst | barecat convert --stdin tar dataset.barecat

   # Supported formats: tar, tar.gz, tar.bz2, tar.xz

Converting Barecat to tar/zip
-----------------------------

Basic Conversion
~~~~~~~~~~~~~~~~

.. code-block:: bash

   # To tar.gz
   barecat convert dataset.barecat dataset.tar.gz

   # To plain tar
   barecat convert dataset.barecat dataset.tar

   # To zip
   barecat convert dataset.barecat dataset.zip

With Root Directory
~~~~~~~~~~~~~~~~~~~

Many tar archives wrap all files in a root directory. To replicate this:

.. code-block:: bash

   barecat convert --root-dir myproject dataset.barecat dataset.tar.gz

This produces a tar where all paths are prefixed with ``myproject/``.

Streaming to stdout
~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Stream to file
   barecat convert --stdout dataset.barecat tar.gz > dataset.tar.gz

   # Pipe to another command
   barecat convert --stdout dataset.barecat tar | ssh remote "tar -xf -"

   # Pipe to compression tool
   barecat convert --stdout dataset.barecat tar | zstd -o dataset.tar.zst

Zero-Copy Wrapping
------------------

For uncompressed tar or zip files, barecat can create an index without copying
data. The original file becomes the shard (via symlink).

.. code-block:: bash

   barecat convert --wrap dataset.tar dataset.barecat

This creates:

- ``dataset.barecat-sqlite-index`` - New SQLite index
- ``dataset.barecat-shard-00000`` - Symlink to ``dataset.tar``

Requirements for ``--wrap``:

- tar must be uncompressed (not .tar.gz, .tar.bz2, etc.)
- zip must be uncompressed (created with ``zip -0``)

To create an uncompressed zip:

.. code-block:: bash

   zip -0 -r dataset.zip directory/

Checking if wrap is possible:

.. code-block:: bash

   # This will error with a helpful message if not possible
   barecat convert --wrap compressed.tar.gz out.barecat
   # Error: Cannot wrap compressed file (gzip)...

Python API
----------

.. code-block:: python

   from barecat import archive2barecat, barecat2archive
   from barecat.cli.impl import wrap_archive

   # tar to barecat
   archive2barecat('data.tar.gz', 'data.barecat', shard_size_limit=50*1024**3)

   # barecat to tar
   barecat2archive('data.barecat', 'data.tar.gz', root_dir='myproject')

   # Zero-copy wrap
   wrap_archive('data.tar', 'data.barecat')

Handling Large Archives
-----------------------

For very large archives (TBs), consider:

1. **Use streaming** - Avoids loading entire archive in memory

   .. code-block:: bash

      cat huge.tar.gz | barecat convert --stdin tar.gz output.barecat

2. **Set shard size** - Smaller shards are easier to handle

   .. code-block:: bash

      barecat convert huge.tar.gz output.barecat -s 10G

3. **Use --wrap for uncompressed** - Zero-copy, instant

   .. code-block:: bash

      barecat convert --wrap huge.tar output.barecat

Troubleshooting
---------------

"Cannot wrap compressed file"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``--wrap`` option only works with uncompressed archives. Decompress first:

.. code-block:: bash

   gunzip dataset.tar.gz
   barecat convert --wrap dataset.tar dataset.barecat

"ZIP has compressed entries"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The zip file uses compression. Either:

1. Convert without ``--wrap`` (copies data):

   .. code-block:: bash

      barecat convert dataset.zip dataset.barecat

2. Create an uncompressed zip:

   .. code-block:: bash

      unzip dataset.zip -d temp/
      zip -0 -r dataset_uncompressed.zip temp/
      barecat convert --wrap dataset_uncompressed.zip dataset.barecat

See Also
--------

- :doc:`merge-archives` - Merge multiple archives
- :doc:`../reference/cli` - CLI reference for ``barecat convert``
