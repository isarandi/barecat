How to Mount with FUSE
======================

Barecat archives can be mounted as a filesystem using FUSE (Filesystem in
Userspace), allowing you to access files with standard tools like ``ls``,
``cat``, ``cp``, etc.

Installation
------------

Install barecat with mount support:

.. code-block:: bash

   pip install barecat[mount]

This enables the ``barecat mount`` subcommand.

Requirements:

- Linux with FUSE support (most distributions)
- macOS with macFUSE (``brew install macfuse``)

Basic Usage
-----------

Mount an archive:

.. code-block:: bash

   mkdir /mnt/myarchive
   barecat mount myarchive.barecat /mnt/myarchive

Now you can use standard filesystem commands:

.. code-block:: bash

   ls /mnt/myarchive
   cat /mnt/myarchive/path/to/file.txt
   cp /mnt/myarchive/image.jpg /tmp/

Unmount when done:

.. code-block:: bash

   fusermount -u /mnt/myarchive
   # or on macOS:
   umount /mnt/myarchive

Read-Write Mode
---------------

By default, the mount is read-only. To enable writes:

.. code-block:: bash

   barecat mount myarchive.barecat /mnt/myarchive -o rw

Now you can create, modify, and delete files:

.. code-block:: bash

   echo "New content" > /mnt/myarchive/newfile.txt
   rm /mnt/myarchive/oldfile.txt

Changes are written directly to the barecat archive.

Mount Options
-------------

Options are passed as a comma-separated list with ``-o``:

.. code-block:: bash

   # Run in foreground (for debugging)
   barecat mount myarchive.barecat /mnt/myarchive -o fg

   # Read-write with memory-mapped I/O
   barecat mount myarchive.barecat /mnt/myarchive -o rw,mmap

   # Read-write, append-only with shard size limit
   barecat mount myarchive.barecat /mnt/myarchive -o rw,append_only,shard_size_limit=10G

Available options:

``ro``
   Read-only mode (default).

``rw``
   Read-write mode.

``fg``, ``foreground``
   Run in the foreground instead of daemonizing.

``mmap``
   Use memory-mapped I/O.

``defrag``
   Enable automatic defragmentation.

``overwrite``
   Allow overwriting existing files.

``append_only``
   Only allow appending new files (no overwrites or deletes).

``shard_size_limit=SIZE``
   Set shard size limit (e.g., ``1G``, ``500M``).


Use Cases
---------

Browsing Archives
~~~~~~~~~~~~~~~~~

Mount and browse with your file manager:

.. code-block:: bash

   barecat mount dataset.barecat ~/mnt/dataset
   nautilus ~/mnt/dataset  # or dolphin, thunar, etc.

Using with Existing Tools
~~~~~~~~~~~~~~~~~~~~~~~~~

Tools that expect filesystem paths work directly:

.. code-block:: bash

   # Image viewer
   feh /mnt/myarchive/images/

   # grep through files
   grep -r "pattern" /mnt/myarchive/

   # rsync to copy
   rsync -av /mnt/myarchive/subset/ /destination/

Jupyter Notebooks
~~~~~~~~~~~~~~~~~

Mount the archive and use standard file I/O in notebooks:

.. code-block:: python

   # After: barecat mount data.barecat /mnt/data
   from PIL import Image

   img = Image.open('/mnt/data/image.jpg')

Docker Integration
~~~~~~~~~~~~~~~~~~

Mount inside a container:

.. code-block:: bash

   # Host: mount the archive
   barecat mount dataset.barecat /data/mounted

   # Docker: bind mount the FUSE mount
   docker run -v /data/mounted:/data:ro myimage

Performance Considerations
--------------------------

FUSE adds overhead compared to direct barecat access:

- **Small files**: FUSE overhead is noticeable
- **Large sequential reads**: Near-native performance
- **Random access**: Good, but direct API is faster

For performance-critical applications (ML training), prefer the Python API:

.. code-block:: python

   # Faster than FUSE
   with barecat.Barecat('data.barecat') as bc:
       data = bc['path/to/file']

FUSE is best for:

- Interactive browsing
- Integration with tools that require filesystem paths
- One-off operations

FUSE may also not be available in some environments (e.g., clusters).


Troubleshooting
---------------

"Transport endpoint is not connected"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The mount was interrupted. Unmount and remount:

.. code-block:: bash

   fusermount -u /mnt/myarchive
   barecat mount myarchive.barecat /mnt/myarchive

"Permission denied" on mount
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check that you have FUSE permissions:

.. code-block:: bash

   # Add yourself to fuse group
   sudo usermod -aG fuse $USER
   # Log out and back in

"Operation not permitted" with allow_other
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Edit ``/etc/fuse.conf`` and uncomment ``user_allow_other``.

See Also
--------

- :doc:`../reference/cli` - Full CLI reference (including ``barecat mount``)
- :doc:`../tutorials/getting-started` - Basic barecat usage
