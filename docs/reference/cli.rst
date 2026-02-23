Command Line Interface
======================

Barecat provides a unified command-line interface through the ``barecat`` command
with various subcommands. This document provides a complete reference for all
available commands and their options.

Synopsis
--------

.. code-block:: text

   barecat <command> [options] [arguments]

Commands
--------

.. contents::
   :local:
   :depth: 1

barecat create
~~~~~~~~~~~~~~

Create a new barecat archive.

**Synopsis:**

.. code-block:: text

   barecat create [options] ARCHIVE [PATHS...]

**Description:**

Creates a new archive from specified files and directories. If the archive
already exists, the command fails unless ``-f`` is specified.

**Options:**

``ARCHIVE``
   Path to the barecat archive to create (without suffix).

``PATHS``
   Files or directories to add. Directories are added recursively.

``-C, --directory DIR``
   Change to DIR before adding files (like tar).

``-T, --files-from FILE``
   Read paths from FILE (one per line). Use ``-`` to read from stdin.

``-0, --null``
   Paths are null-separated (for use with ``find -print0``).

``--exclude PATTERN``
   Exclude files matching PATTERN. Can be repeated.

``-i, --include PATTERN``
   Only include files matching PATTERN. Can be repeated.

``-j, --workers N``
   Number of worker threads for parallel processing.

``-f, --force``
   Overwrite existing archive.

``-s, --shard-size-limit SIZE``
   Maximum shard size (e.g., ``1G``, ``500M``). Default: unlimited.

``--physical-order``
   Add files in physical disk order (optimization for HDDs).

**Examples:**

.. code-block:: bash

   # Create archive from directory
   barecat create myarchive.barecat /path/to/data/

   # Create from find output
   find /data -name '*.jpg' -print0 | barecat create -T - -0 myarchive.barecat

   # Create with shard limit
   barecat create -s 10G myarchive.barecat /path/to/data/


barecat add
~~~~~~~~~~~

Add files to an existing archive.

**Synopsis:**

.. code-block:: text

   barecat add [options] ARCHIVE [PATHS...]

**Description:**

Adds files to an existing archive. By default, fails if the archive does not
exist. Use ``-c`` to create it if missing.

**Options:**

Same as ``barecat create``, plus:

``-c, --create``
   Create the archive if it does not exist.

**Examples:**

.. code-block:: bash

   # Add more files to existing archive
   barecat add myarchive.barecat /more/data/

   # Add with create-if-missing
   barecat add -c myarchive.barecat /data/


barecat extract
~~~~~~~~~~~~~~~

Extract files from an archive.

**Synopsis:**

.. code-block:: text

   barecat extract [options] ARCHIVE [PATHS...]

**Description:**

Extracts files from the archive. If PATHS are specified, only those paths
are extracted. Otherwise, the entire archive is extracted.

**Options:**

``-C, --directory DIR``
   Extract to DIR (default: current directory).

**Examples:**

.. code-block:: bash

   # Extract entire archive
   barecat extract myarchive.barecat -C /output/

   # Extract specific paths
   barecat extract myarchive.barecat subdir/file.txt


barecat list
~~~~~~~~~~~~

List archive contents.

**Synopsis:**

.. code-block:: text

   barecat list [options] ARCHIVE [PATHS...]

**Aliases:** ``ls``, ``l``, ``t``

**Options:**

``-l, --long``
   Long listing format with file sizes.

``-R, --recursive``
   List directories recursively.

**Examples:**

.. code-block:: bash

   barecat list myarchive.barecat
   barecat ls -l myarchive.barecat subdir/
   barecat list -lR myarchive.barecat


barecat cat
~~~~~~~~~~~

Print file contents to standard output.

**Synopsis:**

.. code-block:: text

   barecat cat ARCHIVE PATH

**Examples:**

.. code-block:: bash

   barecat cat myarchive.barecat path/to/file.txt > output.txt


barecat shell
~~~~~~~~~~~~~

Interactive shell for exploring archives.

**Synopsis:**

.. code-block:: text

   barecat shell [options] ARCHIVE

**Description:**

Opens an interactive shell with commands like ``ls``, ``cd``, ``cat``, ``pwd``.

**Options:**

``-c, --cmd COMMAND``
   Execute COMMAND and exit.

``-w, --write``
   Open in write mode (allows modifications).

**Examples:**

.. code-block:: bash

   barecat shell myarchive.barecat
   barecat shell -c "ls subdir/" myarchive.barecat


barecat browse
~~~~~~~~~~~~~~

Ranger-like file browser for archives.

**Synopsis:**

.. code-block:: text

   barecat browse ARCHIVE

**Description:**

Opens a curses-based file browser similar to ranger.


barecat du
~~~~~~~~~~

Show disk usage (like du).

**Synopsis:**

.. code-block:: text

   barecat du [options] ARCHIVE [PATH]

**Description:**

Prints disk usage by directory, similar to the Unix ``du`` command.

**Options:**

``-a, --all``
   Show all files, not just directories.

``-s, --summarize``
   Show only total for each argument.

``-H, --human-readable``
   Print sizes in human-readable format.

``-d, --max-depth N``
   Maximum depth to show.


barecat ncdu
~~~~~~~~~~~~

Interactive disk usage viewer (ncdu-like).

**Synopsis:**

.. code-block:: text

   barecat ncdu ARCHIVE

**Description:**

Opens an ncdu-like curses interface showing disk usage by directory.


barecat verify
~~~~~~~~~~~~~~

Verify archive integrity.

**Synopsis:**

.. code-block:: text

   barecat verify [options] ARCHIVE

**Description:**

Verifies CRC32C checksums and index integrity.

**Options:**

``--quick``
   Quick verification (index integrity + last file CRC check only).

**Exit Status:**

Returns 0 on success, non-zero if verification fails.


barecat defrag
~~~~~~~~~~~~~~

Defragment archive.

**Synopsis:**

.. code-block:: text

   barecat defrag [options] ARCHIVE

**Description:**

Removes gaps left by deleted files, compacting the archive.

**Options:**

``--quick``
   Use best-fit algorithm for faster but less thorough defragmentation.


barecat reshard
~~~~~~~~~~~~~~~

Reshard archive with a new shard size.

**Synopsis:**

.. code-block:: text

   barecat reshard -s SIZE ARCHIVE

**Description:**

Reorganizes the archive with a new shard size limit.

**Options:**

``-s, --shard-size-limit SIZE``
   New shard size limit (required).

**Examples:**

.. code-block:: bash

   # Consolidate into larger shards
   barecat reshard -s 50G myarchive.barecat


barecat merge
~~~~~~~~~~~~~

Merge multiple archives into one.

**Synopsis:**

.. code-block:: text

   barecat merge [options] -o OUTPUT ARCHIVES...

**Description:**

Merges multiple archives (barecat, tar, or zip) into a single barecat archive.

**Options:**

``-o, --output OUTPUT``
   Output archive path (required).

``-s, --shard-size-limit SIZE``
   Shard size limit for output.

``-f, --force``
   Overwrite output if it exists.

``-a, --append``
   Append to output if it exists.

``--symlink``
   Create symlinks to original shards instead of copying (barecat inputs only).

``--ignore-duplicates``
   Skip files that already exist in output.

**Examples:**

.. code-block:: bash

   # Merge barecat archives
   barecat merge -o combined.barecat arch1.barecat arch2.barecat

   # Merge mixed archive types
   barecat merge -o combined.barecat data.tar.gz more.zip existing.barecat


barecat convert
~~~~~~~~~~~~~~~

Convert between barecat and tar/zip formats.

**Synopsis:**

.. code-block:: text

   barecat convert [options] INPUT OUTPUT

**Description:**

Converts between barecat and traditional archive formats. Direction is
auto-detected based on file extensions.

**Options:**

``-s, --shard-size-limit SIZE``
   Shard size limit (tar/zip to barecat only).

``-f, --force``
   Overwrite existing output.

``--stdin``
   Read input from stdin (INPUT specifies format: ``tar``, ``tar.gz``, etc.).

``--stdout``
   Write output to stdout (OUTPUT specifies format: ``tar``, ``tar.gz``, etc.).

``--root-dir NAME``
   Wrap all files in a root directory (barecat to tar only).

``--wrap``
   Zero-copy mode: create barecat index over existing uncompressed tar/zip
   (symlinks to original file).

**Examples:**

.. code-block:: bash

   # Convert tar.gz to barecat
   barecat convert data.tar.gz data.barecat

   # Convert barecat to tar
   barecat convert data.barecat data.tar

   # Stream conversion
   cat data.tar.gz | barecat convert --stdin tar.gz output.barecat
   barecat convert --stdout data.barecat tar.gz > data.tar.gz

   # Zero-copy wrap (uncompressed archives only)
   barecat convert --wrap data.tar data.barecat


barecat to-ncdu-json
~~~~~~~~~~~~~~~~~~~~

Export archive structure as ncdu JSON.

**Synopsis:**

.. code-block:: text

   barecat to-ncdu-json ARCHIVE

**Description:**

Outputs the archive structure in ncdu's JSON format for use with ``ncdu -f``.

**Examples:**

.. code-block:: bash

   barecat to-ncdu-json myarchive.barecat > archive.json
   ncdu -f archive.json


barecat index-to-csv
~~~~~~~~~~~~~~~~~~~~

Export index as CSV.

**Synopsis:**

.. code-block:: text

   barecat index-to-csv ARCHIVE

**Description:**

Dumps the file index as CSV to standard output.


barecat upgrade
~~~~~~~~~~~~~~~

Upgrade index to new schema version.

**Synopsis:**

.. code-block:: text

   barecat upgrade [options] ARCHIVE

**Options:**

``-j, --workers N``
   Number of worker threads.


barecat completion-script
~~~~~~~~~~~~~~~~~~~~~~~~~

Print shell completion script path.

**Synopsis:**

.. code-block:: text

   barecat completion-script bash|zsh

**Description:**

Prints the path to the shell completion script. Source this in your shell
configuration.

**Examples:**

.. code-block:: bash

   # Bash: add to ~/.bashrc
   source $(barecat completion-script bash)

   # Zsh: add to ~/.zshrc
   source $(barecat completion-script zsh)


Environment Variables
---------------------

Barecat does not currently use any environment variables.


Exit Status
-----------

``0``
   Success.

``1``
   General error (file not found, invalid arguments, etc.).

``Non-zero``
   Verification failures, integrity errors.


See Also
--------

- :doc:`../explanation/architecture` - Internal architecture overview
- `ncdu <https://dev.yorhel.nl/ncdu>`_ - NCurses Disk Usage
