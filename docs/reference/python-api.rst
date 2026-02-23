Python API Reference
====================

This is the reference documentation for barecat's Python API.

Barecat Class
-------------

.. py:class:: barecat.Barecat(path, readonly=True, overwrite=False, shard_size_limit=None, threadsafe=False, auto_codec=False)

   Main class for reading and writing barecat archives.

   :param str path: Path to the archive (without suffix).
   :param bool readonly: Open in read-only mode. Default: True.
   :param bool overwrite: Delete existing archive if it exists. Default: False.
   :param int shard_size_limit: Maximum size per shard file in bytes. Default: unlimited.
   :param bool threadsafe: Use thread-local storage for connections. Required for multi-process DataLoader. Default: False.
   :param bool auto_codec: **Deprecated.** Use :class:`DecodedView` instead. Will be removed in 1.0.

   **Context Manager**

   .. code-block:: python

      with barecat.Barecat('archive.barecat') as bc:
          data = bc['file.txt']

   **Dictionary-like Access**

   .. py:method:: __getitem__(path)

      Get file contents by path.

      :param str path: Path to the file.
      :returns: File contents as bytes.
      :raises KeyError: If file does not exist.

   .. py:method:: __setitem__(path, data)

      Set file contents by path.

      :param str path: Path to the file.
      :param bytes data: File contents.

   .. py:method:: __delitem__(path)

      Delete a file by path.

      :param str path: Path to the file.
      :raises KeyError: If file does not exist.

   .. py:method:: __contains__(path)

      Check if a file with the given path exists in the archive.

      Note: This only checks for files, not directories. Use :meth:`exists`
      to check for both files and directories.

      :param str path: Path to the file.
      :returns: True if file exists, False otherwise.
      :rtype: bool

   **Filesystem-like Access**

   .. py:method:: open(path, mode='r')

      Open a file for reading or writing.

      :param str path: Path to the file.
      :param str mode: 'r' for text, 'rb' for binary, 'r+b' for read-write binary.
      :returns: File-like object.

      .. code-block:: python

         with bc.open('file.txt') as f:
             data = f.read(100)
             f.seek(0)

   .. py:method:: listdir(path='')

      List directory contents.

      :param str path: Directory path.
      :returns: List of entry names (not full paths).
      :rtype: list[str]

   .. py:method:: walk(top='')

      Walk directory tree, like os.walk().

      :param str top: Starting directory.
      :yields: (dirpath, dirnames, filenames) tuples.

   .. py:method:: glob(pattern, recursive=False)

      Find files matching a glob pattern.

      :param str pattern: Glob pattern (e.g., '*.jpg', '**/*.txt').
      :param bool recursive: Enable ** for recursive matching.
      :returns: List of matching paths.
      :rtype: list[str]

   .. py:method:: isfile(path)

      Check if path is a file.

      :param str path: Path to check.
      :rtype: bool

   .. py:method:: isdir(path)

      Check if path is a directory.

      :param str path: Path to check.
      :rtype: bool

   **Adding Files**

   .. py:method:: add(item, *, data=None, fileobj=None, dir_exist_ok=False, file_exist_ok=False)

      Add a file or directory to the archive.

      :param item: BarecatFileInfo, BarecatDirInfo, or path string.
      :param bytes data: File contents (if not using fileobj).
      :param fileobj: File-like object to read from.
      :param bool dir_exist_ok: Don't error if directory exists.
      :param bool file_exist_ok: Skip if file exists (for merges).

   .. py:method:: add_by_path(filesystem_path, store_path=None)

      Add a file from the filesystem.

      :param str filesystem_path: Path on the filesystem.
      :param str store_path: Path in the archive (default: same as filesystem_path).

   **Deletion**

   .. py:method:: remove(path)

      Remove a file from the archive.

      :param str path: Path to the file.

   **Properties**

   .. py:attribute:: index

      The Index object managing the SQLite database.

   .. py:attribute:: shard_size_limit

      Maximum shard size in bytes.

BarecatFileInfo Class
---------------------

.. py:class:: barecat.BarecatFileInfo(path=None, mode=None, uid=None, gid=None, mtime_ns=None, shard=None, offset=None, size=None, crc32c=None)

   Describes a file in the archive.

   :param str path: File path within the archive.
   :param int mode: Unix file mode (permissions).
   :param int uid: Owner user ID.
   :param int gid: Owner group ID.
   :param int mtime_ns: Modification time in nanoseconds since epoch.
   :param int shard: Shard number where file data is stored.
   :param int offset: Byte offset within the shard.
   :param int size: File size in bytes.
   :param int crc32c: CRC32C checksum of contents.

   .. py:attribute:: path

      File path (normalized on assignment).

   .. py:attribute:: size

      File size in bytes.

   .. py:attribute:: mtime_dt

      Modification time as datetime object.

   .. py:method:: asdict()

      Return as dictionary.

BarecatDirInfo Class
--------------------

.. py:class:: barecat.BarecatDirInfo(path=None, mode=None, uid=None, gid=None, mtime_ns=None, num_subdirs=None, num_files=None, size_tree=None, num_files_tree=None)

   Describes a directory in the archive.

   :param str path: Directory path within the archive.
   :param int mode: Unix directory mode.
   :param int uid: Owner user ID.
   :param int gid: Owner group ID.
   :param int mtime_ns: Modification time in nanoseconds.
   :param int num_subdirs: Number of immediate subdirectories.
   :param int num_files: Number of immediate files.
   :param int size_tree: Total size of all files recursively.
   :param int num_files_tree: Total number of files recursively.

   .. py:attribute:: path

      Directory path (normalized on assignment).

   .. py:attribute:: num_entries

      Total entries (num_subdirs + num_files).

Index Class
-----------

.. py:class:: barecat.core.index.Index

   Manages the SQLite database. Usually accessed via ``bc.index``.

   .. py:method:: iter_all_fileinfos(order=Order.ANY)

      Iterate over all files in the archive.

      :param Order order: Ordering (ANY, PATH, ADDRESS, RANDOM).
      :yields: BarecatFileInfo objects.

   .. py:method:: iter_all_dirinfos(order=Order.ANY)

      Iterate over all directories.

      :yields: BarecatDirInfo objects.

   .. py:method:: iter_all_paths()

      Iterate over all file paths.

      :yields: Path strings.

   .. py:method:: lookup_file(path)

      Look up file info by path.

      :param str path: File path.
      :returns: BarecatFileInfo
      :raises FileNotFoundBarecatError: If not found.

   .. py:method:: lookup_dir(path)

      Look up directory info by path.

      :param str path: Directory path.
      :returns: BarecatDirInfo
      :raises NotADirectoryBarecatError: If not found.

Order Enum
----------

.. py:class:: barecat.Order

   Ordering options for iteration.

   .. py:attribute:: ANY

      Default order (as returned by SQLite).

   .. py:attribute:: PATH

      Alphabetical by path.

   .. py:attribute:: ADDRESS

      By shard and offset (physical order).

   .. py:attribute:: RANDOM

      Random order.

   .. py:attribute:: DESC

      Descending (combine with PATH or ADDRESS).

   .. code-block:: python

      from barecat import Order

      # Iterate in physical order (optimal for sequential reads)
      for f in bc.index.iter_all_fileinfos(order=Order.ADDRESS):
          ...

      # Iterate in reverse alphabetical order
      for f in bc.index.iter_all_fileinfos(order=Order.PATH | Order.DESC):
          ...

Exceptions
----------

.. py:exception:: barecat.exceptions.BarecatError

   Base exception for barecat errors.

.. py:exception:: barecat.exceptions.FileNotFoundBarecatError

   File not found in archive.

.. py:exception:: barecat.exceptions.FileExistsBarecatError

   File already exists in archive.

.. py:exception:: barecat.exceptions.IsADirectoryBarecatError

   Operation expected file but got directory.

.. py:exception:: barecat.exceptions.NotADirectoryBarecatError

   Operation expected directory but got file.

.. py:exception:: barecat.exceptions.DirectoryNotEmptyBarecatError

   Cannot delete non-empty directory.

DecodedView Class
-----------------

.. py:class:: barecat.DecodedView(store)

   Dict-like view that automatically encodes/decodes based on file extension.

   Wraps a raw bytes store (like ``Barecat``) and automatically encodes on write
   and decodes on read based on the file extension. Raises an error if no
   codec is registered for the extension.

   :param store: A ``MutableMapping[str, bytes]`` to wrap (e.g., a Barecat instance).

   **Basic Usage**

   .. code-block:: python

      import barecat
      from barecat import DecodedView

      with barecat.Barecat('data.barecat', readonly=False) as bc:
          dec = DecodedView(bc)

          # JSON: dict/list ↔ bytes
          dec['config.json'] = {'key': 'value', 'count': 42}
          config = dec['config.json']  # Returns dict

          # Images: numpy array ↔ encoded bytes (via imageio)
          import numpy as np
          dec['image.png'] = np.zeros((100, 100, 3), dtype=np.uint8)
          image = dec['photo.jpg']  # Returns numpy array (H, W, C)

          # Numpy arrays
          dec['data.npy'] = np.array([1, 2, 3])
          arr = dec['data.npy']

          # Pickle: any Python object
          dec['model.pkl'] = {'weights': [...], 'config': {...}}

          # For raw bytes, use the store directly:
          bc['file.bin'] = b'raw binary data'

   **Stacked Compression**

   Compression codecs (``.gz``, ``.xz``, ``.bz2``) can be stacked with other codecs:

   .. code-block:: python

      # JSON compressed with gzip
      dec['config.json.gz'] = {'large': 'data'}
      config = dec['config.json.gz']  # Decompresses, then parses JSON

      # Pickle compressed with lzma
      dec['model.pkl.xz'] = large_object

   **Supported Extensions**

   +------------------------+----------------------------+-------------+
   | Extension              | Type                       | Stackable   |
   +========================+============================+=============+
   | ``.json``              | dict/list                  | No          |
   +------------------------+----------------------------+-------------+
   | ``.pkl``, ``.pickle``  | any (pickle)               | No          |
   +------------------------+----------------------------+-------------+
   | ``.npy``               | numpy array                | No          |
   +------------------------+----------------------------+-------------+
   | ``.npz``               | dict of numpy arrays       | No          |
   +------------------------+----------------------------+-------------+
   | ``.msgpack``           | any (msgpack-numpy)        | No          |
   +------------------------+----------------------------+-------------+
   | ``.jpg``, ``.jpeg``    | numpy array (imageio)      | No          |
   +------------------------+----------------------------+-------------+
   | ``.png``               | numpy array (imageio)      | No          |
   +------------------------+----------------------------+-------------+
   | ``.gif``, ``.bmp``     | numpy array (imageio)      | No          |
   +------------------------+----------------------------+-------------+
   | ``.tiff``, ``.tif``    | numpy array (imageio)      | No          |
   +------------------------+----------------------------+-------------+
   | ``.webp``, ``.exr``    | numpy array (imageio)      | No          |
   +------------------------+----------------------------+-------------+
   | ``.gz``, ``.gzip``     | gzip compression           | Yes         |
   +------------------------+----------------------------+-------------+
   | ``.xz``, ``.lzma``     | lzma compression           | Yes         |
   +------------------------+----------------------------+-------------+
   | ``.bz2``               | bzip2 compression          | Yes         |
   +------------------------+----------------------------+-------------+

   **Custom Codecs**

   .. py:method:: register_codec(exts, encoder, decoder, nonfinal=False)

      Register a custom codec for given extensions.

      :param list[str] exts: List of extensions (e.g., ``['.xyz']``).
      :param callable encoder: Function ``(data) -> bytes``.
      :param callable decoder: Function ``(bytes) -> data``.
      :param bool nonfinal: If True, codec can stack (like compression).

      .. code-block:: python

         import yaml

         dec.register_codec(
             ['.yaml', '.yml'],
             encoder=lambda d: yaml.dump(d).encode('utf-8'),
             decoder=lambda b: yaml.safe_load(b.decode('utf-8')),
         )

         dec['config.yaml'] = {'setting': 'value'}

   .. py:method:: clear_codecs()

      Remove all registered codecs.

   ``DecodedView`` wraps any ``MutableMapping[str, bytes]``.

Deprecated: auto_codec Parameter
--------------------------------

.. deprecated:: 0.3.0

   The ``auto_codec`` parameter is deprecated and will be removed in version 1.0.
   Use :class:`DecodedView` instead.

   **Migration:**

   .. code-block:: python

      # Old (deprecated):
      with barecat.Barecat('data.barecat', auto_codec=True) as bc:
          data = bc['file.json']

      # New:
      with barecat.Barecat('data.barecat') as bc:
          dec = DecodedView(bc)
          data = dec['file.json']
