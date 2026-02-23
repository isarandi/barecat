Getting Started
===============

This tutorial will guide you through the basics of using Barecat to create,
read, and manage archives.

Prerequisites
-------------

Install barecat via pip:

.. code-block:: bash

   pip install barecat

Creating Your First Archive
---------------------------

Let's create a simple archive containing a few text files.

Using the Command Line
~~~~~~~~~~~~~~~~~~~~~~

First, create some sample files:

.. code-block:: bash

   mkdir mydata
   echo "Hello, World!" > mydata/hello.txt
   echo "Goodbye, World!" > mydata/goodbye.txt
   mkdir mydata/subdir
   echo "Nested file" > mydata/subdir/nested.txt

Now create a barecat archive:

.. code-block:: bash

   barecat create myarchive.barecat mydata/

This creates two files:

- ``myarchive.barecat-sqlite-index`` - The SQLite database with file metadata
- ``myarchive.barecat-shard-00000`` - The data file containing concatenated file contents

Using Python
~~~~~~~~~~~~

.. code-block:: python

   import barecat

   # Create a new archive
   with barecat.Barecat('myarchive.barecat', readonly=False) as bc:
       bc['hello.txt'] = b'Hello, World!'
       bc['goodbye.txt'] = b'Goodbye, World!'
       bc['subdir/nested.txt'] = b'Nested file'

Reading from an Archive
-----------------------

Command Line
~~~~~~~~~~~~

List the contents:

.. code-block:: bash

   barecat list myarchive.barecat
   barecat list -l myarchive.barecat        # Long format with sizes
   barecat list -lr myarchive.barecat       # Recursive listing

Extract a single file to stdout:

.. code-block:: bash

   barecat cat myarchive.barecat hello.txt

Extract the entire archive:

.. code-block:: bash

   barecat extract myarchive.barecat -C /output/directory/

Python
~~~~~~

.. code-block:: python

   import barecat

   with barecat.Barecat('myarchive.barecat') as bc:
       # Dictionary-style access
       content = bc['hello.txt']
       print(content)  # b'Hello, World!'

       # Check if file exists
       if 'hello.txt' in bc:
           print("File exists!")

       # List directory contents
       files = bc.listdir('')  # Root directory
       print(files)  # ['hello.txt', 'goodbye.txt', 'subdir']

       # Walk the archive (like os.walk)
       for root, dirs, files in bc.walk(''):
           for f in files:
               print(f"{root}/{f}")

       # File-like access
       with bc.open('hello.txt') as f:
           data = f.read(5)  # Read first 5 bytes
           print(data)  # b'Hello'

Adding Files to an Existing Archive
-----------------------------------

Command Line
~~~~~~~~~~~~

.. code-block:: bash

   # Add more files
   barecat add myarchive.barecat newfile.txt another_directory/

   # Add from stdin (one path per line)
   find /data -name '*.jpg' | barecat add --stdin myarchive.barecat

Python
~~~~~~

.. code-block:: python

   with barecat.Barecat('myarchive.barecat', readonly=False) as bc:
       # Add data directly
       bc['newfile.txt'] = b'New content'

       # Add from filesystem
       bc.add_by_path('/path/to/file.txt')

       # Add with custom path in archive
       bc.add_by_path('/path/to/file.txt', store_path='custom/path.txt')

Automatic Encoding and Decoding
-------------------------------

Barecat stores raw bytes, but you often want to work with structured data like
JSON, numpy arrays, or images. Wrap your archive with ``DecodedView`` to
automatically encode on write and decode on read, based on file extension:

.. code-block:: python

   from barecat import Barecat, DecodedView
   import numpy as np

   with Barecat('data.barecat', readonly=False) as bc:
       dec = DecodedView(bc)

       # Store Python dicts as JSON
       dec['config.json'] = {'learning_rate': 0.001, 'epochs': 100}

       # Store numpy arrays
       dec['weights.npy'] = np.random.randn(100, 100)

       # Store with msgpack (fast, compact, numpy-aware)
       dec['data.msgpack'] = {'arrays': [np.array([1, 2, 3])], 'meta': 'info'}

       # Store images (as numpy arrays, via imageio)
       dec['image.png'] = np.zeros((256, 256, 3), dtype=np.uint8)

   # Reading back
   with Barecat('data.barecat') as bc:
       dec = DecodedView(bc)
       config = dec['config.json']   # Returns dict
       weights = dec['weights.npy']  # Returns numpy array
       data = dec['data.msgpack']    # Returns dict with numpy arrays
       image = dec['image.png']      # Returns numpy array (H, W, C)

Supported formats include ``.json``, ``.pkl``, ``.npy``, ``.npz``, ``.msgpack``,
and images (``.png``, ``.jpg``, etc.). Compression can be stacked: ``config.json.gz``.

For raw bytes, use the underlying ``bc`` directly. See
:doc:`../howto/automatic-encoding` for the full list of formats and custom codecs.

Deleting Files
--------------

.. code-block:: python

   with barecat.Barecat('myarchive.barecat', readonly=False) as bc:
       del bc['hello.txt']

Note: Deleting files leaves gaps in the shard file. Use ``barecat defrag`` to
reclaim this space.

Interactive Exploration
-----------------------

Barecat provides two interactive modes:

Shell
~~~~~

.. code-block:: bash

   barecat shell myarchive.barecat

This opens a command-line shell with familiar commands like ``ls``, ``cd``,
``cat``, and ``pwd``.

Browser
~~~~~~~

.. code-block:: bash

   barecat browse myarchive.barecat

This opens a ranger-like file browser for visual exploration.

Disk Usage Viewer
~~~~~~~~~~~~~~~~~

.. code-block:: bash

   barecat du myarchive.barecat

This opens an ncdu-like interface showing disk usage by directory.

Next Steps
----------

- :doc:`image-datasets` - Learn how to use barecat for ML training data
- :doc:`../howto/convert-archives` - Convert existing tar/zip archives
- :doc:`../reference/cli` - Complete CLI reference
