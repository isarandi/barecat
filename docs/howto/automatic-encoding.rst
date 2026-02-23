How to Automatically Encode and Decode Files
=============================================

This guide shows how to store and retrieve structured data (JSON, numpy arrays,
images, etc.) without manually converting to/from bytes.

Basic Usage
-----------

Wrap your Barecat archive with ``DecodedView`` to get automatic encoding on write
and decoding on read, based on file extension:

.. code-block:: python

   from barecat import Barecat, DecodedView

   with Barecat('data.barecat', readonly=False) as bc:
       dec = DecodedView(bc)

       # Store a Python dict as JSON
       dec['config.json'] = {'learning_rate': 0.001, 'epochs': 100}

       # Store a numpy array
       import numpy as np
       dec['weights.npy'] = np.random.randn(100, 100)

       # Store an image (as numpy array)
       dec['image.png'] = np.zeros((256, 256, 3), dtype=np.uint8)

   # Reading back
   with Barecat('data.barecat') as bc:
       dec = DecodedView(bc)

       config = dec['config.json']      # Returns dict
       weights = dec['weights.npy']     # Returns numpy array
       image = dec['image.png']         # Returns numpy array (H, W, C)

Supported Formats
-----------------

**Data formats:**

- ``.json`` — Python dict/list (via stdlib ``json``)
- ``.pkl``, ``.pickle`` — Any Python object (via ``pickle``)
- ``.npy`` — Single numpy array
- ``.npz`` — Dict of numpy arrays
- ``.msgpack`` — Any object (via ``msgpack-numpy``, must be installed)

**Image formats:**

- ``.jpg``, ``.jpeg`` — JPEG (lossy)
- ``.png`` — PNG (lossless)
- ``.gif``, ``.bmp``, ``.tiff``, ``.tif``, ``.webp``, ``.exr``

Images are stored/returned as numpy arrays with shape ``(H, W, C)``.

The image codec uses the first available backend: jpeg4py (JPEG only) > OpenCV
(``cv2``) > Pillow (``PIL``) > imageio. Install your preferred backend, or let
barecat use whichever is available.

**Compression** (stackable):

- ``.gz``, ``.gzip`` — gzip
- ``.xz``, ``.lzma`` — LZMA
- ``.bz2`` — bzip2

Compressed Files
----------------

Compression extensions can be stacked with other formats:

.. code-block:: python

   # JSON compressed with gzip
   dec['config.json.gz'] = {'large': 'data structure'}

   # Pickle compressed with LZMA (good compression ratio)
   dec['model.pkl.xz'] = trained_model

   # Reading automatically decompresses then decodes
   config = dec['config.json.gz']  # Returns dict

Raw Bytes
---------

For files without a known extension, use the underlying store directly:

.. code-block:: python

   with Barecat('data.barecat', readonly=False) as bc:
       dec = DecodedView(bc)

       # Use dec for known formats
       dec['config.json'] = {'key': 'value'}

       # Use bc directly for raw bytes
       bc['data.bin'] = b'raw binary data'
       bc['custom.xyz'] = some_bytes

   # Reading
   with Barecat('data.barecat') as bc:
       dec = DecodedView(bc)

       config = dec['config.json']  # Decoded
       raw = bc['data.bin']         # Raw bytes

If you try to use ``DecodedView`` with an unknown extension, it raises
``ValueError`` to prevent silent bugs:

.. code-block:: python

   dec['file.xyz'] = data  # ValueError: No codec registered for '.xyz'

Custom Codecs
-------------

Register your own codecs for custom formats:

.. code-block:: python

   import yaml

   dec = DecodedView(bc)
   dec.register_codec(
       ['.yaml', '.yml'],
       encoder=lambda d: yaml.dump(d).encode('utf-8'),
       decoder=lambda b: yaml.safe_load(b.decode('utf-8')),
   )

   dec['config.yaml'] = {'setting': 'value'}

For stackable compression codecs, set ``nonfinal=True``:

.. code-block:: python

   import zstandard as zstd

   dec.register_codec(
       ['.zst'],
       encoder=lambda b: zstd.compress(b),
       decoder=lambda b: zstd.decompress(b),
       nonfinal=True,  # Can stack: .json.zst
   )

With PyTorch DataLoader
-----------------------

.. code-block:: python

   from torch.utils.data import Dataset, DataLoader
   from barecat import Barecat, DecodedView

   class ImageDataset(Dataset):
       def __init__(self, archive_path):
           self.bc = Barecat(archive_path, threadsafe=True)
           self.dec = DecodedView(self.bc)
           self.paths = [p for p in self.bc.index.iter_all_paths()
                         if p.endswith('.png')]

       def __len__(self):
           return len(self.paths)

       def __getitem__(self, idx):
           # Returns numpy array directly
           return self.dec[self.paths[idx]]

       def close(self):
           self.bc.close()

   dataset = ImageDataset('images.barecat')
   loader = DataLoader(dataset, batch_size=32, num_workers=4)

Migration from auto_codec
-------------------------

The ``auto_codec`` parameter is deprecated. Migrate by wrapping with
``DecodedView``:

.. code-block:: python

   # Old (deprecated, emits warning):
   with Barecat('data.barecat', auto_codec=True) as bc:
       data = bc['file.json']

   # New:
   with Barecat('data.barecat') as bc:
       dec = DecodedView(bc)
       data = dec['file.json']

Key difference: ``DecodedView`` raises an error for unknown extensions instead
of silently passing through raw bytes. This catches bugs early.

See Also
--------

- :doc:`../reference/python-api` — Full API reference for DecodedView
- :doc:`pytorch-dataloader` — Using with PyTorch DataLoader
