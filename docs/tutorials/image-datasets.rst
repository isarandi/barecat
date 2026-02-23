Working with Image Datasets
===========================

This tutorial covers the primary use case for Barecat: storing large image
datasets for machine learning training.

Why Barecat for ML?
-------------------

When training deep learning models, you typically need:

1. **Random access** - Training shuffles data each epoch
2. **Fast I/O** - GPUs are hungry; don't let I/O be the bottleneck
3. **Millions of files** - ImageNet has 1.2M images; some datasets have 100M+
4. **Network filesystem friendly** - Clusters often use NFS/Lustre/GPFS

Traditional approaches have problems:

- **Raw files**: Too many small files strain filesystem metadata, hit inode limits
- **tar**: No random access; must scan sequentially
- **zip**: Central directory must be scanned; slow for huge archives
- **HDF5/LMDB**: Complex APIs, can't easily browse contents

Barecat solves these by concatenating files into shards with an SQLite index
for O(1) random lookups.

Creating a Dataset Archive
--------------------------

From a Directory
~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Archive an ImageNet-style dataset
   barecat create imagenet.barecat /data/imagenet/train/ -s 50G

   # With multiple worker threads for faster compression
   barecat create imagenet.barecat /data/imagenet/train/ -s 50G -j 8

The ``-s 50G`` flag limits each shard to 50GB, making the archive easier to
move and distribute.

From find Output
~~~~~~~~~~~~~~~~

For more control over which files to include:

.. code-block:: bash

   # Only JPEG files
   find /data/imagenet -name '*.jpg' -print0 | \
       barecat create -0 imagenet.barecat -s 50G

   # Exclude certain directories
   find /data -name '*.jpg' ! -path '*/bad_samples/*' -print0 | \
       barecat create -0 dataset.barecat

Converting from tar
~~~~~~~~~~~~~~~~~~~

If you already have a tar archive:

.. code-block:: bash

   barecat convert imagenet.tar.gz imagenet.barecat -s 50G

Using with PyTorch
------------------

Basic Dataset
~~~~~~~~~~~~~

.. code-block:: python

   import torch
   from torch.utils.data import Dataset, DataLoader
   import barecat
   from PIL import Image
   import io

   class BarecatImageDataset(Dataset):
       def __init__(self, archive_path, transform=None):
           self.bc = barecat.Barecat(archive_path)
           self.transform = transform
           # Get list of all image paths
           self.paths = [
               f.path for f in self.bc.index.iter_all_fileinfos()
               if f.path.endswith(('.jpg', '.jpeg', '.png'))
           ]

       def __len__(self):
           return len(self.paths)

       def __getitem__(self, idx):
           path = self.paths[idx]
           data = self.bc[path]
           image = Image.open(io.BytesIO(data)).convert('RGB')

           if self.transform:
               image = self.transform(image)

           # Extract label from path (e.g., "class_name/image.jpg")
           label = path.split('/')[0]
           return image, label

       def __del__(self):
           self.bc.close()

   # Usage
   from torchvision import transforms

   transform = transforms.Compose([
       transforms.Resize(256),
       transforms.CenterCrop(224),
       transforms.ToTensor(),
       transforms.Normalize(mean=[0.485, 0.456, 0.406],
                            std=[0.229, 0.224, 0.225]),
   ])

   dataset = BarecatImageDataset('imagenet.barecat', transform=transform)
   loader = DataLoader(dataset, batch_size=64, shuffle=True, num_workers=4)

Multi-Process DataLoader
~~~~~~~~~~~~~~~~~~~~~~~~

For multi-process data loading, use ``threadsafe=True``:

.. code-block:: python

   class BarecatImageDataset(Dataset):
       def __init__(self, archive_path, transform=None):
           # threadsafe=True gives each worker its own file handles
           self.bc = barecat.Barecat(archive_path, threadsafe=True)
           self.transform = transform
           self.paths = list(self.bc.index.iter_all_paths())

       # ... rest same as above

   # Now safe to use multiple workers
   loader = DataLoader(dataset, batch_size=64, shuffle=True, num_workers=8)

With DecodedView
~~~~~~~~~~~~~~~~

Use ``DecodedView`` to automatically encode/decode files based on extension:

.. code-block:: python

   from barecat import DecodedView

   with barecat.Barecat('dataset.barecat') as bc:
       dec = DecodedView(bc)
       # Returns numpy array (H, W, C) directly, not bytes
       image = dec['train/cat/image001.jpg']

       # For raw bytes, use bc directly:
       raw_data = bc['train/cat/image001.jpg']

Using with TensorFlow
---------------------

.. code-block:: python

   import tensorflow as tf
   import barecat

   def make_dataset(archive_path):
       bc = barecat.Barecat(archive_path, threadsafe=True)
       paths = list(bc.index.iter_all_paths())

       def generator():
           for path in paths:
               data = bc[path]
               label = path.split('/')[0]
               yield data, label

       dataset = tf.data.Dataset.from_generator(
           generator,
           output_signature=(
               tf.TensorSpec(shape=(), dtype=tf.string),
               tf.TensorSpec(shape=(), dtype=tf.string),
           )
       )

       def decode(data, label):
           image = tf.io.decode_jpeg(data, channels=3)
           image = tf.image.resize(image, [224, 224])
           image = tf.cast(image, tf.float32) / 255.0
           return image, label

       return dataset.map(decode, num_parallel_calls=tf.data.AUTOTUNE)

Organizing Large Datasets
-------------------------

Directory Structure
~~~~~~~~~~~~~~~~~~~

Barecat preserves directory structure. A typical organization:

.. code-block:: text

   dataset.barecat
   ├── train/
   │   ├── class_001/
   │   │   ├── img_0001.jpg
   │   │   └── img_0002.jpg
   │   └── class_002/
   │       └── ...
   ├── val/
   │   └── ...
   └── metadata.json

Sharding Strategy
~~~~~~~~~~~~~~~~~

For very large datasets (100M+ files), use multiple shards:

.. code-block:: bash

   # 10GB shards are easy to copy/transfer
   barecat create huge_dataset.barecat /data/ -s 10G

This creates:

.. code-block:: text

   huge_dataset.barecat-sqlite-index
   huge_dataset.barecat-shard-00000
   huge_dataset.barecat-shard-00001
   huge_dataset.barecat-shard-00002
   ...

All shards are used transparently - just open the archive normally.

Performance Tips
----------------

1. **Use SSDs** - Random access on HDDs is slow
2. **Set appropriate shard size** - 10-50GB is usually good
3. **Use multiple DataLoader workers** - With ``threadsafe=True``
4. **Pre-shuffle paths** - Load paths once, shuffle in memory each epoch
5. **Consider memory-mapping** - Barecat uses mmap for large reads

Verifying Integrity
-------------------

After creating a dataset, verify it:

.. code-block:: bash

   barecat verify dataset.barecat

This checks CRC32C checksums of all files.

Next Steps
----------

- :doc:`../howto/pytorch-dataloader` - Advanced PyTorch integration
- :doc:`../howto/convert-archives` - Convert existing datasets
- :doc:`../explanation/performance` - Performance characteristics
