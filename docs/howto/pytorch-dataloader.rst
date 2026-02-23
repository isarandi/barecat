How to Use with PyTorch DataLoader
===================================

This guide covers best practices for using barecat with PyTorch's DataLoader.

Basic Setup
-----------

.. code-block:: python

   import torch
   from torch.utils.data import Dataset, DataLoader
   import barecat
   from PIL import Image
   import io

   class BarecatDataset(Dataset):
       def __init__(self, archive_path, transform=None):
           self.bc = barecat.Barecat(archive_path, threadsafe=True)
           self.transform = transform

           # Cache list of paths for fast indexing
           self.paths = [
               f.path for f in self.bc.index.iter_all_fileinfos()
           ]

       def __len__(self):
           return len(self.paths)

       def __getitem__(self, idx):
           path = self.paths[idx]
           data = self.bc[path]
           image = Image.open(io.BytesIO(data)).convert('RGB')

           if self.transform:
               image = self.transform(image)

           return image

       def close(self):
           self.bc.close()

Multi-Worker DataLoader
-----------------------

The key is ``threadsafe=True``, which gives each worker process its own
database connection and file handles:

.. code-block:: python

   dataset = BarecatDataset('data.barecat')
   loader = DataLoader(
       dataset,
       batch_size=64,
       shuffle=True,
       num_workers=8,  # Safe with threadsafe=True
       pin_memory=True,
       prefetch_factor=2,
   )

Without ``threadsafe=True``, workers would share file handles and corrupt
reads.

Efficient Label Extraction
--------------------------

From Path Structure
~~~~~~~~~~~~~~~~~~~

If labels are encoded in paths (e.g., ``class_name/image.jpg``):

.. code-block:: python

   class LabeledBarecatDataset(Dataset):
       def __init__(self, archive_path, transform=None):
           self.bc = barecat.Barecat(archive_path, threadsafe=True)
           self.transform = transform
           self.paths = []
           self.labels = []

           # Build class-to-index mapping
           classes = set()
           for f in self.bc.index.iter_all_fileinfos():
               class_name = f.path.split('/')[0]
               classes.add(class_name)

           self.class_to_idx = {c: i for i, c in enumerate(sorted(classes))}

           # Cache paths and labels
           for f in self.bc.index.iter_all_fileinfos():
               self.paths.append(f.path)
               class_name = f.path.split('/')[0]
               self.labels.append(self.class_to_idx[class_name])

       def __getitem__(self, idx):
           data = self.bc[self.paths[idx]]
           image = Image.open(io.BytesIO(data)).convert('RGB')
           if self.transform:
               image = self.transform(image)
           return image, self.labels[idx]

From Metadata File
~~~~~~~~~~~~~~~~~~

If labels are in a separate file:

.. code-block:: python

   import json

   class MetadataBarecatDataset(Dataset):
       def __init__(self, archive_path, transform=None):
           self.bc = barecat.Barecat(archive_path, threadsafe=True)
           self.transform = transform

           # Load metadata from archive
           metadata = json.loads(self.bc['metadata.json'].decode())
           self.paths = list(metadata.keys())
           self.labels = [metadata[p]['label'] for p in self.paths]

Using DecodedView
-----------------

Use ``DecodedView`` to automatically decode files based on extension:

.. code-block:: python

   from barecat import DecodedView

   class DecodedViewDataset(Dataset):
       def __init__(self, archive_path, transform=None):
           self.bc = barecat.Barecat(archive_path, threadsafe=True)
           self.dec = DecodedView(self.bc)
           self.transform = transform
           self.paths = list(self.bc.index.iter_all_paths())

       def __getitem__(self, idx):
           # Returns numpy array (H, W, C) for .jpg/.png
           image = self.dec[self.paths[idx]]
           if self.transform:
               image = self.transform(image)
           return image

       def close(self):
           self.bc.close()

Supported codecs:

- ``.jpg``, ``.jpeg``, ``.png``, ``.gif``, ``.bmp``, ``.webp`` → numpy array (via imageio)
- ``.npy`` → numpy array
- ``.npz`` → dict of numpy arrays
- ``.pkl``, ``.pickle`` → unpickled object
- ``.json`` → parsed JSON/dict
- ``.msgpack`` → msgpack-decoded object
- ``.gz``, ``.xz``, ``.bz2`` → compression (stackable, e.g., ``.json.gz``)

Memory-Efficient Iteration
--------------------------

For very large datasets, avoid loading all paths into memory:

.. code-block:: python

   class StreamingBarecatDataset(torch.utils.data.IterableDataset):
       def __init__(self, archive_path, transform=None):
           self.archive_path = archive_path
           self.transform = transform

       def __iter__(self):
           # Each worker gets its own barecat instance
           bc = barecat.Barecat(self.archive_path)

           worker_info = torch.utils.data.get_worker_info()
           if worker_info is not None:
               # Split files across workers
               all_paths = list(bc.index.iter_all_paths())
               per_worker = len(all_paths) // worker_info.num_workers
               start = worker_info.id * per_worker
               end = start + per_worker if worker_info.id < worker_info.num_workers - 1 else len(all_paths)
               paths = all_paths[start:end]
           else:
               paths = list(bc.index.iter_all_paths())

           for path in paths:
               data = bc[path]
               image = Image.open(io.BytesIO(data)).convert('RGB')
               if self.transform:
                   image = self.transform(image)
               yield image

           bc.close()

Distributed Training
--------------------

For multi-GPU training with DistributedDataParallel:

.. code-block:: python

   from torch.utils.data.distributed import DistributedSampler

   dataset = BarecatDataset('data.barecat')
   sampler = DistributedSampler(dataset)
   loader = DataLoader(
       dataset,
       batch_size=64,
       sampler=sampler,
       num_workers=4,
       pin_memory=True,
   )

   # In training loop
   for epoch in range(num_epochs):
       sampler.set_epoch(epoch)  # Ensures different shuffling each epoch
       for batch in loader:
           ...

Performance Tips
----------------

1. **Use threadsafe=True** - Essential for multi-worker loading

2. **Cache paths** - Load paths once in ``__init__``, not in ``__getitem__``

3. **Use pin_memory=True** - Speeds up GPU transfer

4. **Tune num_workers** - Usually 4-8 workers per GPU

5. **Use prefetch_factor** - Default is 2, increase for slow I/O

6. **Consider persistent_workers** - Avoids worker restart overhead:

   .. code-block:: python

      loader = DataLoader(
          dataset,
          num_workers=4,
          persistent_workers=True,  # Workers stay alive between epochs
      )

7. **Profile I/O** - Use ``torch.profiler`` to identify bottlenecks

Complete Example
----------------

.. code-block:: python

   import torch
   from torch.utils.data import Dataset, DataLoader
   from torchvision import transforms
   import barecat
   from PIL import Image
   import io

   class ImageNetBarecatDataset(Dataset):
       def __init__(self, archive_path, split='train', transform=None):
           self.bc = barecat.Barecat(archive_path, threadsafe=True)
           self.transform = transform

           # Filter by split
           self.paths = []
           self.labels = []
           classes = sorted(self.bc.listdir(split))
           self.class_to_idx = {c: i for i, c in enumerate(classes)}

           for cls in classes:
               cls_path = f"{split}/{cls}"
               for fname in self.bc.listdir(cls_path):
                   self.paths.append(f"{cls_path}/{fname}")
                   self.labels.append(self.class_to_idx[cls])

       def __len__(self):
           return len(self.paths)

       def __getitem__(self, idx):
           data = self.bc[self.paths[idx]]
           image = Image.open(io.BytesIO(data)).convert('RGB')
           if self.transform:
               image = self.transform(image)
           return image, self.labels[idx]

       def close(self):
           self.bc.close()

   # Usage
   transform = transforms.Compose([
       transforms.RandomResizedCrop(224),
       transforms.RandomHorizontalFlip(),
       transforms.ToTensor(),
       transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
   ])

   train_dataset = ImageNetBarecatDataset(
       'imagenet.barecat',
       split='train',
       transform=transform,
   )

   train_loader = DataLoader(
       train_dataset,
       batch_size=256,
       shuffle=True,
       num_workers=8,
       pin_memory=True,
       persistent_workers=True,
   )

   # Training loop
   for epoch in range(90):
       for images, labels in train_loader:
           images = images.cuda(non_blocking=True)
           labels = labels.cuda(non_blocking=True)
           # ... training step

See Also
--------

- :doc:`../tutorials/image-datasets` - Tutorial on image datasets
- :doc:`../explanation/performance` - Performance characteristics
