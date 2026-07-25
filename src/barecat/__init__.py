"""Barecat is a fast random-access, mountable archive format for storing and accessing many small
files."""

try:
    from ._version import version as __version__
except ImportError:
    __version__ = '0.0.0.dev0'

# Core classes
from .core.barecat import Barecat
from .core.index import Index
from .core.sharder import Sharder

# Data types
from .core.types import (
    BarecatDirInfo,
    BarecatEntryInfo,
    BarecatFileInfo,
    Order,
    SHARD_SIZE_UNLIMITED,
)

# File objects
from .io.fileobj import (
    BarecatFileObject,
    BarecatReadOnlyFileObject,
    BarecatReadWriteFileObject,
)

# Codec view
from .io.codecs import DecodedView

# Exceptions
from .exceptions import (
    BarecatError,
    BarecatIntegrityError,
    BarecatKeyError,
    PathEscapeBarecatError,
    DirectoryNotEmptyBarecatError,
    FileExistsBarecatError,
    FileNotFoundBarecatError,
    FileTooLargeBarecatError,
    IsADirectoryBarecatError,
    NotADirectoryBarecatError,
    NotEnoughSpaceBarecatError,
)

# CLI utilities (for programmatic use)
from .cli.impl import (
    archive2barecat,
    barecat2archive,
    extract,
    merge,
    merge_symlink,
    read_index,
    write_index,
)
from .cli.completions import get_completion_script

# Archive utilities
from .util.misc import exists, find_shard_paths, remove

# Path resolution (public so consumers match this package's archive-resolution
# behavior instead of re-implementing it and drifting)
from .core.paths import normalize_path, resolve_index_path

# Convenience API
from ._api import get_cached_reader, open

__all__ = [
    # Version
    '__version__',
    # Core classes
    'Barecat',
    'Index',
    'Sharder',
    # Data types
    'BarecatDirInfo',
    'BarecatEntryInfo',
    'BarecatFileInfo',
    'Order',
    'SHARD_SIZE_UNLIMITED',
    # File objects
    'BarecatFileObject',
    'BarecatReadOnlyFileObject',
    'BarecatReadWriteFileObject',
    # Codec view
    'DecodedView',
    # Exceptions
    'BarecatError',
    'BarecatIntegrityError',
    'BarecatKeyError',
    'PathEscapeBarecatError',
    'DirectoryNotEmptyBarecatError',
    'FileExistsBarecatError',
    'FileNotFoundBarecatError',
    'FileTooLargeBarecatError',
    'IsADirectoryBarecatError',
    'NotADirectoryBarecatError',
    'NotEnoughSpaceBarecatError',
    # CLI utilities
    'archive2barecat',
    'barecat2archive',
    'extract',
    'merge',
    'merge_symlink',
    'read_index',
    'write_index',
    'get_completion_script',
    # Archive utilities
    'exists',
    'find_shard_paths',
    'remove',
    # Path resolution
    'normalize_path',
    'resolve_index_path',
    # Convenience API
    'get_cached_reader',
    'open',
]
