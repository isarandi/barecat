"""Exceptions indicating various errors related to the use of Barecat archives"""


class BarecatError(Exception):
    """Base class for all exceptions in Barecat"""

    def __init__(self, message: str):
        super().__init__(message)


class FileExistsBarecatError(BarecatError):
    """Exception raised when trying to create a file that already exists

    Fills the role of FileExistsError, but deliberately does not inherit from it —
    catch this class (or BarecatError), not the builtin.

    Args:
        path: path to the file that already exists
    """

    def __init__(self, path: str):
        super().__init__(f'File already exists: {path}')


class FileNotFoundBarecatError(BarecatError):
    """Exception raised when trying to access a file that does not exist

    Fills the role of FileNotFoundError, but deliberately does not inherit from it —
    catch this class (or BarecatError), not the builtin.

    Args:
        path: path to the file that does not exist

    """

    def __init__(self, path: str):
        super().__init__(f'File not found: {path}')


class DirectoryNotEmptyBarecatError(BarecatError):
    """Exception raised when trying to delete a non-empty directory

    Args:
        path: path to the non-empty directory
    """

    def __init__(self, path: str):
        super().__init__(f'Directory not empty: {path}')


class IsADirectoryBarecatError(BarecatError):
    """Exception raised when trying to access a directory as a file.

    Args:
        path: path to the directory

    """

    def __init__(self, path: str):
        super().__init__(f'Is a directory: {path}')


class NotADirectoryBarecatError(BarecatError):
    """Exception raised when trying to access a file as a directory."""

    def __init__(self, message: str):
        super().__init__(message)


class BarecatIntegrityError(BarecatError):
    """Exception raised when the CRC32C checksum of a file does not match the expected checksum"""

    def __init__(self, message: str):
        super().__init__(message)


class PathEscapeBarecatError(BarecatError):
    """Exception raised when a path would escape the archive root (leading '..').

    Args:
        path: the offending path as given by the caller
    """

    def __init__(self, path: str):
        super().__init__(f'Path escapes the archive root: {path!r}')


class BarecatKeyError(BarecatError, KeyError):
    """Exception raised when a key (file path) is not found in the dict interface.

    Inherits from both BarecatError and KeyError so it can be caught by either.

    Args:
        path: path to the file that was not found
    """

    def __init__(self, path: str):
        super().__init__(path)


class NotEnoughSpaceBarecatError(BarecatError):
    """Exception raised when there is not enough space to write a file to the archive"""

    def __init__(self, message: str):
        super().__init__(message)


class FileTooLargeBarecatError(BarecatError, ValueError):
    """Exception raised when a file exceeds the shard_size_limit.

    Inherits from ValueError for backward compatibility with code that catches ValueError.
    """

    def __init__(self, size: int, shard_size_limit: int):
        super().__init__(
            f'File of size {size} is too large to fit into a shard '
            f'(shard_size_limit={shard_size_limit})'
        )
