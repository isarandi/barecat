"""File opening utilities."""

import os


def open_(path, mode, *args, **kwargs):
    """Open a file, supporting 'ax' modes which Python's builtin open() cannot do.

    'ax' variants = exclusive create + append (+ optional read, binary):
    - 'x' = fail if file exists (O_EXCL)
    - 'a' = kernel-enforced append: ALL writes go to end regardless of seek position
    - '+' = read+write (O_RDWR) instead of write-only (O_WRONLY)
    - 'b' = binary mode

    WHY THE 'a' MATTERS:
    O_APPEND is enforced by the kernel, not Python. Even if code does seek(0) then
    write(), the kernel intercepts the syscall and redirects the write to EOF.
    This is OS-level protection against data corruption from seek bugs.

    This is how barecat's append_only mode is enforced at the file level:
    - append_only=True  -> shard_mode_new = 'ax+b' (kernel enforces writes go to end)
    - append_only=False -> shard_mode_new = 'x+b'  (allows seek+overwrite for defrag)

    DO NOT "simplify" to 'x' modes - the 'a' provides kernel-level safety guarantees.
    """
    mode_set = set(mode)
    if 'a' in mode_set and 'x' in mode_set:
        flags = os.O_CREAT | os.O_EXCL | os.O_APPEND
        flags |= os.O_RDWR if '+' in mode_set else os.O_WRONLY
        fdopen_mode = 'a' + ('+' if '+' in mode_set else '') + ('b' if 'b' in mode_set else '')
        fd = os.open(path, flags)
        return os.fdopen(fd, fdopen_mode, *args, **kwargs)
    return open(path, mode, *args, **kwargs)


def reopen(file, mode):
    """Close and reopen a file with a different mode."""
    if file.mode == mode:
        return file
    file.close()
    return open_(file.name, mode)
