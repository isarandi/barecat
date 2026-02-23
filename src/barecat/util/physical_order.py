import fcntl
import os
import struct

# FIEMAP ioctl constants (from linux/fiemap.h)
FS_IOC_FIEMAP = 0xC020660B  # _IOWR('f', 11, struct fiemap)

# struct fiemap (simplified, we only need first extent)
# uint64 fm_start
# uint64 fm_length
# uint32 fm_flags
# uint32 fm_mapped_extents
# uint32 fm_extent_count
# uint32 fm_reserved
# struct fiemap_extent[0]:
#   uint64 fe_logical
#   uint64 fe_physical
#   uint64 fe_length
#   uint64 fe_reserved64[2]
#   uint32 fe_flags
#   uint32 fe_reserved[3]

FIEMAP_SIZE = 32  # Base struct
FIEMAP_EXTENT_SIZE = 56  # One extent


def get_physical_offset(path: str) -> int:
    """
    Get the physical disk offset of a file's first block.
    Returns a large number if unable to determine.

    Uses FIEMAP ioctl - works on ext4, xfs, btrfs.
    """
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return 2**63 - 1

    try:
        # Build fiemap request
        # fm_start=0, fm_length=max, fm_flags=0, fm_extent_count=1
        buf = bytearray(FIEMAP_SIZE + FIEMAP_EXTENT_SIZE)
        struct.pack_into(
            '<QQIIII',
            buf,
            0,
            0,  # fm_start
            2**64 - 1,  # fm_length (max)
            0,  # fm_flags
            0,  # fm_mapped_extents (output)
            1,  # fm_extent_count
            0,  # fm_reserved
        )

        fcntl.ioctl(fd, FS_IOC_FIEMAP, buf)

        # Parse response
        fm_mapped_extents = struct.unpack_from('<I', buf, 16)[0]

        if fm_mapped_extents > 0:
            # fe_physical is at offset 8 in extent (after fe_logical)
            fe_physical = struct.unpack_from('<Q', buf, FIEMAP_SIZE + 8)[0]
            return fe_physical

        return 2**63 - 1

    except (OSError, IOError):
        return 2**63 - 1
    finally:
        os.close(fd)


def get_inode(path: str):
    try:
        return os.stat(path).st_ino
    except OSError:
        return 2**63 - 1
