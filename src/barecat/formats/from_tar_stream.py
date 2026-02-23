import argparse
import sys
import tarfile

from ..core import barecat as barecat_
from ..core.types import BarecatDirInfo, BarecatFileInfo


def main():
    parser = argparse.ArgumentParser(description='Convert a tar stream to a barecat file')
    parser.add_argument('barecat_file', type=str, help='path to the target barecat file')
    parser.add_argument(
        '--shard-size-limit',
        type=str,
        default=None,
        help='maximum size of a shard in bytes (if not specified, '
        'all files will be concatenated into a single shard)',
    )
    parser.add_argument('--overwrite', action='store_true', help='overwrite existing files')
    args = parser.parse_args()

    with barecat_.Barecat(
        args.barecat_file,
        shard_size_limit=args.shard_size_limit,
        readonly=False,
        overwrite=args.overwrite,
    ) as writer:
        with tarfile.open(fileobj=sys.stdin.buffer, mode='r|') as tar:
            for member in tar:
                if member.isdir():
                    dinfo = BarecatDirInfo(
                        path=member.name,
                        mode=member.mode,
                        uid=member.uid,
                        gid=member.gid,
                        mtime_ns=member.mtime * 1_000_000_000,
                    )
                    writer.add(dinfo, dir_exist_ok=True)
                if member.isfile():
                    finfo = BarecatFileInfo(
                        path=member.name,
                        size=member.size,
                        mode=member.mode,
                        uid=member.uid,
                        gid=member.gid,
                        mtime_ns=member.mtime * 1_000_000_000,
                    )
                    with tar.extractfile(member) as file_in_tar:
                        writer.add(finfo, fileobj=file_in_tar)


if __name__ == '__main__':
    main()
