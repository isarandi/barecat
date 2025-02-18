import sys
from barecat.archive_formats import TarWriter
import barecat.core.barecat as barecat_
import argparse
import sys


def main():
    parser = argparse.ArgumentParser(description='Convert a tar stream to a barecat file')
    parser.add_argument('barecat_file', type=str, help='path to the target barecat file')
    parser.add_argument("args", nargs=argparse.REMAINDER, help="Ordered --in and --ex arguments")

    args = parser.parse_args()
    patterns = parse_in_ex_patterns(args)

    with (
        barecat_.Barecat(args.barecat_file, readonly=True) as bc_reader,
        TarWriter(fileobj=sys.stdout.buffer, mode='w|') as tar_writer,
    ):
        for finfo in bc_reader.index.raw_iterglob_infos_incl_excl(
            patterns=patterns, only_files=True
        ):
            with bc_reader.open(finfo.path) as fileobj:
                tar_writer.add(finfo, fileobj)


def parse_in_ex_patterns(args):
    patterns = []
    i = 0
    while i < len(args.args):
        arg = args.args[i]

        if arg.startswith("--in="):
            patterns.append((True, arg.split("=", 1)[1]))

        elif arg.startswith("--ex="):
            patterns.append((False, arg.split("=", 1)[1]))

        elif arg == "--in":
            if i + 1 < len(args.args):
                patterns.append((True, args.args[i + 1]))
                i += 1

        elif arg == "--ex":
            if i + 1 < len(args.args):
                patterns.append((False, args.args[i + 1]))
                i += 1

        i += 1

    return patterns

if __name__ == '__main__':
    main()
