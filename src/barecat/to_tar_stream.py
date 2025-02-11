import sys
from barecat.archive_formats import TarWriter
import barecat.core.barecat as barecat_

def tar_stream_from_glob(barecat_path, pattern):

    with (
        barecat_.Barecat(barecat_path) as bc_reader,
        TarWriter(fileobj=sys.stdout.buffer, mode='w|') as tar_writer,
    ):
        for finfo in bc_reader.index.iterglob_infos(
                pattern, recursive=True, only_files=True, include_hidden=True):
            with bc_reader.open(finfo.path) as fileobj:
                tar_writer.add(finfo, fileobj)

if __name__ == '__main__':
    tar_stream_from_glob(sys.argv[1], sys.argv[2])