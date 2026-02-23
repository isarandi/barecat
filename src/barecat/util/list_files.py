import sys
from ..core import barecat as barecat_


def main(barecat_path):
    with barecat_.Barecat(barecat_path) as bc_reader:
        for finfo in bc_reader.index.iter_all_fileinfos():
            print(finfo.path)


if __name__ == '__main__':
    main(sys.argv[1])
