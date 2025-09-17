import argparse
import os.path
import sqlite3

import barecat
import barecat_cython
from barecat.consumed_threadpool import ConsumedThreadPool
from barecat.progbar import progressbar


def main():
    parser = argparse.ArgumentParser(description='Migrate index database to new version')
    parser.add_argument('path', type=str, help='Path to the old barecat')
    parser.add_argument(
        '--workers', type=int, default=8, help='Number of workers for calculating crc32c'
    )

    args = parser.parse_args()
    dbase_path = args.path + '-sqlite-index'
    if not os.path.exists(dbase_path):
        raise FileNotFoundError(f'{dbase_path} does not exist!')

    os.rename(args.path + '-sqlite-index', args.path + '-sqlite-index.old')
    upgrade_schema(args.path)
    update_crc32c(args.path, workers=args.workers)


def upgrade_schema(path: str):
    with barecat.Index(path + '-sqlite-index', readonly=False) as index_out:
        c = index_out.cursor
        c.execute('COMMIT')
        c.execute('PRAGMA foreign_keys=OFF')
        c.execute('PRAGMA synchronous=OFF')
        c.execute('PRAGMA journal_mode=OFF')
        c.execute('PRAGMA recursive_triggers=ON')
        c.execute(f'ATTACH DATABASE "file:{path}-sqlite-index.old?mode=ro" AS source')
        print('Migrating dir metadata...')
        c.execute(
            """
            INSERT INTO dirs (path)
            SELECT path FROM source.directories
            WHERE path != ''
            """
        )
        print('Migrating file metadata...')
        c.execute(
            f"""
            INSERT INTO files (path, shard, offset, size)
            SELECT path, shard, offset, size
            FROM source.files
            """
        )

        c.execute('COMMIT')
        c.execute("DETACH DATABASE source")


def update_crc32c(path_out: str, workers=8):
    with (
        barecat_cython.BarecatMmapCython(path_out) as sh,
        barecat.Index(path_out + '-sqlite-index', readonly=False) as index,
    ):
        c = index.cursor
        c.execute('COMMIT')
        c.execute('PRAGMA synchronous=OFF')
        c.execute('PRAGMA journal_mode=OFF')
        index._triggers_enabled = False

        print('Calculating crc32c for all files to separate database...')
        path_newcrc_temp = f'{path_out}-sqlite-index-newcrc-temp'
        with ConsumedThreadPool(
            temp_crc_writer_main,
            main_args=(path_newcrc_temp,),
            max_workers=workers,
            queue_size=1024,
        ) as ctp:
            for fi in progressbar(
                index.iter_all_fileinfos(order=barecat.Order.ADDRESS), total=index.num_files
            ):
                ctp.submit(
                    sh.crc32c_from_address, userdata=fi.path, args=(fi.shard, fi.offset, fi.size)
                )

        print('Updating crc32c in the barecat index...')
        c.execute(f'ATTACH DATABASE "file:{path_newcrc_temp}?mode=ro" AS newdb')
        c.execute(
            """
            UPDATE files 
            SET crc32c=newdb.crc32c.crc32c
            FROM newdb.crc32c
            WHERE files.path=newdb.crc32c.path
            """
        )
        c.execute('COMMIT')
        c.execute('DETACH DATABASE newdb')

    os.remove(path_newcrc_temp)


def temp_crc_writer_main(dbpath, future_iter):
    with sqlite3.connect(dbpath) as conn:
        c = conn.cursor()
        c.execute('PRAGMA synchronous=OFF')
        c.execute('PRAGMA journal_mode=OFF')
        c.execute("CREATE TABLE IF NOT EXISTS crc32c (path TEXT PRIMARY KEY, crc32c INTEGER)")
        for future in future_iter:
            path = future.userdata
            crc32c = future.result()
            c.execute("INSERT INTO crc32c (path, crc32c) VALUES (?, ?)", (path, crc32c))


if __name__ == '__main__':
    main()
