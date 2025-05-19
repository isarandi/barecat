import argparse
import os.path

import barecat


def main():
    parser = argparse.ArgumentParser(description='Migrate index database to new version')
    parser.add_argument('path_in', type=str, help='Path to the old barecat')
    parser.add_argument('path_out', type=str, help='Path to the new barecat')

    args = parser.parse_args()
    upgrade_schema(args.path_in, args.path_out)


def upgrade_schema(path_in: str, path_out: str):
    if os.path.exists(path_out + '-sqlite-index'):
        raise FileExistsError(f'Output path {path_out}-sqlite-index already exists')
    with barecat.Index(path_out + '-sqlite-index', readonly=False) as index_out:
        c = index_out.cursor
        c.execute('COMMIT')
        c.execute('PRAGMA foreign_keys=OFF')
        c.execute('PRAGMA synchronous=OFF')
        c.execute('PRAGMA journal_mode=OFF')
        c.execute(f'ATTACH DATABASE "file:{path_in}-sqlite-index?mode=ro" AS source')

        with index_out.no_triggers(), index_out.no_foreign_keys():
            print('Migrating dir metadata...')
            c.execute(
                """
                INSERT INTO dirs (
                    path, num_subdirs, num_files, num_files_tree, size_tree, mode, uid, gid,
                    mtime_ns)
                SELECT path, num_subdirs, num_files, num_files_tree, size_tree, mode, uid,
                    gid, mtime_ns
                FROM source.dirs
                WHERE path != ''
                """
            )
            c.execute("""
                UPDATE dirs
                SET (num_subdirs, num_files, num_files_tree, size_tree, mode, uid, gid, mtime_ns) =
                    (SELECT num_subdirs, num_files, num_files_tree, size_tree, mode, uid, gid, mtime_ns 
                     FROM source.dirs WHERE path = '')
                WHERE path = ''
            """)


            print('Migrating file metadata...')
            c.execute(
                f"""
                INSERT INTO files (
                    path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns) 
                SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
                FROM source.files
                """
            )

            c.execute(
                f"""
                INSERT OR REPLACE INTO config (key, value_text, value_int)
                SELECT key, value_text, value_int
                FROM source.config
                """
            )

            index_out.conn.commit()
            c.execute("DETACH DATABASE source")
            index_out.optimize()


if __name__ == '__main__':
    main()
