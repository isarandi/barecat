import argparse
import os
import sqlite3
import warnings

import barecat
import barecat_cython
from ..core.types import SCHEMA_VERSION_MAJOR, SCHEMA_VERSION_MINOR
from ..core.paths import resolve_index_path
from ..util.consumed_threadpool import ConsumedThreadPool
from ..util.progbar import progressbar


def main():
    warnings.warn(
        "barecat-upgrade-database is deprecated, use 'barecat upgrade' instead",
        DeprecationWarning, stacklevel=2)
    parser = argparse.ArgumentParser(description='Migrate index database to new version')
    parser.add_argument('path', type=str, help='Path to the old barecat')
    parser.add_argument(
        '--workers', type=int, default=8, help='Number of workers for calculating crc32c'
    )

    args = parser.parse_args()
    upgrade(args.path, workers=args.workers)


def upgrade(path: str, workers: int = 8, preserve_backup: bool = True):
    """Upgrade a barecat archive to the current schema version.

    Detects the current schema version and applies the appropriate upgrade path.

    Args:
        path: Path to the barecat archive
        workers: Number of workers for calculating crc32c
        preserve_backup: If True, keep the .old backup file after upgrade
    """
    dbase_path = resolve_index_path(path)
    if not os.path.exists(dbase_path):
        raise FileNotFoundError(f'{dbase_path} does not exist!')

    db_major, db_minor = get_schema_version(dbase_path)

    if db_major == SCHEMA_VERSION_MAJOR and db_minor == SCHEMA_VERSION_MINOR:
        print(f'Database is already at version {db_major}.{db_minor}, nothing to do.')
        return

    if db_major > SCHEMA_VERSION_MAJOR:
        raise ValueError(
            f'Database version {db_major}.{db_minor} is newer than supported '
            f'{SCHEMA_VERSION_MAJOR}.{SCHEMA_VERSION_MINOR}. Upgrade barecat first.'
        )

    backup_path = dbase_path + '.old'

    if db_major < SCHEMA_VERSION_MAJOR:
        # Pre-versioned or old major version: full migration
        print(f'Upgrading from pre-versioned format to {SCHEMA_VERSION_MAJOR}.{SCHEMA_VERSION_MINOR}...')
        os.rename(dbase_path, backup_path)
        upgrade_from_unversioned(path)
        update_crc32c(path, workers=workers)
    elif db_minor < SCHEMA_VERSION_MINOR:
        # Same major, older minor: incremental upgrade
        print(f'Upgrading from {db_major}.{db_minor} to {SCHEMA_VERSION_MAJOR}.{SCHEMA_VERSION_MINOR}...')
        if db_minor in (1, 2):
            upgrade_0_x_to_0_3(path)

    if not preserve_backup and os.path.exists(backup_path):
        print(f'Removing backup: {backup_path}')
        os.remove(backup_path)

    print('Upgrade complete.')


def get_schema_version(dbase_path: str) -> tuple:
    """Get the schema version from a barecat database."""
    try:
        with sqlite3.connect(f'file:{dbase_path}?mode=ro', uri=True) as conn:
            c = conn.cursor()
            c.execute("SELECT value_int FROM config WHERE key='schema_version_major'")
            row = c.fetchone()
            if row is None:
                return (SCHEMA_VERSION_MAJOR - 1, 0)
            db_major = int(row[0])
            c.execute("SELECT value_int FROM config WHERE key='schema_version_minor'")
            row = c.fetchone()
            db_minor = int(row[0]) if row else 0
            return (db_major, db_minor)
    except sqlite3.OperationalError:
        # No config table
        return (SCHEMA_VERSION_MAJOR - 1, 0)


def upgrade_0_x_to_0_3(path: str):
    """Upgrade from schema 0.1 or 0.2 to 0.3 (new schema with rowid, trigger fixes)."""
    from .upgrade_database2 import upgrade_schema

    dbase_path = resolve_index_path(path)
    temp_path = path + '-temp-upgrade'

    print('Creating new database with updated schema...')
    upgrade_schema(path, temp_path)

    print('Replacing old database...')
    os.rename(dbase_path, dbase_path + '.old')
    # New format: temp_path IS the index (upgrade_schema uses new format)
    os.rename(temp_path, dbase_path)

    print('Rebuilding directory tree statistics...')
    with barecat.Index(dbase_path, readonly=False) as index:
        index.update_treestats()


def upgrade_from_unversioned(path: str):
    dbase_path = resolve_index_path(path)
    with barecat.Index(dbase_path, readonly=False) as index_out:
        c = index_out.cursor
        c.execute('COMMIT')
        c.execute('PRAGMA foreign_keys=OFF')
        c.execute(f'ATTACH DATABASE "file:{dbase_path}.old?mode=ro" AS source')
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
    dbase_path = resolve_index_path(path_out)
    with barecat_cython.BarecatMmapCython(path_out) as sh, \
            barecat.Index(dbase_path, readonly=False) as index:
        c = index.cursor
        c.execute('COMMIT')
        index._triggers_enabled = False

        print('Calculating crc32c for all files to separate database...')
        path_newcrc_temp = f'{dbase_path}-newcrc-temp'
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
        c.execute("CREATE TABLE IF NOT EXISTS crc32c (path TEXT PRIMARY KEY, crc32c INTEGER)")
        for future in future_iter:
            path = future.userdata
            crc32c = future.result()
            c.execute("INSERT INTO crc32c (path, crc32c) VALUES (?, ?)", (path, crc32c))


if __name__ == '__main__':
    main()
