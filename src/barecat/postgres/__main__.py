"""CLI for barecat PostgreSQL parallel ingestion.

Usage:
    python -m barecat.postgres init DBNAME [-H HOST[:PORT]] [-s SHARD_SIZE]
    python -m barecat.postgres export DBNAME ARCHIVE [-H HOST[:PORT]] [--verify MODE] [-f]
    python -m barecat.postgres import DBNAME ARCHIVE [-H HOST[:PORT]]
    python -m barecat.postgres stats DBNAME [-H HOST[:PORT]]

The coordinator is typically not on this machine (e.g. a dedicated SLURM job
publishing its endpoint); every command therefore takes the server location via
-H/--host, a full --conninfo, or the BARECAT_PG environment variable. With none
of these, the local default socket is used — beware that this silently talks to
whichever PostgreSQL instance owns that socket.
"""

import argparse
import os

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from .admin import setup_schema, export_to_sqlite, import_from_sqlite, get_stats


def resolve_conninfo(args, dbname=None):
    """Build a conninfo from --conninfo / --host / $BARECAT_PG plus the dbname.

    ``--host`` also accepts ``host:port`` and the path of an endpoint file as
    written by a coordinator job (a file containing ``host:port``).
    """
    if args.conninfo:
        base = args.conninfo
    elif args.host:
        host = args.host
        if os.path.isfile(host):
            host = open(host).read().strip()
        if ':' in host and not host.startswith('/'):
            host, _, port = host.rpartition(':')
            base = make_conninfo(host=host, port=int(port))
        else:
            base = make_conninfo(host=host)
    elif os.environ.get('BARECAT_PG'):
        base = os.environ['BARECAT_PG']
    else:
        base = ''
    return make_conninfo(base, dbname=dbname) if dbname else base


def create_database(args):
    """Create the database, connecting to the server's default 'postgres' DB."""
    with psycopg.connect(resolve_conninfo(args, 'postgres'), autocommit=True) as conn:
        try:
            conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(args.dbname)))
        except psycopg.errors.DuplicateDatabase:
            print(f'Database {args.dbname!r} already exists, setting up schema.')
        else:
            print(f'Created database {args.dbname!r}.')


def cmd_init(args):
    create_database(args)
    setup_schema(resolve_conninfo(args, args.dbname), shard_size_limit=args.shard_size)
    print(f'Schema initialized in {args.dbname!r}.')


def cmd_import(args):
    print(f'Importing {args.archive} -> {args.dbname!r}')
    import_from_sqlite(resolve_conninfo(args, args.dbname), args.archive)
    print('Done.')


def cmd_export(args):
    print(f'Exporting {args.dbname!r} -> {args.archive}')
    bad = export_to_sqlite(
        resolve_conninfo(args, args.dbname),
        args.archive,
        verify=args.verify,
        overwrite=args.force,
    )
    if bad:
        print(f'WARNING: {len(bad)} row(s) excluded by verification (see log).')
    print('Done.')


def cmd_stats(args):
    stats = get_stats(resolve_conninfo(args, args.dbname))
    print(f'Files:  {stats["num_files"]}')
    print(f'Size:   {stats["total_size"]} bytes')
    print(f'Shards: {stats["num_shards"]}')


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog='python -m barecat.postgres',
        description='Barecat PostgreSQL parallel ingestion',
    )
    sub = parser.add_subparsers(dest='command', required=True)

    def add_common(p):
        p.add_argument('dbname', help='PostgreSQL database name')
        p.add_argument(
            '-H',
            '--host',
            default=None,
            help='Server host, host:port, or path of an endpoint file containing host:port',
        )
        p.add_argument(
            '--conninfo',
            default=None,
            help='Full libpq conninfo (overrides --host); dbname is added from the positional',
        )

    p_init = sub.add_parser('init', help='Create database and initialize schema')
    add_common(p_init)
    p_init.add_argument('-s', '--shard-size', default=None, help="Shard size limit (e.g., '4G')")
    p_init.set_defaults(func=cmd_init)

    p_export = sub.add_parser('export', help='Export PG index to barecat SQLite archive')
    add_common(p_export)
    p_export.add_argument('archive', help='Output barecat archive path')
    p_export.add_argument(
        '--verify',
        choices=['none', 'length', 'full'],
        default='length',
        help="Verification of shard bytes: 'length' (default) drops rows past the "
        "physical shard end; 'full' additionally re-reads all data and checks crc32c",
    )
    p_export.add_argument(
        '-f', '--force', action='store_true', help='Replace an existing index file'
    )
    p_export.set_defaults(func=cmd_export)

    p_import = sub.add_parser('import', help='Import existing barecat archive into PG')
    add_common(p_import)
    p_import.add_argument('archive', help='Existing barecat archive path')
    p_import.set_defaults(func=cmd_import)

    p_stats = sub.add_parser('stats', help='Show ingestion statistics')
    add_common(p_stats)
    p_stats.set_defaults(func=cmd_stats)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == '__main__':
    main()
