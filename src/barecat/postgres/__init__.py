"""PostgreSQL backend for parallel ingestion into barecat archives (experimental).

The API and CLI may change; the produced archives are standard barecat archives.

Requires: pip install barecat[postgres]

Usage::

    # 1. Setup (once, before workers start)
    from barecat.postgres import setup_schema
    setup_schema('postgresql:///mydb', shard_size_limit='4G')

    # 2. Each worker
    from barecat.postgres import BarecatPostgresWriter
    with BarecatPostgresWriter('postgresql:///mydb', '/shared/archive.barecat') as w:
        w['train/img001.jpg'] = jpeg_bytes
        w.add_from_file('train/img002.jpg', '/data/img002.jpg')

    # 3. Finalize (once, after all workers done)
    from barecat.postgres import export_to_sqlite
    export_to_sqlite('postgresql:///mydb', '/shared/archive.barecat')
"""

from .writer import BarecatPostgresWriter
from .admin import setup_schema, export_to_sqlite, import_from_sqlite, get_stats

__all__ = [
    'BarecatPostgresWriter',
    'setup_schema',
    'export_to_sqlite',
    'import_from_sqlite',
    'get_stats',
]
