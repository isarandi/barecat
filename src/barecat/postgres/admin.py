"""Schema setup and export utilities for the PostgreSQL ingestion backend."""

import psycopg

from ..core.types import SHARD_SIZE_UNLIMITED
from ..util.misc import parse_size

SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS files (
    path TEXT PRIMARY KEY,
    shard INTEGER DEFAULT NULL,
    "offset" BIGINT DEFAULT NULL,
    size BIGINT NOT NULL DEFAULT 0,
    crc32c BIGINT DEFAULT NULL,
    mode INTEGER DEFAULT NULL,
    uid INTEGER DEFAULT NULL,
    gid INTEGER DEFAULT NULL,
    mtime_ns BIGINT DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS shard_state (
    current_shard INTEGER NOT NULL DEFAULT 0,
    current_offset BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value_text TEXT DEFAULT NULL,
    value_int BIGINT DEFAULT NULL
);

CREATE OR REPLACE FUNCTION alloc_space() RETURNS TRIGGER AS $$
DECLARE
    s INTEGER;
    o BIGINT;
    lim BIGINT;
BEGIN
    SELECT current_shard, current_offset INTO s, o FROM shard_state FOR UPDATE;
    SELECT value_int INTO lim FROM config WHERE key = 'shard_size_limit';

    IF lim IS NOT NULL AND lim < 9223372036854775807 THEN
        IF NEW.size > lim THEN
            RAISE EXCEPTION 'File size % exceeds shard size limit %', NEW.size, lim;
        END IF;
        IF o + NEW.size > lim THEN
            s := s + 1;
            o := 0;
        END IF;
    END IF;

    NEW.shard := s;
    NEW."offset" := o;

    UPDATE shard_state SET current_shard = s, current_offset = o + NEW.size;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS alloc_space_trigger ON files;
CREATE TRIGGER alloc_space_trigger
    BEFORE INSERT ON files
    FOR EACH ROW
    WHEN (NEW.shard IS NULL)
    EXECUTE FUNCTION alloc_space();

INSERT INTO shard_state (current_shard, current_offset)
    SELECT 0, 0 WHERE NOT EXISTS (SELECT 1 FROM shard_state);
"""


def setup_schema(conninfo, shard_size_limit=None):
    """Create PostgreSQL schema for barecat parallel ingestion.

    Idempotent — safe to call multiple times.

    Args:
        conninfo: PostgreSQL connection string (e.g., 'postgresql:///mydb').
        shard_size_limit: Max shard size in bytes, or a string like '4G'.
            If None, unlimited.
    """
    if isinstance(shard_size_limit, str):
        shard_size_limit = parse_size(shard_size_limit)
    if shard_size_limit is None:
        shard_size_limit = SHARD_SIZE_UNLIMITED

    with psycopg.connect(conninfo) as conn:
        conn.execute(SCHEMA_SQL)
        conn.execute(
            "INSERT INTO config (key, value_int) VALUES ('shard_size_limit', %s) "
            'ON CONFLICT (key) DO UPDATE SET value_int = EXCLUDED.value_int',
            (shard_size_limit,),
        )
        conn.commit()


def export_to_sqlite(conninfo, archive_path, verify='length', overwrite=False):
    """Export PostgreSQL index to barecat SQLite format.

    Creates the SQLite index file at archive_path, populates it with all files
    from the PostgreSQL database, then builds directory entries and tree statistics.

    This is the final step after parallel ingestion. The resulting SQLite index
    is a fully functional barecat archive index, ready for reading.

    Workers commit metadata before writing bytes, so a worker killed mid-batch
    can leave rows whose shard bytes are absent or partial. The ``verify`` pass
    is what turns that accepted crash window into a clean final archive:
    offending rows are reported and excluded from the export.

    Args:
        conninfo: PostgreSQL connection string.
        archive_path: Path to the barecat archive (the SQLite index will be
            created here, e.g., '/shared/archive.barecat').
        verify: ``'length'`` (default) drops rows extending past the physical
            end of their shard file — catching a dead worker's unwritten tail
            batch wholesale, without reading data. ``'full'`` additionally
            re-reads every span and compares crc32c (a complete data scan; use
            for final production archives). ``'none'`` skips verification.
        overwrite: If True, an existing index file at archive_path is replaced
            (shard files are never touched — they hold the exported data).
            If False (default), an existing index is an error.

    Returns:
        List of (path, reason) tuples for rows excluded by verification.
    """
    import logging
    import os

    import crc32c as crc32c_lib

    from ..core.index import Index
    from ..core.types import BarecatFileInfo

    logger = logging.getLogger(__name__)

    if verify not in ('none', 'length', 'full'):
        raise ValueError(f"verify must be 'none', 'length' or 'full', got {verify!r}")
    if os.path.exists(archive_path):
        if not overwrite:
            raise FileExistsError(
                f'{archive_path} already exists. Pass overwrite=True to replace the '
                f'index (shard files are left untouched).'
            )
        os.remove(archive_path)

    bad_rows = []
    shard_sizes = {}
    shard_fds = {}

    def physical_size(shard):
        if shard not in shard_sizes:
            path = f'{archive_path}-shard-{shard:05d}'
            try:
                shard_sizes[shard] = os.path.getsize(path)
            except FileNotFoundError:
                shard_sizes[shard] = 0
        return shard_sizes[shard]

    def crc_matches(shard, offset, size, expected):
        if shard not in shard_fds:
            shard_fds[shard] = os.open(f'{archive_path}-shard-{shard:05d}', os.O_RDONLY)
        crc = 0
        remaining = size
        pos = offset
        while remaining > 0:
            chunk = os.pread(shard_fds[shard], min(remaining, 8 * 1024 * 1024), pos)
            if not chunk:
                return False
            crc = crc32c_lib.crc32c(chunk, crc)
            pos += len(chunk)
            remaining -= len(chunk)
        return crc == expected

    try:
        with psycopg.connect(conninfo) as pg_conn:
            row = pg_conn.execute(
                "SELECT value_int FROM config WHERE key = 'shard_size_limit'"
            ).fetchone()
            shard_size_limit = row[0] if row else SHARD_SIZE_UNLIMITED

            with Index(archive_path, readonly=False, shard_size_limit=shard_size_limit) as index:
                with index.bulk_mode(drop_indexes=True):
                    with pg_conn.cursor(name='export_cursor') as cur:
                        cur.execute(
                            'SELECT path, shard, "offset", size, crc32c, '
                            'mode, uid, gid, mtime_ns '
                            'FROM files ORDER BY shard, "offset"'
                        )
                        batch = []
                        while rows := cur.fetchmany(10000):
                            for row in rows:
                                path, shard, offset, size, crc = (
                                    row[0],
                                    row[1],
                                    row[2],
                                    row[3],
                                    row[4],
                                )
                                if verify != 'none' and offset + size > physical_size(shard):
                                    bad_rows.append((path, 'extends past physical shard end'))
                                    continue
                                if (
                                    verify == 'full'
                                    and size > 0
                                    and not crc_matches(shard, offset, size, crc)
                                ):
                                    bad_rows.append((path, 'crc32c mismatch'))
                                    continue
                                finfo = BarecatFileInfo.__new__(BarecatFileInfo)
                                finfo._path = path
                                finfo.shard = shard
                                finfo.offset = offset
                                finfo.size = size
                                finfo.crc32c = crc
                                finfo.mode = row[5]
                                finfo.uid = row[6]
                                finfo.gid = row[7]
                                finfo.mtime_ns = row[8]
                                batch.append(finfo)

                            index.add_files(batch)
                            batch.clear()
    finally:
        for fd in shard_fds.values():
            os.close(fd)

    if bad_rows:
        logger.warning(
            'Export excluded %d row(s) whose shard bytes are missing or corrupt '
            '(likely from workers killed mid-batch):',
            len(bad_rows),
        )
        for path, reason in bad_rows[:20]:
            logger.warning('  %s: %s', path, reason)
        if len(bad_rows) > 20:
            logger.warning('  ... and %d more', len(bad_rows) - 20)
    return bad_rows


def import_from_sqlite(conninfo, archive_path):
    """Import an existing barecat SQLite index into PostgreSQL.

    After importing, new files can be added via parallel workers. The
    alloc_space trigger is skipped for imported rows (they have explicit
    shard/offset), and shard_state is set to the end of the last shard.

    Args:
        conninfo: PostgreSQL connection string.
        archive_path: Path to the existing barecat archive (SQLite index).
    """
    from ..core.index import Index

    with Index(archive_path, readonly=True) as index:
        with psycopg.connect(conninfo) as pg_conn:
            with pg_conn.cursor() as cur:
                with cur.copy(
                    'COPY files (path, shard, "offset", size, crc32c, '
                    'mode, uid, gid, mtime_ns) FROM STDIN'
                ) as copy:
                    for finfo in index.iter_all_fileinfos():
                        copy.write_row(
                            (
                                finfo.path,
                                finfo.shard,
                                finfo.offset,
                                finfo.size,
                                finfo.crc32c,
                                finfo.mode,
                                finfo.uid,
                                finfo.gid,
                                finfo.mtime_ns,
                            )
                        )

            # Set shard_state to the end of the last shard
            row = pg_conn.execute(
                'SELECT shard, "offset" + size FROM files '
                'ORDER BY shard DESC, "offset" DESC LIMIT 1'
            ).fetchone()
            if row:
                pg_conn.execute(
                    'UPDATE shard_state SET current_shard = %s, current_offset = %s',
                    (row[0], row[1]),
                )
            pg_conn.commit()


def get_stats(conninfo):
    """Get current ingestion statistics from PostgreSQL.

    Args:
        conninfo: PostgreSQL connection string.

    Returns:
        Dict with 'num_files', 'total_size', 'num_shards'.
    """
    with psycopg.connect(conninfo) as conn:
        row = conn.execute('SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files').fetchone()
        shard_row = conn.execute('SELECT current_shard FROM shard_state').fetchone()
        return {
            'num_files': row[0],
            'total_size': row[1],
            'num_shards': shard_row[0] + 1 if shard_row else 0,
        }
