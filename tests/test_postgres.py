"""Tests for the PostgreSQL parallel-ingestion backend.

Spins up a throwaway PostgreSQL instance (initdb + pg_ctl on a unix socket);
skipped entirely when the PostgreSQL binaries or psycopg are unavailable.
"""

import os
import random
import shutil
import subprocess
import sys

import pytest

from barecat import Barecat, PathEscapeBarecatError

psycopg = pytest.importorskip('psycopg')

pytestmark = pytest.mark.skipif(
    shutil.which('initdb') is None or shutil.which('pg_ctl') is None,
    reason='PostgreSQL binaries not available',
)


@pytest.fixture(scope='session')
def pg_server(tmp_path_factory):
    """One throwaway PostgreSQL instance for the whole session."""
    work = tmp_path_factory.mktemp('pg')
    pgdata = work / 'pgdata'
    sock = work / 'sock'
    sock.mkdir()

    subprocess.run(['initdb', '-D', str(pgdata), '-A', 'trust'], check=True, capture_output=True)
    subprocess.run(
        [
            'pg_ctl',
            '-D',
            str(pgdata),
            '-o',
            f"-k {sock} -c listen_addresses=''",
            '-l',
            str(work / 'pg.log'),
            'start',
        ],
        check=True,
        capture_output=True,
    )
    try:
        yield f'host={sock} dbname=postgres'
    finally:
        subprocess.run(
            ['pg_ctl', '-D', str(pgdata), 'stop', '-m', 'immediate'], capture_output=True
        )


@pytest.fixture
def pg_db(pg_server, request):
    """A fresh database with the barecat schema (1 MB shard limit)."""
    from barecat.postgres import setup_schema

    dbname = f'bc_{abs(hash(request.node.name)) % 10**8}'
    with psycopg.connect(pg_server, autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS {dbname}')
        conn.execute(f'CREATE DATABASE {dbname}')
    conninfo = pg_server.replace('dbname=postgres', f'dbname={dbname}')
    setup_schema(conninfo, '1M')
    return conninfo


@pytest.fixture
def archive(tmp_path):
    return str(tmp_path / 'arch.barecat')


def make_writer(conninfo, archive, **kwargs):
    from barecat.postgres import BarecatPostgresWriter

    return BarecatPostgresWriter(conninfo, archive, **kwargs)


# =============================================================================
# End to end: concurrent workers -> export -> verified archive
# =============================================================================

WORKER_SNIPPET = """
import random, sys
from barecat.postgres import BarecatPostgresWriter
conninfo, archive, wid = sys.argv[1], sys.argv[2], int(sys.argv[3])
rng = random.Random(wid)
with BarecatPostgresWriter(conninfo, archive, batch_size=50, ignore_duplicates=True) as w:
    for i in range(200):
        w[f'w{wid}/f{i:04d}.bin'] = rng.randbytes(rng.randint(100, 4000))
    w['shared/dup.bin'] = b'dup-from-worker-%d' % wid
"""


def test_concurrent_workers_export_verify(pg_db, archive):
    from barecat.postgres import export_to_sqlite, get_stats

    procs = [
        subprocess.Popen([sys.executable, '-c', WORKER_SNIPPET, pg_db, archive, str(wid)])
        for wid in range(3)
    ]
    assert all(p.wait() == 0 for p in procs)

    stats = get_stats(pg_db)
    assert stats['num_files'] == 3 * 200 + 1
    assert stats['num_shards'] > 1  # the 1M limit forced a rollover

    assert export_to_sqlite(pg_db, archive, verify='full') == []

    with Barecat(archive, readonly=True) as bc:
        assert bc.index.verify_integrity()
        for wid in range(3):
            rng = random.Random(wid)
            for i in range(200):
                assert bc[f'w{wid}/f{i:04d}.bin'] == rng.randbytes(rng.randint(100, 4000))
        assert bc['shared/dup.bin'] in {b'dup-from-worker-%d' % w for w in range(3)}


# =============================================================================
# B1/B2: flush correctness
# =============================================================================


def test_intra_batch_duplicate_stores_first_coherently(pg_db, archive):
    """Same path twice in one batch used to pwrite both payloads to one offset
    while storing the first's CRC — a self-corrupting entry (B2)."""
    from barecat.postgres import export_to_sqlite

    with make_writer(pg_db, archive, ignore_duplicates=True) as w:
        w['dup.bin'] = b'first version'
        w['dup.bin'] = b'second version, different length'
        w['other.bin'] = b'x'

    assert export_to_sqlite(pg_db, archive, verify='full') == []
    with Barecat(archive, readonly=True) as bc:
        assert bc['dup.bin'] == b'first version'  # ON CONFLICT semantics: first wins


def test_intra_batch_duplicate_raises_without_ignore(pg_db, archive):
    with make_writer(pg_db, archive) as w:
        w._pending.append(('d.bin', b'a', 1, 0, None, None, None, None))
        w._pending.append(('d.bin', b'b', 1, 0, None, None, None, None))
        with pytest.raises(ValueError, match='Duplicate path'):
            w.flush()
        w._pending.clear()  # allow clean close


def test_cross_batch_duplicate_skipped(pg_db, archive):
    from barecat.postgres import export_to_sqlite

    with make_writer(pg_db, archive, ignore_duplicates=True) as w:
        w['k.bin'] = b'original'
        w.flush()
        w['k.bin'] = b'imposter'
        w.flush()

    export_to_sqlite(pg_db, archive, verify='full')
    with Barecat(archive, readonly=True) as bc:
        assert bc['k.bin'] == b'original'


# =============================================================================
# B3: path normalization and escape rejection
# =============================================================================


def test_paths_are_normalized_and_escapes_rejected(pg_db, archive):
    from barecat.postgres import export_to_sqlite

    with make_writer(pg_db, archive) as w:
        w['a//b/../c/./d.bin'] = b'normalized'  # inner .. collapses safely
        with pytest.raises(PathEscapeBarecatError):
            w['../escape.bin'] = b'evil'
        assert 'a/c/d.bin' in w

    export_to_sqlite(pg_db, archive)
    with Barecat(archive, readonly=True) as bc:
        assert bc['a/c/d.bin'] == b'normalized'


# =============================================================================
# B5: no idle connection between flushes
# =============================================================================


def test_connection_released_after_flush(pg_db, archive):
    with make_writer(pg_db, archive) as w:
        w['f.bin'] = b'x'
        w.flush()
        assert w._conn is None  # released; idle workers must not hold server slots


# =============================================================================
# B6: export verification drops rows from workers killed mid-batch
# =============================================================================


def test_export_drops_rows_whose_bytes_were_never_written(pg_db, archive):
    """Simulate a worker killed after its INSERT committed but before pwrite."""
    from barecat.postgres import export_to_sqlite

    with make_writer(pg_db, archive) as w:
        w['good.bin'] = b'written for real'

    # The dead worker's rows: committed metadata, no bytes ever written
    with psycopg.connect(pg_db) as conn:
        conn.execute(
            'INSERT INTO files (path, size, crc32c) VALUES (%s, %s, %s)',
            ('dead/tail.bin', 5000, 12345),
        )
        conn.commit()

    bad = export_to_sqlite(pg_db, archive, verify='length')
    assert [p for p, _ in bad] == ['dead/tail.bin']
    with Barecat(archive, readonly=True) as bc:
        assert bc.index.verify_integrity()
        assert sorted(bc.files()) == ['good.bin']


def test_export_full_verify_drops_corrupt_bytes_within_bounds(pg_db, archive):
    """Bytes present but wrong (partial overwrite before death): only 'full' sees it."""
    from barecat.postgres import export_to_sqlite

    with make_writer(pg_db, archive) as w:
        w['good.bin'] = b'A' * 3000
        w['victim.bin'] = b'B' * 3000

    # Corrupt victim.bin's bytes in the shard, leaving its row intact
    with psycopg.connect(pg_db) as conn:
        shard, offset = conn.execute(
            'SELECT shard, "offset" FROM files WHERE path = %s', ('victim.bin',)
        ).fetchone()
    fd = os.open(f'{archive}-shard-{shard:05d}', os.O_WRONLY)
    try:
        os.pwrite(fd, b'XXXX', offset + 100)
    finally:
        os.close(fd)

    assert export_to_sqlite(pg_db, archive, verify='length') == []  # length can't see it
    bad = export_to_sqlite(pg_db, archive, verify='full', overwrite=True)
    assert [p for p, _ in bad] == ['victim.bin']
    with Barecat(archive, readonly=True) as bc:
        assert sorted(bc.files()) == ['good.bin']
        assert bc.index.verify_integrity()


# =============================================================================
# B7: export overwrite semantics
# =============================================================================


def test_export_refuses_existing_index_without_overwrite(pg_db, archive):
    from barecat.postgres import export_to_sqlite

    with make_writer(pg_db, archive) as w:
        w['f.bin'] = b'x'

    export_to_sqlite(pg_db, archive)
    with pytest.raises(FileExistsError):
        export_to_sqlite(pg_db, archive)
    export_to_sqlite(pg_db, archive, overwrite=True)  # replaces the index only

    with Barecat(archive, readonly=True) as bc:
        assert bc['f.bin'] == b'x'


# =============================================================================
# B8: a failed pwrite is retried without re-INSERTing
# =============================================================================


def test_flush_retries_pwrites_after_midbatch_failure(pg_db, archive, monkeypatch):
    from barecat.postgres import export_to_sqlite

    w = make_writer(pg_db, archive)
    try:
        w['a.bin'] = b'payload a'
        w['b.bin'] = b'payload b'

        real_pwrite = os.pwrite
        calls = []

        def failing_pwrite(fd, data, offset):
            calls.append(data)
            if len(calls) == 1:
                raise OSError(28, 'No space left on device (simulated)')
            return real_pwrite(fd, data, offset)

        monkeypatch.setattr(os, 'pwrite', failing_pwrite)
        with pytest.raises(OSError):
            w.flush()
        monkeypatch.setattr(os, 'pwrite', real_pwrite)

        w.flush()  # completes the backlog; must not re-INSERT (would be UniqueViolation)
    finally:
        w.close()

    assert export_to_sqlite(pg_db, archive, verify='full') == []
    with Barecat(archive, readonly=True) as bc:
        assert bc['a.bin'] == b'payload a'
        assert bc['b.bin'] == b'payload b'


# =============================================================================
# B4: CLI conninfo resolution
# =============================================================================


def test_cli_host_endpoint_file_and_stats(pg_db, archive, capsys):
    """-H accepts a socket dir / endpoint file; commands reach the right server."""
    from barecat.postgres.__main__ import main

    # pg_db conninfo is 'host=<sockdir> dbname=<name>'; recover the pieces
    parts = dict(kv.split('=', 1) for kv in pg_db.split())
    sockdir, dbname = parts['host'], parts['dbname']

    with make_writer(pg_db, archive) as w:
        w['f.bin'] = b'x' * 42

    main(['stats', dbname, '-H', sockdir])
    out = capsys.readouterr().out
    assert 'Files:  1' in out

    main(['export', dbname, archive, '-H', sockdir])
    with Barecat(archive, readonly=True) as bc:
        assert bc['f.bin'] == b'x' * 42
