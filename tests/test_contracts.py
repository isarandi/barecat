"""Contract tests pinning the behaviors decided in the audit (architecture item E).

Each test asserts the *decided* contract, not incidental current behavior. Tests for
not-yet-implemented decisions are marked ``xfail(strict=True)`` with the audit item
number — landing the fix flips them to XPASS-error, forcing the marker's removal.
"""

import io
import os.path as osp
import tempfile

import pytest

import barecat
from barecat import Barecat, BarecatError, FileExistsBarecatError, FileNotFoundBarecatError


@pytest.fixture
def tempdir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def archive(tempdir):
    return osp.join(tempdir, 'test.barecat')


# =============================================================================
# Path legality (audit item 2)
# =============================================================================


def test_parent_escape_rejected(archive):
    """Paths escaping the archive root are rejected, not stored."""
    with Barecat(archive, readonly=False) as bc:
        with pytest.raises(BarecatError):
            bc['../evil.txt'] = b'x'
        with pytest.raises(BarecatError):
            bc['a/../../evil.txt'] = b'x'


def test_inner_dotdot_collapsed(archive):
    """Inner .. components are collapsed by normalization (works today, keep it)."""
    with Barecat(archive, readonly=False) as bc:
        bc['a/b/../c.txt'] = b'x'
        assert bc['a/c.txt'] == b'x'


# =============================================================================
# rename_dir validation (audit item 4)
# =============================================================================


def test_rename_dir_missing_dest_parent_raises(archive):
    with Barecat(archive, readonly=False) as bc:
        bc['a/f.txt'] = b'x'
        with pytest.raises(FileNotFoundBarecatError):
            bc.rename('a', 'missing/b')
        assert bc['a/f.txt'] == b'x'
        assert bc.verify_integrity()


def test_rename_dir_into_itself_raises(archive):
    with Barecat(archive, readonly=False) as bc:
        bc['d/f.txt'] = b'x'
        with pytest.raises(BarecatError):
            bc.rename('d', 'd/sub')
        assert bc['d/f.txt'] == b'x'
        assert bc.verify_integrity()


def test_rename_dir_to_sibling_prefix_name_allowed(archive):
    """rename('a', 'ab') is legal — the inside-source check must be component-wise."""
    with Barecat(archive, readonly=False) as bc:
        bc['a/f.txt'] = b'x'
        bc.rename('a', 'ab')
        assert bc['ab/f.txt'] == b'x'
        assert bc.verify_integrity()


# =============================================================================
# mkdir exist_ok (audit item 18)
# =============================================================================


@pytest.mark.xfail(strict=True, reason='audit item 18: exist_ok raises on non-empty dir')
def test_mkdir_exist_ok_nonempty_dir_is_noop(archive):
    with Barecat(archive, readonly=False) as bc:
        bc['d/f.txt'] = b'x'
        bc.mkdir('d', exist_ok=True)  # must not raise
        assert bc['d/f.txt'] == b'x'


@pytest.mark.xfail(strict=True, reason='audit item 18: exist_ok overwrites empty-dir metadata')
def test_mkdir_exist_ok_preserves_existing_metadata(archive):
    with Barecat(archive, readonly=False) as bc:
        bc.mkdir('d', mode=0o700)
        bc.mkdir('d', mode=0o755, exist_ok=True)
        assert bc.index.lookup_dir('d').mode == 0o700


# =============================================================================
# Constructor flags (audit item 34)
# =============================================================================


def test_overwrite_with_exist_ok_false_is_contradiction(archive):
    with Barecat(archive, readonly=False):
        pass
    with pytest.raises(ValueError):
        Barecat(archive, readonly=False, overwrite=True, exist_ok=False)


def test_overwrite_with_exist_ok_false_raises_even_for_missing_archive(archive):
    """The flags contradict each other regardless of what is on disk."""
    with pytest.raises(ValueError):
        Barecat(archive, readonly=False, overwrite=True, exist_ok=False)


# =============================================================================
# Deprecated open() shim (audit item 37)
# =============================================================================


def test_open_rplus_missing_archive_raises(archive):
    with pytest.warns(DeprecationWarning):
        with pytest.raises(FileNotFoundError):
            barecat.open(archive, 'r+')
    assert not osp.exists(archive)


# =============================================================================
# Dict interface deviations (audit item 30 — decided: keep the raise)
# =============================================================================


def test_setitem_existing_key_raises(archive):
    """Intentional MutableMapping deviation: silent overwrite is treated as a bug signal."""
    with Barecat(archive, readonly=False) as bc:
        bc['k.bin'] = b'v1'
        with pytest.raises(FileExistsBarecatError):
            bc['k.bin'] = b'v2'
        assert bc['k.bin'] == b'v1'


# =============================================================================
# update_file with short source (audit item 6 — decided: rsync-style store + warn)
# =============================================================================


def test_update_file_short_source_stores_partial_coherently(archive):
    with Barecat(archive, readonly=False) as bc:
        bc['f.bin'] = b'A' * 100
        bc.update_file('f.bin', fileobj=io.BytesIO(b'B' * 10), size=50)
        assert bc['f.bin'] == b'B' * 10
        assert bc.verify_integrity()


# =============================================================================
# append_only write refusal (audit item 7)
# =============================================================================


def test_append_only_refuses_rplus_open(archive):
    with Barecat(archive, readonly=False, append_only=True) as bc:
        bc['f.bin'] = b'0123456789'
        with pytest.raises((ValueError, BarecatError)):
            bc.open('f.bin', 'r+b')


# =============================================================================
# Stored shard-size limit is authoritative (audit items 22 + 46)
# =============================================================================


@pytest.mark.parametrize('workers', [None, 2], ids=['inline', 'workers'])
def test_add_keeps_stored_shard_size_limit(tempdir, workers):
    from barecat.cli import impl

    f1 = osp.join(tempdir, 'f1.bin')
    f2 = osp.join(tempdir, 'f2.bin')
    for f in (f1, f2):
        with open(f, 'wb') as fh:
            fh.write(b'x' * 10)

    archive = osp.join(tempdir, 'a.barecat')
    impl.create([(f1, 'f1.bin')], archive, shard_size_limit=1024, workers=workers)
    with Barecat(archive, readonly=True) as bc:
        assert bc.shard_size_limit == 1024

    # Adding without a limit must keep the stored one
    impl.create([(f2, 'f2.bin')], archive, shard_size_limit=None, workers=workers)
    with Barecat(archive, readonly=True) as bc:
        assert bc.shard_size_limit == 1024
        assert sorted(bc.files()) == ['f1.bin', 'f2.bin']

    # merge -a variant arrives via impl.merge
    src = osp.join(tempdir, 'src.barecat')
    with Barecat(src, readonly=False) as bc:
        bc['extra.bin'] = b'y' * 10
    impl.merge([src], archive, shard_size_limit=None, ignore_duplicates=True)
    with Barecat(archive, readonly=True) as bc:
        assert bc.shard_size_limit == 1024

    # An explicit limit on add changes the stored one
    f3 = osp.join(tempdir, 'f3.bin')
    with open(f3, 'wb') as fh:
        fh.write(b'z' * 10)
    impl.create([(f3, 'f3.bin')], archive, shard_size_limit=2048, workers=workers)
    with Barecat(archive, readonly=True) as bc:
        assert bc.shard_size_limit == 2048


# =============================================================================
# Legacy archive upgrade (audit item 15)
# =============================================================================


def _make_unversioned_archive(tempdir, old_naming):
    """Minimal pre-versioned barecat: old-schema index (files+directories) + one shard."""
    import sqlite3

    archive_path = osp.join(tempdir, 'arch.barecat')
    index_path = archive_path + '-sqlite-index' if old_naming else archive_path
    conn = sqlite3.connect(index_path)
    conn.execute(
        'CREATE TABLE files (path TEXT PRIMARY KEY, shard INTEGER, offset INTEGER, size INTEGER)'
    )
    conn.execute('CREATE TABLE directories (path TEXT PRIMARY KEY)')
    conn.execute("INSERT INTO directories VALUES ('d')")
    conn.execute("INSERT INTO files VALUES ('d/f.bin', 0, 0, 5)")
    conn.commit()
    conn.close()
    with open(archive_path + '-shard-00000', 'wb') as f:
        f.write(b'hello')
    return archive_path


@pytest.mark.parametrize('old_naming', [False, True], ids=['new-naming', 'old-naming'])
def test_upgrade_unversioned_archive(tempdir, old_naming):
    from barecat.maintenance.upgrade_database import upgrade

    archive_path = _make_unversioned_archive(tempdir, old_naming)
    upgrade(archive_path, workers=2, preserve_backup=True)
    with Barecat(archive_path, readonly=True) as bc:
        assert bc['d/f.bin'] == b'hello'
        assert bc.verify_integrity()


# =============================================================================
# Transaction discipline (audit items 16 / B / A-corollary)
# =============================================================================


def test_no_commit_inside_no_triggers(archive):
    """Entering/leaving no_triggers() must not commit the open transaction."""
    with Barecat(archive, readonly=False) as bc:
        bc['a/f.bin'] = b'x'
        assert bc.index.conn.in_transaction
        with bc.index.no_triggers():
            assert bc.index.conn.in_transaction
        assert bc.index.conn.in_transaction


def test_deferred_restrict_delete_pin(archive):
    """SQLite behavior pin: PRAGMA defer_foreign_keys defers ON DELETE RESTRICT.

    The whole defer design rests on this (verified on SQLite 3.45.1; the docs sentence
    'RESTRICT is enforced immediately' applies only to DEFERRABLE INITIALLY DEFERRED
    clauses, not to the pragma). If this test fails on some SQLite build, the
    no_triggers/no_foreign_keys mechanism must be revisited for that build.
    """
    import sqlite3

    with Barecat(archive, readonly=False) as bc:
        bc['d/f.bin'] = b'x'
        bc.index.conn.commit()
        cur = bc.index.cursor
        cur.execute('PRAGMA defer_foreign_keys = ON')
        # files.parent references dirs.path with ON DELETE RESTRICT; under deferral
        # this delete must be accepted now and rejected only at COMMIT.
        cur.execute("DELETE FROM dirs WHERE path = 'd'")
        with pytest.raises(sqlite3.IntegrityError):
            bc.index.conn.commit()
        bc.index.conn.rollback()
        assert bc['d/f.bin'] == b'x'
        assert bc.verify_integrity()


def test_exception_exit_rolls_back_uncommitted_work(archive):
    """An exception inside `with Barecat(...)` discards pending index changes."""
    with Barecat(archive, readonly=False) as bc:
        bc['committed.bin'] = b'1'
    with pytest.raises(RuntimeError):
        with Barecat(archive, readonly=False) as bc:
            bc['pending.bin'] = b'2'
            raise RuntimeError('abort mid-ingest')
    with Barecat(archive, readonly=True) as bc:
        assert 'committed.bin' in bc
        assert 'pending.bin' not in bc
        assert bc.verify_integrity()


def test_clean_exit_commits(archive):
    with Barecat(archive, readonly=False) as bc:
        bc['f.bin'] = b'x'
    with Barecat(archive, readonly=True) as bc:
        assert bc['f.bin'] == b'x'


# =============================================================================
# Repeated bulk operations (audit item 17)
# =============================================================================


def test_two_bulk_mode_blocks_on_one_connection(archive):
    with Barecat(archive, readonly=False) as bc:
        with bc.index.bulk_mode():
            bc['a/one.bin'] = b'1'
        with bc.index.bulk_mode():
            bc['b/two.bin'] = b'2'
        assert bc.verify_integrity()


# =============================================================================
# ConsumedThreadPool shutdown (audit item 5)
# =============================================================================


def test_consumed_threadpool_normal_operation():
    from barecat.util.consumed_threadpool import ConsumedThreadPool

    received = []

    def consumer_main(future_iter):
        for future in future_iter:
            received.append(future.result())

    with ConsumedThreadPool(consumer_main, max_workers=4) as pool:
        for i in range(50):
            pool.submit(lambda x: x * 2, args=(i,))
    assert sorted(received) == [i * 2 for i in range(50)]


def test_consumed_threadpool_consumer_error_propagates():
    """A consumer exception surfaces as RuntimeError instead of deadlocking close()."""
    from barecat.util.consumed_threadpool import ConsumedThreadPool

    def consumer_main(future_iter):
        for i, _future in enumerate(future_iter):
            if i == 3:
                raise ValueError('consumer died')

    with pytest.raises(RuntimeError, match='Consumer thread raised an exception'):
        with ConsumedThreadPool(consumer_main, max_workers=4) as pool:
            for i in range(100):
                pool.submit(lambda x: x, args=(i,))


def test_consumed_threadpool_consumer_early_return():
    """A consumer returning before the sentinel must not deadlock close()."""
    from barecat.util.consumed_threadpool import ConsumedThreadPool

    def consumer_main(future_iter):
        for i, _future in enumerate(future_iter):
            if i == 3:
                return

    with ConsumedThreadPool(consumer_main, max_workers=4) as pool:
        for i in range(100):
            pool.submit(lambda x: x, args=(i,))


def test_verify_integrity_survives_leaked_temp_table(archive):
    """verify_integrity works even if a previous call errored out mid-way (item 31)."""
    with Barecat(archive, readonly=False) as bc:
        bc['f.bin'] = b'x'
        bc.index.cursor.execute('CREATE TEMPORARY TABLE tmp_verify_treestats (dummy)')
        assert bc.verify_integrity()
        assert bc.verify_integrity()
