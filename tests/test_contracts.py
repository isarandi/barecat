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


@pytest.mark.xfail(strict=True, reason='audit item 2: leading .. paths accepted into the index')
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


@pytest.mark.xfail(strict=True, reason='audit item 4: missing dest parent corrupts index')
def test_rename_dir_missing_dest_parent_raises(archive):
    with Barecat(archive, readonly=False) as bc:
        bc['a/f.txt'] = b'x'
        with pytest.raises(FileNotFoundBarecatError):
            bc.rename('a', 'missing/b')
        assert bc['a/f.txt'] == b'x'
        assert bc.verify_integrity()


@pytest.mark.xfail(strict=True, reason='audit item 4: dir-into-itself corrupts index')
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


@pytest.mark.xfail(
    strict=True, reason='audit item 34: contradictory overwrite+exist_ok not yet a ValueError'
)
def test_overwrite_with_exist_ok_false_is_contradiction(archive):
    with Barecat(archive, readonly=False):
        pass
    with pytest.raises(ValueError):
        Barecat(archive, readonly=False, overwrite=True, exist_ok=False)


# =============================================================================
# Deprecated open() shim (audit item 37)
# =============================================================================


@pytest.mark.xfail(strict=True, reason="audit item 37: 'r+' still creates a missing archive")
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


@pytest.mark.xfail(
    strict=True,
    reason='audit item 6: short source currently corrupts the entry instead of storing '
    'the partial content coherently',
)
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
