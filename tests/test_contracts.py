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


def test_mkdir_exist_ok_nonempty_dir_is_noop(archive):
    with Barecat(archive, readonly=False) as bc:
        bc['d/f.txt'] = b'x'
        bc.mkdir('d', exist_ok=True)  # must not raise
        assert bc['d/f.txt'] == b'x'


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


# =============================================================================
# update_file never renames (audit item 19)
# =============================================================================


def test_update_file_rejects_a_string_new_item(archive):
    """The str form was documented as a rename but silently did nothing."""
    with Barecat(archive, readonly=False) as bc:
        bc['a.txt'] = b'old'
        with pytest.raises(TypeError):
            bc.update_file('a.txt', 'b.txt', data=b'new')
        assert 'a.txt' in bc and 'b.txt' not in bc


def test_update_file_ignores_the_path_of_new_item(archive):
    """A BarecatFileInfo's path is metadata only; renaming is rename()'s job."""
    from barecat.core.types import BarecatFileInfo

    with Barecat(archive, readonly=False) as bc:
        bc['a.txt'] = b'old'
        bc.update_file('a.txt', BarecatFileInfo(path='elsewhere.txt', mode=0o644), data=b'new')
        assert bc['a.txt'] == b'new'
        assert 'elsewhere.txt' not in bc


# =============================================================================
# find_space removed (audit item 20)
# =============================================================================


def test_find_space_is_gone(archive):
    """It answered with the size delta instead of the full size, corrupting neighbors."""
    with Barecat(archive, readonly=False) as bc:
        assert not hasattr(bc.index, 'find_space')


# =============================================================================
# Archive paths with URI-reserved characters (audit items 40 + 66)
# =============================================================================


@pytest.mark.parametrize(
    'name',
    [
        'q?mark.barecat',
        'ha#sh.barecat',
        'per%cent.barecat',
        "quo'te.barecat",
        'sp ace.barecat',
        'amp&and.barecat',
    ],
)
def test_archive_path_with_uri_reserved_characters(tempdir, name):
    """SQLite reads a URI filename as a URI, so '?', '#' and '%' need encoding."""
    path = osp.join(tempdir, name)
    with Barecat(path, readonly=False) as bc:
        bc['a.txt'] = b'hello'
        bc['d/b.txt'] = b'world'

    assert osp.exists(path), 'the index was created somewhere other than the given path'

    with Barecat(path, readonly=True) as bc:
        assert bc['a.txt'] == b'hello'
        assert bc['d/b.txt'] == b'world'
        assert bc.index.verify_integrity()


def test_lookup_files_beyond_the_sql_variable_limit(archive):
    """One placeholder per path used to exceed SQLite's variable limit (item 41)."""
    paths = [f'f{i:05d}.bin' for i in range(2500)]
    with Barecat(archive, readonly=False) as bc:
        for p in paths:
            bc[p] = b'x'

    with Barecat(archive, readonly=True) as bc:
        assert [i.path for i in bc.index.lookup_files(paths)] == paths
        # Chunking must not disturb ordering or duplicate handling
        repeated = ['f00001.bin', 'f00001.bin', 'f00000.bin']
        assert [i.path for i in bc.index.lookup_files(repeated)] == repeated


def test_fetch_many_without_explicit_bufsize(archive):
    """fetchmany(None) is a TypeError; fetch_many must fall back like fetch_iter (item 39)."""
    with Barecat(archive, readonly=False) as bc:
        bc['a.txt'] = b'x'
        rows = bc.index.fetch_many('SELECT path FROM files')
        assert [r['path'] for r in rows] == ['a.txt']


# =============================================================================
# Silent no-ops in index mutations (audit item 38)
# =============================================================================


def test_rename_file_reports_a_missing_source(archive):
    """The UPDATE matched nothing and returned quietly, despite the docstring."""
    with Barecat(archive, readonly=False) as bc:
        with pytest.raises(FileNotFoundBarecatError):
            bc.index.rename_file('nope.txt', 'other.txt')


def test_remove_files_reports_a_missing_file(archive):
    with Barecat(archive, readonly=False) as bc:
        bc['a.txt'] = b'a'
        with pytest.raises(FileNotFoundBarecatError):
            bc.index.remove_files(['a.txt', 'nope.txt'])


def test_remove_files_succeeds_when_all_exist(archive):
    with Barecat(archive, readonly=False) as bc:
        bc['a.txt'] = b'a'
        bc['b.txt'] = b'b'
        bc.index.remove_files(['a.txt', 'b.txt'])
        assert sorted(bc.files()) == []


def test_remove_empty_dir_refuses_the_root(archive):
    """Deleting the root row leaves an archive nothing can be added to."""
    with Barecat(archive, readonly=False) as bc:
        with pytest.raises(BarecatError):
            bc.index.remove_empty_dir('')
        bc['a.txt'] = b'a'
        assert bc['a.txt'] == b'a'


# =============================================================================
# Symlink-merge directory statistics (audit item 52)
# =============================================================================


def _build(path, entries):
    with Barecat(path, readonly=False) as bc:
        for k, v in entries.items():
            bc[k] = v


def test_index_merge_counts_a_shared_subdir_once(tempdir):
    """num_subdirs was summed, so a subdirectory present in both archives counted twice."""
    src1 = osp.join(tempdir, 's1.barecat')
    src2 = osp.join(tempdir, 's2.barecat')
    # Both archives contain shared/a, so the additive upsert gave shared 1 + 1 subdirs
    _build(src1, {'shared/a/f1.bin': b'1'})
    _build(src2, {'shared/a/f2.bin': b'2'})

    out = osp.join(tempdir, 'out.barecat')
    with Barecat(out, readonly=False) as bc:
        for src in (src1, src2):
            bc.index.merge_from_other_barecat(src)

    with Barecat(out, readonly=True) as bc:
        assert bc.index.verify_integrity()
        assert bc.index.lookup_dir('shared').num_subdirs == 1


def test_index_merge_with_prefix_rebuilds_treestats(tempdir):
    """The rebuild was an elif, so a prefixed merge kept counts for dropped duplicates."""
    src1 = osp.join(tempdir, 's1.barecat')
    src2 = osp.join(tempdir, 's2.barecat')
    _build(src1, {'d/dup.bin': b'DD', 'd/one.bin': b'1'})
    _build(src2, {'d/dup.bin': b'DD', 'd/two.bin': b'2'})

    out = osp.join(tempdir, 'out.barecat')
    with Barecat(out, readonly=False) as bc:
        for src in (src1, src2):
            bc.index.merge_from_other_barecat(src, ignore_duplicates=True, prefix='pfx')

    with Barecat(out, readonly=True) as bc:
        assert bc.index.verify_integrity()
        info = bc.index.lookup_dir('pfx/d')
        # dup.bin is stored once, so three files of 2 + 1 + 1 bytes
        assert info.num_files_tree == 3
        assert info.size_tree == 4


# =============================================================================
# Minor sweep: parse_size, tar links, mtime, info defaults (items 58-62)
# =============================================================================


@pytest.mark.parametrize(
    'text,expected',
    [
        ('10', 10),
        ('512K', 512 * 1024),
        ('1M', 1024**2),
        ('1MiB', 1024**2),
        ('1MB', 1000**2),
        ('1.5GiB', int(1.5 * 1024**3)),
        ('2GB', 2 * 1000**3),
        ('  4 G ', 4 * 1024**3),
    ],
)
def test_parse_size_follows_the_coreutils_convention(text, expected):
    """Bare letters stay binary; a B suffix is decimal, iB binary (item 61)."""
    from barecat.util.misc import parse_size

    assert parse_size(text) == expected


@pytest.mark.parametrize('text', ['1K5', '1XB', 'abc', ''])
def test_parse_size_rejects_garbage(text):
    """'1K5' silently parsed as 15 * 1024, and unknown suffixes crashed (item 61)."""
    from barecat.util.misc import parse_size

    with pytest.raises(ValueError):
        parse_size(text)


def test_tar_conversion_warns_about_dropped_links(tempdir):
    """Links cannot be stored, so their loss must at least be visible (item 58)."""
    import io as _io
    import tarfile
    import warnings

    from barecat.formats.archive_formats import iter_tarfile

    tar_path = osp.join(tempdir, 't.tar')
    with tarfile.open(tar_path, 'w') as tf:
        info = tarfile.TarInfo('real.txt')
        info.size = 5
        info.mtime = 1700000000.75  # PAX float mtime, see item 59
        tf.addfile(info, _io.BytesIO(b'hello'))
        for name, kind in [('sym.txt', tarfile.SYMTYPE), ('hard.txt', tarfile.LNKTYPE)]:
            link = tarfile.TarInfo(name)
            link.type = kind
            link.linkname = 'real.txt'
            tf.addfile(link)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        entries = [info for info, _ in iter_tarfile(tar_path)]

    assert [e.path for e in entries] == ['real.txt']
    assert all(isinstance(e.mtime_ns, int) for e in entries)  # item 59
    assert any('symlink/hardlink' in str(c.message) for c in caught)


def test_entry_info_defaults_and_epoch_zero_mtime():
    """path=None passes through, and mtime_ns=0 is a real timestamp (item 60)."""
    from barecat.core.types import BarecatDirInfo, BarecatFileInfo

    finfo = BarecatFileInfo()
    assert finfo.path is None
    finfo.path = 'a/b.txt'
    assert finfo.path == 'a/b.txt'

    finfo.mtime_ns = 0
    assert finfo.mtime_dt is not None

    # num_subdirs is no longer derived from st_nlink, which is 1 on btrfs/overlayfs
    assert BarecatDirInfo().num_subdirs is None


# =============================================================================
# FIXME items from dependent-package audits (barecat_cython, barecat_mount)
# =============================================================================


def test_path_helpers_are_public(tempdir):
    """Consumers re-implemented these and drifted; they are part of the API now (FIXME 1)."""
    assert barecat.normalize_path('a//b/') == 'a/b'
    archive = osp.join(tempdir, 'x.barecat')
    with Barecat(archive, readonly=False, shard_size_limit=100) as bc:
        bc['f.bin'] = b'x' * 60
    assert barecat.resolve_index_path(archive) == archive
    assert barecat.find_shard_paths(archive) == [f'{archive}-shard-00000']


def test_gapped_shard_list_refuses_to_open(tempdir):
    """A gap in the shard numbering means damage; positional indexing would read the
    wrong shard, and only CRC-checked whole-file reads would notice (FIXME 2)."""
    import os

    archive = osp.join(tempdir, 'g.barecat')
    with Barecat(archive, readonly=False, shard_size_limit=100) as bc:
        for i in range(3):
            bc[f'f{i}.bin'] = bytes([i]) * 60  # 60+60 > 100, so one file per shard

    from barecat import BarecatIntegrityError

    os.remove(f'{archive}-shard-00001')
    with pytest.raises(BarecatIntegrityError, match='shard-00001'):
        Barecat(archive, readonly=True)

    # A missing shard 0 is the same damage
    os.rename(f'{archive}-shard-00002', f'{archive}-shard-00001')
    os.remove(f'{archive}-shard-00000')
    with pytest.raises(BarecatIntegrityError, match='shard-00000'):
        Barecat(archive, readonly=True)


def test_close_flushes_shards_before_committing_index(archive):
    """Committing first opens a power-loss window where the index references bytes
    that were never written -- an archive that opens cleanly and reads garbage
    (FIXME 4)."""
    bc = Barecat(archive, readonly=False)
    bc['f.bin'] = b'x'

    order = []
    orig_flush = bc.sharder.flush
    orig_close = bc.index.close
    bc.sharder.flush = lambda: (order.append('shards'), orig_flush())[1]
    bc.index.close = lambda commit=True: (order.append('index'), orig_close(commit=commit))[1]
    bc.close()
    assert order == ['shards', 'index']


def test_flush_is_a_public_durability_point(archive):
    """A consumer handling fsync (e.g. a FUSE mount) needs something to call (FIXME 4)."""
    bc = Barecat(archive, readonly=False)
    try:
        bc['f.bin'] = b'x'
        bc.flush()
        # The committed state is now visible to an independent connection
        with Barecat(archive, readonly=True) as reader:
            assert reader['f.bin'] == b'x'
    finally:
        bc.close()


