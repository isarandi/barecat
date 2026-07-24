"""Property-based tests pinning barecat's core data-path contracts.

These correspond to audit architecture item G. Each property that is currently broken
carries a deterministic reproduction and an ``xfail(strict=True)`` marker naming the
audit item; removing the marker is part of landing that item's fix. The hypothesis
properties run on throwaway tmp archives with bounded example counts.
"""

import os.path as osp
import tempfile

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from barecat import Barecat
from barecat.maintenance.reshard import reshard

HYP_SETTINGS = settings(
    max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow]
)

component = st.text(alphabet='abcdefghijklmnopqrstuvwxyz0123456789_', min_size=1, max_size=8)
path_strategy = st.builds('/'.join, st.lists(component, min_size=1, max_size=3))
blob_strategy = st.binary(max_size=2048)


def _no_file_dir_conflicts(files):
    keys = set(files)
    return not any(k2.startswith(k1 + '/') for k1 in keys for k2 in keys if k1 != k2)


files_strategy = st.dictionaries(path_strategy, blob_strategy, min_size=1, max_size=15).filter(
    _no_file_dir_conflicts
)


def _make_archive(tempdir, files, name='arch.barecat', shard_size_limit=None):
    archive_path = osp.join(tempdir, name)
    with Barecat(archive_path, readonly=False, shard_size_limit=shard_size_limit) as bc:
        for path, data in files.items():
            bc[path] = data
    return archive_path


def _assert_content(archive_path, expected):
    with Barecat(archive_path, readonly=True) as bc:
        for path, data in expected.items():
            assert bc[path] == data
        assert sorted(f for f in bc.files()) == sorted(expected)


# =============================================================================
# Store -> read identity
# =============================================================================


@HYP_SETTINGS
@given(files=files_strategy)
def test_store_read_identity(files):
    """Writing arbitrary {path: bytes} and reading back returns identical content."""
    with tempfile.TemporaryDirectory() as tempdir:
        archive_path = _make_archive(tempdir, files)
        _assert_content(archive_path, files)
        with Barecat(archive_path, readonly=True) as bc:
            assert bc.verify_integrity()


def test_truncating_open_with_write_replaces_content():
    """open(path, 'w') + write replaces the content (works today via the grew branch)."""
    with tempfile.TemporaryDirectory() as tempdir:
        archive_path = osp.join(tempdir, 'arch.barecat')
        with Barecat(archive_path, readonly=False) as bc:
            bc['f.bin'] = b'0123456789'
            with bc.open('f.bin', 'wb') as f:
                f.write(b'new')
            assert bc['f.bin'] == b'new'
        with Barecat(archive_path, readonly=True) as bc:
            assert bc['f.bin'] == b'new'
            assert bc.verify_integrity()


def test_truncating_open_without_write_empties_file():
    """open(path, 'w') + close with no writes truncates the file to empty."""
    with tempfile.TemporaryDirectory() as tempdir:
        archive_path = osp.join(tempdir, 'arch.barecat')
        with Barecat(archive_path, readonly=False) as bc:
            bc['f.bin'] = b'0123456789'
            with bc.open('f.bin', 'wb'):
                pass
            assert bc['f.bin'] == b''
        with Barecat(archive_path, readonly=True) as bc:
            assert bc['f.bin'] == b''
            assert bc.verify_integrity()


# =============================================================================
# RW fileobj position and zero-fill semantics (audit item 8, decided: POSIX)
# =============================================================================


def test_read_past_eof_keeps_position():
    with tempfile.TemporaryDirectory() as tempdir:
        archive_path = osp.join(tempdir, 'arch.barecat')
        with Barecat(archive_path, readonly=False) as bc:
            bc['f.bin'] = b'0123456789'
            with bc.open('f.bin', 'r+b') as f:
                f.seek(20)
                assert f.read() == b''
                assert f.tell() == 20


def test_negative_read_size_means_read_all():
    with tempfile.TemporaryDirectory() as tempdir:
        archive_path = osp.join(tempdir, 'arch.barecat')
        with Barecat(archive_path, readonly=False) as bc:
            bc['f.bin'] = b'0123456789'
            with bc.open('f.bin', 'r+b') as f:
                assert f.read(-3) == b'0123456789'
                assert f.tell() == 10


def test_truncate_then_grow_gap_is_zeros():
    with tempfile.TemporaryDirectory() as tempdir:
        archive_path = osp.join(tempdir, 'arch.barecat')
        with Barecat(archive_path, readonly=False) as bc:
            bc['f.bin'] = b'ABCDEFGHIJ'
            with bc.open('f.bin', 'r+b') as f:
                f.truncate(3)
                f.seek(6)
                f.write(b'XY')
            assert bc['f.bin'] == b'ABC\x00\x00\x00XY'


# =============================================================================
# Maintenance operations preserve content
# =============================================================================


@HYP_SETTINGS
@given(files=files_strategy, data=st.data())
def test_defrag_preserves_content(files, data):
    """Removing a subset of files (creating gaps) and defragmenting keeps the rest intact."""
    removed = data.draw(st.sets(st.sampled_from(sorted(files)), max_size=len(files) - 1))
    remaining = {p: d for p, d in files.items() if p not in removed}
    with tempfile.TemporaryDirectory() as tempdir:
        archive_path = _make_archive(tempdir, files)
        with Barecat(archive_path, readonly=False) as bc:
            for path in removed:
                bc.remove(path)
            bc.defrag()
            assert bc.verify_integrity()
        _assert_content(archive_path, remaining)


@HYP_SETTINGS
@given(files=files_strategy, limit_factor=st.integers(min_value=1, max_value=4))
def test_reshard_preserves_content(files, limit_factor):
    """Resharding to a different shard size keeps all content readable."""
    max_size = max(len(d) for d in files.values())
    initial_limit = max(max_size, 1) * 2
    target_limit = max(max_size, 1) * limit_factor
    with tempfile.TemporaryDirectory() as tempdir:
        archive_path = _make_archive(tempdir, files, shard_size_limit=initial_limit)
        with Barecat(archive_path, readonly=False) as bc:
            reshard(bc, target_limit)
            assert bc.verify_integrity()
        _assert_content(archive_path, files)


@HYP_SETTINGS
@given(
    files1=files_strategy.filter(lambda d: any(d.values())),
    files2=files_strategy.filter(lambda d: any(d.values())),
)
def test_merge_preserves_content(files1, files2):
    """Merging two archives into a fresh one yields exactly the union of their contents.

    Sources are constrained to hold at least one non-empty file each: a source shard of
    logical size zero loses its entries entirely — audit item 64, pinned separately in
    test_merge_only_empty_files_preserved.
    """
    src1 = {'a/' + p: d for p, d in files1.items()}
    src2 = {'b/' + p: d for p, d in files2.items()}
    with tempfile.TemporaryDirectory() as tempdir:
        src1_path = _make_archive(tempdir, src1, name='src1.barecat')
        src2_path = _make_archive(tempdir, src2, name='src2.barecat')
        dest_path = osp.join(tempdir, 'dest.barecat')
        with Barecat(dest_path, readonly=False) as bc:
            bc.merge_from_other_barecat(src1_path)
            bc.merge_from_other_barecat(src2_path)
            assert bc.verify_integrity()
        _assert_content(dest_path, {**src1, **src2})


def test_merge_only_empty_files_preserved():
    """A source archive holding only zero-byte files must merge without losing entries."""
    with tempfile.TemporaryDirectory() as tempdir:
        src_path = _make_archive(tempdir, {'a/empty.bin': b''}, name='src.barecat')
        dest_path = osp.join(tempdir, 'dest.barecat')
        with Barecat(dest_path, readonly=False) as bc:
            bc.merge_from_other_barecat(src_path)
            assert sorted(bc.files()) == ['a/empty.bin']
            assert bc['a/empty.bin'] == b''
            assert bc.verify_integrity()


def test_merge_gap_exceeding_dest_limit_terminates():
    """A source gap larger than the dest shard limit must not hang the merge."""
    with tempfile.TemporaryDirectory() as tempdir:
        src_path = osp.join(tempdir, 'src.barecat')
        with Barecat(src_path, readonly=False) as bc:
            bc['big.bin'] = b'x' * 100
            bc['next.bin'] = b'y' * 10
            bc.remove('big.bin')  # gap [0, 100) before next.bin
        dest_path = osp.join(tempdir, 'dest.barecat')
        with Barecat(dest_path, readonly=False, shard_size_limit=50) as bc:
            bc.merge_from_other_barecat(src_path)
        _assert_content(dest_path, {'next.bin': b'y' * 10})


# =============================================================================
# Stats consistency under operation sequences
# =============================================================================


_op_strategy = st.one_of(
    st.tuples(st.just('add'), path_strategy, blob_strategy),
    st.tuples(st.just('remove'), st.integers(min_value=0, max_value=100)),
    st.tuples(st.just('rename'), st.integers(min_value=0, max_value=100), component),
)


@HYP_SETTINGS
@given(ops=st.lists(_op_strategy, min_size=1, max_size=30))
def test_stats_consistency_after_operations(ops):
    """After arbitrary add/remove/rename sequences, the index passes verify_integrity
    and the file set matches a reference model.

    Renames are constrained to targets whose parent exists (same directory), since
    renames to missing parents are audit item 4 (pinned separately in test_contracts).
    """
    reference = {}
    with tempfile.TemporaryDirectory() as tempdir:
        archive_path = osp.join(tempdir, 'arch.barecat')
        with Barecat(archive_path, readonly=False) as bc:
            for op in ops:
                if op[0] == 'add':
                    _, path, data = op
                    if path in reference or any(
                        k == path or k.startswith(path + '/') or path.startswith(k + '/')
                        for k in reference
                    ):
                        continue
                    bc[path] = data
                    reference[path] = data
                elif op[0] == 'remove':
                    _, idx = op
                    if not reference:
                        continue
                    path = sorted(reference)[idx % len(reference)]
                    bc.remove(path)
                    del reference[path]
                elif op[0] == 'rename':
                    _, idx, new_name = op
                    if not reference:
                        continue
                    old_path = sorted(reference)[idx % len(reference)]
                    parent = osp.dirname(old_path)
                    new_path = f'{parent}/{new_name}' if parent else new_name
                    if new_path in reference or any(
                        k.startswith(new_path + '/') for k in reference
                    ):
                        continue
                    bc.rename(old_path, new_path)
                    reference[new_path] = reference.pop(old_path)
            assert bc.verify_integrity()
        _assert_content(archive_path, reference)


# =============================================================================
# Codec round-trips (DecodedView)
# =============================================================================


def _roundtrip(filename, value):
    from barecat.io.codecs import DecodedView

    with tempfile.TemporaryDirectory() as tempdir:
        archive_path = osp.join(tempdir, 'arch.barecat')
        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec[filename] = value
            return dec[filename]


def test_codec_roundtrip_json():
    value = {'key': 'value', 'n': [1, 2, 3]}
    assert _roundtrip('data.json', value) == value


def test_codec_roundtrip_pickle():
    value = {'tuple': (1, 'two'), 'nested': {'a': None}}
    assert _roundtrip('data.pkl', value) == value


def test_codec_roundtrip_compound_json_gz():
    value = {'key': 'value'}
    assert _roundtrip('data.json.gz', value) == value


@pytest.mark.xfail(
    strict=True, reason='audit item 49: unknown inner extension (.txt) breaks compound codecs'
)
def test_codec_roundtrip_compound_unknown_inner():
    """.txt.gz should gzip the raw bytes (identity fallback for the inner extension)."""
    assert _roundtrip('notes.txt.gz', b'hello world') == b'hello world'


def test_codec_roundtrip_npy():
    np = pytest.importorskip('numpy')
    value = np.arange(24, dtype=np.uint8).reshape(2, 3, 4)
    result = _roundtrip('data.npy', value)
    np.testing.assert_array_equal(result, value)


def test_codec_roundtrip_png_rgb():
    np = pytest.importorskip('numpy')
    from barecat.io.codecs import _HAS_CV2, _HAS_PIL

    if not (_HAS_CV2 or _HAS_PIL):
        pytest.skip('no image backend available')
    rng = np.random.default_rng(0)
    value = rng.integers(0, 256, size=(5, 7, 3), dtype=np.uint8)
    result = _roundtrip('img.png', value)
    np.testing.assert_array_equal(result, value)


@pytest.mark.xfail(
    strict=False, reason='audit item 50: grayscale decode shape is backend-dependent (cv2: 3ch)'
)
def test_codec_roundtrip_png_grayscale_stays_2d():
    """Decided contract (item 50): grayscale images decode as 2-D arrays."""
    np = pytest.importorskip('numpy')
    from barecat.io.codecs import _HAS_CV2, _HAS_PIL

    if not (_HAS_CV2 or _HAS_PIL):
        pytest.skip('no image backend available')
    value = np.arange(35, dtype=np.uint8).reshape(5, 7)
    result = _roundtrip('img.png', value)
    assert result.ndim == 2
    np.testing.assert_array_equal(result, value)


@pytest.mark.xfail(
    strict=False, reason='audit item 51: EXR channel order scrambled at >10 channels'
)
def test_codec_roundtrip_exr_many_channels():
    np = pytest.importorskip('numpy')
    from barecat.io.codecs import _HAS_OPENEXR

    if not _HAS_OPENEXR:
        pytest.skip('OpenEXR not available')
    rng = np.random.default_rng(0)
    value = rng.random(size=(4, 6, 12), dtype=np.float32)
    result = _roundtrip('data.exr', value)
    np.testing.assert_array_equal(result, value)
