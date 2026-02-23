import os
import os.path as osp
import tempfile

import pytest

import barecat
from barecat import Barecat


def test_merge_no_prefix():
    """Test basic merge without prefix."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source archive
        with Barecat(source_path, readonly=False) as bc:
            bc['file1.txt'] = b'content1'
            bc['dir/file2.txt'] = b'content2'
            bc['dir/subdir/file3.txt'] = b'content3'

        # Create target archive
        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'

        # Merge source into target
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path)
            assert bc.verify_integrity()

        # Verify contents
        with Barecat(target_path, readonly=True) as bc:
            assert bc['existing.txt'] == b'existing'
            assert bc['file1.txt'] == b'content1'
            assert bc['dir/file2.txt'] == b'content2'
            assert bc['dir/subdir/file3.txt'] == b'content3'
            assert bc.verify_integrity()


def test_merge_with_prefix():
    """Test merge with a prefix path."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source archive
        with Barecat(source_path, readonly=False) as bc:
            bc['file1.txt'] = b'content1'
            bc['dir/file2.txt'] = b'content2'

        # Create target archive
        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'

        # Merge source into target with prefix
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='data/train')
            assert bc.verify_integrity()

        # Verify contents
        with Barecat(target_path, readonly=True) as bc:
            assert bc['existing.txt'] == b'existing'
            assert bc['data/train/file1.txt'] == b'content1'
            assert bc['data/train/dir/file2.txt'] == b'content2'
            assert bc.verify_integrity()


def test_merge_with_nested_prefix():
    """Test merge with a deeply nested prefix."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source archive
        with Barecat(source_path, readonly=False) as bc:
            bc['a.txt'] = b'a'
            bc['b/c.txt'] = b'c'

        # Create empty target archive
        with Barecat(target_path, readonly=False) as bc:
            pass

        # Merge with deep prefix
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='x/y/z')
            assert bc.verify_integrity()

        # Verify
        with Barecat(target_path, readonly=True) as bc:
            assert bc['x/y/z/a.txt'] == b'a'
            assert bc['x/y/z/b/c.txt'] == b'c'
            # Verify directory structure
            assert 'x' in bc.listdir('')
            assert 'y' in bc.listdir('x')
            assert 'z' in bc.listdir('x/y')
            assert bc.verify_integrity()


def test_merge_multiple_archives_same_prefix():
    """Test merging multiple archives into the same prefix."""
    with tempfile.TemporaryDirectory() as tempdir:
        source1_path = osp.join(tempdir, 'source1.barecat')
        source2_path = osp.join(tempdir, 'source2.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source archives
        with Barecat(source1_path, readonly=False) as bc:
            bc['file1.txt'] = b'from_source1'

        with Barecat(source2_path, readonly=False) as bc:
            bc['file2.txt'] = b'from_source2'

        # Create target and merge both
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source1_path, prefix='data')
            bc.merge_from_other_barecat(source2_path, prefix='data')
            assert bc.verify_integrity()

        # Verify
        with Barecat(target_path, readonly=True) as bc:
            assert bc['data/file1.txt'] == b'from_source1'
            assert bc['data/file2.txt'] == b'from_source2'
            assert bc.verify_integrity()


def test_merge_multiple_archives_different_prefixes():
    """Test merging multiple archives into different prefixes."""
    with tempfile.TemporaryDirectory() as tempdir:
        source1_path = osp.join(tempdir, 'source1.barecat')
        source2_path = osp.join(tempdir, 'source2.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source archives with same filenames
        with Barecat(source1_path, readonly=False) as bc:
            bc['file.txt'] = b'from_source1'
            bc['dir/nested.txt'] = b'nested1'

        with Barecat(source2_path, readonly=False) as bc:
            bc['file.txt'] = b'from_source2'
            bc['dir/nested.txt'] = b'nested2'

        # Create target and merge with different prefixes
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source1_path, prefix='archive1')
            bc.merge_from_other_barecat(source2_path, prefix='archive2')
            assert bc.verify_integrity()

        # Verify no collision
        with Barecat(target_path, readonly=True) as bc:
            assert bc['archive1/file.txt'] == b'from_source1'
            assert bc['archive2/file.txt'] == b'from_source2'
            assert bc['archive1/dir/nested.txt'] == b'nested1'
            assert bc['archive2/dir/nested.txt'] == b'nested2'
            assert bc.verify_integrity()


def test_merge_ignore_duplicates():
    """Test merge with ignore_duplicates when files overlap."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source archive
        with Barecat(source_path, readonly=False) as bc:
            bc['file.txt'] = b'from_source'
            bc['new.txt'] = b'new_content'

        # Create target with overlapping file
        with Barecat(target_path, readonly=False) as bc:
            bc['file.txt'] = b'from_target'

        # Merge with ignore_duplicates
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, ignore_duplicates=True)
            assert bc.verify_integrity()

        # Verify: original file kept, new file added
        with Barecat(target_path, readonly=True) as bc:
            assert bc['file.txt'] == b'from_target'  # original kept
            assert bc['new.txt'] == b'new_content'  # new file added
            assert bc.verify_integrity()


def test_merge_ignore_duplicates_with_prefix():
    """Test merge with ignore_duplicates and prefix."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source archive
        with Barecat(source_path, readonly=False) as bc:
            bc['file.txt'] = b'from_source'
            bc['new.txt'] = b'new_content'

        # Create target with overlapping file at prefix location
        with Barecat(target_path, readonly=False) as bc:
            bc['data/file.txt'] = b'from_target'

        # Merge with prefix and ignore_duplicates
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='data', ignore_duplicates=True)
            assert bc.verify_integrity()

        # Verify
        with Barecat(target_path, readonly=True) as bc:
            assert bc['data/file.txt'] == b'from_target'  # original kept
            assert bc['data/new.txt'] == b'new_content'  # new file added
            assert bc.verify_integrity()


def test_merge_stats_correctness():
    """Test that directory stats are correct after merge."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source archive with known structure
        with Barecat(source_path, readonly=False) as bc:
            bc['a.txt'] = b'12345'  # 5 bytes
            bc['b.txt'] = b'67890'  # 5 bytes
            bc['dir/c.txt'] = b'abc'  # 3 bytes

        # Create target
        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'xx'  # 2 bytes

        # Merge with prefix
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='prefix')

        # Verify stats
        with Barecat(target_path, readonly=True) as bc:
            # Root should have: existing.txt (2 bytes) + prefix subtree (13 bytes)
            root_info = bc.index.lookup_dir('')
            assert root_info.size_tree == 15
            assert root_info.num_files_tree == 4
            assert root_info.num_files == 1  # existing.txt
            assert root_info.num_subdirs == 1  # prefix

            # prefix dir should have source's stats
            prefix_info = bc.index.lookup_dir('prefix')
            assert prefix_info.size_tree == 13
            assert prefix_info.num_files_tree == 3
            assert prefix_info.num_files == 2  # a.txt, b.txt
            assert prefix_info.num_subdirs == 1  # dir

            assert bc.verify_integrity()


def test_merge_empty_source():
    """Test merging an empty source archive."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create empty source archive
        with Barecat(source_path, readonly=False) as bc:
            pass

        # Create target with content
        with Barecat(target_path, readonly=False) as bc:
            bc['file.txt'] = b'content'

        # Merge empty source
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='empty')
            assert bc.verify_integrity()

        # Verify target unchanged (except for empty prefix dir)
        with Barecat(target_path, readonly=True) as bc:
            assert bc['file.txt'] == b'content'
            assert bc.listdir('empty') == []
            assert bc.verify_integrity()


def test_merge_into_existing_prefix():
    """Test merging into a prefix that already has content."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source
        with Barecat(source_path, readonly=False) as bc:
            bc['new.txt'] = b'new'
            bc['subdir/file.txt'] = b'subfile'

        # Create target with existing content at prefix location
        with Barecat(target_path, readonly=False) as bc:
            bc['data/existing.txt'] = b'existing'
            bc['data/subdir/other.txt'] = b'other'

        # Merge into existing prefix
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='data')
            assert bc.verify_integrity()

        # Verify both old and new content exists
        with Barecat(target_path, readonly=True) as bc:
            assert bc['data/existing.txt'] == b'existing'
            assert bc['data/new.txt'] == b'new'
            assert bc['data/subdir/other.txt'] == b'other'
            assert bc['data/subdir/file.txt'] == b'subfile'

            # Check subdir has correct count
            subdir_info = bc.index.lookup_dir('data/subdir')
            assert subdir_info.num_files == 2

            assert bc.verify_integrity()


# =============================================================================
# EDGE CASES
# =============================================================================


def test_merge_duplicate_error_without_ignore():
    """Test that duplicate files raise error without ignore_duplicates."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['file.txt'] = b'from_source'

        with Barecat(target_path, readonly=False) as bc:
            bc['file.txt'] = b'from_target'

        with Barecat(target_path, readonly=False) as bc:
            with pytest.raises(Exception):  # IntegrityError or similar
                bc.merge_from_other_barecat(source_path, ignore_duplicates=False)


def test_merge_duplicate_error_with_prefix():
    """Test duplicate error when prefix causes collision."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['file.txt'] = b'from_source'

        with Barecat(target_path, readonly=False) as bc:
            bc['data/file.txt'] = b'from_target'

        with Barecat(target_path, readonly=False) as bc:
            with pytest.raises(Exception):
                bc.merge_from_other_barecat(source_path, prefix='data', ignore_duplicates=False)


def test_merge_single_file_at_root():
    """Test merging source with only one file at root (no directories)."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['only_file.txt'] = b'only'

        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='imported')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['imported/only_file.txt'] == b'only'
            assert bc['existing.txt'] == b'existing'
            assert bc.verify_integrity()


def test_merge_into_empty_target():
    """Test merging into a completely empty target archive."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['a.txt'] = b'a'
            bc['b/c.txt'] = b'c'

        with Barecat(target_path, readonly=False) as bc:
            pass  # empty target

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path)
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['a.txt'] == b'a'
            assert bc['b/c.txt'] == b'c'
            assert bc.verify_integrity()


def test_merge_empty_source_no_prefix():
    """Test merging empty source without prefix."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            pass

        with Barecat(target_path, readonly=False) as bc:
            bc['file.txt'] = b'content'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path)  # no prefix
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['file.txt'] == b'content'
            assert bc.num_files == 1
            assert bc.verify_integrity()


def test_merge_empty_into_empty():
    """Test merging empty source into empty target."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            pass

        with Barecat(target_path, readonly=False) as bc:
            pass

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='empty')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc.num_files == 0
            assert bc.verify_integrity()


def test_merge_multi_shard_source():
    """Test merging source that spans multiple shards."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source with small shard limit to force multiple shards
        with Barecat(source_path, readonly=False, shard_size_limit=100) as bc:
            bc['file1.txt'] = b'x' * 50
            bc['file2.txt'] = b'y' * 50
            bc['file3.txt'] = b'z' * 50  # should go to shard 1
            bc['dir/file4.txt'] = b'w' * 50

        # Verify source has multiple shards
        assert osp.exists(f'{source_path}-shard-00001')

        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='data')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['existing.txt'] == b'existing'
            assert bc['data/file1.txt'] == b'x' * 50
            assert bc['data/file2.txt'] == b'y' * 50
            assert bc['data/file3.txt'] == b'z' * 50
            assert bc['data/dir/file4.txt'] == b'w' * 50
            assert bc.verify_integrity()


def test_merge_causes_shard_rotation_in_target():
    """Test that target rotates shards correctly during merge."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['big1.txt'] = b'a' * 100
            bc['big2.txt'] = b'b' * 100

        # Target with small shard limit
        with Barecat(target_path, readonly=False, shard_size_limit=150) as bc:
            bc['existing.txt'] = b'x' * 50

        with Barecat(target_path, readonly=False, shard_size_limit=150) as bc:
            bc.merge_from_other_barecat(source_path)
            assert bc.verify_integrity()

        # Verify target has multiple shards after merge
        assert osp.exists(f'{target_path}-shard-00001')

        with Barecat(target_path, readonly=True) as bc:
            assert bc['existing.txt'] == b'x' * 50
            assert bc['big1.txt'] == b'a' * 100
            assert bc['big2.txt'] == b'b' * 100
            assert bc.verify_integrity()


def test_merge_all_duplicates_ignored():
    """Test merge where ALL source files are duplicates (with ignore)."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['a.txt'] = b'source_a'
            bc['b.txt'] = b'source_b'
            bc['dir/c.txt'] = b'source_c'

        # Target has all the same paths
        with Barecat(target_path, readonly=False) as bc:
            bc['a.txt'] = b'target_a'
            bc['b.txt'] = b'target_b'
            bc['dir/c.txt'] = b'target_c'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, ignore_duplicates=True)
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            # All original values kept
            assert bc['a.txt'] == b'target_a'
            assert bc['b.txt'] == b'target_b'
            assert bc['dir/c.txt'] == b'target_c'
            assert bc.num_files == 3
            assert bc.verify_integrity()


def test_merge_deeply_nested_source():
    """Test merging source with very deep directory nesting."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        deep_path = '/'.join(['d'] * 20) + '/file.txt'

        with Barecat(source_path, readonly=False) as bc:
            bc[deep_path] = b'deep'

        with Barecat(target_path, readonly=False) as bc:
            bc['shallow.txt'] = b'shallow'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='imported')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['shallow.txt'] == b'shallow'
            assert bc['imported/' + deep_path] == b'deep'
            assert bc.verify_integrity()


def test_merge_unicode_paths():
    """Test merging with unicode characters in paths."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['日本語/ファイル.txt'] = b'japanese'
            bc['émojis/\U0001f389\U0001f38a.txt'] = b'party'
            bc['Ü∈ñîçödé.txt'] = b'unicode'

        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='data')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['data/日本語/ファイル.txt'] == b'japanese'
            assert bc['data/émojis/\U0001f389\U0001f38a.txt'] == b'party'
            assert bc['data/Ü∈ñîçödé.txt'] == b'unicode'
            assert bc.verify_integrity()


def test_merge_many_small_files():
    """Test merging source with many small files."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            for i in range(500):
                bc[f'files/file_{i:04d}.txt'] = f'content_{i}'.encode()

        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='imported')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc.num_files == 501
            assert bc['imported/files/file_0000.txt'] == b'content_0'
            assert bc['imported/files/file_0499.txt'] == b'content_499'
            assert bc.verify_integrity()


def test_merge_preserves_file_metadata():
    """Test that file metadata (mode, mtime, etc.) is preserved during merge."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')
        from io import BytesIO

        with Barecat(source_path, readonly=False) as bc:
            bc.add(
                barecat.BarecatFileInfo(
                    'file.txt', size=5, mode=0o755, mtime_ns=1234567890000000000
                ),
                fileobj=BytesIO(b'hello'),
            )

        with Barecat(target_path, readonly=False) as bc:
            pass

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='data')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            info = bc.index.lookup_file('data/file.txt')
            assert info.mode == 0o755
            assert info.mtime_ns == 1234567890000000000
            assert bc.verify_integrity()


def test_merge_sequential_same_prefix():
    """Test multiple sequential merges into the same prefix."""
    with tempfile.TemporaryDirectory() as tempdir:
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(target_path, readonly=False) as bc:
            pass

        for i in range(5):
            source_path = osp.join(tempdir, f'source{i}.barecat')
            with Barecat(source_path, readonly=False) as bc:
                bc[f'file{i}.txt'] = f'content{i}'.encode()
                bc[f'subdir/nested{i}.txt'] = f'nested{i}'.encode()

            with Barecat(target_path, readonly=False) as bc:
                bc.merge_from_other_barecat(source_path, prefix='data')
                assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc.num_files == 10
            for i in range(5):
                assert bc[f'data/file{i}.txt'] == f'content{i}'.encode()
                assert bc[f'data/subdir/nested{i}.txt'] == f'nested{i}'.encode()
            assert bc.verify_integrity()


def test_merge_partial_overlap_dirs():
    """Test merge where source and target have overlapping directory structure."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['shared/new.txt'] = b'new'
            bc['shared/sub/new_nested.txt'] = b'new_nested'
            bc['only_source/file.txt'] = b'only_source'

        with Barecat(target_path, readonly=False) as bc:
            bc['shared/existing.txt'] = b'existing'
            bc['shared/sub/existing_nested.txt'] = b'existing_nested'
            bc['only_target/file.txt'] = b'only_target'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path)
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['shared/new.txt'] == b'new'
            assert bc['shared/existing.txt'] == b'existing'
            assert bc['shared/sub/new_nested.txt'] == b'new_nested'
            assert bc['shared/sub/existing_nested.txt'] == b'existing_nested'
            assert bc['only_source/file.txt'] == b'only_source'
            assert bc['only_target/file.txt'] == b'only_target'

            # Check stats
            shared_info = bc.index.lookup_dir('shared')
            assert shared_info.num_files == 2
            assert shared_info.num_subdirs == 1
            assert shared_info.num_files_tree == 4

            assert bc.verify_integrity()


def test_merge_source_only_directories():
    """Test merging source that has explicit directory entries but no files."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source with a file, then delete it to leave empty dirs
        with Barecat(source_path, readonly=False) as bc:
            bc['dir/subdir/file.txt'] = b'temp'
            del bc['dir/subdir/file.txt']

        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='imported')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['existing.txt'] == b'existing'
            # Empty directory structure should exist
            assert 'imported' in bc.listdir('')
            assert bc.verify_integrity()


def test_merge_binary_content():
    """Test merging files with various binary content including null bytes."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        binary_data = bytes(range(256)) * 10  # All possible byte values

        with Barecat(source_path, readonly=False) as bc:
            bc['binary.bin'] = binary_data
            bc['nulls.bin'] = b'\x00' * 1000
            bc['mixed.bin'] = b'\x00\xff\x00\xff' * 250

        with Barecat(target_path, readonly=False) as bc:
            pass

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='data')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['data/binary.bin'] == binary_data
            assert bc['data/nulls.bin'] == b'\x00' * 1000
            assert bc['data/mixed.bin'] == b'\x00\xff\x00\xff' * 250
            assert bc.verify_integrity()


def test_merge_with_shard_size_larger_than_source():
    """Test merge where target shard limit is larger than entire source."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['small.txt'] = b'tiny'

        with Barecat(target_path, readonly=False, shard_size_limit=1000000) as bc:
            bc['existing.txt'] = b'existing'

        with Barecat(target_path, readonly=False, shard_size_limit=1000000) as bc:
            bc.merge_from_other_barecat(source_path)
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['small.txt'] == b'tiny'
            assert bc['existing.txt'] == b'existing'
            assert bc.verify_integrity()


def test_merge_large_file():
    """Test merging a large file (bigger than typical buffer sizes)."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        large_data = os.urandom(1024 * 1024)  # 1 MB

        with Barecat(source_path, readonly=False) as bc:
            bc['large.bin'] = large_data

        with Barecat(target_path, readonly=False) as bc:
            pass

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='data')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['data/large.bin'] == large_data
            assert bc.verify_integrity()


def test_merge_stats_with_ignore_duplicates_complex():
    """Test that stats are correct after ignore_duplicates with complex overlap."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Source has mix of new and duplicate files
        with Barecat(source_path, readonly=False) as bc:
            bc['dup1.txt'] = b'12345'  # 5 bytes, will be skipped
            bc['new1.txt'] = b'abc'  # 3 bytes, will be added
            bc['dir/dup2.txt'] = b'xyz'  # 3 bytes, will be skipped
            bc['dir/new2.txt'] = b'qwerty'  # 6 bytes, will be added

        with Barecat(target_path, readonly=False) as bc:
            bc['dup1.txt'] = b'AAAAA'  # 5 bytes
            bc['dir/dup2.txt'] = b'BBB'  # 3 bytes
            bc['other.txt'] = b'CC'  # 2 bytes

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, ignore_duplicates=True)
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            # Original files kept
            assert bc['dup1.txt'] == b'AAAAA'
            assert bc['dir/dup2.txt'] == b'BBB'
            assert bc['other.txt'] == b'CC'
            # New files added
            assert bc['new1.txt'] == b'abc'
            assert bc['dir/new2.txt'] == b'qwerty'

            # Check stats
            root_info = bc.index.lookup_dir('')
            assert root_info.num_files == 3  # dup1, new1, other
            assert root_info.num_files_tree == 5  # all files
            # 5 (AAAAA) + 3 (abc) + 2 (CC) + 3 (BBB) + 6 (qwerty) = 19
            assert root_info.size_tree == 19

            dir_info = bc.index.lookup_dir('dir')
            assert dir_info.num_files == 2  # dup2, new2
            assert dir_info.size_tree == 9  # 3 + 6

            assert bc.verify_integrity()


def test_merge_crc32c_integrity():
    """Test that CRC32C checksums are correct after merge."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        test_data = b'test data for crc verification'

        with Barecat(source_path, readonly=False) as bc:
            bc['file.txt'] = test_data

        with Barecat(target_path, readonly=False) as bc:
            pass

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='data')

        with Barecat(target_path, readonly=True) as bc:
            # verify_integrity checks CRC32C
            assert bc.verify_integrity()
            # Also verify content matches
            assert bc['data/file.txt'] == test_data


def test_merge_special_filenames():
    """Test merging files with special characters in names."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['file with spaces.txt'] = b'spaces'
            bc['file-with-dashes.txt'] = b'dashes'
            bc['file_with_underscores.txt'] = b'underscores'
            bc['file.multiple.dots.txt'] = b'dots'
            bc["file'with'quotes.txt"] = b'quotes'
            bc['file(with)parens.txt'] = b'parens'
            bc['file[with]brackets.txt'] = b'brackets'

        with Barecat(target_path, readonly=False) as bc:
            pass

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path)
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['file with spaces.txt'] == b'spaces'
            assert bc['file.multiple.dots.txt'] == b'dots'
            assert bc.verify_integrity()


def test_merge_prefix_conflicts_with_file():
    """Test that merge fails if prefix conflicts with an existing file."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['file.txt'] = b'source'

        # Target has 'data' as a FILE
        with Barecat(target_path, readonly=False) as bc:
            bc['data'] = b'im a file not a dir'

        # Merge with prefix='data' should fail
        with Barecat(target_path, readonly=False) as bc:
            with pytest.raises(Exception):
                bc.merge_from_other_barecat(source_path, prefix='data')


def test_merge_prefix_conflicts_with_file_nested():
    """Test that merge fails if any prefix component conflicts with a file."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['file.txt'] = b'source'

        # Target has 'a/b' as a FILE
        with Barecat(target_path, readonly=False) as bc:
            bc['a/b'] = b'im a file'

        # Merge with prefix='a/b/c' should fail (b is a file, not a dir)
        with Barecat(target_path, readonly=False) as bc:
            with pytest.raises(Exception):
                bc.merge_from_other_barecat(source_path, prefix='a/b/c')


def test_merge_source_file_conflicts_with_target_dir():
    """Test that merge fails if source file path matches target directory."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Source has 'data' as a FILE
        with Barecat(source_path, readonly=False) as bc:
            bc['data'] = b'im a file'

        # Target has 'data' as a DIRECTORY (with a file inside)
        with Barecat(target_path, readonly=False) as bc:
            bc['data/file.txt'] = b'nested'

        with Barecat(target_path, readonly=False) as bc:
            with pytest.raises(ValueError, match='conflicts with target directory'):
                bc.merge_from_other_barecat(source_path)


def test_merge_source_dir_conflicts_with_target_file():
    """Test that merge fails if source directory path matches target file."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Source has 'data' as a DIRECTORY
        with Barecat(source_path, readonly=False) as bc:
            bc['data/file.txt'] = b'nested'

        # Target has 'data' as a FILE
        with Barecat(target_path, readonly=False) as bc:
            bc['data'] = b'im a file'

        with Barecat(target_path, readonly=False) as bc:
            with pytest.raises(ValueError, match='conflicts with target file'):
                bc.merge_from_other_barecat(source_path)


def test_merge_preserves_empty_files():
    """Test that empty files are preserved during merge."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['empty.txt'] = b''
            bc['nonempty.txt'] = b'content'
            bc['dir/empty_nested.txt'] = b''

        with Barecat(target_path, readonly=False) as bc:
            pass

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, prefix='data')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['data/empty.txt'] == b''
            assert bc['data/nonempty.txt'] == b'content'
            assert bc['data/dir/empty_nested.txt'] == b''
            assert bc.num_files == 3
            assert bc.verify_integrity()


# ============================================================================
# Symlink-based merge tests (Index.merge_from_other_barecat)
# ============================================================================


def symlink_shards(source_path, target_path, target_num_shards):
    """Symlink source shards to target with adjusted numbering."""
    shard_num = 0
    while True:
        src_shard = f'{source_path}-shard-{shard_num:05d}'
        if not osp.exists(src_shard):
            break
        dst_shard = f'{target_path}-shard-{target_num_shards + shard_num:05d}'
        os.symlink(osp.abspath(src_shard), dst_shard)
        shard_num += 1
    return shard_num


def test_symlink_merge_no_prefix():
    """Test symlink-based merge without prefix."""
    from barecat.core.index import Index

    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['file1.txt'] = b'source1'
            bc['dir/file2.txt'] = b'source2'

        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'
            target_num_shards = bc.sharder.num_shards

        # Symlink source shards to target
        symlink_shards(source_path, target_path, target_num_shards)

        # Merge indexes only (using Index directly, no shard access needed)
        with Index(target_path, readonly=False) as idx:
            idx.merge_from_other_barecat(source_path)

        with Barecat(target_path, readonly=True) as bc:
            assert bc['existing.txt'] == b'existing'
            assert bc['file1.txt'] == b'source1'
            assert bc['dir/file2.txt'] == b'source2'
            assert bc.num_files == 3
            assert bc.verify_integrity()


def test_symlink_merge_with_prefix():
    """Test symlink-based merge with prefix."""
    from barecat.core.index import Index

    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['file1.txt'] = b'source1'
            bc['dir/file2.txt'] = b'source2'

        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'
            target_num_shards = bc.sharder.num_shards

        symlink_shards(source_path, target_path, target_num_shards)

        with Index(target_path, readonly=False) as idx:
            idx.merge_from_other_barecat(source_path, prefix='imported')

        with Barecat(target_path, readonly=True) as bc:
            assert bc['existing.txt'] == b'existing'
            assert bc['imported/file1.txt'] == b'source1'
            assert bc['imported/dir/file2.txt'] == b'source2'
            assert bc.num_files == 3
            assert bc.verify_integrity()


def test_symlink_merge_ignore_duplicates():
    """Test symlink-based merge with ignore_duplicates."""
    from barecat.core.index import Index

    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['file1.txt'] = b'source_version'
            bc['unique.txt'] = b'unique'

        with Barecat(target_path, readonly=False) as bc:
            bc['file1.txt'] = b'target_version'
            target_num_shards = bc.sharder.num_shards

        symlink_shards(source_path, target_path, target_num_shards)

        with Index(target_path, readonly=False) as idx:
            idx.merge_from_other_barecat(source_path, ignore_duplicates=True)

        with Barecat(target_path, readonly=True) as bc:
            # Target version should be preserved
            assert bc['file1.txt'] == b'target_version'
            assert bc['unique.txt'] == b'unique'
            assert bc.num_files == 2
            assert bc.verify_integrity()


def test_symlink_merge_conflict_file_vs_dir():
    """Test symlink merge fails when source file conflicts with target dir."""
    from barecat.core.index import Index

    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['data'] = b'im a file'

        with Barecat(target_path, readonly=False) as bc:
            bc['data/nested.txt'] = b'nested'
            target_num_shards = bc.sharder.num_shards

        symlink_shards(source_path, target_path, target_num_shards)

        with Index(target_path, readonly=False) as idx:
            with pytest.raises(ValueError, match='conflicts with target directory'):
                idx.merge_from_other_barecat(source_path)


def test_symlink_merge_conflict_dir_vs_file():
    """Test symlink merge fails when source dir conflicts with target file."""
    from barecat.core.index import Index

    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['data/nested.txt'] = b'nested'

        with Barecat(target_path, readonly=False) as bc:
            bc['data'] = b'im a file'
            target_num_shards = bc.sharder.num_shards

        symlink_shards(source_path, target_path, target_num_shards)

        with Index(target_path, readonly=False) as idx:
            with pytest.raises(ValueError, match='conflicts with target file'):
                idx.merge_from_other_barecat(source_path)


def test_symlink_merge_prefix_conflicts_with_file():
    """Test symlink merge fails when prefix conflicts with existing file."""
    from barecat.core.index import Index

    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['file.txt'] = b'content'

        with Barecat(target_path, readonly=False) as bc:
            bc['imported'] = b'im a file, not a dir'
            target_num_shards = bc.sharder.num_shards

        symlink_shards(source_path, target_path, target_num_shards)

        with Index(target_path, readonly=False) as idx:
            with pytest.raises(ValueError, match='exists as a file'):
                idx.merge_from_other_barecat(source_path, prefix='imported')


def test_symlink_merge_multi_shard():
    """Test symlink-based merge with multi-shard source."""
    from barecat.core.index import Index

    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create multi-shard source
        with Barecat(source_path, readonly=False, shard_size_limit=100) as bc:
            for i in range(20):
                bc[f'file{i}.txt'] = f'content{i}'.encode() * 10

        with Barecat(source_path, readonly=True) as bc:
            source_num_shards = bc.sharder.num_shards
            assert source_num_shards > 1, 'Source should have multiple shards'

        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'
            target_num_shards = bc.sharder.num_shards

        num_linked = symlink_shards(source_path, target_path, target_num_shards)
        assert num_linked == source_num_shards

        with Index(target_path, readonly=False) as idx:
            idx.merge_from_other_barecat(source_path, prefix='data')

        with Barecat(target_path, readonly=True) as bc:
            assert bc['existing.txt'] == b'existing'
            for i in range(20):
                assert bc[f'data/file{i}.txt'] == f'content{i}'.encode() * 10
            assert bc.num_files == 21
            assert bc.verify_integrity()


def test_merge_with_pattern():
    """Test merge with a glob pattern filter."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source archive with mixed file types
        with Barecat(source_path, readonly=False) as bc:
            bc['file1.txt'] = b'text1'
            bc['file2.txt'] = b'text2'
            bc['image1.jpg'] = b'jpg1'
            bc['image2.jpg'] = b'jpg2'
            bc['dir/file3.txt'] = b'text3'
            bc['dir/image3.jpg'] = b'jpg3'
            bc['dir/sub/file4.txt'] = b'text4'

        # Create target archive
        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'

        # Merge only .jpg files
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='**/*.jpg')
            assert bc.verify_integrity()

        # Verify only jpg files were merged
        with Barecat(target_path, readonly=True) as bc:
            assert bc['existing.txt'] == b'existing'
            assert bc['image1.jpg'] == b'jpg1'
            assert bc['image2.jpg'] == b'jpg2'
            assert bc['dir/image3.jpg'] == b'jpg3'
            assert bc.num_files == 4
            assert 'file1.txt' not in bc
            assert 'dir/file3.txt' not in bc
            assert bc.verify_integrity()


def test_merge_with_pattern_and_prefix():
    """Test merge with pattern and prefix."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['a.txt'] = b'a'
            bc['b.jpg'] = b'b'
            bc['sub/c.txt'] = b'c'
            bc['sub/d.jpg'] = b'd'

        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='**/*.jpg', prefix='images')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['existing.txt'] == b'existing'
            assert bc['images/b.jpg'] == b'b'
            assert bc['images/sub/d.jpg'] == b'd'
            assert bc.num_files == 3
            assert 'images/a.txt' not in bc
            assert bc.verify_integrity()


def test_merge_with_filter_rules():
    """Test merge with rsync-style include/exclude rules."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['keep.txt'] = b'keep'
            bc['skip.log'] = b'skip'
            bc['data/important.txt'] = b'important'
            bc['data/cache.tmp'] = b'cache'
            bc['data/sub/file.txt'] = b'file'
            bc['thumbs/small.jpg'] = b'small'
            bc['thumbs/important.jpg'] = b'important_thumb'

        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'

        # Include all txt, exclude thumbs except important.jpg
        filter_rules = [
            ('+', '**/important.jpg'),  # include this specific file
            ('-', 'thumbs/**'),  # exclude thumbs dir
            ('-', '**/*.tmp'),  # exclude temp files
            ('-', '**/*.log'),  # exclude log files
        ]

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, filter_rules=filter_rules)
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['existing.txt'] == b'existing'
            assert bc['keep.txt'] == b'keep'
            assert bc['data/important.txt'] == b'important'
            assert bc['data/sub/file.txt'] == b'file'
            assert bc['thumbs/important.jpg'] == b'important_thumb'
            assert 'skip.log' not in bc
            assert 'data/cache.tmp' not in bc
            assert 'thumbs/small.jpg' not in bc
            assert bc.verify_integrity()


def test_merge_filtered_contiguous_blocks():
    """Test that filtered merge efficiently copies contiguous blocks."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source with files that will be contiguous
        with Barecat(source_path, readonly=False) as bc:
            # These will be written contiguously
            bc['a.txt'] = b'a' * 100
            bc['b.txt'] = b'b' * 100
            bc['c.txt'] = b'c' * 100
            bc['d.jpg'] = b'd' * 100  # not matched
            bc['e.txt'] = b'e' * 100
            bc['f.txt'] = b'f' * 100

        with Barecat(target_path, readonly=False) as bc:
            pass  # empty target

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='**/*.txt')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['a.txt'] == b'a' * 100
            assert bc['b.txt'] == b'b' * 100
            assert bc['c.txt'] == b'c' * 100
            assert bc['e.txt'] == b'e' * 100
            assert bc['f.txt'] == b'f' * 100
            assert 'd.jpg' not in bc
            assert bc.num_files == 5
            assert bc.verify_integrity()


def test_merge_filtered_with_shard_rotation():
    """Test filtered merge with shard size limit causing rotation."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        # Create source with enough data to span shards
        with Barecat(source_path, readonly=False) as bc:
            for i in range(10):
                bc[f'file{i}.txt'] = b'x' * 100
                bc[f'file{i}.log'] = b'y' * 100  # won't be merged

        # Target with small shard size
        with Barecat(target_path, readonly=False, shard_size_limit=250) as bc:
            bc['existing.txt'] = b'existing'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='**/*.txt')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            for i in range(10):
                assert bc[f'file{i}.txt'] == b'x' * 100
                assert f'file{i}.log' not in bc
            assert bc.num_files == 11  # 10 + existing
            assert bc.verify_integrity()


def test_merge_filtered_empty_result():
    """Test filtered merge when no files match."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['file1.txt'] = b'text'
            bc['file2.txt'] = b'text'

        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='**/*.jpg')
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc.num_files == 1
            assert bc['existing.txt'] == b'existing'
            assert bc.verify_integrity()


def test_merge_filtered_ignore_duplicates():
    """Test filtered merge with ignore_duplicates."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['dup.txt'] = b'source_version'
            bc['new.txt'] = b'new_file'
            bc['other.log'] = b'log'

        with Barecat(target_path, readonly=False) as bc:
            bc['dup.txt'] = b'target_version'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='**/*.txt', ignore_duplicates=True)
            assert bc.verify_integrity()

        with Barecat(target_path, readonly=True) as bc:
            assert bc['dup.txt'] == b'target_version'  # not overwritten
            assert bc['new.txt'] == b'new_file'
            assert 'other.log' not in bc
            assert bc.num_files == 2
            assert bc.verify_integrity()


# ============================================================================
# Edge case tests for filter mechanism
# ============================================================================


def test_filter_bracket_expressions():
    """Test glob bracket expressions [abc], [!abc], [a-z]."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['file_a.txt'] = b'a'
            bc['file_b.txt'] = b'b'
            bc['file_c.txt'] = b'c'
            bc['file_x.txt'] = b'x'
            bc['file_1.txt'] = b'1'

        with Barecat(target_path, readonly=False) as bc:
            # Include only files matching [abc]
            bc.merge_from_other_barecat(source_path, pattern='**/file_[abc].txt')

        with Barecat(target_path, readonly=True) as bc:
            assert bc['file_a.txt'] == b'a'
            assert bc['file_b.txt'] == b'b'
            assert bc['file_c.txt'] == b'c'
            assert 'file_x.txt' not in bc
            assert 'file_1.txt' not in bc
            assert bc.verify_integrity()


def test_filter_negated_bracket():
    """Test negated bracket [!abc]."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['file_a.txt'] = b'a'
            bc['file_b.txt'] = b'b'
            bc['file_x.txt'] = b'x'
            bc['file_y.txt'] = b'y'

        with Barecat(target_path, readonly=False) as bc:
            # Include files NOT matching [ab]
            bc.merge_from_other_barecat(source_path, pattern='**/file_[!ab].txt')

        with Barecat(target_path, readonly=True) as bc:
            assert 'file_a.txt' not in bc
            assert 'file_b.txt' not in bc
            assert bc['file_x.txt'] == b'x'
            assert bc['file_y.txt'] == b'y'
            assert bc.verify_integrity()


def test_filter_question_mark():
    """Test ? wildcard matching single character."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['a.txt'] = b'a'
            bc['ab.txt'] = b'ab'
            bc['abc.txt'] = b'abc'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='**/?.txt')

        with Barecat(target_path, readonly=True) as bc:
            assert bc['a.txt'] == b'a'
            assert 'ab.txt' not in bc
            assert 'abc.txt' not in bc
            assert bc.verify_integrity()


def test_filter_first_match_wins_order_matters():
    """Test that rule order matters (first match wins)."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target1_path = osp.join(tempdir, 'target1.barecat')
        target2_path = osp.join(tempdir, 'target2.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['data/file.txt'] = b'file'

        # Order 1: include first, then exclude
        with Barecat(target1_path, readonly=False) as bc:
            bc.merge_from_other_barecat(
                source_path,
                filter_rules=[
                    ('+', '**/*.txt'),  # matches first -> included
                    ('-', 'data/**'),  # never reached
                ],
            )

        # Order 2: exclude first, then include
        with Barecat(target2_path, readonly=False) as bc:
            bc.merge_from_other_barecat(
                source_path,
                filter_rules=[
                    ('-', 'data/**'),  # matches first -> excluded
                    ('+', '**/*.txt'),  # never reached
                ],
            )

        with Barecat(target1_path, readonly=True) as bc:
            assert 'data/file.txt' in bc  # included (order 1)

        with Barecat(target2_path, readonly=True) as bc:
            assert 'data/file.txt' not in bc  # excluded (order 2)


def test_filter_all_excludes_default_include():
    """Test all exclude rules with default_include=True."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['keep.txt'] = b'keep'
            bc['skip.log'] = b'skip'
            bc['skip.tmp'] = b'tmp'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(
                source_path,
                filter_rules=[
                    ('-', '**/*.log'),
                    ('-', '**/*.tmp'),
                ],
            )

        with Barecat(target_path, readonly=True) as bc:
            assert bc['keep.txt'] == b'keep'  # default include
            assert 'skip.log' not in bc
            assert 'skip.tmp' not in bc
            assert bc.verify_integrity()


def test_filter_hidden_files():
    """Test patterns with hidden files (dotfiles)."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['.hidden'] = b'hidden'
            bc['.config/settings'] = b'settings'
            bc['visible.txt'] = b'visible'
            bc['dir/.gitignore'] = b'gitignore'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(
                source_path,
                filter_rules=[
                    ('-', '**/.*'),  # exclude hidden files
                ],
            )

        with Barecat(target_path, readonly=True) as bc:
            assert bc['visible.txt'] == b'visible'
            # Hidden files should be excluded
            assert '.hidden' not in bc
            assert 'dir/.gitignore' not in bc
            assert bc.verify_integrity()


def test_filter_deeply_nested():
    """Test patterns with deeply nested paths."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['a/b/c/d/e/f/deep.txt'] = b'deep'
            bc['a/b/c/d/e/f/deep.log'] = b'log'
            bc['shallow.txt'] = b'shallow'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='**/*.txt')

        with Barecat(target_path, readonly=True) as bc:
            assert bc['a/b/c/d/e/f/deep.txt'] == b'deep'
            assert bc['shallow.txt'] == b'shallow'
            assert 'a/b/c/d/e/f/deep.log' not in bc
            assert bc.verify_integrity()


def test_filter_unicode_paths():
    """Test patterns with unicode in paths."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['données/файл.txt'] = b'unicode'
            bc['données/skip.log'] = b'skip'
            bc['日本語/文書.txt'] = b'japanese'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='**/*.txt')

        with Barecat(target_path, readonly=True) as bc:
            assert bc['données/файл.txt'] == b'unicode'
            assert bc['日本語/文書.txt'] == b'japanese'
            assert 'données/skip.log' not in bc
            assert bc.verify_integrity()


def test_filter_empty_rules():
    """Test empty filter rules (should include all with default_include=True)."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['a.txt'] = b'a'
            bc['b.txt'] = b'b'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, filter_rules=[])

        with Barecat(target_path, readonly=True) as bc:
            assert bc['a.txt'] == b'a'
            assert bc['b.txt'] == b'b'
            assert bc.verify_integrity()


def test_filter_pattern_matches_nothing():
    """Test pattern that matches no files."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['a.txt'] = b'a'
            bc['b.txt'] = b'b'

        with Barecat(target_path, readonly=False) as bc:
            bc['existing.txt'] = b'existing'
            bc.merge_from_other_barecat(source_path, pattern='**/*.nonexistent')

        with Barecat(target_path, readonly=True) as bc:
            assert bc['existing.txt'] == b'existing'
            assert 'a.txt' not in bc
            assert 'b.txt' not in bc
            assert bc.num_files == 1
            assert bc.verify_integrity()


def test_filter_overlapping_patterns():
    """Test overlapping include/exclude patterns."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['data/important/file.txt'] = b'important'
            bc['data/cache/file.txt'] = b'cache'
            bc['data/other/file.txt'] = b'other'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(
                source_path,
                filter_rules=[
                    ('+', '**/important/**'),  # include important subdir
                    ('-', 'data/**'),  # exclude all of data
                    ('+', '**/*.txt'),  # this won't matter for data/*
                ],
            )

        with Barecat(target_path, readonly=True) as bc:
            assert bc['data/important/file.txt'] == b'important'  # matched first rule
            assert 'data/cache/file.txt' not in bc  # matched second rule
            assert 'data/other/file.txt' not in bc  # matched second rule
            assert bc.verify_integrity()


def test_filter_literal_special_chars():
    """Test patterns with literal special characters in filenames."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['file[1].txt'] = b'bracket'
            bc['file(2).txt'] = b'paren'
            bc['file{3}.txt'] = b'brace'
            bc['file+4.txt'] = b'plus'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='**/*.txt')

        with Barecat(target_path, readonly=True) as bc:
            assert bc['file[1].txt'] == b'bracket'
            assert bc['file(2).txt'] == b'paren'
            assert bc['file{3}.txt'] == b'brace'
            assert bc['file+4.txt'] == b'plus'
            assert bc.verify_integrity()


def test_filter_doublestar_edge_cases():
    """Test ** edge cases: at start, middle, end, alone."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['root.txt'] = b'root'
            bc['a/mid.txt'] = b'mid'
            bc['a/b/deep.txt'] = b'deep'
            bc['a/b/c/deeper.txt'] = b'deeper'

        # Test **/file (at start)
        target1 = osp.join(tempdir, 't1.barecat')
        with Barecat(target1, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='**/deep.txt')
        with Barecat(target1, readonly=True) as bc:
            assert bc['a/b/deep.txt'] == b'deep'
            assert bc.num_files == 1

        # Test dir/** (at end)
        target2 = osp.join(tempdir, 't2.barecat')
        with Barecat(target2, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='a/b/**')
        with Barecat(target2, readonly=True) as bc:
            assert bc['a/b/deep.txt'] == b'deep'
            assert bc['a/b/c/deeper.txt'] == b'deeper'
            assert 'a/mid.txt' not in bc

        # Test a/**/c (in middle)
        target3 = osp.join(tempdir, 't3.barecat')
        with Barecat(target3, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='a/**/c/*')
        with Barecat(target3, readonly=True) as bc:
            assert bc['a/b/c/deeper.txt'] == b'deeper'
            assert bc.num_files == 1

        # Test ** alone (everything)
        target4 = osp.join(tempdir, 't4.barecat')
        with Barecat(target4, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='**')
        with Barecat(target4, readonly=True) as bc:
            assert bc.num_files == 4


def test_filter_consecutive_wildcards():
    """Test patterns with consecutive wildcards like ***."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['a.txt'] = b'a'
            bc['aa.txt'] = b'aa'
            bc['aaa.txt'] = b'aaa'

        # *** should behave like * (consecutive stars collapse)
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(source_path, pattern='***.txt')

        with Barecat(target_path, readonly=True) as bc:
            assert bc['a.txt'] == b'a'
            assert bc['aa.txt'] == b'aa'
            assert bc['aaa.txt'] == b'aaa'
            assert bc.verify_integrity()


def test_filter_root_level_only():
    """Test patterns that should only match root level."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['root.txt'] = b'root'
            bc['sub/nested.txt'] = b'nested'

        with Barecat(target_path, readonly=False) as bc:
            # *.txt without ** should only match root
            bc.merge_from_other_barecat(source_path, pattern='*.txt')

        with Barecat(target_path, readonly=True) as bc:
            assert bc['root.txt'] == b'root'
            assert 'sub/nested.txt' not in bc
            assert bc.verify_integrity()


def test_filter_include_then_exclude_same_pattern():
    """Test include then exclude with overlapping pattern specificity."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['logs/app.log'] = b'app'
            bc['logs/error.log'] = b'error'
            bc['logs/debug.log'] = b'debug'

        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(
                source_path,
                filter_rules=[
                    ('+', '**/error.log'),  # specifically include error.log
                    ('-', '**/*.log'),  # exclude all other logs
                ],
            )

        with Barecat(target_path, readonly=True) as bc:
            assert bc['logs/error.log'] == b'error'
            assert 'logs/app.log' not in bc
            assert 'logs/debug.log' not in bc
            assert bc.verify_integrity()


def test_filter_multiple_extensions():
    """Test pattern matching multiple extensions."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['image.jpg'] = b'jpg'
            bc['image.jpeg'] = b'jpeg'
            bc['image.png'] = b'png'
            bc['image.gif'] = b'gif'
            bc['doc.txt'] = b'txt'

        # Use bracket to match multiple extensions
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(
                source_path,
                filter_rules=[
                    ('+', '**/*.jpg'),
                    ('+', '**/*.jpeg'),
                    ('+', '**/*.png'),
                    ('-', '**/*'),  # exclude everything else
                ],
            )

        with Barecat(target_path, readonly=True) as bc:
            assert bc['image.jpg'] == b'jpg'
            assert bc['image.jpeg'] == b'jpeg'
            assert bc['image.png'] == b'png'
            assert 'image.gif' not in bc
            assert 'doc.txt' not in bc
            assert bc.verify_integrity()


def test_filter_trailing_slash_pattern():
    """Test that trailing slash patterns match directory and contents (rsync convention).

    In rsync, 'dir/' means the directory and everything inside it.
    The CLI normalizes 'dir/' to 'dir/**' before passing to the filter.
    """
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['thumbs/a.jpg'] = b'thumb_a'
            bc['thumbs/b.jpg'] = b'thumb_b'
            bc['thumbs/sub/c.jpg'] = b'thumb_c'
            bc['images/photo.jpg'] = b'photo'
            bc['docs/readme.txt'] = b'readme'
            bc['root.txt'] = b'root'

        # Exclude thumbs/ (normalized to thumbs/**)
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(
                source_path,
                filter_rules=[
                    ('-', 'thumbs/**'),  # CLI would normalize 'thumbs/' to this
                ],
            )

        with Barecat(target_path, readonly=True) as bc:
            # thumbs directory and all contents excluded
            assert 'thumbs/a.jpg' not in bc
            assert 'thumbs/b.jpg' not in bc
            assert 'thumbs/sub/c.jpg' not in bc
            # Everything else included
            assert bc['images/photo.jpg'] == b'photo'
            assert bc['docs/readme.txt'] == b'readme'
            assert bc['root.txt'] == b'root'
            assert bc.verify_integrity()


def test_filter_trailing_slash_include():
    """Test trailing slash for include patterns."""
    with tempfile.TemporaryDirectory() as tempdir:
        source_path = osp.join(tempdir, 'source.barecat')
        target_path = osp.join(tempdir, 'target.barecat')

        with Barecat(source_path, readonly=False) as bc:
            bc['keep/a.txt'] = b'a'
            bc['keep/sub/b.txt'] = b'b'
            bc['drop/c.txt'] = b'c'
            bc['root.txt'] = b'root'

        # Only include keep/ (normalized to keep/**)
        with Barecat(target_path, readonly=False) as bc:
            bc.merge_from_other_barecat(
                source_path,
                filter_rules=[
                    ('+', 'keep/**'),  # CLI would normalize 'keep/' to this
                    ('-', '**'),  # exclude everything else
                ],
            )

        with Barecat(target_path, readonly=True) as bc:
            assert bc['keep/a.txt'] == b'a'
            assert bc['keep/sub/b.txt'] == b'b'
            assert 'drop/c.txt' not in bc
            assert 'root.txt' not in bc
            assert bc.verify_integrity()
