import os
import tempfile
import pytest
from barecat import Barecat, BarecatFileInfo
from barecat.exceptions import FileNotFoundBarecatError, FileExistsBarecatError


@pytest.fixture
def bc_path():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield os.path.join(tmpdir, 'test.barecat')


class TestOpenReadOnly:
    def test_read_existing_file(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'Hello, world!'

        with Barecat(bc_path, readonly=True) as bc:
            with bc.open('file.txt', 'rb') as f:
                assert f.read() == b'Hello, world!'

    def test_read_nonexistent_file_raises(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            pass

        with Barecat(bc_path, readonly=True) as bc:
            with pytest.raises(FileNotFoundBarecatError):
                bc.open('nonexistent.txt', 'rb')

    def test_read_with_seek(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'0123456789'

        with Barecat(bc_path, readonly=True) as bc:
            with bc.open('file.txt', 'rb') as f:
                f.seek(5)
                assert f.read(3) == b'567'
                f.seek(-2, os.SEEK_CUR)
                assert f.read(2) == b'67'
                f.seek(-3, os.SEEK_END)
                assert f.read() == b'789'

    def test_read_partial(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'AAAABBBBCCCC'

        with Barecat(bc_path, readonly=True) as bc:
            with bc.open('file.txt', 'rb') as f:
                assert f.read(4) == b'AAAA'
                assert f.read(4) == b'BBBB'
                assert f.read(4) == b'CCCC'
                assert f.read(4) == b''


class TestOpenReadWrite:
    def test_modify_in_place_same_size(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'Hello, world!'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.seek(7)
                f.write(b'WORLD!')

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'Hello, WORLD!'

    def test_modify_in_place_partial_overwrite(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'AAAABBBBCCCC'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.seek(4)
                f.write(b'XX')

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'AAAAXXBBCCCC'

    def test_expand_file_into_spillover(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'Hello'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.seek(0, os.SEEK_END)
                f.write(b', world!')

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'Hello, world!'

    def test_read_after_write_spanning_shard_and_spillover(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'AAAA'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.seek(2)
                f.write(b'BBBBBB')  # Overwrites AA, adds BBBB to spillover
                f.seek(0)
                assert f.read() == b'AABBBBBB'

    def test_interleaved_read_write(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'0123456789'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                assert f.read(4) == b'0123'
                f.write(b'XXXX')
                f.seek(0)
                assert f.read() == b'0123XXXX89'

    def test_rplus_nonexistent_raises(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            pass

        with Barecat(bc_path, readonly=False) as bc:
            with pytest.raises(FileNotFoundBarecatError):
                bc.open('nonexistent.txt', 'r+b')


class TestOpenWrite:
    def test_write_new_file(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('new.txt', 'wb') as f:
                f.write(b'Brand new file')

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['new.txt'] == b'Brand new file'

    def test_write_truncates_existing(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'Original content that is long'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'wb') as f:
                f.write(b'Short')

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'Short'

    def test_write_empty_file(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('empty.txt', 'wb') as f:
                pass  # Write nothing

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['empty.txt'] == b''

    def test_wplus_read_after_write(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'w+b') as f:
                f.write(b'Hello')
                f.seek(0)
                assert f.read() == b'Hello'
                f.write(b', world!')
                f.seek(0)
                assert f.read() == b'Hello, world!'


class TestOpenExclusive:
    def test_exclusive_create_new(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('new.txt', 'xb') as f:
                f.write(b'Exclusive!')

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['new.txt'] == b'Exclusive!'

    def test_exclusive_existing_raises(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'Already here'

        with Barecat(bc_path, readonly=False) as bc:
            with pytest.raises(FileExistsBarecatError):
                bc.open('file.txt', 'xb')

    def test_xplus_read_write(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('new.txt', 'x+b') as f:
                f.write(b'Data')
                f.seek(0)
                assert f.read() == b'Data'


class TestOpenAppend:
    def test_append_existing(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'Hello'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'ab') as f:
                f.write(b', world!')

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'Hello, world!'

    def test_append_creates_new(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('new.txt', 'ab') as f:
                f.write(b'Appended to new')

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['new.txt'] == b'Appended to new'

    def test_aplus_read_and_append(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'Start'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'a+b') as f:
                f.write(b'End')
                f.seek(0)
                assert f.read() == b'StartEnd'


class TestTruncate:
    def test_truncate_shrink(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'0123456789'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.truncate(5)

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'01234'

    def test_truncate_expand_with_zeros(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'Hi'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.truncate(10)

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'Hi\x00\x00\x00\x00\x00\x00\x00\x00'

    def test_truncate_at_position(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'0123456789'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.seek(3)
                f.truncate()  # Truncate at current position

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'012'

    def test_truncate_shrink_then_expand(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'AAAAAAAAAA'  # 10 bytes

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.truncate(3)
                f.truncate(7)

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'AAA\x00\x00\x00\x00'

    def test_truncate_expand_into_spillover(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'AAAA'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.truncate(10)  # Expands 6 bytes into spillover

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'AAAA\x00\x00\x00\x00\x00\x00'

    def test_truncate_shrink_from_spillover(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'AAAA'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.seek(0, os.SEEK_END)
                f.write(b'BBBBBB')  # Now 10 bytes, 6 in spillover
                f.truncate(6)  # Shrink spillover to 2 bytes

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'AAAABB'


class TestEdgeCases:
    def test_write_at_various_positions(self, bc_path):
        """Write at start, middle, end, and beyond end."""
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'0123456789'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.seek(0)
                f.write(b'A')  # Start
                f.seek(5)
                f.write(b'B')  # Middle
                f.seek(9)
                f.write(b'C')  # Last byte
                f.seek(10)
                f.write(b'D')  # Beyond end (spillover)

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'A1234B678CD'

    def test_seek_beyond_end_then_write(self, bc_path):
        """Seek beyond EOF creates sparse region filled with zeros."""
        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'wb') as f:
                f.write(b'Hi')
                f.seek(10)
                f.write(b'!')

        with Barecat(bc_path, readonly=True) as bc:
            result = bc['file.txt']
            assert result == b'Hi\x00\x00\x00\x00\x00\x00\x00\x00!'

    def test_large_spillover(self, bc_path):
        """Write more to spillover than original file size."""
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'X'

        large_data = b'A' * 100000
        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.seek(0, os.SEEK_END)
                f.write(large_data)

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'X' + large_data

    def test_multiple_opens_same_file(self, bc_path):
        """Multiple sequential opens and modifications."""
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'Version1'

        for i in range(2, 6):
            with Barecat(bc_path, readonly=False) as bc:
                with bc.open('file.txt', 'wb') as f:
                    f.write(f'Version{i}'.encode())

        with Barecat(bc_path, readonly=True) as bc:
            assert bc['file.txt'] == b'Version5'

    def test_read_spanning_shard_and_spillover(self, bc_path):
        """Read that spans both shard data and spillover."""
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'SHARD'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.seek(0, os.SEEK_END)
                f.write(b'SPILL')

                # Read spanning boundary
                f.seek(3)
                assert f.read(4) == b'RDSP'

                # Read all
                f.seek(0)
                assert f.read() == b'SHARDSPILL'

    def test_no_modification_no_rewrite(self, bc_path):
        """Opening for write but not modifying shouldn't change anything."""
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'Original'

        with Barecat(bc_path, readonly=False) as bc:
            original_finfo = bc.index.lookup_file('file.txt')
            with bc.open('file.txt', 'r+b') as f:
                _ = f.read()  # Just read, don't write

        with Barecat(bc_path, readonly=False) as bc:
            new_finfo = bc.index.lookup_file('file.txt')
            assert new_finfo.shard == original_finfo.shard
            assert new_finfo.offset == original_finfo.offset
            assert new_finfo.size == original_finfo.size
            assert new_finfo.crc32c == original_finfo.crc32c

    def test_empty_operations(self, bc_path):
        """Various empty operations."""
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'Data'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.write(b'')  # Empty write
                assert f.read(0) == b''  # Zero-length read
                f.seek(0)
                assert f.read() == b'Data'  # Original unchanged

    def test_binary_mode_suffix(self, bc_path):
        """Test that 'rb', 'wb', etc. work same as 'r', 'w'."""
        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'wb') as f:
                f.write(b'\x00\x01\x02\xff')

        with Barecat(bc_path, readonly=True) as bc:
            with bc.open('file.txt', 'rb') as f:
                assert f.read() == b'\x00\x01\x02\xff'


class TestCRC32CIntegrity:
    def test_crc_updated_on_modify(self, bc_path):
        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'Original'
            crc1 = bc.index.lookup_file('file.txt').crc32c

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.write(b'Modified')
            crc2 = bc.index.lookup_file('file.txt').crc32c

        assert crc1 != crc2

    def test_crc_correct_after_expand(self, bc_path):
        import crc32c as crc32c_lib

        with Barecat(bc_path, readonly=False) as bc:
            bc['file.txt'] = b'Hi'

        with Barecat(bc_path, readonly=False) as bc:
            with bc.open('file.txt', 'r+b') as f:
                f.seek(0, os.SEEK_END)
                f.write(b' there!')

        with Barecat(bc_path, readonly=True) as bc:
            data = bc['file.txt']
            expected_crc = crc32c_lib.crc32c(data)
            actual_crc = bc.index.lookup_file('file.txt').crc32c
            assert actual_crc == expected_crc


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
