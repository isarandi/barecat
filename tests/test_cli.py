import subprocess

import barecat
import pytest


@pytest.fixture
def temp_jpeg_dir(tmp_path):
    """
    Creates a complex temporary directory with sample JPEG files.
    """
    (tmp_path / "dir1").mkdir()
    (tmp_path / "dir1/subdir1").mkdir()
    (tmp_path / "dir1/subdir1/test1.jpg").write_bytes(b"dummy data1")
    (tmp_path / "dir1/subdir2").mkdir()
    (tmp_path / "dir1/subdir2/test2.jpg").write_bytes(b"dummy data2")
    (tmp_path / "dir2").mkdir()
    (tmp_path / "dir2/test3.jpg").write_bytes(b"dummy data3")
    (tmp_path / "dir2/empty_subdir").mkdir()
    (tmp_path / "dir3").mkdir()
    return tmp_path


@pytest.fixture
def barecat_archive(temp_jpeg_dir):
    """
    Creates a standard Barecat archive for testing.
    """
    archive_file = temp_jpeg_dir / "mydata.barecat"

    create_cmd = [
        "barecat-create-recursive",
        "--file", str(archive_file),
        "--overwrite",
        str(temp_jpeg_dir / "dir1"),
        str(temp_jpeg_dir / "dir2"),
        str(temp_jpeg_dir / "dir3"),
        '--shard-size=22'
    ]
    subprocess.run(create_cmd, check=True)

    return archive_file


def test_barecat_creation(temp_jpeg_dir):
    """
    Runs `find` with `barecat-create` and verifies output.
    """
    output_file = temp_jpeg_dir / "mydata.barecat"
    cmd = f"cd {temp_jpeg_dir}; find . -name '*.jpg' -print0 | sort | barecat-create --null --file={output_file} --overwrite --shard-size=22"

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    with barecat.Barecat(output_file) as reader:
        file_list = list(reader)
        assert len(file_list) == 3, "Expected 3 files in the archive"
        assert "dir1/subdir1/test1.jpg" in file_list, "Expected dir1/subdir1/test1.jpg in the archive"
        assert "dir1/subdir2/test2.jpg" in file_list, "Expected dir1/subdir2/test2.jpg in the archive"
        assert "dir2/test3.jpg" in file_list, "Expected dir2/test3.jpg in the archive"
        assert reader[
                   "dir1/subdir1/test1.jpg"] == b"dummy data", "Expected dir1/subdir1/test1.jpg to contain 'dummy data'"
        assert reader[
                   "dir1/subdir2/test2.jpg"] == b"dummy data2", "Expected dir1/subdir2/test2.jpg to contain 'dummy data2'"
        assert reader[
                   "dir2/test3.jpg"] == b"dummy data3", "Expected dir2/test3.jpg to contain 'dummy data3'"
        assert reader.sharder.num_shards == 2, "Expected 2 shards in the archive"

    assert result.returncode == 0, f"Command failed: {result.stderr}"
    assert (temp_jpeg_dir / "mydata.barecat-sqlite-index").exists(), "Output file was not created"


def test_extract_single(barecat_archive):
    """
    Tests `barecat-extract-single` to ensure a specific file is correctly extracted from the archive.
    """
    extract_cmd = [
        "barecat-extract-single",
        "--barecat-file", str(barecat_archive),
        "--path", "dir1/subdir1/test1.jpg"
    ]

    result = subprocess.run(extract_cmd, capture_output=True)

    assert result.stdout == b"dummy data", "Unexpected content in extracted file"
    assert result.returncode == 0, f"Command failed: {result.stderr}"


def test_defrag(barecat_archive):
    """
    Tests `barecat-defrag` to ensure the archive can be defragmented properly.
    """


    with barecat.Barecat(barecat_archive, readonly=False) as bc:
        first_file = next(iter(bc.index.iter_all_filepaths(barecat.Order.ADDRESS)))

        del bc[first_file]
        assert first_file not in bc
        assert bc.total_logical_size != bc.total_physical_size_seek


    defrag_cmd = [
        "barecat-defrag",
        str(barecat_archive)
    ]

    result = subprocess.run(defrag_cmd, capture_output=True, text=True)

    with barecat.Barecat(barecat_archive) as reader:
        assert reader.total_logical_size == reader.total_physical_size_seek
        assert reader.sharder.num_shards == 1


    assert result.returncode == 0, f"Command failed: {result.stderr}"


def test_verify_integrity(barecat_archive):
    """
    Tests `barecat-verify` to ensure the archive's integrity.
    """
    verify_cmd = [
        "barecat-verify",
        str(barecat_archive)
    ]

    result = subprocess.run(verify_cmd, capture_output=True, text=True)

    assert result.returncode == 0, f"Command failed: {result.stderr}"

    # now edit the file and verify again
    with open(f'{barecat_archive}-shard-00000', "r+b") as f:
        f.seek(0)
        f.write(b"junk")

    result = subprocess.run(verify_cmd, capture_output=True, text=True)
    assert result.returncode != 0, f"Command should have failed: {result.stderr}"
    assert 'CRC32C' in result.stdout, "Expected CRC mismatch error message"


def test_index_to_csv(barecat_archive):
    """
    Tests `barecat-index-to-csv` to ensure index can be dumped as CSV.
    """
    csv_cmd = [
        "barecat-index-to-csv",
        str(barecat_archive) + "-sqlite-index"
    ]

    result = subprocess.run(csv_cmd, capture_output=True, text=True)

    assert "path,shard,offset,size,crc32c" in result.stdout, "CSV output missing expected header"
    assert result.returncode == 0, f"Command failed: {result.stderr}"
