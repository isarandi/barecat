"""Tests for DecodedView and deprecated auto_codec functionality."""

import gzip
import json
import lzma
import pickle
import tempfile
import os.path as osp
import warnings

import numpy as np
import pytest

import barecat
from barecat import Barecat, DecodedView


@pytest.fixture
def archive_path():
    """Create a temporary directory and return archive path."""
    tempdir = tempfile.mkdtemp()
    return osp.join(tempdir, 'test.barecat')


class TestDecodedView:
    """Tests for the new DecodedView codec system."""

    def test_json_codec(self, archive_path):
        """Test JSON encoding/decoding."""
        data = {'key': 'value', 'number': 42, 'nested': {'a': [1, 2, 3]}}

        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['config.json'] = data

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            assert dec['config.json'] == data
            # Verify raw bytes are valid JSON
            assert json.loads(bc['config.json'].decode('utf-8')) == data

    def test_pickle_codec(self, archive_path):
        """Test pickle encoding/decoding."""
        data = {'array': np.array([1, 2, 3]), 'tuple': (1, 'two', 3.0)}

        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['data.pkl'] = data

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            result = dec['data.pkl']
            np.testing.assert_array_equal(result['array'], data['array'])
            assert result['tuple'] == data['tuple']

    def test_pickle_extension_variant(self, archive_path):
        """Test .pickle extension works same as .pkl."""
        data = [1, 2, 3]

        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['data.pickle'] = data

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            assert dec['data.pickle'] == data

    def test_numpy_npy_codec(self, archive_path):
        """Test numpy .npy encoding/decoding."""
        arr = np.random.rand(10, 20).astype(np.float32)

        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['array.npy'] = arr

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            np.testing.assert_array_equal(dec['array.npy'], arr)

    def test_numpy_npz_codec(self, archive_path):
        """Test numpy .npz encoding/decoding."""
        arrays = {'a': np.array([1, 2, 3]), 'b': np.zeros((5, 5))}

        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['arrays.npz'] = arrays

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            result = dec['arrays.npz']
            np.testing.assert_array_equal(result['a'], arrays['a'])
            np.testing.assert_array_equal(result['b'], arrays['b'])

    def test_image_png_codec(self, archive_path):
        """Test PNG image encoding/decoding via imageio."""
        # Create a simple RGB image
        img = np.zeros((100, 100, 3), dtype=np.uint8)
        img[25:75, 25:75] = [255, 0, 0]  # Red square

        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['image.png'] = img

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            result = dec['image.png']
            assert result.shape == img.shape
            np.testing.assert_array_equal(result, img)

    def test_image_jpg_codec(self, archive_path):
        """Test JPEG image encoding/decoding (lossy, so approximate check)."""
        img = np.zeros((100, 100, 3), dtype=np.uint8)
        img[:, :] = [128, 128, 128]  # Gray

        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['image.jpg'] = img

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            result = dec['image.jpg']
            assert result.shape == img.shape
            # JPEG is lossy, check approximate equality
            assert np.abs(result.astype(int) - img.astype(int)).max() < 10

    def test_gzip_compression_stacked(self, archive_path):
        """Test gzip compression stacked with JSON."""
        data = {'large': 'data' * 1000}

        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['config.json.gz'] = data

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            assert dec['config.json.gz'] == data
            # Verify raw bytes are gzipped
            raw = bc['config.json.gz']
            assert gzip.decompress(raw) == json.dumps(data).encode('utf-8')

    def test_lzma_compression_stacked(self, archive_path):
        """Test lzma compression stacked with pickle."""
        data = {'array': list(range(1000))}

        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['data.pkl.xz'] = data

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            assert dec['data.pkl.xz'] == data
            # Verify raw bytes are lzma compressed
            raw = bc['data.pkl.xz']
            assert pickle.loads(lzma.decompress(raw)) == data

    def test_bz2_compression_stacked(self, archive_path):
        """Test bz2 compression stacked with JSON."""
        data = [1, 2, 3, 4, 5]

        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['list.json.bz2'] = data

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            assert dec['list.json.bz2'] == data

    def test_unknown_extension_raises_error(self, archive_path):
        """Test that unknown extensions raise ValueError."""
        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            with pytest.raises(ValueError, match="No codec registered for '.xyz'"):
                dec['file.xyz'] = b'data'

    def test_unknown_extension_on_read_raises_error(self, archive_path):
        """Test that reading unknown extensions raises ValueError."""
        with Barecat(archive_path, readonly=False) as bc:
            bc['file.xyz'] = b'raw data'

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            with pytest.raises(ValueError, match="No codec registered for '.xyz'"):
                _ = dec['file.xyz']

    def test_raw_bytes_via_store(self, archive_path):
        """Test that raw bytes can be accessed via the underlying store."""
        raw_data = b'binary data here'

        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            # Write raw bytes via store
            bc['file.bin'] = raw_data
            # Write encoded data via dec
            dec['config.json'] = {'key': 'value'}

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            # Read raw bytes via store
            assert bc['file.bin'] == raw_data
            # Read decoded data via dec
            assert dec['config.json'] == {'key': 'value'}

    def test_custom_codec_registration(self, archive_path):
        """Test registering a custom codec."""
        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)

            # Register a simple custom codec
            dec.register_codec(
                ['.upper'],
                encoder=lambda s: s.upper().encode('utf-8'),
                decoder=lambda b: b.decode('utf-8').lower(),
            )

            dec['text.upper'] = 'hello world'

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            dec.register_codec(
                ['.upper'],
                encoder=lambda s: s.upper().encode('utf-8'),
                decoder=lambda b: b.decode('utf-8').lower(),
            )

            assert dec['text.upper'] == 'hello world'
            # Raw bytes should be uppercase
            assert bc['text.upper'] == b'HELLO WORLD'

    def test_clear_codecs(self, archive_path):
        """Test clearing all codecs."""
        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec.clear_codecs()

            # Now even .json should fail
            with pytest.raises(ValueError, match="No codec registered"):
                dec['config.json'] = {'key': 'value'}

    def test_case_insensitive_extensions(self, archive_path):
        """Test that extensions are case-insensitive."""
        data = {'key': 'value'}

        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['config.JSON'] = data
            dec['image.PNG'] = np.zeros((10, 10, 3), dtype=np.uint8)

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            assert dec['config.JSON'] == data
            assert dec['image.PNG'].shape == (10, 10, 3)

    def test_iteration_and_len(self, archive_path):
        """Test that iteration and len delegate to store."""
        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['a.json'] = [1]
            dec['b.json'] = [2]
            dec['c.json'] = [3]

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            assert len(dec) == 3
            assert set(dec.keys()) == {'a.json', 'b.json', 'c.json'}
            assert 'a.json' in dec
            assert 'd.json' not in dec

    def test_items_and_values(self, archive_path):
        """Test items() and values() decode properly."""
        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['a.json'] = {'a': 1}
            dec['b.json'] = {'b': 2}

        with Barecat(archive_path, readonly=True) as bc:
            dec = DecodedView(bc)
            items = dict(dec.items())
            assert items == {'a.json': {'a': 1}, 'b.json': {'b': 2}}

            values = list(dec.values())
            assert {'a': 1} in values
            assert {'b': 2} in values

    def test_deletion(self, archive_path):
        """Test deleting via DecodedView."""
        with Barecat(archive_path, readonly=False) as bc:
            dec = DecodedView(bc)
            dec['config.json'] = {'key': 'value'}
            assert 'config.json' in dec
            del dec['config.json']
            assert 'config.json' not in dec


class TestAutoCodecBackwardsCompatibility:
    """Tests for deprecated auto_codec parameter (backwards compatibility)."""

    def test_auto_codec_emits_deprecation_warning(self, archive_path):
        """Test that auto_codec=True emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with Barecat(archive_path, readonly=False, auto_codec=True) as bc:
                pass

            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert 'auto_codec' in str(w[0].message)
            assert 'DecodedView' in str(w[0].message)

    def test_auto_codec_json_read_write(self, archive_path):
        """Test auto_codec still works for JSON."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)

            with Barecat(archive_path, readonly=False, auto_codec=True) as bc:
                bc['config.json'] = {'key': 'value'}

            with Barecat(archive_path, readonly=True, auto_codec=True) as bc:
                assert bc['config.json'] == {'key': 'value'}

    def test_auto_codec_numpy(self, archive_path):
        """Test auto_codec still works for numpy arrays."""
        arr = np.array([1, 2, 3, 4, 5])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)

            with Barecat(archive_path, readonly=False, auto_codec=True) as bc:
                bc['data.npy'] = arr

            with Barecat(archive_path, readonly=True, auto_codec=True) as bc:
                np.testing.assert_array_equal(bc['data.npy'], arr)

    def test_auto_codec_pickle(self, archive_path):
        """Test auto_codec still works for pickle."""
        data = {'list': [1, 2, 3], 'set': {4, 5, 6}}

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)

            with Barecat(archive_path, readonly=False, auto_codec=True) as bc:
                bc['data.pkl'] = data

            with Barecat(archive_path, readonly=True, auto_codec=True) as bc:
                assert bc['data.pkl'] == data

    def test_auto_codec_image(self, archive_path):
        """Test auto_codec still works for images."""
        img = np.zeros((50, 50, 3), dtype=np.uint8)
        img[10:40, 10:40] = [0, 255, 0]

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)

            with Barecat(archive_path, readonly=False, auto_codec=True) as bc:
                bc['image.png'] = img

            with Barecat(archive_path, readonly=True, auto_codec=True) as bc:
                result = bc['image.png']
                np.testing.assert_array_equal(result, img)

    def test_auto_codec_compression_stacked(self, archive_path):
        """Test auto_codec works with stacked compression."""
        data = {'key': 'value' * 100}

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)

            with Barecat(archive_path, readonly=False, auto_codec=True) as bc:
                bc['config.json.gz'] = data

            with Barecat(archive_path, readonly=True, auto_codec=True) as bc:
                assert bc['config.json.gz'] == data

    def test_auto_codec_unknown_extension_passthrough(self, archive_path):
        """Test auto_codec passes through unknown extensions as raw bytes."""
        raw_data = b'raw binary data'

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)

            with Barecat(archive_path, readonly=False, auto_codec=True) as bc:
                bc['file.xyz'] = raw_data

            with Barecat(archive_path, readonly=True, auto_codec=True) as bc:
                # auto_codec should pass through unknown extensions
                assert bc['file.xyz'] == raw_data

    def test_barecat_open_auto_codec_warning(self, archive_path):
        """Test barecat.open() with auto_codec emits warning."""
        # First create the archive
        with Barecat(archive_path, readonly=False) as bc:
            bc['test.txt'] = b'hello'

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with barecat.open(archive_path, mode='r', auto_codec=True) as bc:
                pass

            # Both barecat.open() and Barecat.__init__() emit warnings
            deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(deprecation_warnings) >= 1
            assert any('auto_codec' in str(x.message) for x in deprecation_warnings)
