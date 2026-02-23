import io
import os.path as osp
from collections.abc import MutableMapping, Iterator, Callable
from functools import partial
from typing import Any

# === Library availability detection (cached at import time) ===

_HAS_NUMPY = False
_HAS_MSGPACK_NUMPY = False
_HAS_CV2 = False
_HAS_JPEG4PY = False
_HAS_PIL = False
_HAS_IMAGEIO = False
_HAS_OPENEXR = False

try:
    import numpy as _np

    _HAS_NUMPY = True
except ImportError:
    pass

try:
    import msgpack_numpy as _msgpack_numpy

    _HAS_MSGPACK_NUMPY = True
except ImportError:
    pass

try:
    import cv2 as _cv2

    _HAS_CV2 = True
except ImportError:
    pass

try:
    import jpeg4py as _jpeg4py

    _HAS_JPEG4PY = True
except ImportError:
    pass

try:
    from PIL import Image as _PILImage

    _HAS_PIL = True
except ImportError:
    pass

try:
    import imageio.v3 as _iio

    _HAS_IMAGEIO = True
except ImportError:
    pass

try:
    import OpenEXR as _OpenEXR
    import Imath as _Imath

    _HAS_OPENEXR = True
except ImportError:
    pass


# === Stdlib codecs ===


def encode_json(data):
    import json

    return json.dumps(data).encode('utf-8')


def decode_json(data):
    import json

    return json.loads(data.decode('utf-8'))


def encode_pickle(data):
    import pickle

    return pickle.dumps(data)


def decode_pickle(data):
    import pickle

    return pickle.loads(data)


def encode_gzip(data):
    import gzip

    return gzip.compress(data)


def decode_gzip(data):
    import gzip

    return gzip.decompress(data)


def encode_lzma(data):
    import lzma

    return lzma.compress(data)


def decode_lzma(data):
    import lzma

    return lzma.decompress(data)


def encode_bz2(data):
    import bz2

    return bz2.compress(data, 9)


def decode_bz2(data):
    import bz2

    return bz2.decompress(data)


# === Numpy codecs ===

if _HAS_NUMPY:

    def encode_npy(data):
        with io.BytesIO() as f:
            _np.save(f, data)
            return f.getvalue()

    def decode_npy(data):
        with io.BytesIO(data) as f:
            return _np.load(f)

    def encode_npz(data):
        with io.BytesIO() as f:
            _np.savez(f, **data)
            return f.getvalue()

    def decode_npz(data):
        with io.BytesIO(data) as f:
            return dict(_np.load(f))


# === Msgpack codec ===

if _HAS_MSGPACK_NUMPY:

    def encode_msgpack(data):
        return _msgpack_numpy.packb(data)

    def decode_msgpack(data):
        return _msgpack_numpy.unpackb(data)


# === Image codecs ===
# Priority: cv2 > PIL > imageio (cv2 is fastest for encode/decode)
# For JPEG decode: jpeg4py > cv2 > PIL > imageio (jpeg4py is fastest)


def _no_image_lib():
    raise ImportError('No image library available. Install one of: opencv-python, Pillow, imageio')


def encode_jpeg(data, quality=95):
    """Encode numpy array to JPEG bytes. Priority: cv2 > PIL > imageio."""
    if _HAS_CV2:
        if data.ndim == 3 and data.shape[2] == 4:
            data = data[:, :, :3]  # Drop alpha (JPEG doesn't support it)
        if data.ndim == 3 and data.shape[2] == 3:
            data = _cv2.cvtColor(data, _cv2.COLOR_RGB2BGR)
        # Grayscale (ndim==2) needs no conversion
        _, buf = _cv2.imencode('.jpg', data, [_cv2.IMWRITE_JPEG_QUALITY, quality])
        return buf.tobytes()
    elif _HAS_PIL:
        pil_img = _PILImage.fromarray(data)
        buf = io.BytesIO()
        pil_img.save(buf, format='JPEG', quality=quality)
        return buf.getvalue()
    elif _HAS_IMAGEIO:
        return _iio.imwrite('<bytes>', data, extension='.jpg', quality=quality)
    else:
        _no_image_lib()


def decode_jpeg(data):
    """Decode JPEG bytes to numpy array. Priority: jpeg4py > cv2 > PIL > imageio."""
    if _HAS_JPEG4PY:
        return _jpeg4py.JPEG(_np.frombuffer(data, _np.uint8)).decode()
    elif _HAS_CV2:
        arr = _cv2.imdecode(_np.frombuffer(data, _np.uint8), _cv2.IMREAD_COLOR)
        return _cv2.cvtColor(arr, _cv2.COLOR_BGR2RGB)
    elif _HAS_PIL and _HAS_NUMPY:
        return _np.array(_PILImage.open(io.BytesIO(data)))
    elif _HAS_IMAGEIO:
        return _iio.imread(data)
    else:
        _no_image_lib()


def encode_png(data, compression=None):
    """Encode numpy array to PNG bytes. Priority: cv2 > PIL > imageio."""
    if _HAS_CV2:
        # cv2 default is fast with reasonable compression
        if data.ndim == 3 and data.shape[2] in (3, 4):
            img = _cv2.cvtColor(
                data, _cv2.COLOR_RGB2BGR if data.shape[2] == 3 else _cv2.COLOR_RGBA2BGRA
            )
        else:
            img = data
        _, buf = _cv2.imencode('.png', img)
        return buf.tobytes()
    elif _HAS_PIL:
        pil_img = _PILImage.fromarray(data)
        buf = io.BytesIO()
        kwargs = {'compress_level': compression} if compression is not None else {}
        pil_img.save(buf, format='PNG', **kwargs)
        return buf.getvalue()
    elif _HAS_IMAGEIO:
        return _iio.imwrite('<bytes>', data, extension='.png')
    else:
        _no_image_lib()


def decode_png(data):
    """Decode PNG bytes to numpy array. Priority: cv2 > PIL > imageio."""
    if _HAS_CV2:
        arr = _cv2.imdecode(_np.frombuffer(data, _np.uint8), _cv2.IMREAD_UNCHANGED)
        if arr.ndim == 3:
            if arr.shape[2] == 3:
                return _cv2.cvtColor(arr, _cv2.COLOR_BGR2RGB)
            elif arr.shape[2] == 4:
                return _cv2.cvtColor(arr, _cv2.COLOR_BGRA2RGBA)
        return arr
    elif _HAS_PIL and _HAS_NUMPY:
        return _np.array(_PILImage.open(io.BytesIO(data)))
    elif _HAS_IMAGEIO:
        return _iio.imread(data)
    else:
        _no_image_lib()


def encode_bmp(data):
    """Encode numpy array to BMP bytes. Priority: cv2 > PIL > imageio."""
    if _HAS_CV2:
        if data.ndim == 3 and data.shape[2] == 3:
            img = _cv2.cvtColor(data, _cv2.COLOR_RGB2BGR)
        else:
            img = data
        _, buf = _cv2.imencode('.bmp', img)
        return buf.tobytes()
    elif _HAS_PIL:
        pil_img = _PILImage.fromarray(data)
        buf = io.BytesIO()
        pil_img.save(buf, format='BMP')
        return buf.getvalue()
    elif _HAS_IMAGEIO:
        return _iio.imwrite('<bytes>', data, extension='.bmp')
    else:
        _no_image_lib()


def decode_bmp(data):
    """Decode BMP bytes to numpy array. Priority: cv2 > PIL > imageio."""
    if _HAS_CV2:
        arr = _cv2.imdecode(_np.frombuffer(data, _np.uint8), _cv2.IMREAD_COLOR)
        return _cv2.cvtColor(arr, _cv2.COLOR_BGR2RGB)
    elif _HAS_PIL and _HAS_NUMPY:
        return _np.array(_PILImage.open(io.BytesIO(data)))
    elif _HAS_IMAGEIO:
        return _iio.imread(data)
    else:
        _no_image_lib()


def encode_exr(data):
    """Encode numpy array to EXR bytes. Priority: OpenEXR > imageio."""
    import tempfile
    import os

    if _HAS_OPENEXR:
        # OpenEXR gives lossless float32
        data = _np.asarray(data, dtype=_np.float32)
        if data.ndim == 2:
            data = data[:, :, _np.newaxis]
        h, w = data.shape[:2]
        n_channels = data.shape[2] if data.ndim == 3 else 1

        header = _OpenEXR.Header(w, h)
        channel_names = (
            ['R', 'G', 'B', 'A'][:n_channels]
            if n_channels <= 4
            else [f'C{i}' for i in range(n_channels)]
        )
        header['channels'] = {
            name: _Imath.Channel(_Imath.PixelType(_Imath.PixelType.FLOAT))
            for name in channel_names
        }

        # OpenEXR requires file, use tempfile
        with tempfile.NamedTemporaryFile(suffix='.exr', delete=False) as f:
            tmp_path = f.name
        try:
            out = _OpenEXR.OutputFile(tmp_path, header)
            channel_data = {channel_names[i]: data[:, :, i].tobytes() for i in range(n_channels)}
            out.writePixels(channel_data)
            out.close()
            with open(tmp_path, 'rb') as f:
                return f.read()
        finally:
            os.unlink(tmp_path)
    elif _HAS_IMAGEIO:
        # imageio uses float16 by default (lossy)
        return _iio.imwrite('<bytes>', data, extension='.exr')
    else:
        raise ImportError('EXR requires OpenEXR or imageio. Install: pip install OpenEXR')


def decode_exr(data):
    """Decode EXR bytes to numpy array. Priority: OpenEXR > imageio."""
    import tempfile
    import os

    if _HAS_OPENEXR:
        # OpenEXR requires file, use tempfile
        with tempfile.NamedTemporaryFile(suffix='.exr', delete=False) as f:
            f.write(data)
            tmp_path = f.name
        try:
            exr = _OpenEXR.InputFile(tmp_path)
            header = exr.header()
            dw = header['dataWindow']
            w = dw.max.x - dw.min.x + 1
            h = dw.max.y - dw.min.y + 1

            channels = header['channels']
            n_channels = len(channels)
            # Preserve RGB(A) order if standard channels present
            if n_channels <= 4 and all(c in channels for c in ['R', 'G', 'B', 'A'][:n_channels]):
                channel_names = ['R', 'G', 'B', 'A'][:n_channels]
            else:
                channel_names = sorted(channels.keys())

            pt = _Imath.PixelType(_Imath.PixelType.FLOAT)

            channel_data = []
            for name in channel_names:
                raw = exr.channel(name, pt)
                arr = _np.frombuffer(raw, dtype=_np.float32).reshape(h, w)
                channel_data.append(arr)

            if len(channel_data) == 1:
                return channel_data[0]
            return _np.stack(channel_data, axis=-1)
        finally:
            os.unlink(tmp_path)
    elif _HAS_IMAGEIO:
        return _iio.imread(data, extension='.exr')
    else:
        raise ImportError('EXR requires OpenEXR or imageio. Install: pip install OpenEXR')


def encode_image(data, fmt):
    """Encode to other formats via imageio (fallback for tiff, gif, webp)."""
    fmt_lower = fmt.lower()
    if _HAS_IMAGEIO:
        return _iio.imwrite('<bytes>', data, extension=f'.{fmt}')
    elif _HAS_PIL and fmt_lower in ('tiff', 'tif', 'gif', 'webp'):
        pil_img = _PILImage.fromarray(data)
        buf = io.BytesIO()
        pil_img.save(buf, format='TIFF' if fmt_lower in ('tiff', 'tif') else fmt.upper())
        return buf.getvalue()
    else:
        raise ImportError(f'No library available for {fmt}. Install imageio or Pillow.')


def decode_image(data, fmt=None):
    """Decode other formats via imageio (fallback for tiff, gif, webp, exr)."""
    if _HAS_IMAGEIO:
        # Pass extension hint for formats like EXR that need it
        kwargs = {'extension': f'.{fmt}'} if fmt else {}
        return _iio.imread(data, **kwargs)
    elif _HAS_PIL and _HAS_NUMPY:
        return _np.array(_PILImage.open(io.BytesIO(data)))
    else:
        raise ImportError('No library available. Install imageio or Pillow.')


# === DecodedView ===


class DecodedView(MutableMapping[str, Any]):
    """Dict-like view that always encodes/decodes based on file extension.

    Wraps a raw bytes store (like Barecat) and automatically encodes on write
    and decodes on read based on the file extension. Raises an error if no
    codec is registered for the extension.

    Args:
        store: A MutableMapping[str, bytes] to wrap (e.g., a Barecat instance).

    Examples:
        >>> bc = Barecat('data.barecat', readonly=False)
        >>> dec = DecodedView(bc)
        >>> dec['config.json'] = {'key': 'value'}
        >>> dec['config.json']
        {'key': 'value'}
        >>> dec['image.png'] = numpy_array
        >>> # For raw bytes, use the store directly: bc['data.raw'] = b'...'
    """

    ALL_CODECS: dict[str, tuple[Callable, Callable, bool]] = {
        # Stdlib
        '.json': (encode_json, decode_json, False),
        '.pkl': (encode_pickle, decode_pickle, False),
        '.pickle': (encode_pickle, decode_pickle, False),
        '.gz': (encode_gzip, decode_gzip, True),
        '.gzip': (encode_gzip, decode_gzip, True),
        '.xz': (encode_lzma, decode_lzma, True),
        '.lzma': (encode_lzma, decode_lzma, True),
        '.bz2': (encode_bz2, decode_bz2, True),
        # Images
        '.jpg': (encode_jpeg, decode_jpeg, False),
        '.jpeg': (encode_jpeg, decode_jpeg, False),
        '.png': (encode_png, decode_png, False),
        '.bmp': (encode_bmp, decode_bmp, False),
        '.gif': (partial(encode_image, fmt='gif'), partial(decode_image, fmt='gif'), False),
        '.tiff': (partial(encode_image, fmt='tiff'), partial(decode_image, fmt='tiff'), False),
        '.tif': (partial(encode_image, fmt='tif'), partial(decode_image, fmt='tif'), False),
        '.webp': (partial(encode_image, fmt='webp'), partial(decode_image, fmt='webp'), False),
        '.exr': (encode_exr, decode_exr, False),
    }

    # Add numpy codecs if available
    if _HAS_NUMPY:
        ALL_CODECS['.npy'] = (encode_npy, decode_npy, False)
        ALL_CODECS['.npz'] = (encode_npz, decode_npz, False)

    # Add msgpack codec if available
    if _HAS_MSGPACK_NUMPY:
        ALL_CODECS['.msgpack'] = (encode_msgpack, decode_msgpack, False)

    def __init__(self, store: MutableMapping[str, bytes]):
        self._store = store
        self.codecs = dict(self.ALL_CODECS)

    def register_codec(
        self,
        exts: list[str],
        encoder: Callable[[Any], bytes],
        decoder: Callable[[bytes], Any],
        nonfinal: bool = False,
    ):
        """Register a codec for the given extensions.

        Args:
            exts: List of file extensions (e.g., ['.xyz']).
            encoder: Function to encode data to bytes.
            decoder: Function to decode bytes to data.
            nonfinal: If True, allows stacking (e.g., .json.gz).
        """
        for ext in exts:
            self.codecs[ext] = (encoder, decoder, nonfinal)

    def clear_codecs(self):
        """Remove all registered codecs."""
        self.codecs.clear()

    def _encode(self, path: str, data: Any) -> bytes:
        noext, ext = osp.splitext(path)
        ext = ext.lower()

        if ext not in self.codecs:
            raise ValueError(
                f"No codec registered for '{ext}'. Use the store directly for raw bytes."
            )

        encoder, decoder, nonfinal = self.codecs[ext]

        if nonfinal:
            data = self._encode(noext, data)

        return encoder(data)

    def _decode(self, path: str, data: bytes) -> Any:
        noext, ext = osp.splitext(path)
        ext = ext.lower()

        if ext not in self.codecs:
            raise ValueError(
                f"No codec registered for '{ext}'. Use the store directly for raw bytes."
            )

        encoder, decoder, nonfinal = self.codecs[ext]
        data = decoder(data)

        if nonfinal:
            data = self._decode(noext, data)

        return data

    # MutableMapping abstract methods

    def __getitem__(self, path: str) -> Any:
        return self._decode(path, self._store[path])

    def __setitem__(self, path: str, value: Any) -> None:
        self._store[path] = self._encode(path, value)

    def __delitem__(self, path: str) -> None:
        del self._store[path]

    def __iter__(self) -> Iterator[str]:
        return iter(self._store)

    def __len__(self) -> int:
        return len(self._store)

    # Override for performance (delegate to store's fast implementations)

    def __contains__(self, path: object) -> bool:
        return path in self._store

    def keys(self):
        return self._store.keys()

    def items(self):
        for path, raw in self._store.items():
            yield path, self._decode(path, raw)

    def values(self):
        for path, raw in self._store.items():
            yield self._decode(path, raw)


# === Legacy CodecRegistry (deprecated, for backwards compatibility) ===


class CodecRegistry:
    """
    .. deprecated::
        Use :class:`DecodedView` instead. Will be removed in version 1.0.
    """

    def __init__(self, auto_codec=True):
        self.codecs = {}
        if auto_codec:
            # Stdlib
            self.register_codec(['.json'], encode_json, decode_json)
            self.register_codec(['.pkl', '.pickle'], encode_pickle, decode_pickle)
            self.register_codec(['.gz', '.gzip'], encode_gzip, decode_gzip, nonfinal=True)
            self.register_codec(['.xz', '.lzma'], encode_lzma, decode_lzma, nonfinal=True)
            self.register_codec(['.bz2'], encode_bz2, decode_bz2, nonfinal=True)
            # Numpy (if available)
            if _HAS_NUMPY:
                self.register_codec(['.npy'], encode_npy, decode_npy)
                self.register_codec(['.npz'], encode_npz, decode_npz)
            # Msgpack (if available)
            if _HAS_MSGPACK_NUMPY:
                self.register_codec(['.msgpack'], encode_msgpack, decode_msgpack)
            # Images
            self.register_codec(['.jpg', '.jpeg'], encode_jpeg, decode_jpeg)
            self.register_codec(['.png'], encode_png, decode_png)
            self.register_codec(['.bmp'], encode_bmp, decode_bmp)
            self.register_codec(
                ['.gif'], partial(encode_image, fmt='gif'), partial(decode_image, fmt='gif')
            )
            self.register_codec(
                ['.tiff'], partial(encode_image, fmt='tiff'), partial(decode_image, fmt='tiff')
            )
            self.register_codec(
                ['.tif'], partial(encode_image, fmt='tif'), partial(decode_image, fmt='tif')
            )
            self.register_codec(
                ['.webp'], partial(encode_image, fmt='webp'), partial(decode_image, fmt='webp')
            )
            self.register_codec(['.exr'], encode_exr, decode_exr)

    def register_codec(self, exts, encoder, decoder, nonfinal=False):
        for ext in exts:
            self.codecs[ext] = (encoder, decoder, nonfinal)

    def encode(self, path, data):
        if not self.codecs:
            return data

        noext, ext = osp.splitext(path)
        try:
            encoder, decoder, nonfinal = self.codecs[ext.lower()]
        except KeyError:
            return data

        if encoder is None:
            raise ValueError(f'No encoder available for {ext} (read-only format)')

        if nonfinal:
            data = self.encode(noext, data)
        return encoder(data)

    def decode(self, path, data):
        if not self.codecs:
            return data

        noext, ext = osp.splitext(path)
        try:
            encoder, decoder, nonfinal = self.codecs[ext.lower()]
        except KeyError:
            return data

        data = decoder(data)
        if nonfinal:
            data = self.decode(noext, data)
        return data
