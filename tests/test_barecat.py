import os

import barecat
from barecat import Barecat, BarecatFileInfo, BarecatDirInfo
import pytest
import tempfile
import os.path as osp


def test_barecat():
    tempdir = tempfile.mkdtemp()
    filepath = osp.join(tempdir, 'test.barecat')
    with barecat.Barecat(filepath, readonly=False) as bc:
        bc['some/path.txt'] = b'hello'

    with barecat.Barecat(filepath, readonly=True) as bc:
        assert bc['some/path.txt'] == b'hello'

    with barecat.Barecat(filepath, readonly=False, overwrite=True) as bc:
        bc.add(BarecatFileInfo(path='some/path.txt', mode=0o666), data=b'hello world')
        bc.add(BarecatDirInfo(path='some/dir', mode=0o777))

    with barecat.Barecat(filepath, readonly=True) as bc:
        assert bc['some/path.txt'] == b'hello world'
        assert bc.listdir('some/dir') == []

    with barecat.Barecat(filepath, readonly=False, overwrite=True) as bc:
        bc['some/path.txt'] = b'hello world'
        assert bc['some/path.txt'] == b'hello world'
        del bc['some/path.txt']
        with pytest.raises(KeyError):
            a = bc['some/path.txt']

    with barecat.Barecat(filepath, readonly=False, overwrite=True) as bc:
        bc['some/path.txt'] = b'hello world'

    with barecat.Barecat(filepath, readonly=True) as bc:
        with bc.open('some/path.txt') as f:
            f.seek(6)
            assert f.read() == b'world'

    with barecat.Barecat(filepath, readonly=False, overwrite=True) as bc:
        bc['dir/file.txt'] = b'Hello, world!'
        bc['dir/subdir/file2.txt'] = b'Hello, world2!'

    with barecat.Barecat(filepath, readonly=True) as bc:
        assert bc.listdir('dir/subdir') == ['file2.txt']

        assert list(bc.walk('dir')) == [
            ('dir', ['subdir'], ['file.txt']),
            ('dir/subdir', [], ['file2.txt']),
        ]

    with open(osp.join(tempdir, 'file.txt'), 'wb') as f:
        f.write(b'Hello, world!')
    os.mkdir(osp.join(tempdir, 'dir2'))

    with barecat.Barecat(filepath, readonly=False, overwrite=True) as bc:
        bc.add_by_path(osp.join(tempdir, 'file.txt'))
        bc.add_by_path(osp.join(tempdir, 'dir2'), store_path='dir')

    with barecat.Barecat(filepath, readonly=True) as bc:
        assert bc[osp.join(tempdir, 'file.txt')] == b'Hello, world!'
        assert bc.listdir('dir') == []

    with Barecat(filepath, readonly=False, overwrite=True) as bc:
        bc.add(BarecatFileInfo(path='file.txt', mode=0o666), data=b'Hello, world!')
        bc.add(BarecatDirInfo(path='dir', mode=0o777))

    with Barecat(filepath, readonly=True) as bc:
        assert bc['file.txt'] == b'Hello, world!'
        assert bc.listdir('dir') == []

