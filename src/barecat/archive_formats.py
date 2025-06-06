import os.path as osp
import shutil
import tarfile
import zipfile
from datetime import datetime

from barecat.core.index import BarecatDirInfo, BarecatFileInfo, BarecatEntryInfo
from barecat.progbar import progressbar


def iter_archive(src_path):
    if src_path.endswith(('.tar', '.tar.gz', '.tar.bz2', '.tar.xz')):
        return iter_tarfile(src_path)
    elif src_path.endswith('.zip'):
        return iter_zipfile(src_path)
    else:
        raise ValueError('Unsupported archive format')


def iter_archive_nocontent(src_path):
    if src_path.endswith(('.tar', '.tar.gz', '.tar.bz2', '.tar.xz')):
        return iter_tarfile_nocontent(src_path)
    elif src_path.endswith('.zip'):
        return iter_zipfile_nocontent(src_path)
    else:
        raise ValueError('Unsupported archive format')


def iter_zipfile(path):
    with zipfile.ZipFile(path, mode='r') as zipf:
        for member in progressbar(zipf.infolist(), desc='Packing files', unit=' files'):
            if member.is_dir():
                di = BarecatDirInfo(path=member.filename)
                di.mtime_dt = datetime(*member.date_time)
                yield di, None
            else:
                fi = BarecatFileInfo(path=member.filename, size=member.file_size)
                fi.mtime_dt = datetime(*member.date_time)
                with zipf.open(member) as file_in_zip:
                    yield fi, file_in_zip


def iter_zipfile_nocontent(path):
    with open(path, 'rb') as f:
        with zipfile.ZipFile(f, mode='r') as zipf:
            for member in progressbar(zipf.infolist(), desc='Packing files', unit=' files'):
                if member.is_dir():
                    di = BarecatDirInfo(path=member.filename)
                    di.mtime_dt = datetime(*member.date_time)
                    yield di
                else:
                    f.seek(member.header_offset + 26)
                    namelen = int.from_bytes(f.read(2), byteorder='little')
                    extralen = int.from_bytes(f.read(2), byteorder='little')
                    data_offset = member.header_offset + 30 + namelen + extralen

                    fi = BarecatFileInfo(
                        path=member.filename, shard=0, offset=data_offset, size=member.file_size
                    )
                    fi.mtime_dt = datetime(*member.date_time)
                    yield fi


def iter_tarfile(path):
    tar_file_size = osp.getsize(path) // 1024 // 1024
    pbar = progressbar(None, desc='Packing files', unit=' MB', total=tar_file_size)
    progpos = 0

    with tarfile.open(path, mode='r|*') as tar:
        for member in tar:
            if member.isdir():
                di = BarecatDirInfo(
                    path=member.name,
                    mode=member.mode,
                    uid=member.uid,
                    gid=member.gid,
                    mtime_ns=member.mtime * 1_000_000_000,
                )
                yield di, None
            if member.isfile():
                fi = BarecatFileInfo(
                    path=member.name,
                    size=member.size,
                    mode=member.mode,
                    uid=member.uid,
                    gid=member.gid,
                    mtime_ns=member.mtime * 1_000_000_000,
                )

                with tar.extractfile(member) as file_in_tar:
                    yield fi, file_in_tar

                new_pos = tar.fileobj.tell() // 1024 // 1024
                delta = new_pos - progpos
                pbar.update(delta)
                progpos += delta


def iter_tarfile_nocontent(path):
    tar_file_size = osp.getsize(path) // 1024 // 1024
    pbar = progressbar(None, desc='Packing files', unit=' MB', total=tar_file_size)
    progpos = 0

    with tarfile.open(path, mode='r|*') as tar:
        for member in tar:
            if member.isdir():
                di = BarecatDirInfo(
                    path=member.name,
                    mode=member.mode,
                    uid=member.uid,
                    gid=member.gid,
                    mtime_ns=member.mtime * 1_000_000_000,
                )
                yield di
            if member.isfile():
                fi = BarecatFileInfo(
                    path=member.name,
                    shard=0,
                    offset=member.offset_data,
                    size=member.size,
                    mode=member.mode,
                    uid=member.uid,
                    gid=member.gid,
                    mtime_ns=member.mtime * 1_000_000_000,
                )
                yield fi
                new_pos = tar.fileobj.tell() // 1024 // 1024
                delta = new_pos - progpos
                pbar.update(delta)
                progpos += delta


def get_archive_writer(target_path):
    if target_path.endswith(('.tar', '.tar.gz', '.tar.bz2', '.tar.xz')):
        return TarWriter(target_path)
    elif target_path.endswith('.zip'):
        return ZipWriter(target_path)
    else:
        raise ValueError('Unsupported archive format')


class ZipWriter:
    def __init__(self, target_path):
        self.zip = zipfile.ZipFile(target_path, mode='w')

    def add(self, info: BarecatEntryInfo, fileobj=None):
        if isinstance(info, BarecatDirInfo):
            zipinfo = zipfile.ZipInfo(info.path + '/')
            zipinfo.date_time = info.mtime_dt.timetuple()[:6]
            self.zip.writestr(zipinfo, '')
        else:
            zipinfo = zipfile.ZipInfo(info.path)
            zipinfo.date_time = info.mtime_dt.timetuple()[:6]
            zipinfo.file_size = info.size
            with self.zip.open(zipinfo, 'w') as file_in_zip:
                shutil.copyfileobj(fileobj, file_in_zip)

    def close(self):
        self.zip.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class TarWriter:
    def __init__(self, *args, **kwargs):
        if 'mode' not in kwargs:
            kwargs['mode'] = 'w'
        self.tar = tarfile.open(*args, **kwargs)

    def add(self, info: BarecatEntryInfo, fileobj=None):
        tarinfo = tarfile.TarInfo(info.path)
        tarinfo.uid = info.uid or 0
        tarinfo.gid = info.gid or 0
        if info.mtime_ns is not None:
            tarinfo.mtime = info.mtime_ns // 1_000_000_000
        if isinstance(info, BarecatDirInfo):
            tarinfo.type = tarfile.DIRTYPE
            tarinfo.mode = 0o755 if info.mode is None else info.mode
            self.tar.addfile(tarinfo)
        else:
            tarinfo.size = info.size
            tarinfo.mode = 0o644 if info.mode is None else info.mode
            self.tar.addfile(tarinfo, fileobj)

    def close(self):
        self.tar.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
