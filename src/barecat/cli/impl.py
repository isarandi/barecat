import glob
import itertools
import json
import os
import os.path as osp
import shutil
import stat
import sys
import time

from ..util import misc
from ..util import physical_order as barecat_physical_order
from ..io import copyfile as barecat_copyfile
from ..formats.archive_formats import (
    get_archive_writer,
    iter_archive,
    iter_archive_nocontent,
)
from ..util.consumed_threadpool import ConsumedThreadPool
from ..core.types import BarecatDirInfo, BarecatFileInfo, Order
from ..core import barecat as barecat_
from ..core.sharder import Sharder
from ..core.paths import resolve_index_path
from ..util.progbar import progressbar


# =============================================================================
# Main entry points
# =============================================================================

def create(
    filesys_and_store_path_pairs, target_path, shard_size_limit, overwrite=False,
    exist_ok=True, workers=8
):
    if workers is None:
        create_without_workers(
            filesys_and_store_path_pairs, target_path, shard_size_limit, overwrite, exist_ok
        )
    else:
        create_with_workers(
            filesys_and_store_path_pairs, target_path, shard_size_limit, overwrite, exist_ok,
            workers
        )


def merge(
    source_paths, target_path, shard_size_limit, overwrite=False, ignore_duplicates=False,
    as_subdirs=False, prefix=None, pattern=None, filter_rules=None
):
    with barecat_.Barecat(
        target_path, shard_size_limit=shard_size_limit, readonly=False, overwrite=overwrite
    ) as writer:
        for source_path in source_paths:
            # Build prefix for this archive
            parts = []
            if prefix:
                parts.append(prefix)
            if as_subdirs:
                parts.append(get_subdir_name(source_path))
            path_prefix = '/'.join(parts)

            print(f'Merging files from {source_path}' + (f' -> {path_prefix}/' if path_prefix else ''))

            if is_traditional_archive(source_path):
                for file_or_dir_info, fileobj in iter_archive(source_path):
                    if not _should_include(file_or_dir_info.path, pattern, filter_rules):
                        continue
                    if path_prefix:
                        file_or_dir_info.path = f'{path_prefix}/{file_or_dir_info.path}'
                    writer.add(
                        file_or_dir_info,
                        fileobj=fileobj,
                        dir_exist_ok=True,
                        file_exist_ok=ignore_duplicates,
                    )
            else:
                writer.merge_from_other_barecat(
                    source_path,
                    ignore_duplicates=ignore_duplicates,
                    prefix=path_prefix or None,
                    pattern=pattern,
                    filter_rules=filter_rules,
                )


def merge_symlink(
    source_paths, target_path, overwrite=False, ignore_duplicates=False,
    as_subdirs=False, prefix=None, pattern=None, filter_rules=None
):
    if pattern or filter_rules:
        print("Error: --pattern/--include/--exclude not supported with --symlink", file=sys.stderr)
        sys.exit(1)

    # New format: target_path IS the index file
    if overwrite and osp.exists(target_path):
        os.remove(target_path)

    with barecat_.Index(target_path, readonly=False) as index_writer:
        c = index_writer.cursor
        c.execute("COMMIT")
        c.execute('PRAGMA synchronous=OFF')
        c.execute('PRAGMA journal_mode=OFF')

        i_out_shard = 0
        for source_path in source_paths:
            # Build prefix for this archive
            parts = []
            if prefix:
                parts.append(prefix)
            if as_subdirs:
                parts.append(get_subdir_name(source_path))
            path_prefix = '/'.join(parts) or None

            print(f'Merging files from {source_path}' + (f' -> {path_prefix}/' if path_prefix else ''))

            index_writer.merge_from_other_barecat(
                resolve_index_path(source_path),
                ignore_duplicates=ignore_duplicates,
                prefix=path_prefix,
            )
            for shard_path in sorted(glob.glob(f'{source_path}-shard-*')):
                os.symlink(
                    osp.relpath(shard_path, start=osp.dirname(target_path)),
                    f'{target_path}-shard-{i_out_shard:05d}',
                )
                i_out_shard += 1


def extract(barecat_path, target_directory):
    from ..io.copyfile import copy

    with barecat_.Barecat(barecat_path) as reader:
        for path_in_archive in progressbar(reader, desc='Extracting files', unit=' files'):
            target_path = osp.join(target_directory, path_in_archive)
            os.makedirs(osp.dirname(target_path), exist_ok=True)
            with open(target_path, 'wb') as output_file:
                copy(reader.open(path_in_archive), output_file)


def archive2barecat(src_path, target_path, shard_size_limit, overwrite=False):
    with barecat_.Barecat(
        target_path, shard_size_limit=shard_size_limit, readonly=False, overwrite=overwrite
    ) as writer:
        for file_or_dir_info, fileobj in iter_archive(src_path):
            writer.add(file_or_dir_info, fileobj=fileobj, dir_exist_ok=True)


def barecat2archive(src_path, target_path, root_dir=None):
    with barecat_.Barecat(src_path, readonly=True) as bc:
        with get_archive_writer(target_path) as target_archive:
            infos = bc.index.iter_all_infos(order=Order.PATH)
            num_total = bc.index.num_files + bc.index.num_dirs
            for entry in progressbar(infos, total=num_total, desc='Writing', unit=' entries'):
                # Skip root directory
                if not entry.path:
                    continue
                original_path = entry.path
                # Prepend root_dir if specified
                if root_dir:
                    entry.path = osp.join(root_dir, entry.path)
                if isinstance(entry, BarecatDirInfo):
                    target_archive.add(entry)
                else:
                    with bc.open(original_path, 'rb') as file_in_barecat:
                        target_archive.add(entry, fileobj=file_in_barecat)


def wrap_archive(src_path, target_path, overwrite=False):
    # New format: target_path IS the index file
    index_path = target_path
    shard_path = f'{target_path}-shard-00000'

    # Check for compression regardless of extension
    compression = is_compressed_file(src_path)
    if compression:
        raise ValueError(
            f"Cannot wrap compressed file ({compression}). "
            "Wrap only works with uncompressed .tar or .zip files."
        )

    # Check ZIP-specific issues (compression, encryption)
    if src_path.lower().endswith('.zip'):
        zip_error = check_zip_wrappable(src_path)
        if zip_error:
            raise ValueError(zip_error)

    if overwrite:
        if osp.exists(index_path):
            os.remove(index_path)
        if osp.lexists(shard_path):
            os.remove(shard_path)

    with barecat_.Index(index_path, readonly=False) as index:
        for file_or_dir_info in iter_archive_nocontent(src_path):
            index.add(file_or_dir_info)

    os.symlink(osp.abspath(src_path), shard_path)


def stdin_tar2barecat(tar_format, target_path, shard_size_limit, overwrite=False):
    """Read tar from stdin and write to barecat."""
    import tarfile

    # Map format to tarfile mode
    mode_map = {
        'tar': 'r|',
        'tar.gz': 'r|gz',
        'tar.bz2': 'r|bz2',
        'tar.xz': 'r|xz',
    }
    mode = mode_map[tar_format]

    with barecat_.Barecat(
        target_path, shard_size_limit=shard_size_limit, readonly=False, overwrite=overwrite
    ) as writer:
        with tarfile.open(fileobj=sys.stdin.buffer, mode=mode) as tar:
            for member in tar:
                if member.isdir():
                    dinfo = BarecatDirInfo(
                        path=member.name,
                        mode=member.mode,
                        uid=member.uid,
                        gid=member.gid,
                        mtime_ns=int(member.mtime * 1_000_000_000),
                    )
                    writer.add(dinfo, dir_exist_ok=True)
                elif member.isfile():
                    finfo = BarecatFileInfo(
                        path=member.name,
                        size=member.size,
                        mode=member.mode,
                        uid=member.uid,
                        gid=member.gid,
                        mtime_ns=int(member.mtime * 1_000_000_000),
                    )
                    with tar.extractfile(member) as file_in_tar:
                        writer.add(finfo, fileobj=file_in_tar, dir_exist_ok=True)


def barecat2stdout_tar(src_path, tar_format, root_dir=None):
    """Read barecat and write tar to stdout."""
    import tarfile

    # Map format to tarfile mode
    mode_map = {
        'tar': 'w|',
        'tar.gz': 'w|gz',
        'tar.bz2': 'w|bz2',
        'tar.xz': 'w|xz',
    }
    mode = mode_map[tar_format]

    with barecat_.Barecat(src_path, readonly=True) as bc:
        with tarfile.open(fileobj=sys.stdout.buffer, mode=mode) as tar:
            for entry in bc.index.iter_all_infos(order=Order.PATH):
                # Skip root directory (empty path)
                if not entry.path:
                    continue
                original_path = entry.path
                tar_path = osp.join(root_dir, entry.path) if root_dir else entry.path
                if isinstance(entry, BarecatDirInfo):
                    tinfo = tarfile.TarInfo(name=tar_path)
                    tinfo.type = tarfile.DIRTYPE
                    tinfo.mode = entry.mode if entry.mode else 0o755
                    tinfo.uid = entry.uid if entry.uid else 0
                    tinfo.gid = entry.gid if entry.gid else 0
                    tinfo.mtime = entry.mtime_ns // 1_000_000_000 if entry.mtime_ns else 0
                    tar.addfile(tinfo)
                else:
                    tinfo = tarfile.TarInfo(name=tar_path)
                    tinfo.size = entry.size
                    tinfo.mode = entry.mode if entry.mode else 0o644
                    tinfo.uid = entry.uid if entry.uid else 0
                    tinfo.gid = entry.gid if entry.gid else 0
                    tinfo.mtime = entry.mtime_ns // 1_000_000_000 if entry.mtime_ns else 0
                    with bc.open(original_path, 'rb') as file_in_barecat:
                        tar.addfile(tinfo, file_in_barecat)


def print_ncdu_json(path):
    timestamp = time.time()
    import importlib.metadata

    progver = importlib.metadata.version('barecat')
    progver = '.'.join(progver.split('.')[:3])

    print(f'[1,1,{{"progname":"barecat","progver":"{progver}","timestamp":{timestamp}}},')
    with barecat_.Index(path) as index_reader:
        _print_ncdu_json(index_reader, '')
    print(']')


# =============================================================================
# Higher-level create wrappers
# =============================================================================

def create_from_stdin_paths(
    target_path, shard_size_limit, zero_terminated=False, overwrite=False, workers=None
):
    iterator = generate_from_stdin(zero_terminated)
    create(iterator, target_path, shard_size_limit, overwrite, workers)


def create_recursive(
    target_path, shard_size_limit, roots, overwrite, strip_root, workers=None, physical_order=False
):
    iterator = generate_from_walks(roots, strip_root)

    if physical_order:
        iterator = iter(
            sorted(iterator, key=lambda x: barecat_physical_order.get_physical_offset(x[0]))
        )

    iterator = progressbar(iterator, desc='Packing files', unit=' files')
    create(iterator, target_path, shard_size_limit, overwrite, workers)


def generate_from_stdin(zero_terminated=False):
    if zero_terminated:
        input_paths = iterate_zero_terminated(sys.stdin.buffer)
    else:
        input_paths = (l.rstrip('\n') for l in sys.stdin)

    for input_path in progressbar(input_paths, desc='Packing files', unit=' files'):
        yield input_path, input_path


def generate_from_walks(roots, strip_root):
    for root in roots:
        if not strip_root:
            yield root, osp.basename(root)

        for dirpath, subdirnames, filenames in os.walk(root):
            for entryname in itertools.chain(filenames, subdirnames):
                full_path = osp.join(dirpath, entryname)
                relpath = osp.relpath(full_path, start=root)
                if not strip_root:
                    store_path = osp.join(osp.basename(root), relpath)
                else:
                    store_path = relpath
                yield full_path, store_path


# =============================================================================
# Implementation helpers
# =============================================================================

def create_without_workers(
    filesys_and_store_path_pairs, target_path, shard_size_limit, overwrite=False, exist_ok=True
):
    with barecat_.Barecat(
        target_path,
        shard_size_limit=shard_size_limit,
        readonly=False,
        overwrite=overwrite,
        exist_ok=exist_ok,
        append_only=False,
    ) as writer:
        for filesys_path, store_path in filesys_and_store_path_pairs:
            writer.add_by_path(filesys_path, store_path)


def create_with_workers(
    filesys_and_store_path_pairs, target_path, shard_size_limit, overwrite=False, exist_ok=True,
    workers=8
):
    if misc.exists(target_path):
        if not exist_ok:
            raise FileExistsError(target_path)
        if overwrite:
            misc.remove(target_path)

    with Sharder(
            target_path,
            shard_size_limit=shard_size_limit,
            readonly=False,
            append_only=False,
            threadsafe=True,
            allow_writing_symlinked_shard=False,
    ) as sharder, ConsumedThreadPool(
            index_writer_main, main_args=(target_path,), max_workers=workers
    ) as ctp:


        for filesys_path, store_path in filesys_and_store_path_pairs:
            statresult = os.stat(filesys_path)

            if stat.S_ISDIR(statresult.st_mode):
                dinfo = BarecatDirInfo(path=store_path)
                dinfo.fill_from_statresult(statresult)
                ctp.submit(userdata=dinfo)
            else:
                finfo = BarecatFileInfo(path=store_path)
                finfo.fill_from_statresult(statresult)
                finfo.shard, finfo.offset = sharder.reserve(finfo.size)
                ctp.submit(
                    sharder.add_by_path,
                    userdata=finfo,
                    args=(filesys_path, finfo.shard, finfo.offset, finfo.size),
                    kwargs=dict(raise_if_cannot_fit=True),
                )


def index_writer_main(target_path, future_iter):
    with barecat_.Index(target_path, readonly=False) as index_writer:
        for future in future_iter:
            info = future.userdata
            if isinstance(info, BarecatDirInfo):
                index_writer.add_dir(info)
                continue

            shard_real, offset_real, size_real, crc32c = future.result()
            info.shard = shard_real
            info.offset = offset_real
            info.crc32c = crc32c

            if info.size != size_real:
                raise ValueError('Size mismatch!')
            index_writer.add_file(info)


# =============================================================================
# Archive format helpers
# =============================================================================

ARCHIVE_EXTENSIONS = ('.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz', '.zip')
ALL_ARCHIVE_EXTENSIONS = ARCHIVE_EXTENSIONS + ('.barecat',)


def is_traditional_archive(path):
    """Check if path looks like a tar/zip archive based on extension."""
    return path.lower().endswith(ARCHIVE_EXTENSIONS)


def get_subdir_name(archive_path):
    """Extract subdir name from archive path by stripping one archive extension."""
    name = osp.basename(archive_path)
    lower = name.lower()
    for ext in ALL_ARCHIVE_EXTENSIONS:
        if lower.endswith(ext):
            return lower.removesuffix(ext)
    return name


def _should_include(path, pattern, filter_rules):
    """Check if path should be included based on pattern or filter rules."""
    import re
    from ..util.glob_to_regex import glob_to_regex

    if pattern is not None:
        recursive = '**' in pattern
        regex = glob_to_regex(pattern, recursive=recursive, include_hidden=True)
        return bool(re.match(regex, path))

    if filter_rules:
        # rsync-style first-match-wins
        for sign, pat in filter_rules:
            recursive = '**' in pat
            regex = glob_to_regex(pat, recursive=recursive, include_hidden=True)
            if re.match(regex, path):
                return sign == '+'
        return True  # default include

    return True


def write_index(dictionary, target_path):
    with barecat_.Index(target_path, readonly=False) as index_writer:
        for path, (shard, offset, size) in dictionary.items():
            index_writer.add_file(
                BarecatFileInfo(path=path, shard=shard, offset=offset, size=size)
            )


def read_index(path):
    with barecat_.Index(path) as reader:
        return dict(reader.items())


def iterate_zero_terminated(fileobj):
    partial_path = b''
    while chunk := fileobj.read(4096):
        parts = chunk.split(b'\x00')
        parts[0] = partial_path + parts[0]
        partial_path = parts.pop()

        for input_path in parts:
            input_path = input_path.decode()
            yield input_path


def is_compressed_file(path):
    """Check if file is compressed by reading magic bytes."""
    with open(path, 'rb') as f:
        magic = f.read(6)
    # gzip: 1f 8b, bzip2: 42 5a 68 (BZh), xz: fd 37 7a 58 5a 00
    if magic[:2] == b'\x1f\x8b':
        return 'gzip'
    if magic[:3] == b'BZh':
        return 'bzip2'
    if magic[:6] == b'\xfd7zXZ\x00':
        return 'xz'
    return None


def check_zip_wrappable(path):
    """Check if ZIP file can be wrapped. Returns error message or None if OK."""
    import zipfile
    with zipfile.ZipFile(path, 'r') as zf:
        # Check for multi-disk/split archives
        if zf.namelist() and hasattr(zf, 'fp') and zf.fp:
            # Read end of central directory to check disk number
            pass  # zipfile would fail to open split archives anyway

        for info in zf.infolist():
            if info.compress_type != zipfile.ZIP_STORED:
                return (
                    "ZIP has compressed entries. "
                    "Use 'zip -0' to create uncompressed ZIP, or convert without --wrap."
                )
            # Check for encryption (bit 0 of flag_bits)
            if info.flag_bits & 0x1:
                return "ZIP has encrypted entries. Cannot wrap encrypted ZIP files."
    return None


def _print_ncdu_json(index_reader, dirpath):
    basename = '/' if dirpath == '' else osp.basename(dirpath)

    print('[', json.dumps(dict(name=basename, asize=4096, ino=0)), end='')
    infos = index_reader.listdir_infos(dirpath)
    file_infos = [f for f in infos if isinstance(f, BarecatFileInfo)]
    subdir_infos = [d for d in infos if isinstance(d, BarecatDirInfo)]
    del infos

    if file_infos:
        filedump = json.dumps(
            [
                dict(name=osp.basename(fi.path), asize=fi.size, dsize=fi.size, ino=0)
                for fi in file_infos
            ]
        )
        print(',', filedump[1:-1], end='')
    del file_infos

    for subdir in subdir_infos:
        print(',')
        _print_ncdu_json(index_reader, subdir.path)

    print(']', end='')
