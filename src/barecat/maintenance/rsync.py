"""rsync-like interface for barecat operations.

Supports:
    barecat rsync local/ archive.barecat::      # add contents to root
    barecat rsync local archive.barecat::       # add as subdirectory
    barecat rsync archive.barecat::/ local/     # extract contents
    barecat rsync archive.barecat:: local/      # extract as subdirectory
    barecat rsync arch1.barecat:: arch2.barecat:: dest.barecat::  # merge
    barecat rsync --delete src/ archive.barecat::  # sync with deletion
"""
import os
import os.path as osp
import fnmatch
import re
import shutil
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import barecat
from ..core.paths import resolve_index_path
from ..util.misc import exists as barecat_exists


class PathType(Enum):
    LOCAL = 'local'           # Local file/directory
    LOCAL_ARCHIVE = 'local_archive'  # Local barecat archive
    TAR_ZIP = 'tar_zip'       # Local tar/zip archive (read-only source)
    SSH = 'ssh'               # Remote via SSH
    SSH_ARCHIVE = 'ssh_archive'  # Remote barecat archive via SSH
    BARECAT_SERVER = 'barecat_server'  # Barecat serve-remote


# Extensions for tar/zip detection
TAR_EXTENSIONS = ('.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz')
ZIP_EXTENSIONS = ('.zip',)


@dataclass
class ParsedPath:
    """Parsed source or destination path."""
    path_type: PathType
    host: Optional[str]          # SSH host or barecat server host:port
    user: Optional[str]          # SSH user (optional)
    filesystem_path: str         # Path on filesystem (local or remote)
    archive_path: Optional[str]  # Path to .barecat archive (None if not archive)
    inner_path: str              # Path within archive (empty if not archive)
    trailing_slash: bool         # Whether path had trailing slash

    @property
    def contents_mode(self) -> bool:
        """True if trailing slash = copy contents, not directory itself."""
        return self.trailing_slash

    @property
    def is_archive(self) -> bool:
        """True if this is a barecat archive path."""
        return self.path_type in (
            PathType.LOCAL_ARCHIVE, PathType.SSH_ARCHIVE, PathType.BARECAT_SERVER)

    @property
    def is_tar_zip(self) -> bool:
        """True if this is a tar/zip archive."""
        return self.path_type == PathType.TAR_ZIP

    @property
    def is_remote(self) -> bool:
        """True if this is a remote path (SSH or barecat server)."""
        return self.path_type in (
            PathType.SSH, PathType.SSH_ARCHIVE, PathType.BARECAT_SERVER)

    @property
    def archive_basename(self) -> str:
        """Archive name without .barecat extension."""
        if not self.archive_path:
            return ''
        name = osp.basename(self.archive_path)
        # Strip extensions (old format: -sqlite-index, new format: .barecat is the file itself)
        return name.removesuffix('-sqlite-index').removesuffix('.barecat')


@dataclass
class RsyncOptions:
    """rsync-like options."""
    delete: bool = False          # --delete: remove extraneous files from dest
    dry_run: bool = False         # -n, --dry-run: show what would be done
    verbose: bool = False         # -v, --verbose: increase verbosity
    progress: bool = False        # --progress: show progress
    checksum: bool = False        # -c, --checksum: compare by checksum (barecat→barecat only)
    update: bool = False          # -u, --update: skip files newer on dest
    ignore_existing: bool = False # --ignore-existing: skip files that exist on dest
    existing: bool = False        # --existing: only update existing files, skip new
    size_only: bool = False       # --size-only: compare by size only, not mtime
    times: bool = True            # -t, --times: preserve modification times (default True)
    max_size: int = None          # --max-size: skip files larger than this
    min_size: int = None          # --min-size: skip files smaller than this
    include: list = None          # --include patterns
    exclude: list = None          # --exclude patterns
    recursive: bool = True        # -r (default True for dirs)

    def __post_init__(self):
        self.include = self.include or []
        self.exclude = self.exclude or []


def rsync(sources: list[str], dest: str, options: RsyncOptions = None):
    """Main rsync entry point.

    Args:
        sources: List of source paths
        dest: Destination path
        options: RsyncOptions

    Syntax:
        ./data/                      - local directory
        ./archive.barecat::          - local archive root
        ./archive.barecat::images/   - local archive with inner path
        host:/path/                  - remote directory via SSH
        host:/path/archive.barecat:: - remote archive via SSH
        barecat://host:port/arch::   - barecat server
    """
    options = options or RsyncOptions()

    parsed_sources = [parse_path(s) for s in sources]
    parsed_dest = parse_path(dest)

    # Validate: can't have local dir -> local dir (use regular rsync)
    if all(s.path_type == PathType.LOCAL for s in parsed_sources) and \
       parsed_dest.path_type == PathType.LOCAL:
        raise ValueError("Use regular rsync for local-to-local directory sync")

    # SSH directories not yet supported (only SSH archives)
    if parsed_dest.path_type == PathType.SSH:
        raise NotImplementedError("SSH directory destinations not yet supported (use archive)")
    if any(s.path_type == PathType.SSH for s in parsed_sources):
        raise NotImplementedError("SSH directory sources not yet supported (use archive)")

    # Determine operation type
    if parsed_dest.is_archive:
        _rsync_to_archive(parsed_sources, parsed_dest, options)
    else:
        _rsync_to_local(parsed_sources, parsed_dest, options)


def _rsync_to_archive(sources: list[ParsedPath], dest: ParsedPath, options: RsyncOptions):
    """Sync from local/archive sources to archive destination (local or remote)."""

    # In dry-run mode for local archives, don't create the archive
    if options.dry_run and dest.path_type == PathType.LOCAL_ARCHIVE:
        if barecat_exists(dest.archive_path):
            dest_bc = barecat.Barecat(dest.archive_path, readonly=True)
        else:
            dest_bc = None  # Archive doesn't exist, all files are "new"
    elif options.dry_run:
        # For remote, try to open readonly (will fail if doesn't exist)
        try:
            dest_bc = _open_archive(dest, readonly=True)
        except Exception:
            dest_bc = None
    else:
        dest_bc = _open_archive(dest, readonly=False)

    try:
        dest_prefix = dest.inner_path

        for src in sources:
            if src.path_type == PathType.LOCAL:
                _sync_local_to_archive(src, dest_bc, dest_prefix, options)
            elif src.is_tar_zip:
                _sync_tarzip_to_archive(src, dest_bc, dest_prefix, options)
            elif src.is_archive:
                _sync_archive_to_archive(src, dest_bc, dest_prefix, options)
            else:
                raise NotImplementedError(f"Source type {src.path_type} not yet supported")

        if options.delete and dest_bc is not None:
            _delete_extraneous_in_archive(sources, dest_bc, dest_prefix, options)
    finally:
        if dest_bc is not None:
            dest_bc.close()


def _rsync_to_local(sources: list[ParsedPath], dest: ParsedPath, options: RsyncOptions):
    """Sync from archive sources to local directory destination."""

    dest_dir = dest.filesystem_path
    os.makedirs(dest_dir, exist_ok=True)

    for src in sources:
        if src.is_archive:
            _sync_archive_to_local(src, dest_dir, options)
        elif src.is_tar_zip:
            _sync_tarzip_to_local(src, dest_dir, options)
        else:
            raise ValueError("Use regular rsync for local-to-local sync")

    if options.delete:
        _delete_extraneous_local(sources, dest_dir, options)


# -----------------------------------------------------------------------------
# Parsing and utility functions
# -----------------------------------------------------------------------------

def _find_unescaped(s: str, needle: str) -> int:
    """Find first occurrence of needle not preceded by odd number of backslashes.

    Returns index or -1 if not found.
    """
    i = 0
    while i < len(s):
        idx = s.find(needle, i)
        if idx == -1:
            return -1
        # Count preceding backslashes
        num_backslashes = 0
        j = idx - 1
        while j >= 0 and s[j] == '\\':
            num_backslashes += 1
            j -= 1
        # If even number of backslashes, this is unescaped
        if num_backslashes % 2 == 0:
            return idx
        i = idx + 1
    return -1


def _unescape(s: str) -> str:
    """Unescape \\:: -> :: and \\\\ -> \\."""
    # Process character by character
    result = []
    i = 0
    while i < len(s):
        if s[i] == '\\' and i + 1 < len(s):
            next_char = s[i + 1]
            if next_char == '\\':
                result.append('\\')
                i += 2
            elif i + 2 < len(s) and s[i + 1:i + 3] == '::':
                result.append('::')
                i += 3
            else:
                result.append(s[i])
                i += 1
        else:
            result.append(s[i])
            i += 1
    return ''.join(result)


def parse_path(path: str) -> ParsedPath:
    """Parse a path with new syntax.

    Syntax:
        :: = archive inner path delimiter
        : after host = SSH remote
        \\:: = literal :: in path
        \\\\ = literal \\ in path

    Examples:
        './data/'                        -> local directory
        './archive.barecat::'            -> local archive root
        './archive.barecat::images/'     -> local archive inner path
        './my\\::\\:weird.barecat::'     -> file named 'my::weird.barecat'
        'host:/path/dir/'                -> SSH remote directory
        'host:/path/archive.barecat::'   -> SSH remote archive
        'user@host:/path/archive.barecat::train/'
        'barecat://host:50003/archive::images/'
    """
    trailing_slash = path.endswith('/')
    path = path.rstrip('/')

    user = None
    host = None

    # Check for barecat:// protocol
    if path.startswith('barecat://'):
        rest = path[len('barecat://'):]
        # Format: host:port/archive::inner
        slash_idx = rest.find('/')
        if slash_idx == -1:
            raise ValueError(f"Invalid barecat:// URL: {path}")
        host = rest[:slash_idx]
        rest = rest[slash_idx + 1:]

        delim_idx = _find_unescaped(rest, '::')
        if delim_idx != -1:
            archive_name = _unescape(rest[:delim_idx])
            inner_path = _unescape(rest[delim_idx + 2:])
        else:
            archive_name = _unescape(rest)
            inner_path = ''

        return ParsedPath(
            path_type=PathType.BARECAT_SERVER,
            host=host,
            user=None,
            filesystem_path='',
            archive_path=archive_name,
            inner_path=inner_path.lstrip('/'),
            trailing_slash=trailing_slash,
        )

    # Check for SSH: [user@]host:/path
    # Pattern: optional user@, then hostname, then :, then / (absolute path)
    ssh_match = re.match(r'^(?:([^@]+)@)?([^:/]+):(/.*?)$', path)
    if ssh_match:
        user = ssh_match.group(1)
        host = ssh_match.group(2)
        remote_path = ssh_match.group(3)

        # Check if remote path contains unescaped :: (archive)
        delim_idx = _find_unescaped(remote_path, '::')
        if delim_idx != -1:
            archive_path = _unescape(remote_path[:delim_idx])
            inner_path = _unescape(remote_path[delim_idx + 2:])
            return ParsedPath(
                path_type=PathType.SSH_ARCHIVE,
                host=host,
                user=user,
                filesystem_path=_unescape(remote_path),
                archive_path=archive_path,
                inner_path=inner_path.lstrip('/'),
                trailing_slash=trailing_slash,
            )
        else:
            return ParsedPath(
                path_type=PathType.SSH,
                host=host,
                user=user,
                filesystem_path=_unescape(remote_path),
                archive_path=None,
                inner_path='',
                trailing_slash=trailing_slash,
            )

    # Local path - check for unescaped :: (archive delimiter)
    delim_idx = _find_unescaped(path, '::')
    if delim_idx != -1:
        archive_path = _unescape(path[:delim_idx])
        inner_path = _unescape(path[delim_idx + 2:])
        lower_archive = archive_path.lower()

        # Check if it's a tar/zip archive
        if any(lower_archive.endswith(ext) for ext in TAR_EXTENSIONS + ZIP_EXTENSIONS):
            return ParsedPath(
                path_type=PathType.TAR_ZIP,
                host=None,
                user=None,
                filesystem_path=archive_path,
                archive_path=archive_path,
                inner_path=inner_path.lstrip('/'),
                trailing_slash=trailing_slash,
            )

        # Otherwise it's a barecat archive
        return ParsedPath(
            path_type=PathType.LOCAL_ARCHIVE,
            host=None,
            user=None,
            filesystem_path=_unescape(path),
            archive_path=archive_path,
            inner_path=inner_path.lstrip('/'),
            trailing_slash=trailing_slash,
        )

    # Plain local path (file or directory)
    unescaped_path = _unescape(path)
    return ParsedPath(
        path_type=PathType.LOCAL,
        host=None,
        user=None,
        filesystem_path=unescaped_path,
        archive_path=None,
        inner_path='',
        trailing_slash=trailing_slash,
    )


def _open_archive(parsed: ParsedPath, readonly: bool):
    """Open a barecat archive (local or remote) based on parsed path.

    Returns a context manager that yields an archive-like object.
    """
    if parsed.path_type == PathType.LOCAL_ARCHIVE:
        return barecat.Barecat(parsed.archive_path, readonly=readonly)

    elif parsed.path_type == PathType.BARECAT_SERVER:
        try:
            from ..distributed.remote import BarecatRemoteClient
        except ImportError:
            raise ImportError(
                "Remote rsync requires the distributed module. "
                "Install barecat from the dev branch for this feature."
            )
        host_port = parsed.host
        if ':' in host_port:
            host, port = host_port.rsplit(':', 1)
            port = int(port)
        else:
            host, port = host_port, 50003
        return BarecatRemoteClient(parsed.archive_path, host=host, port=port)

    elif parsed.path_type == PathType.SSH_ARCHIVE:
        try:
            from ..distributed.ssh import SSHBarecatClient
        except ImportError:
            raise ImportError(
                "SSH rsync requires the distributed module. "
                "Install barecat from the dev branch for this feature."
            )
        return SSHBarecatClient(
            host=parsed.host,
            user=parsed.user,
            archive_path=parsed.archive_path,
            readonly=readonly,
        )

    else:
        raise ValueError(f"Cannot open archive for path type: {parsed.path_type}")


def _get_file_infos(archive, pattern: str):
    """Get file infos from any archive type (local or remote).

    Returns iterable of file info objects with path, size, mtime_ns, etc.
    Works with Barecat, SSHBarecatClient, and BarecatRemoteClient.
    """
    if hasattr(archive, 'glob_infos'):
        # Remote client (SSH or barecat server)
        return archive.glob_infos(pattern)
    else:
        # Local barecat - use index
        return archive.index.iterglob_infos(pattern, recursive=True, include_hidden=True)


def _lookup_file(archive, path: str):
    """Look up file info from any archive type.

    Returns file info object with path, size, mtime_ns, etc.
    """
    if hasattr(archive, 'index'):
        # Local barecat
        return archive.index.lookup_file(path)
    else:
        # Remote client - get info via glob (or dedicated info method if available)
        # For now, return a simple object with size/mtime from glob
        infos = archive.glob_infos(path)
        if infos:
            return infos[0]
        raise KeyError(path)


# -----------------------------------------------------------------------------
# Sync implementation functions
# -----------------------------------------------------------------------------

def _sync_local_to_archive(
    src: ParsedPath,
    dest_bc: barecat.Barecat,
    dest_prefix: str,
    options: RsyncOptions
):
    """Sync local directory/file to archive."""
    from ..util.progbar import progressbar

    src_path = src.filesystem_path

    if not osp.exists(src_path):
        raise FileNotFoundError(f"Source not found: {src_path}")

    # Determine destination prefix based on trailing slash
    if src.contents_mode:
        # src/ -> copy contents directly to dest_prefix
        prefix = dest_prefix
    else:
        # src -> copy as dest_prefix/src_basename
        prefix = osp.join(dest_prefix, osp.basename(src_path))

    if osp.isfile(src_path):
        _sync_one_file_to_archive(src_path, prefix, dest_bc, options)
    else:
        # Collect files for progress bar
        file_list = []
        for root, dirs, files in os.walk(src_path):
            dirs[:] = [d for d in dirs if not _is_excluded(d, options)]
            rel_root = osp.relpath(root, src_path)
            if rel_root == '.':
                rel_root = ''
            for fname in files:
                if _is_excluded(fname, options):
                    continue
                local_path = osp.join(root, fname)
                rel_path = osp.join(rel_root, fname) if rel_root else fname
                archive_path = osp.join(prefix, rel_path) if prefix else rel_path
                file_list.append((local_path, archive_path))

        if options.progress:
            file_list = progressbar(file_list, desc="Adding", unit="files")

        for local_path, archive_path in file_list:
            _sync_one_file_to_archive(local_path, archive_path, dest_bc, options)


def _sync_one_file_to_archive(
    local_path: str,
    archive_path: str,
    dest_bc,  # Optional[barecat.Barecat] - None in dry-run when archive doesn't exist
    options: RsyncOptions
):
    """Sync a single file from local to archive."""
    from ..core.types import BarecatFileInfo

    archive_path = archive_path.lstrip('/')
    src_stat = os.stat(local_path)

    # Size filters
    if options.max_size is not None and src_stat.st_size > options.max_size:
        return
    if options.min_size is not None and src_stat.st_size < options.min_size:
        return

    # Check if update needed
    exists = dest_bc is not None and archive_path in dest_bc

    # --existing: only update files that already exist
    if options.existing and not exists:
        if options.verbose:
            print(f"skip (new): {archive_path}")
        return

    # --ignore-existing: skip files that exist
    if options.ignore_existing and exists:
        if options.verbose:
            print(f"skip (exists): {archive_path}")
        return

    if exists:
        dest_info = dest_bc.index.lookup_file(archive_path)

        if options.update:
            # Skip if dest is newer
            if dest_info.mtime_ns and dest_info.mtime_ns / 1e9 >= src_stat.st_mtime:
                if options.verbose:
                    print(f"skip (newer): {archive_path}")
                return

        # Note: --checksum only compares checksums for barecat→barecat
        # For local→barecat, --checksum just always copies
        if not options.checksum:
            if options.size_only:
                # Compare by size only
                if dest_info.size == src_stat.st_size:
                    if options.verbose:
                        print(f"skip (same size): {archive_path}")
                    return
            else:
                # Compare size and mtime
                if (dest_info.size == src_stat.st_size and
                    dest_info.mtime_ns == src_stat.st_mtime_ns):
                    if options.verbose:
                        print(f"skip (unchanged): {archive_path}")
                    return

    if options.dry_run:
        print(f"would {'update' if exists else 'add'}: {archive_path}")
        return

    if options.verbose:
        print(f"{'update' if exists else 'add'}: {archive_path}")

    # Create file info with metadata
    finfo = BarecatFileInfo(path=archive_path)
    finfo.fill_from_statresult(src_stat)

    # If not preserving times, use current time
    if not options.times:
        finfo.mtime_ns = time.time_ns()

    with open(local_path, 'rb') as f:
        if exists:
            dest_bc.update_file(archive_path, finfo, fileobj=f)
        else:
            dest_bc.add(finfo, fileobj=f)


def _sync_tarzip_to_archive(
    src: ParsedPath,
    dest_bc,  # Optional - None in dry-run when archive doesn't exist
    dest_prefix: str,
    options: RsyncOptions
):
    """Sync tar/zip archive to barecat archive."""
    from ..formats.archive_formats import iter_archive
    from ..core.types import BarecatFileInfo

    src_path = src.filesystem_path
    src_inner = src.inner_path  # Inner path within tar/zip

    if not osp.exists(src_path):
        raise FileNotFoundError(f"Source not found: {src_path}")

    # Determine destination prefix based on trailing slash
    if src.contents_mode:
        # src.tar.gz::path/ -> copy contents directly to dest_prefix
        prefix = dest_prefix
    else:
        # src.tar.gz::path -> copy as dest_prefix/basename
        if src_inner:
            basename = osp.basename(src_inner)
        else:
            basename = osp.basename(src_path)
            # Strip archive extensions
            for ext in TAR_EXTENSIONS + ZIP_EXTENSIONS:
                if basename.lower().endswith(ext):
                    basename = basename[:-len(ext)]
                    break
        prefix = osp.join(dest_prefix, basename) if dest_prefix else basename

    for src_info, fileobj in iter_archive(src_path):
        if not src_info.isfile():
            continue

        # Filter by inner path if specified
        if src_inner:
            if not (src_info.path == src_inner or src_info.path.startswith(src_inner + '/')):
                if fileobj:
                    fileobj.read()  # Must consume
                continue
            # Compute relative path from inner path
            rel_path = src_info.path[len(src_inner):].lstrip('/')
        else:
            rel_path = src_info.path

        if _is_excluded(osp.basename(rel_path), options):
            if fileobj:
                fileobj.read()
            continue

        # Size filters
        if options.max_size is not None and src_info.size > options.max_size:
            if fileobj:
                fileobj.read()
            continue
        if options.min_size is not None and src_info.size < options.min_size:
            if fileobj:
                fileobj.read()
            continue

        # Compute destination path
        dest_path = osp.join(prefix, rel_path) if prefix else rel_path
        dest_path = dest_path.lstrip('/')

        # Check if needs sync
        exists = dest_bc is not None and dest_path in dest_bc

        # --existing: only update files that already exist
        if options.existing and not exists:
            if options.verbose:
                print(f"skip (new): {dest_path}")
            if fileobj:
                fileobj.read()
            continue

        # --ignore-existing: skip files that exist
        if options.ignore_existing and exists:
            if options.verbose:
                print(f"skip (exists): {dest_path}")
            if fileobj:
                fileobj.read()
            continue

        if exists:
            dest_info = _lookup_file(dest_bc, dest_path)

            if options.update:
                if dest_info.mtime_ns and src_info.mtime_ns and dest_info.mtime_ns >= src_info.mtime_ns:
                    if options.verbose:
                        print(f"skip (newer): {dest_path}")
                    if fileobj:
                        fileobj.read()
                    continue

            # Note: --checksum for tar/zip always copies since source has no stored checksum
            if not options.checksum:
                if options.size_only:
                    if src_info.size == dest_info.size:
                        if options.verbose:
                            print(f"skip (same size): {dest_path}")
                        if fileobj:
                            fileobj.read()
                        continue
                else:
                    if src_info.size == dest_info.size and src_info.mtime_ns == dest_info.mtime_ns:
                        if options.verbose:
                            print(f"skip (unchanged): {dest_path}")
                        if fileobj:
                            fileobj.read()
                        continue

        if options.dry_run:
            print(f"would {'update' if exists else 'copy'}: {src_info.path} -> {dest_path}")
            # Must consume fileobj before continuing
            if fileobj:
                fileobj.read()
            continue

        if options.verbose or options.progress:
            print(f"{'update' if exists else 'copy'}: {dest_path}")

        # Create file info
        finfo = BarecatFileInfo(
            path=dest_path,
            size=src_info.size,
            mode=src_info.mode,
            uid=src_info.uid,
            gid=src_info.gid,
            mtime_ns=src_info.mtime_ns if options.times else time.time_ns(),
        )

        if exists:
            if fileobj:
                dest_bc.update_file(dest_path, finfo, fileobj=fileobj)
            else:
                dest_bc.update_file(dest_path, finfo, data=b'')
        else:
            if fileobj:
                dest_bc.add(finfo, fileobj=fileobj)
            else:
                dest_bc.add(finfo, data=b'')


def _sync_archive_to_local(src: ParsedPath, dest_dir: str, options: RsyncOptions):
    """Sync archive (local or remote) to local directory."""
    from ..util.progbar import progressbar

    with _open_archive(src, readonly=True) as src_bc:
        src_prefix = src.inner_path

        # Determine destination based on trailing slash
        if src.contents_mode:
            # archive:path/ -> extract contents to dest_dir
            out_dir = dest_dir
        else:
            # archive:path -> extract as dest_dir/basename
            if src_prefix:
                basename = osp.basename(src_prefix)
            else:
                basename = src.archive_basename
            out_dir = osp.join(dest_dir, basename)

        # Iterate files with metadata (works for both local and remote)
        pattern = f"{src_prefix}/**" if src_prefix else "**"
        infos = _get_file_infos(src_bc, pattern)

        if options.progress:
            infos = progressbar(infos, desc="Extracting", unit="files")

        for info in infos:
            if not info.isfile():  # skip directories
                continue
            if _is_excluded(osp.basename(info.path), options):
                continue

            # Compute relative path
            if src_prefix:
                rel_path = info.path[len(src_prefix):].lstrip('/')
            else:
                rel_path = info.path

            local_path = osp.join(out_dir, rel_path)
            _sync_one_file_to_local(src_bc, info, local_path, options)


def _sync_one_file_to_local(
    src_bc,  # Barecat, SSHBarecatClient, or BarecatRemoteClient
    src_info,  # BarecatFileInfo, SSHFileInfo, or RemoteFileInfo
    local_path: str,
    options: RsyncOptions
):
    """Sync a single file from archive to local."""

    # Size filters
    if options.max_size is not None and src_info.size > options.max_size:
        return
    if options.min_size is not None and src_info.size < options.min_size:
        return

    exists = osp.exists(local_path)

    # --existing: only update files that already exist
    if options.existing and not exists:
        if options.verbose:
            print(f"skip (new): {local_path}")
        return

    # --ignore-existing: skip files that exist
    if options.ignore_existing and exists:
        if options.verbose:
            print(f"skip (exists): {local_path}")
        return

    if exists:
        if options.update:
            dest_mtime = osp.getmtime(local_path)
            if src_info.mtime_ns and dest_mtime >= src_info.mtime_ns / 1e9:
                if options.verbose:
                    print(f"skip (newer): {local_path}")
                return

        # Note: --checksum only compares checksums for barecat→barecat
        # For barecat→local, --checksum just always copies
        if not options.checksum:
            dest_stat = os.stat(local_path)
            if options.size_only:
                if src_info.size == dest_stat.st_size:
                    if options.verbose:
                        print(f"skip (same size): {local_path}")
                    return
            else:
                if (src_info.size == dest_stat.st_size and
                    src_info.mtime_ns == int(dest_stat.st_mtime_ns)):
                    if options.verbose:
                        print(f"skip (unchanged): {local_path}")
                    return

    if options.dry_run:
        print(f"would extract: {local_path}")
        return

    if options.verbose:
        print(f"extract: {local_path}")

    os.makedirs(osp.dirname(local_path), exist_ok=True)
    with src_bc.open(src_info.path, 'rb') as src_f, open(local_path, 'wb') as dst_f:
        shutil.copyfileobj(src_f, dst_f)

    # Preserve metadata
    if src_info.mtime_ns:
        os.utime(local_path, ns=(src_info.mtime_ns, src_info.mtime_ns))


def _sync_tarzip_to_local(src: ParsedPath, dest_dir: str, options: RsyncOptions):
    """Sync tar/zip archive to local directory."""
    from ..formats.archive_formats import iter_archive

    src_path = src.filesystem_path
    src_inner = src.inner_path  # Inner path within tar/zip

    if not osp.exists(src_path):
        raise FileNotFoundError(f"Source not found: {src_path}")

    # Determine destination based on trailing slash
    if src.contents_mode:
        # src.tar.gz::path/ -> extract contents to dest_dir
        out_dir = dest_dir
    else:
        # src.tar.gz::path -> extract as dest_dir/basename
        if src_inner:
            basename = osp.basename(src_inner)
        else:
            basename = osp.basename(src_path)
            for ext in TAR_EXTENSIONS + ZIP_EXTENSIONS:
                if basename.lower().endswith(ext):
                    basename = basename[:-len(ext)]
                    break
        out_dir = osp.join(dest_dir, basename)

    for src_info, fileobj in iter_archive(src_path):
        if not src_info.isfile():
            continue

        # Filter by inner path if specified
        if src_inner:
            if not (src_info.path == src_inner or src_info.path.startswith(src_inner + '/')):
                if fileobj:
                    fileobj.read()  # Must consume
                continue
            # Compute relative path from inner path
            rel_path = src_info.path[len(src_inner):].lstrip('/')
        else:
            rel_path = src_info.path

        if _is_excluded(osp.basename(rel_path), options):
            if fileobj:
                fileobj.read()
            continue

        # Size filters
        if options.max_size is not None and src_info.size > options.max_size:
            if fileobj:
                fileobj.read()
            continue
        if options.min_size is not None and src_info.size < options.min_size:
            if fileobj:
                fileobj.read()
            continue

        local_path = osp.join(out_dir, rel_path)
        exists = osp.exists(local_path)

        # --existing: only update files that already exist
        if options.existing and not exists:
            if options.verbose:
                print(f"skip (new): {local_path}")
            if fileobj:
                fileobj.read()
            continue

        # --ignore-existing: skip files that exist
        if options.ignore_existing and exists:
            if options.verbose:
                print(f"skip (exists): {local_path}")
            if fileobj:
                fileobj.read()
            continue

        if exists:
            if options.update:
                dest_mtime = osp.getmtime(local_path)
                if src_info.mtime_ns and dest_mtime >= src_info.mtime_ns / 1e9:
                    if options.verbose:
                        print(f"skip (newer): {local_path}")
                    if fileobj:
                        fileobj.read()
                    continue

            if not options.checksum:
                dest_stat = os.stat(local_path)
                if options.size_only:
                    if src_info.size == dest_stat.st_size:
                        if options.verbose:
                            print(f"skip (same size): {local_path}")
                        if fileobj:
                            fileobj.read()
                        continue
                else:
                    if (src_info.size == dest_stat.st_size and
                        src_info.mtime_ns == int(dest_stat.st_mtime_ns)):
                        if options.verbose:
                            print(f"skip (unchanged): {local_path}")
                        if fileobj:
                            fileobj.read()
                        continue

        if options.dry_run:
            print(f"would extract: {local_path}")
            if fileobj:
                fileobj.read()
            continue

        if options.verbose:
            print(f"extract: {local_path}")

        os.makedirs(osp.dirname(local_path), exist_ok=True)
        with open(local_path, 'wb') as f:
            if fileobj:
                f.write(fileobj.read())

        # Preserve metadata
        if src_info.mtime_ns:
            mtime_ns = int(src_info.mtime_ns)
            os.utime(local_path, ns=(mtime_ns, mtime_ns))


def _sync_archive_to_archive(
    src: ParsedPath,
    dest_bc,  # Optional - None in dry-run when archive doesn't exist
    dest_prefix: str,
    options: RsyncOptions
):
    """Sync from one archive to another (merge). Source can be local or remote."""

    with _open_archive(src, readonly=True) as src_bc:
        src_prefix = src.inner_path

        # Determine destination prefix based on trailing slash
        if src.contents_mode:
            prefix = dest_prefix
        else:
            if src_prefix:
                basename = osp.basename(src_prefix)
            else:
                basename = src.archive_basename
            prefix = osp.join(dest_prefix, basename) if dest_prefix else basename

        # Iterate source files with metadata (works for both local and remote)
        pattern = f"{src_prefix}/**" if src_prefix else "**"
        for src_info in _get_file_infos(src_bc, pattern):
            if not src_info.isfile():
                continue
            if _is_excluded(osp.basename(src_info.path), options):
                continue

            # Size filters
            if options.max_size is not None and src_info.size > options.max_size:
                continue
            if options.min_size is not None and src_info.size < options.min_size:
                continue

            # Compute destination path
            if src_prefix:
                rel_path = src_info.path[len(src_prefix):].lstrip('/')
            else:
                rel_path = src_info.path

            dest_path = osp.join(prefix, rel_path) if prefix else rel_path
            dest_path = dest_path.lstrip('/')

            # Check if needs sync
            exists = dest_bc is not None and dest_path in dest_bc

            # --existing: only update files that already exist
            if options.existing and not exists:
                if options.verbose:
                    print(f"skip (new): {dest_path}")
                continue

            # --ignore-existing: skip files that exist
            if options.ignore_existing and exists:
                if options.verbose:
                    print(f"skip (exists): {dest_path}")
                continue

            if exists:
                dest_info = _lookup_file(dest_bc, dest_path)

                if options.update:
                    # Skip if dest is newer
                    if dest_info.mtime_ns and src_info.mtime_ns and dest_info.mtime_ns >= src_info.mtime_ns:
                        if options.verbose:
                            print(f"skip (newer): {dest_path}")
                        continue

                if options.checksum:
                    # Compare by checksum (both sides have crc32c in database)
                    if src_info.crc32c == dest_info.crc32c:
                        if options.verbose:
                            print(f"skip (checksum match): {dest_path}")
                        continue
                else:
                    if options.size_only:
                        if src_info.size == dest_info.size:
                            if options.verbose:
                                print(f"skip (same size): {dest_path}")
                            continue
                    else:
                        # Compare size and mtime
                        if src_info.size == dest_info.size and src_info.mtime_ns == dest_info.mtime_ns:
                            if options.verbose:
                                print(f"skip (unchanged): {dest_path}")
                            continue

            if options.dry_run:
                print(f"would {'update' if exists else 'copy'}: {src_info.path} -> {dest_path}")
                continue

            if options.verbose or options.progress:
                print(f"{'update' if exists else 'copy'}: {dest_path}")

            # Create file info with metadata from source
            from ..core.types import BarecatFileInfo
            finfo = BarecatFileInfo(
                path=dest_path,
                size=src_info.size,
                mode=src_info.mode,
                uid=src_info.uid,
                gid=src_info.gid,
                mtime_ns=src_info.mtime_ns if options.times else time.time_ns(),
            )

            with src_bc.open(src_info.path, 'rb') as f:
                if exists:
                    dest_bc.update_file(dest_path, finfo, fileobj=f)
                else:
                    dest_bc.add(finfo, fileobj=f)


def _delete_extraneous_in_archive(
    sources: list[ParsedPath],
    dest_bc,  # Barecat (destination is always local/writable for now)
    dest_prefix: str,
    options: RsyncOptions
):
    """Delete files in dest archive not present in sources."""
    # Build set of expected files
    expected = set()

    for src in sources:
        if src.path_type == PathType.LOCAL:
            # Local filesystem source
            src_path = src.filesystem_path
            if src.contents_mode:
                prefix = dest_prefix
            else:
                prefix = osp.join(dest_prefix, osp.basename(src_path))

            for root, dirs, files in os.walk(src_path):
                rel_root = osp.relpath(root, src_path)
                if rel_root == '.':
                    rel_root = ''
                for fname in files:
                    rel_path = osp.join(rel_root, fname) if rel_root else fname
                    archive_path = osp.join(prefix, rel_path) if prefix else rel_path
                    expected.add(archive_path.lstrip('/'))

        elif src.is_tar_zip:
            # Tar/zip source
            from ..formats.archive_formats import iter_archive_nocontent
            src_path = src.filesystem_path
            src_inner = src.inner_path

            if src.contents_mode:
                prefix = dest_prefix
            else:
                if src_inner:
                    basename = osp.basename(src_inner)
                else:
                    basename = osp.basename(src_path)
                    for ext in TAR_EXTENSIONS + ZIP_EXTENSIONS:
                        if basename.lower().endswith(ext):
                            basename = basename[:-len(ext)]
                            break
                prefix = osp.join(dest_prefix, basename) if dest_prefix else basename

            for info in iter_archive_nocontent(src_path):
                if not info.isfile():
                    continue
                # Filter by inner path if specified
                if src_inner:
                    if not (info.path == src_inner or info.path.startswith(src_inner + '/')):
                        continue
                    rel_path = info.path[len(src_inner):].lstrip('/')
                else:
                    rel_path = info.path
                archive_path = osp.join(prefix, rel_path) if prefix else rel_path
                expected.add(archive_path.lstrip('/'))

        elif src.is_archive:
            # Archive source (local or remote)
            src_prefix = src.inner_path
            if src.contents_mode:
                prefix = dest_prefix
            else:
                if src_prefix:
                    basename = osp.basename(src_prefix)
                else:
                    basename = src.archive_basename
                prefix = osp.join(dest_prefix, basename) if dest_prefix else basename

            with _open_archive(src, readonly=True) as src_bc:
                pattern = f"{src_prefix}/**" if src_prefix else "**"
                for info in _get_file_infos(src_bc, pattern):
                    if not info.isfile():
                        continue
                    if src_prefix:
                        rel_path = info.path[len(src_prefix):].lstrip('/')
                    else:
                        rel_path = info.path
                    archive_path = osp.join(prefix, rel_path) if prefix else rel_path
                    expected.add(archive_path.lstrip('/'))

    # Find and delete extraneous
    pattern = f"{dest_prefix}/**" if dest_prefix else "**"
    existing = set(
        info.path for info in _get_file_infos(dest_bc, pattern)
        if info.isfile()
    )

    for path in existing - expected:
        if options.dry_run:
            print(f"would delete: {path}")
        else:
            if options.verbose:
                print(f"delete: {path}")
            del dest_bc[path]


def _delete_extraneous_local(
    sources: list[ParsedPath],
    dest_dir: str,
    options: RsyncOptions
):
    """Delete local files not present in archive sources (local, remote, or tar/zip)."""
    expected = set()

    for src in sources:
        if src.is_tar_zip:
            # Tar/zip source
            from ..formats.archive_formats import iter_archive_nocontent
            src_path = src.filesystem_path
            src_inner = src.inner_path

            if src.contents_mode:
                out_dir = dest_dir
            else:
                if src_inner:
                    basename = osp.basename(src_inner)
                else:
                    basename = osp.basename(src_path)
                    for ext in TAR_EXTENSIONS + ZIP_EXTENSIONS:
                        if basename.lower().endswith(ext):
                            basename = basename[:-len(ext)]
                            break
                out_dir = osp.join(dest_dir, basename)

            for info in iter_archive_nocontent(src_path):
                if not info.isfile():
                    continue
                # Filter by inner path if specified
                if src_inner:
                    if not (info.path == src_inner or info.path.startswith(src_inner + '/')):
                        continue
                    rel_path = info.path[len(src_inner):].lstrip('/')
                else:
                    rel_path = info.path
                expected.add(osp.join(out_dir, rel_path))

        elif src.is_archive:
            with _open_archive(src, readonly=True) as src_bc:
                src_prefix = src.inner_path

                if src.contents_mode:
                    out_dir = dest_dir
                else:
                    if src_prefix:
                        basename = osp.basename(src_prefix)
                    else:
                        basename = src.archive_basename
                    out_dir = osp.join(dest_dir, basename)

                pattern = f"{src_prefix}/**" if src_prefix else "**"
                for info in _get_file_infos(src_bc, pattern):
                    if not info.isfile():
                        continue
                    archive_path = info.path
                    if src_prefix:
                        rel_path = archive_path[len(src_prefix):].lstrip('/')
                    else:
                        rel_path = archive_path
                    expected.add(osp.join(out_dir, rel_path))

    # Find and delete extraneous local files
    for root, dirs, files in os.walk(dest_dir):
        for fname in files:
            local_path = osp.join(root, fname)
            if local_path not in expected:
                if options.dry_run:
                    print(f"would delete: {local_path}")
                else:
                    if options.verbose:
                        print(f"delete: {local_path}")
                    os.remove(local_path)


def _is_excluded(name: str, options: RsyncOptions) -> bool:
    """Check if file/dir should be excluded."""
    for pattern in options.exclude:
        if fnmatch.fnmatch(name, pattern):
            # Check if explicitly included
            for inc in options.include:
                if fnmatch.fnmatch(name, inc):
                    return False
            return True
    return False
