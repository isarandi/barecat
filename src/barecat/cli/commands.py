"""Shared command implementations for CLI and shell."""

import fnmatch
import os.path as osp
import stat
import sys
from typing import Optional, Callable


# =============================================================================
# ls command
# =============================================================================


def list_entries(
    index,
    paths: list[str],
    long_format: bool = False,
    recursive: bool = False,
    jsonl: bool = False,
    output: Callable[[str], None] = print,
):
    """List archive entries. Mimics the `ls` command.

    Args:
        index: Barecat index
        paths: Paths or glob patterns to list
        long_format: Show detailed info (-l)
        recursive: List recursively (-R)
        jsonl: Output as JSON lines
        output: Function to output each line
    """
    import json

    def format_entry(mode, nlink, user, group, size, mtime_ns, name):
        mode_str = _format_mode(mode)
        time_str = _format_time(mtime_ns)
        return f'{mode_str} {nlink:>4} {user:<8} {group:<8} {size:>8} {time_str} {name}'

    def print_file(finfo, name=None):
        if name is None:
            name = finfo.path
        if jsonl:
            output(
                json.dumps(
                    {
                        'type': 'file',
                        'path': finfo.path,
                        'size': finfo.size,
                        'mtime_ns': finfo.mtime_ns,
                        'crc32c': finfo.crc32c,
                        'mode': finfo.mode,
                        'uid': finfo.uid,
                        'gid': finfo.gid,
                    }
                )
            )
        elif long_format:
            user, group = _get_user_group(finfo.uid, finfo.gid)
            output(format_entry(finfo.mode, 1, user, group, finfo.size, finfo.mtime_ns, name))
        else:
            output(name)

    def print_dir(dinfo, name=None):
        if name is None:
            name = dinfo.path
        if jsonl:
            output(
                json.dumps(
                    {
                        'type': 'dir',
                        'path': dinfo.path,
                        'size_tree': dinfo.size_tree,
                        'num_files_tree': dinfo.num_files_tree,
                        'mtime_ns': dinfo.mtime_ns,
                        'mode': dinfo.mode,
                        'uid': dinfo.uid,
                        'gid': dinfo.gid,
                    }
                )
            )
        elif long_format:
            user, group = _get_user_group(dinfo.uid, dinfo.gid)
            nlink = (dinfo.num_subdirs or 0) + 2
            output(
                format_entry(dinfo.mode, nlink, user, group, dinfo.size_tree, dinfo.mtime_ns, name)
            )
        else:
            # Add trailing / for directories in short format (like shell ls)
            output(name + '/' if name else name)

    paths = paths if paths else ['']

    for path in paths:
        path = _normalize_pattern(path)

        # If path contains glob characters, use glob matching
        if _is_glob(path):
            has_doublestar = '**' in path
            for info in index.iterglob_infos(path, recursive=has_doublestar):
                if hasattr(info, 'shard'):
                    print_file(info)
                else:
                    print_dir(info)
            continue

        if recursive:
            if path == '' or index.isdir(path):
                for dinfo, subdirs, files in index.walk_infos(path):
                    for finfo in files:
                        print_file(finfo)
            elif index.isfile(path):
                finfo = index.lookup_file(path)
                print_file(finfo)
        else:
            if path == '' or index.isdir(path):
                entries = []
                total_size = 0
                for name in sorted(index.iterdir_names(path)):
                    full_path = osp.join(path, name) if path else name
                    if index.isdir(full_path):
                        dinfo = index.lookup_dir(full_path)
                        entries.append(('dir', dinfo, name))
                        total_size += dinfo.size_tree or 0
                    else:
                        finfo = index.lookup_file(full_path)
                        entries.append(('file', finfo, name))
                        total_size += finfo.size or 0

                if long_format and not jsonl:
                    output(f'total {total_size}')

                for kind, info, name in entries:
                    if kind == 'dir':
                        print_dir(info, name)
                    else:
                        print_file(info, name)
            elif index.isfile(path):
                finfo = index.lookup_file(path)
                print_file(finfo)
            else:
                output(f'ls: {path}: No such file or directory')


# =============================================================================
# find command
# =============================================================================


def find_entries(
    index,
    path: str = '',
    name: Optional[str] = None,
    pathpattern: Optional[str] = None,
    ftype: Optional[str] = None,  # 'f' or 'd'
    size: Optional[str] = None,  # e.g. '+1M', '-100k'
    maxdepth: Optional[int] = None,
    print0: bool = False,
    output: Callable[[str], None] = print,
):
    """Find files in archive. Mimics the `find` command.

    Args:
        index: Barecat index
        path: Starting path
        name: Match basename against glob pattern
        pathpattern: Match full path against glob pattern
        ftype: File type ('f' for files, 'd' for directories)
        size: Size filter ('+N' larger, '-N' smaller, 'N' exact)
        maxdepth: Maximum depth to descend
        print0: Use null separator
        output: Function to output each line
    """
    root_path = path.rstrip('/') if path else ''
    root_depth = root_path.count('/') + 1 if root_path else 0

    # Parse size filter
    size_op, size_val = None, None
    if size:
        if size[0] == '+':
            size_op, size_val = 'gt', _parse_size(size[1:])
        elif size[0] == '-':
            size_op, size_val = 'lt', _parse_size(size[1:])
        else:
            size_op, size_val = 'eq', _parse_size(size)

    # Build SQL query parts
    conditions = []
    params = []

    if pathpattern:
        pattern = _normalize_pattern(pathpattern)
        conditions.append('path GLOB ?')
        params.append(pattern)

    if size_op == 'gt':
        conditions.append('size > ?')
        params.append(size_val)
    elif size_op == 'lt':
        conditions.append('size < ?')
        params.append(size_val)
    elif size_op == 'eq':
        conditions.append('size = ?')
        params.append(size_val)

    if maxdepth is not None:
        max_abs_depth = root_depth + maxdepth
        depth_expr = "length(path) - length(replace(path, '/', '')) + (path != '')"
        conditions.append(f'{depth_expr} <= ?')
        params.append(max_abs_depth)

    def out(p):
        if print0:
            sys.stdout.write(p + '\0')
        else:
            output(p)

    # Query files
    if ftype != 'd':
        file_conditions = conditions.copy()
        file_params = params.copy()

        if root_path:
            file_conditions.insert(0, '(path = ? OR path GLOB ?)')
            file_params = [root_path, f'{root_path}/*'] + file_params

        if file_conditions:
            file_query = f"SELECT path, size FROM files WHERE {' AND '.join(file_conditions)}"
        else:
            file_query = 'SELECT path, size FROM files'

        for row in index.fetch_iter(file_query, tuple(file_params)):
            p = row['path']
            if name and not fnmatch.fnmatch(osp.basename(p), name):
                continue
            out(p)

    # Query directories
    if ftype != 'f':
        dir_conditions = conditions.copy()
        dir_params = params.copy()

        for i, cond in enumerate(dir_conditions):
            if 'size' in cond:
                dir_conditions[i] = cond.replace('size', 'size_tree')

        if root_path:
            dir_conditions.insert(0, '(path = ? OR path GLOB ?)')
            dir_params = [root_path, f'{root_path}/*'] + dir_params

        if dir_conditions:
            dir_query = f"SELECT path, size_tree FROM dirs WHERE {' AND '.join(dir_conditions)}"
        else:
            dir_query = 'SELECT path, size_tree FROM dirs'

        for row in index.fetch_iter(dir_query, tuple(dir_params)):
            p = row['path'] or '.'
            if name:
                basename = osp.basename(p) if p != '.' else '.'
                if not fnmatch.fnmatch(basename, name):
                    continue
            out(p)


# =============================================================================
# tree command
# =============================================================================


def tree_entries(
    index,
    path: str = '',
    level: Optional[int] = None,
    dirs_only: bool = False,
    output: Callable[[str], None] = print,
):
    """Display directory tree.

    Args:
        index: Barecat index
        path: Starting path
        level: Maximum depth (-L)
        dirs_only: Show only directories (-d)
        output: Function to output each line
    """
    root_path = path.rstrip('/') if path else ''

    # Print root
    output(root_path if root_path else '.')

    dir_count = 1
    file_count = 0

    def print_tree(current_path, prefix, depth):
        nonlocal dir_count, file_count

        if level is not None and depth > level:
            return

        try:
            entries = sorted(index.iterdir_names(current_path))
        except (KeyError, Exception):
            return

        # Separate dirs and files
        dirs = []
        files = []
        for name in entries:
            full = osp.join(current_path, name) if current_path else name
            if index.isdir(full):
                dirs.append(name)
            else:
                files.append(name)

        if dirs_only:
            items = [(name, True) for name in dirs]
        else:
            items = [(name, True) for name in dirs] + [(name, False) for name in files]

        for i, (name, is_dir) in enumerate(items):
            is_last = i == len(items) - 1
            connector = '└── ' if is_last else '├── '
            child_prefix = prefix + ('    ' if is_last else '│   ')
            full_path = osp.join(current_path, name) if current_path else name

            if is_dir:
                output(f'{prefix}{connector}{name}')
                dir_count += 1
                print_tree(full_path, child_prefix, depth + 1)
            else:
                output(f'{prefix}{connector}{name}')
                file_count += 1

    print_tree(root_path, '', 1)

    if dirs_only:
        output(f'\n{dir_count} directories')
    else:
        output(f'\n{dir_count} directories, {file_count} files')


# =============================================================================
# du command
# =============================================================================


def du_entries(
    index,
    path: str = '',
    all_files: bool = False,
    summarize: bool = False,
    human_readable: bool = False,
    max_depth: Optional[int] = None,
    output: Callable[[str], None] = print,
):
    """Show disk usage.

    Args:
        index: Barecat index
        path: Starting path
        all_files: Show all files (-a)
        summarize: Show only total (-s)
        human_readable: Human readable sizes (-H)
        max_depth: Maximum depth (-d)
        output: Function to output each line
    """
    root_path = path.rstrip('/') if path else ''

    def fmt_size(size):
        return _format_size(size, human_readable)

    if summarize:
        if root_path == '':
            dinfo = index.lookup_dir('')
            size = dinfo.size_tree or 0
        elif index.isdir(root_path):
            dinfo = index.lookup_dir(root_path)
            size = dinfo.size_tree or 0
        else:
            finfo = index.lookup_file(root_path)
            size = finfo.size or 0
        display_path = root_path if root_path else '.'
        output(f'{fmt_size(size)}\t{display_path}')
        return

    root_depth = root_path.count('/') + 1 if root_path else 0
    max_abs_depth = root_depth + max_depth if max_depth is not None else None

    depth_expr = "length(path) - length(replace(path, '/', '')) + (path != '')"
    depth_filter = f' AND {depth_expr} <= ?' if max_abs_depth is not None else ''

    # Query directories
    if root_path:
        dir_query = (
            f'SELECT path, size_tree FROM dirs WHERE (path = ? OR path GLOB ?){depth_filter}'
        )
        dir_params = (root_path, f'{root_path}/*') + (
            (max_abs_depth,) if max_abs_depth is not None else ()
        )
    else:
        dir_query = (
            f"SELECT path, size_tree FROM dirs{depth_filter.replace(' AND ', ' WHERE ', 1)}"
        )
        dir_params = (max_abs_depth,) if max_abs_depth is not None else ()

    entries = [
        (row['path'] or '.', row['size_tree'] or 0)
        for row in index.fetch_iter(dir_query, dir_params)
    ]

    # Query files if -a flag
    if all_files:
        if root_path:
            file_query = f'SELECT path, size FROM files WHERE path GLOB ?{depth_filter}'
            file_params = (f'{root_path}/*',) + (
                (max_abs_depth,) if max_abs_depth is not None else ()
            )
        else:
            file_query = f'SELECT path, size FROM files WHERE 1{depth_filter}'
            file_params = (max_abs_depth,) if max_abs_depth is not None else ()

        entries += [
            (row['path'], row['size'] or 0) for row in index.fetch_iter(file_query, file_params)
        ]

    # Sort: deeper paths first, then alphabetically
    entries.sort(key=lambda x: (-x[0].count('/'), x[0]))

    for p, size in entries:
        output(f'{fmt_size(size)}\t{p}')


# =============================================================================
# Helper functions (internal)
# =============================================================================

_user_group_cache = {}


def _format_mode(mode):
    """Format file mode like ls -l."""
    if mode is None:
        return '----------'

    if stat.S_ISDIR(mode):
        s = 'd'
    elif stat.S_ISLNK(mode):
        s = 'l'
    else:
        s = '-'

    s += 'r' if mode & stat.S_IRUSR else '-'
    s += 'w' if mode & stat.S_IWUSR else '-'
    s += 'x' if mode & stat.S_IXUSR else '-'
    s += 'r' if mode & stat.S_IRGRP else '-'
    s += 'w' if mode & stat.S_IWGRP else '-'
    s += 'x' if mode & stat.S_IXGRP else '-'
    s += 'r' if mode & stat.S_IROTH else '-'
    s += 'w' if mode & stat.S_IWOTH else '-'
    s += 'x' if mode & stat.S_IXOTH else '-'
    return s


def _format_time(mtime_ns):
    """Format mtime like ls."""
    from datetime import datetime
    import locale

    if mtime_ns is None:
        return '            '

    try:
        locale.setlocale(locale.LC_TIME, '')
    except locale.Error:
        pass

    dt = datetime.fromtimestamp(mtime_ns / 1e9)
    now = datetime.now()

    if dt.year == now.year:
        return dt.strftime('%b %d %H:%M')
    else:
        return dt.strftime('%b %d  %Y')


def _get_user_group(uid, gid):
    """Get username and groupname from uid/gid."""
    import pwd
    import grp

    cache = _user_group_cache

    if uid not in cache:
        try:
            cache[uid] = pwd.getpwuid(uid).pw_name
        except (KeyError, TypeError):
            cache[uid] = str(uid) if uid is not None else '-'

    if gid not in cache:
        try:
            cache[gid] = grp.getgrgid(gid).gr_name
        except (KeyError, TypeError):
            cache[gid] = str(gid) if gid is not None else '-'

    return cache[uid], cache[gid]


def _format_size(size, human_readable=False):
    """Format size for output."""
    if not human_readable:
        return str(size)

    for unit in ['', 'K', 'M', 'G', 'T']:
        if abs(size) < 1024:
            if unit == '':
                return str(size)
            return f'{size:.1f}{unit}'
        size /= 1024
    return f'{size:.1f}P'


def _is_glob(path):
    """Check if path contains glob characters."""
    return any(c in path for c in '*?[')


def _normalize_pattern(pattern):
    """Normalize glob pattern - trailing slash means match directory contents."""
    if pattern.endswith('/') and not pattern.endswith('**/'):
        return pattern + '**'
    return pattern


def _parse_size(size_str):
    """Parse size string like 1M, 500k, 2G."""
    size_str = size_str.strip().upper()
    multipliers = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4}
    if size_str and size_str[-1] in multipliers:
        return int(float(size_str[:-1]) * multipliers[size_str[-1]])
    return int(size_str)
