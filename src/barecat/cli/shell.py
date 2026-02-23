"""Interactive shell for exploring barecat archives."""

import cmd
import os
import os.path as osp
import shlex
import sys
from typing import Optional

import barecat
from ..core.paths import normalize_path


class BarecatShell(cmd.Cmd):
    """Interactive shell for barecat archives, similar to sqlite3."""

    intro = 'Barecat shell. Type help or ? for commands, .quit to exit.'

    def __init__(self, archive_path: str, readonly: bool = True):
        super().__init__()
        self.archive_path = archive_path
        self.readonly = readonly
        self.bc = barecat.Barecat(archive_path, readonly=readonly, readonly_is_immutable=False)
        self.cwd = ''  # Current working directory within archive
        self.local_cwd = os.getcwd()  # Local working directory
        self._update_prompt()
        self._configure_readline()

    def _configure_readline(self):
        """Configure readline for better completion behavior."""
        try:
            import readline

            # Show all completions on first TAB if ambiguous
            readline.parse_and_bind('set show-all-if-ambiguous on')
        except (ImportError, Exception):
            pass  # readline not available or config failed

    def _update_prompt(self):
        display_cwd = '/' + self.cwd if self.cwd else '/'
        self.prompt = f'barecat:{display_cwd}> '

    def _resolve_path(self, path: str) -> str:
        """Resolve archive path relative to cwd."""
        if not path:
            return self.cwd
        if path.startswith('/'):
            return normalize_path(path)
        return normalize_path(osp.join(self.cwd, path))

    def _resolve_local_path(self, path: str) -> str:
        """Resolve local filesystem path relative to local_cwd."""
        if not path:
            return self.local_cwd
        if osp.isabs(path):
            return path
        return osp.join(self.local_cwd, path)

    def _parse_args(self, arg: str) -> list:
        """Parse arguments, handling quotes."""
        try:
            return shlex.split(arg)
        except ValueError:
            return arg.split()

    # --- Navigation ---

    def do_cd(self, arg: str):
        """Change directory: cd [path]"""
        path = self._resolve_path(arg.strip() or '/')
        if path == '':
            self.cwd = ''
        elif self.bc.index.isdir(path):
            self.cwd = path
        else:
            print(f'cd: not a directory: {path}')
        self._update_prompt()

    def do_pwd(self, arg: str):
        """Print working directory"""
        print('/' + self.cwd if self.cwd else '/')

    # --- Listing ---

    def do_ls(self, arg: str):
        """List directory contents: ls [-l] [-R] [path_or_glob]"""
        from .commands import list_entries

        args = self._parse_args(arg)
        long_format = '-l' in args
        recursive = '-R' in args
        args = [a for a in args if a not in ('-l', '-R')]
        path = self._resolve_path(args[0] if args else '')

        list_entries(
            self.bc.index,
            paths=[path],
            long_format=long_format,
            recursive=recursive,
        )

    def do_lss(self, arg: str):
        """List with sequence summarization: lss [path]

        Groups files with numeric patterns like frame_0001.jpg into:
        frame_****.jpg  (1201 files, 0000–1200)
        """
        path = self._resolve_path(arg.strip() or '')

        if not self.bc.index.isdir(path):
            print(f'lss: not a directory: {path}')
            return

        try:
            entries = sorted(self.bc.listdir(path))
        except KeyError:
            print(f"lss: cannot access '{path}': No such file or directory")
            return

        # Separate dirs and files
        dirs = []
        files = []
        for name in entries:
            full_path = osp.join(path, name) if path else name
            if self.bc.index.isdir(full_path):
                dirs.append(name)
            else:
                files.append(name)

        # Print directories first
        for name in dirs:
            print(f'{name}/')

        # Group files by numeric pattern
        groups = self._group_by_numeric_pattern(files)
        for pattern, group_files, num_width, num_min, num_max in groups:
            if pattern is not None:
                # Summarized form (pattern already has stars)
                print(f'{pattern}  ({len(group_files)} files, {num_min}–{num_max})')
            else:
                # Individual files
                for name in group_files:
                    print(name)

    def _group_by_numeric_pattern(self, files: list) -> list:
        """Group files by numeric pattern, summarizing the most-varying numeric chunk.

        Returns list of (pattern, files, num_width, num_min, num_max).
        Pattern uses {} as placeholder for the summarized numeric part.
        """
        import re
        from collections import defaultdict

        if not files:
            return []

        # Parse each file into list of (text, num, text, num, ..., text)
        def parse_chunks(name):
            """Split filename into alternating text and numeric chunks."""
            chunks = []
            last_end = 0
            for match in re.finditer(r'\d+', name):
                if match.start() > last_end:
                    chunks.append(('text', name[last_end : match.start()]))
                chunks.append(('num', match.group()))
                last_end = match.end()
            if last_end < len(name):
                chunks.append(('text', name[last_end:]))
            return chunks

        parsed = [(name, parse_chunks(name)) for name in files]

        # Group by structure (sequence of chunk types and text values, with num widths)
        def get_structure(chunks):
            """Get structure key: text chunks + num widths."""
            return tuple((typ, val if typ == 'text' else len(val)) for typ, val in chunks)

        structure_groups = defaultdict(list)
        for name, chunks in parsed:
            structure_groups[get_structure(chunks)].append((name, chunks))

        result = []
        for structure, items in sorted(structure_groups.items()):
            num_positions = [i for i, (typ, _) in enumerate(structure) if typ == 'num']

            if not num_positions or len(items) < 5:
                # No numbers or too few files - list individually
                result.append((None, [name for name, _ in items], None, None, None))
                continue

            # Find which numeric position varies most
            max_variance_pos = None
            max_unique = 0
            for pos in num_positions:
                unique_vals = set(chunks[pos][1] for _, chunks in items)
                if len(unique_vals) > max_unique:
                    max_unique = len(unique_vals)
                    max_variance_pos = pos

            # Group by all OTHER numeric values, summarize the most-varying one
            def group_key(chunks):
                return tuple(chunks[i][1] for i in range(len(chunks)) if i != max_variance_pos)

            subgroups = defaultdict(list)
            for name, chunks in items:
                subgroups[group_key(chunks)].append((name, chunks))

            for key, subitems in sorted(subgroups.items()):
                if len(subitems) >= 5:
                    # Build pattern with stars for the varying position
                    sample_chunks = subitems[0][1]
                    num_width = len(sample_chunks[max_variance_pos][1])
                    pattern_parts = []
                    for i, (typ, val) in enumerate(sample_chunks):
                        if i == max_variance_pos:
                            pattern_parts.append('*' * num_width)
                        else:
                            pattern_parts.append(val)
                    pattern = ''.join(pattern_parts)

                    # Get min/max of the varying numbers
                    varying_nums = [chunks[max_variance_pos][1] for _, chunks in subitems]
                    group_files = [name for name, _ in subitems]
                    result.append(
                        (pattern, group_files, num_width, min(varying_nums), max(varying_nums))
                    )
                else:
                    # Too few - list individually
                    result.append((None, [name for name, _ in subitems], None, None, None))

        return result

    def _print_entry_long(self, path: str, display_name: str):
        """Print entry in long format."""
        if self.bc.index.isdir(path):
            info = self.bc.index.lookup_dir(path)
            print(f'd  {info.size_tree:>12}  {info.num_files_tree:>6} files  {display_name}/')
        else:
            self._print_file_long(path, display_name)

    def _print_file_long(self, path: str, display_name: Optional[str] = None):
        """Print file in long format."""
        info = self.bc.index.lookup_file(path)
        display = display_name or osp.basename(path)
        print(f'-  {info.size:>12}  shard:{info.shard:<3} @{info.offset:<10}  {display}')

    def _print_dir_long(self, path: str, display_name: Optional[str] = None):
        """Print directory in long format."""
        info = self.bc.index.lookup_dir(path)
        display = display_name or (osp.basename(path) if path else '/')
        print(f'd  {info.size_tree:>12}  {info.num_files_tree:>6} files  {display}/')

    def do_tree(self, arg: str):
        """Show directory tree: tree [-L N] [-d] [path]"""
        from .commands import tree_entries

        args = self._parse_args(arg)
        level = None
        dirs_only = '-d' in args
        path = ''

        filtered = [a for a in args if a != '-d']
        i = 0
        while i < len(filtered):
            a = filtered[i]
            if a in ('-L', '--depth') and i + 1 < len(filtered):
                level = int(filtered[i + 1])
                i += 2
            elif not a.startswith('-'):
                path = a
                i += 1
            else:
                i += 1

        tree_entries(
            self.bc.index,
            path=self._resolve_path(path),
            level=level,
            dirs_only=dirs_only,
        )

    # --- File operations ---

    def do_cat(self, arg: str):
        """Print file contents: cat <path>"""
        path = self._resolve_path(arg.strip())
        if not path:
            print('cat: missing path')
            return
        try:
            data = self.bc[path]
            sys.stdout.buffer.write(data)
            if not data.endswith(b'\n'):
                print()  # Ensure newline at end
        except KeyError:
            print(f'cat: {path}: No such file')

    def do_head(self, arg: str):
        """Print first N bytes: head [-n N] <path>"""
        args = self._parse_args(arg)
        n = 1024
        path = None

        i = 0
        while i < len(args):
            if args[i] == '-n' and i + 1 < len(args):
                n = int(args[i + 1])
                i += 2
            else:
                path = args[i]
                i += 1

        if not path:
            print('head: missing path')
            return

        path = self._resolve_path(path)
        try:
            data = self.bc[path][:n]
            sys.stdout.buffer.write(data)
            if not data.endswith(b'\n'):
                print()
        except KeyError:
            print(f'head: {path}: No such file')

    def do_stat(self, arg: str):
        """Show file/directory metadata: stat <path>"""
        path = self._resolve_path(arg.strip())
        if not path:
            print('stat: missing path')
            return

        try:
            if self.bc.index.isfile(path):
                info = self.bc.index.lookup_file(path)
                print(f'  File: {info.path}')
                print(f'  Size: {info.size}')
                print(f' Shard: {info.shard}')
                print(f'Offset: {info.offset}')
                print(f'CRC32C: {info.crc32c:#010x}' if info.crc32c else 'CRC32C: (none)')
            elif self.bc.index.isdir(path):
                info = self.bc.index.lookup_dir(path)
                print(f'   Dir: /{info.path}')
                print(f' Files: {info.num_files} (direct)')
                print(f' Total: {info.num_files_tree} files, {info.size_tree} bytes')
            else:
                print(f'stat: {path}: No such file or directory')
        except KeyError:
            print(f'stat: {path}: No such file or directory')

    def do_find(self, arg: str):
        """Find files like /usr/bin/find: find [path] [-name PAT] [-type f|d] [-size [+-]N] [-maxdepth N]"""
        from .commands import find_entries

        args = self._parse_args(arg)

        # Parse arguments
        start_path = ''
        name_pattern = None
        path_pattern = None
        file_type = None
        size = None
        maxdepth = None

        i = 0
        while i < len(args):
            a = args[i]
            if a == '-name' and i + 1 < len(args):
                name_pattern = args[i + 1]
                i += 2
            elif a == '-path' and i + 1 < len(args):
                path_pattern = args[i + 1]
                i += 2
            elif a == '-type' and i + 1 < len(args):
                file_type = args[i + 1]
                i += 2
            elif a == '-size' and i + 1 < len(args):
                size = args[i + 1]
                i += 2
            elif a == '-maxdepth' and i + 1 < len(args):
                maxdepth = int(args[i + 1])
                i += 2
            elif not a.startswith('-'):
                start_path = a
                i += 1
            else:
                print(f'find: unknown option: {a}')
                return

        find_entries(
            self.bc.index,
            path=self._resolve_path(start_path),
            name=name_pattern,
            pathpattern=path_pattern,
            ftype=file_type,
            size=size,
            maxdepth=maxdepth,
        )

    def do_du(self, arg: str):
        """Show disk usage: du [-a] [-s] [-H] [-d N] [path]"""
        from .commands import du_entries

        args = self._parse_args(arg)
        all_files = '-a' in args
        summarize = '-s' in args
        human_readable = '-H' in args
        max_depth = None
        path = ''

        i = 0
        filtered = [a for a in args if a not in ('-a', '-s', '-H')]
        while i < len(filtered):
            a = filtered[i]
            if a == '-d' and i + 1 < len(filtered):
                max_depth = int(filtered[i + 1])
                i += 2
            elif not a.startswith('-'):
                path = a
                i += 1
            else:
                i += 1

        # Default to summarize if no options
        if not all_files and max_depth is None:
            summarize = True

        du_entries(
            self.bc.index,
            path=self._resolve_path(path),
            all_files=all_files,
            summarize=summarize,
            human_readable=human_readable,
            max_depth=max_depth,
        )

    # --- Local navigation ---

    def do_lcd(self, arg: str):
        """Change local directory: lcd [path]"""
        path = arg.strip() or os.path.expanduser('~')
        path = self._resolve_local_path(path)
        if osp.isdir(path):
            self.local_cwd = path
            print(f'Local directory: {self.local_cwd}')
        else:
            print(f'lcd: not a directory: {path}')

    def do_lpwd(self, arg: str):
        """Print local working directory"""
        print(self.local_cwd)

    def do_lls(self, arg: str):
        """List local directory: lls [path]"""
        path = self._resolve_local_path(arg.strip()) if arg.strip() else self.local_cwd
        try:
            for name in sorted(os.listdir(path)):
                full = osp.join(path, name)
                if osp.isdir(full):
                    print(f'{name}/')
                else:
                    print(name)
        except OSError as e:
            print(f'lls: {e}')

    # --- File transfer (extract) ---

    def do_get(self, arg: str):
        """Extract file: get <archive_path> [local_dest]"""
        args = self._parse_args(arg)
        if not args:
            print('get: missing archive path')
            return

        archive_path = self._resolve_path(args[0])

        if len(args) > 1:
            local_dest = self._resolve_local_path(args[1])
        else:
            local_dest = self._resolve_local_path(osp.basename(archive_path))

        try:
            if self.bc.index.isfile(archive_path):
                # Single file
                if osp.isdir(local_dest):
                    local_dest = osp.join(local_dest, osp.basename(archive_path))
                os.makedirs(osp.dirname(local_dest) or '.', exist_ok=True)
                data = self.bc[archive_path]
                with open(local_dest, 'wb') as f:
                    f.write(data)
                print(f'Extracted: {archive_path} -> {local_dest}')
            elif self.bc.index.isdir(archive_path):
                # Directory - recursive extract
                self._extract_dir(archive_path, local_dest)
            else:
                print(f'get: {archive_path}: No such file or directory')
        except Exception as e:
            print(f'get: {e}')

    def _extract_dir(self, archive_dir: str, local_dest: str):
        """Recursively extract a directory."""
        count = 0
        # Use walk to traverse directory tree
        for dirpath, dirnames, filenames in self.bc.walk(archive_dir):
            for filename in filenames:
                archive_path = osp.join(dirpath, filename) if dirpath else filename
                # Compute relative path within the extracted dir
                if archive_dir:
                    rel_path = archive_path[len(archive_dir) + 1 :]
                else:
                    rel_path = archive_path
                local_path = osp.join(local_dest, rel_path)
                os.makedirs(osp.dirname(local_path) or '.', exist_ok=True)
                data = self.bc[archive_path]
                with open(local_path, 'wb') as f:
                    f.write(data)
                count += 1
        print(f'Extracted {count} files to {local_dest}')

    def do_mget(self, arg: str):
        """Extract files matching pattern: mget <pattern> [local_dest]"""
        args = self._parse_args(arg)
        if not args:
            print('mget: missing pattern')
            return

        pattern = args[0]
        if not pattern.startswith('/') and self.cwd:
            pattern = osp.join(self.cwd, pattern)

        local_dest = self._resolve_local_path(args[1]) if len(args) > 1 else self.local_cwd

        count = 0
        # Use iterglob_infos with only_files=True for efficiency
        for finfo in self.bc.index.iterglob_infos(pattern, recursive=True, only_files=True):
            rel_path = osp.basename(finfo.path)
            local_path = osp.join(local_dest, rel_path)
            os.makedirs(osp.dirname(local_path) or '.', exist_ok=True)
            data = self.bc.sharder.read_from_address(finfo.shard, finfo.offset, finfo.size)
            with open(local_path, 'wb') as f:
                f.write(data)
            count += 1
            print(f'  {finfo.path} -> {local_path}')
        print(f'Extracted {count} files')

    # --- Write operations ---

    def do_put(self, arg: str):
        """Add file to archive: put <local_path> [archive_path]"""
        if self.readonly:
            print('put: archive is read-only (use barecat-shell --write)')
            return

        args = self._parse_args(arg)
        if not args:
            print('put: missing local path')
            return

        local_path = self._resolve_local_path(args[0])

        if len(args) > 1:
            archive_path = self._resolve_path(args[1])
        else:
            archive_path = self._resolve_path(osp.basename(local_path))

        if not osp.isfile(local_path):
            print(f'put: {local_path}: No such file')
            return

        try:
            with open(local_path, 'rb') as f:
                data = f.read()
            self.bc[archive_path] = data
            print(f'Added: {local_path} -> {archive_path}')
        except Exception as e:
            print(f'put: {e}')

    def do_mput(self, arg: str):
        """Add files matching glob: mput <pattern> [archive_dir]"""
        if self.readonly:
            print('mput: archive is read-only (use barecat-shell --write)')
            return

        args = self._parse_args(arg)
        if not args:
            print('mput: missing pattern')
            return

        import glob as glob_module

        pattern = self._resolve_local_path(args[0])
        archive_dir = self._resolve_path(args[1]) if len(args) > 1 else self.cwd

        count = 0
        for local_path in glob_module.glob(pattern, recursive=True):
            if osp.isfile(local_path):
                name = osp.basename(local_path)
                archive_path = osp.join(archive_dir, name) if archive_dir else name
                with open(local_path, 'rb') as f:
                    data = f.read()
                self.bc[archive_path] = data
                print(f'  {local_path} -> {archive_path}')
                count += 1
        print(f'Added {count} files')

    def do_rm(self, arg: str):
        """Remove file from archive: rm <path>"""
        if self.readonly:
            print('rm: archive is read-only (use barecat-shell --write)')
            return

        path = self._resolve_path(arg.strip())
        if not path:
            print('rm: missing path')
            return

        try:
            if self.bc.index.isfile(path):
                del self.bc[path]
                print(f'Removed: {path}')
            elif self.bc.index.isdir(path):
                print(f'rm: {path}: Is a directory (use rmdir or rm -r)')
            else:
                print(f'rm: {path}: No such file')
        except Exception as e:
            print(f'rm: {e}')

    def do_mv(self, arg: str):
        """Rename/move file in archive: mv <old> <new>"""
        if self.readonly:
            print('mv: archive is read-only (use barecat-shell --write)')
            return

        args = self._parse_args(arg)
        if len(args) < 2:
            print('mv: need source and destination')
            return

        old_path = self._resolve_path(args[0])
        new_path = self._resolve_path(args[1])

        try:
            self.bc.rename(old_path, new_path)
            print(f'Renamed: {old_path} -> {new_path}')
        except Exception as e:
            print(f'mv: {e}')

    # --- Info commands ---

    def do_info(self, arg: str):
        """Show archive info"""
        print(f'Archive: {self.archive_path}')
        print(f'  Files: {self.bc.index.num_files}')
        print(f'   Dirs: {self.bc.index.num_dirs}')
        root = self.bc.index.lookup_dir('')
        print(f'  Total: {root.size_tree} bytes')
        print(f' Shards: {self.bc.sharder.num_shards}')

    # --- SQL access ---

    def do_sql(self, arg: str):
        """Execute SQL query or start SQL REPL: sql [query]"""
        query = arg.strip()
        if not query:
            # Launch sqlite3 REPL if available (Python 3.11+)
            try:
                from sqlite3.__main__ import SqliteInteractiveConsole

                print('Type .quit or Ctrl-D to return to barecat shell')
                console = SqliteInteractiveConsole(self.bc.index.connection)
                console.interact(banner='', exitmsg='')
            except ImportError:
                print('sql: provide a query, or upgrade to Python 3.11+ for interactive REPL')
            return

        try:
            cursor = self.bc.index.cursor
            cursor.execute(query)
            rows = cursor.fetchall()
            if rows:
                # Print column headers
                cols = [desc[0] for desc in cursor.description]
                print('\t'.join(cols))
                print('-' * 40)
                for row in rows:
                    print('\t'.join(str(v) for v in row))
            print(f'({len(rows)} rows)')
        except Exception as e:
            print(f'sql error: {e}', file=sys.stderr)

    # --- Exit ---

    def do_quit(self, arg: str):
        """Exit the shell"""
        return True

    def do_exit(self, arg: str):
        """Exit the shell"""
        return True

    def do_EOF(self, arg: str):
        """Exit on Ctrl-D"""
        print()
        return True

    # Dot-command aliases (sqlite style) and shell escape
    def default(self, line: str):
        """Handle .commands and ! shell escape"""
        if line.startswith('!'):
            # Shell escape - run local command
            cmd = line[1:].strip()
            if cmd:
                import subprocess

                subprocess.run(cmd, shell=True, cwd=self.local_cwd)
            else:
                print('!: missing command')
        elif line.startswith('.'):
            cmd_name = line[1:].split()[0]
            rest = line[len(cmd_name) + 1 :].strip()
            method = getattr(self, f'do_{cmd_name}', None)
            if method:
                return method(rest)
            print(f'Unknown command: {line}')
        else:
            print(f'Unknown command: {line}')

    # --- Tab completion ---

    # Commands that complete directories only
    _complete_dirs_cmds = {'cd', 'tree', 'lss'}
    # Commands that complete archive paths (files + dirs for navigation)
    _complete_paths_cmds = {'ls', 'cat', 'head', 'stat', 'du', 'get', 'rm', 'mv', 'find'}

    def completedefault(self, text, line, begidx, endidx):
        """Default completion handler."""
        cmd = line.split()[0] if line.split() else ''
        if cmd in self._complete_dirs_cmds:
            return self._complete_archive_path(text, dirs_only=True)
        elif cmd in self._complete_paths_cmds:
            return self._complete_archive_path(text, dirs_only=False)
        return []

    def _complete_archive_path(self, text: str, dirs_only: bool = False) -> list:
        """Complete archive paths. Shows dirs (with /) and optionally files."""
        if '/' in text:
            dir_part, prefix = osp.dirname(text), osp.basename(text)
        else:
            dir_part, prefix = '', text

        search_dir = self._resolve_path(dir_part)

        try:
            entries = self.bc.index.iterdir_names(search_dir)
        except (KeyError, Exception):
            return []

        matches = []
        for name in entries:
            if not name.startswith(prefix):
                continue
            full_path = osp.join(search_dir, name) if search_dir else name
            is_dir = self.bc.index.isdir(full_path)
            if is_dir:
                matches.append((dir_part + '/' + name if dir_part else name) + '/')
            elif not dirs_only:
                matches.append(dir_part + '/' + name if dir_part else name)
            if len(matches) >= 200:
                break

        return matches

    def emptyline(self):
        """Do nothing on empty line (don't repeat last command)."""
        pass

    def cmdloop(self, intro=None):
        """Command loop with Ctrl+C handling."""
        if intro is not None:
            self.intro = intro
        if self.intro:
            print(self.intro)
        while True:
            try:
                super().cmdloop(intro='')
                break  # Normal exit (quit/exit/EOF)
            except KeyboardInterrupt:
                print('^C')  # Show that Ctrl+C was pressed
                # Continue the loop with fresh prompt

    def close(self):
        """Close the archive."""
        self.bc.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def main():
    """Entry point for barecat-shell command."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Interactive shell for exploring barecat archives'
    )
    parser.add_argument('archive', help='Path to the barecat archive')
    parser.add_argument('-c', '--command', help='Execute command and exit (like sqlite3 -c)')
    parser.add_argument(
        '-w',
        '--write',
        action='store_true',
        help='Open archive for writing (enables put, rm, mv commands)',
    )
    args = parser.parse_args()

    with BarecatShell(args.archive, readonly=not args.write) as shell:
        if args.command:
            shell.onecmd(args.command)
        else:
            shell.cmdloop()


if __name__ == '__main__':
    main()
