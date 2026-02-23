"""Unified CLI for barecat with subcommands."""

import argparse
import csv
import re
import sys

from ..util.glob_to_regex import glob_to_regex

import barecat
from ..cli import impl as impl
from ..util import misc
from ..util import physical_order as barecat_physical_order
from ..core.types import Order, SHARD_SIZE_UNLIMITED
from ..core.paths import resolve_index_path
from ..maintenance.defrag import BarecatDefragger
from ..util.misc import parse_size


def parse_shard_size(value):
    """Parse shard size, defaulting to unlimited if None."""
    if value is None:
        return SHARD_SIZE_UNLIMITED
    return parse_size(value)


def _print_defrag_stats(stats):
    """Print defragmentation statistics."""

    def fmt(size):
        return _format_size(size, human_readable=True)

    print(f"Physical size:       {fmt(stats['physical_size'])}")
    print(f"Logical size:        {fmt(stats['logical_size'])}")
    print(f"Freeable space:      {fmt(stats['total_gap_size'])}")
    print(f"Number of gaps:      {stats['num_gaps']}")
    print(f"Fragmentation ratio: {stats['fragmentation_ratio']:.3f}")

    if stats['gap_sizes']:
        gap_sizes = sorted(stats['gap_sizes'])
        n = len(gap_sizes)

        print('\nGap size distribution:')
        print(f'  Min:    {fmt(gap_sizes[0])}')
        print(f'  p25:    {fmt(gap_sizes[n // 4])}')
        print(f'  p50:    {fmt(gap_sizes[n // 2])}')
        print(f'  p75:    {fmt(gap_sizes[3 * n // 4])}')
        print(f'  p90:    {fmt(gap_sizes[int(n * 0.9)])}')
        print(f'  p99:    {fmt(gap_sizes[int(n * 0.99)])}')
        print(f'  Max:    {fmt(gap_sizes[-1])}')
        print(f'  Mean:   {fmt(sum(gap_sizes) / n)}')

        print('\nGaps by shard:')
        for shard, gaps in sorted(stats['gaps_by_shard'].items()):
            total = sum(size for _, size in gaps)
            print(f'  Shard {shard}: {len(gaps)} gaps, {fmt(total)} total')


def main():
    parser = argparse.ArgumentParser(
        prog='barecat',
        description='Scalable archive format for storing millions of small files.',
    )
    subparsers = parser.add_subparsers(dest='command', title='commands')

    # create - create new archive
    p = subparsers.add_parser(
        'create',
        aliases=['c'],
        help='Create new archive (error if exists, use -f to overwrite)',
    )
    p.add_argument('archive', type=str, help='Barecat archive path')
    p.add_argument('paths', type=str, nargs='*', help='Files/directories to add')
    p.add_argument(
        '-T',
        '--files-from',
        type=str,
        metavar='FILE',
        help='Read paths from FILE (use - for stdin)',
    )
    p.add_argument(
        '-0',
        '--null',
        action='store_true',
        help='Paths are null-separated (for use with find -print0)',
    )
    p.add_argument(
        '-C',
        '--directory',
        type=str,
        default=None,
        metavar='DIR',
        help='Change to DIR before adding files',
    )
    p.add_argument(
        '--exclude',
        action='append',
        default=[],
        metavar='PATTERN',
        help='Exclude files matching glob pattern, use **/ for recursive (repeatable)',
    )
    p.add_argument(
        '-i',
        '--include',
        action='append',
        default=[],
        metavar='PATTERN',
        help='Only include files matching glob pattern, use **/ for recursive (repeatable)',
    )
    p.add_argument('-j', '--workers', type=int, default=None, help='Number of worker threads')
    p.add_argument('-f', '--force', action='store_true', help='Overwrite existing archive')
    p.add_argument(
        '-s',
        '--shard-size-limit',
        type=str,
        default=None,
        metavar='SIZE',
        help='Shard size limit (e.g., 1G, 500M)',
    )
    p.add_argument(
        '--physical-order', action='store_true', help='Add files in physical disk order (for HDDs)'
    )

    # add - add to existing archive
    p = subparsers.add_parser(
        'add',
        aliases=['a'],
        help='Add files to existing archive (use -c to create if missing)',
    )
    p.add_argument('archive', type=str, help='Barecat archive path')
    p.add_argument('paths', type=str, nargs='*', help='Files/directories to add')
    p.add_argument(
        '-T',
        '--files-from',
        type=str,
        metavar='FILE',
        help='Read paths from FILE (use - for stdin)',
    )
    p.add_argument(
        '-0',
        '--null',
        action='store_true',
        help='Paths are null-separated (for use with find -print0)',
    )
    p.add_argument(
        '-C',
        '--directory',
        type=str,
        default=None,
        metavar='DIR',
        help='Change to DIR before adding files',
    )
    p.add_argument(
        '--exclude',
        action='append',
        default=[],
        metavar='PATTERN',
        help='Exclude files matching glob pattern, use **/ for recursive (repeatable)',
    )
    p.add_argument(
        '-i',
        '--include',
        action='append',
        default=[],
        metavar='PATTERN',
        help='Only include files matching glob pattern, use **/ for recursive (repeatable)',
    )
    p.add_argument('-j', '--workers', type=int, default=None, help='Number of worker threads')
    p.add_argument(
        '-c', '--create', action='store_true', help='Create archive if it does not exist'
    )
    p.add_argument(
        '-s',
        '--shard-size-limit',
        type=str,
        default=None,
        metavar='SIZE',
        help='Shard size limit for new archive (e.g., 1G, 500M)',
    )
    p.add_argument(
        '--physical-order', action='store_true', help='Add files in physical disk order (for HDDs)'
    )

    # extract
    p = subparsers.add_parser(
        'extract',
        aliases=['x'],
        help='Extract files from archive',
    )
    p.add_argument('archive', type=str, help='Barecat archive path')
    p.add_argument('paths', type=str, nargs='*', help='Paths to extract (default: all)')
    p.add_argument(
        '-C',
        '--directory',
        type=str,
        default='.',
        help='Extract to directory (default: current dir)',
    )
    p.add_argument(
        '--pattern',
        type=str,
        default=None,
        metavar='GLOB',
        help='Only include files matching glob pattern (incompatible with -i/-x)',
    )
    p.add_argument(
        '--exclude',
        '-x',
        action='append',
        default=[],
        metavar='PATTERN',
        help='Exclude files matching glob pattern (repeatable)',
    )
    p.add_argument(
        '--include',
        '-i',
        action='append',
        default=[],
        metavar='PATTERN',
        help='Only include files matching glob pattern (repeatable)',
    )

    # list
    p = subparsers.add_parser(
        'list',
        aliases=['ls', 'l', 't'],
        help='List archive contents',
    )
    p.add_argument('archive', type=str, help='Barecat archive path')
    p.add_argument(
        'paths', type=str, nargs='*', help='Paths or glob patterns to list (default: root)'
    )
    p.add_argument('-l', '--long', action='store_true', help='Long listing with sizes')
    p.add_argument('-R', '--recursive', action='store_true', help='List recursively')
    p.add_argument(
        '--jsonl', action='store_true', help='Output as JSON lines (one JSON object per line)'
    )

    # cat - print file to stdout
    p = subparsers.add_parser(
        'cat',
        help='Print file contents to stdout',
    )
    p.add_argument('archive', type=str, help='Barecat archive path')
    p.add_argument('path', type=str, help='Path within archive')

    # find - search for files
    p = subparsers.add_parser(
        'find',
        help='Search for files in archive (like /usr/bin/find)',
    )
    p.add_argument('archive', type=str, help='Barecat archive path')
    p.add_argument('path', type=str, nargs='?', default='', help='Starting path (default: root)')
    p.add_argument(
        '-name', type=str, metavar='PATTERN', help='Match basename against glob pattern'
    )
    p.add_argument(
        '-path',
        '-wholename',
        type=str,
        metavar='PATTERN',
        dest='pathpattern',
        help='Match full path against glob pattern',
    )
    p.add_argument(
        '-type', type=str, choices=['f', 'd'], dest='ftype', help='File type: f=file, d=directory'
    )
    p.add_argument(
        '-size',
        type=str,
        metavar='[+-]N[kMG]',
        help='Size filter: +N (larger), -N (smaller), N (exact)',
    )
    p.add_argument('-maxdepth', type=int, metavar='N', help='Maximum depth to descend')
    p.add_argument('-print0', action='store_true', help='Print paths separated by null character')

    # shell
    p = subparsers.add_parser('shell', help='Interactive shell for exploring archives')
    p.add_argument('archive', type=str, help='Path to the barecat archive')
    p.add_argument('-c', '--cmd', type=str, dest='shell_cmd', help='Execute command and exit')
    p.add_argument('-w', '--write', action='store_true', help='Open in write mode')

    # browse
    p = subparsers.add_parser('browse', help='Ranger-like file browser for archives')
    p.add_argument('archive', type=str, help='Path to the barecat archive')

    # du
    p = subparsers.add_parser('du', help='Show disk usage (like du)')
    p.add_argument('archive', type=str, help='Path to the barecat archive')
    p.add_argument(
        'path', type=str, nargs='?', default='', help='Path within archive (default: root)'
    )
    p.add_argument('-a', '--all', action='store_true', help='Show all files, not just directories')
    p.add_argument(
        '-s', '--summarize', action='store_true', help='Show only total for each argument'
    )
    p.add_argument(
        '-H', '--human-readable', action='store_true', help='Print sizes in human readable format'
    )
    p.add_argument(
        '-d', '--max-depth', type=int, default=None, metavar='N', help='Max depth to show'
    )

    # ncdu
    p = subparsers.add_parser('ncdu', help='ncdu-like disk usage viewer (TUI)')
    p.add_argument('archive', type=str, help='Path to the barecat archive')

    # tree
    p = subparsers.add_parser('tree', help='Display directory tree')
    p.add_argument('archive', type=str, help='Path to the barecat archive')
    p.add_argument(
        'path', type=str, nargs='?', default='', help='Path within archive (default: root)'
    )
    p.add_argument(
        '-L', '--level', type=int, default=None, metavar='N', help='Limit depth to N levels'
    )
    p.add_argument('-d', '--dirs-only', action='store_true', help='List directories only')

    # verify
    p = subparsers.add_parser('verify', help='Verify archive integrity')
    p.add_argument('archive', type=str, help='Path to the archive')
    p.add_argument('--quick', action='store_true', help='Quick check (index only, no CRC)')

    # defrag
    p = subparsers.add_parser('defrag', help='Defragment archive')
    p.add_argument('archive', type=str, help='Path to the archive')
    p.add_argument('--quick', action='store_true', help='Quick defrag (time-limited, partial)')
    p.add_argument(
        '--smart',
        action='store_true',
        help='Smart defrag (copies contiguous chunks, more efficient)',
    )
    p.add_argument(
        '--max-seconds',
        type=float,
        default=300,
        metavar='SEC',
        help='Time limit for --quick mode (default: 300)',
    )
    p.add_argument(
        '-n',
        '--dry-run',
        action='store_true',
        help='Show statistics about freeable space without defragmenting',
    )

    # reshard
    p = subparsers.add_parser('reshard', help='Reshard archive with a new shard size limit')
    p.add_argument('archive', type=str, help='Path to the archive')
    p.add_argument(
        '-s',
        '--shard-size-limit',
        type=str,
        required=True,
        metavar='SIZE',
        help='New shard size limit (e.g., 1G, 500M)',
    )

    # subset - create filtered copy of archive
    p = subparsers.add_parser('subset', help='Create new archive with subset of files')
    p.add_argument('archive', type=str, help='Source archive')
    p.add_argument('-o', '--output', required=True, help='Output archive')
    p.add_argument(
        '-s',
        '--shard-size-limit',
        type=str,
        default=None,
        metavar='SIZE',
        help='Shard size limit (e.g., 1G, 500M)',
    )
    p.add_argument('-f', '--force', action='store_true', help='Overwrite if output exists')
    p.add_argument(
        '--pattern',
        type=str,
        default=None,
        metavar='GLOB',
        help='Only include files matching glob pattern (incompatible with -i/-x)',
    )
    p.add_argument(
        '-i',
        '--include',
        action='append',
        default=[],
        metavar='PATTERN',
        help='Include files matching glob pattern, rsync-style (repeatable)',
    )
    p.add_argument(
        '-x',
        '--exclude',
        action='append',
        default=[],
        metavar='PATTERN',
        help='Exclude files matching glob pattern, rsync-style (repeatable)',
    )

    # merge
    p = subparsers.add_parser(
        'merge', help='Merge multiple archives (barecat, tar, zip) into one barecat'
    )
    p.add_argument('archives', metavar='ARCHIVE', type=str, nargs='+', help='Archives to merge')
    p.add_argument('-o', '--output', required=True, help='Output archive')
    p.add_argument(
        '-s', '--shard-size-limit', type=str, default=None, metavar='SIZE', help='Shard size limit'
    )
    p.add_argument('-f', '--force', action='store_true', help='Overwrite if output exists')
    p.add_argument(
        '-a',
        '--append',
        action='store_true',
        help='Append to output if it exists (not supported with --symlink)',
    )
    p.add_argument(
        '--symlink',
        action='store_true',
        help='Create symlinks to original shards instead of copying data',
    )
    p.add_argument(
        '--ignore-duplicates', action='store_true', help='Skip files that already exist in output'
    )
    p.add_argument(
        '--as-subdirs',
        action='store_true',
        help='Put each archive in a subdir named after its basename (minus extension)',
    )
    p.add_argument(
        '--prefix',
        type=str,
        default=None,
        metavar='PATH',
        help='Prefix path for all merged files (combines with --as-subdirs)',
    )
    p.add_argument(
        '--pattern',
        type=str,
        default=None,
        metavar='GLOB',
        help='Only include files matching glob pattern (incompatible with --include/--exclude)',
    )
    p.add_argument(
        '-i',
        '--include',
        action='append',
        default=[],
        metavar='PATTERN',
        help='Include files matching glob pattern, rsync-style first-match-wins (repeatable)',
    )
    p.add_argument(
        '-x',
        '--exclude',
        action='append',
        default=[],
        metavar='PATTERN',
        help='Exclude files matching glob pattern, rsync-style first-match-wins (repeatable)',
    )

    # index-to-csv
    p = subparsers.add_parser('index-to-csv', help='Dump index as CSV')
    p.add_argument('archive', type=str, help='Path to the archive')

    # to-ncdu-json
    p = subparsers.add_parser('to-ncdu-json', help='Print as ncdu JSON')
    p.add_argument('archive', type=str, help='Path to the archive')

    # convert - convert between barecat and tar/zip
    p = subparsers.add_parser(
        'convert', help='Convert between barecat and tar/zip (auto-detects direction)'
    )
    p.add_argument(
        'input', type=str, help='Input file or format (tar.gz, tar.bz2, etc. with --stdin)'
    )
    p.add_argument(
        'output', type=str, help='Output file or format (tar.gz, tar.bz2, etc. with --stdout)'
    )
    p.add_argument(
        '-s',
        '--shard-size-limit',
        type=str,
        default=None,
        metavar='SIZE',
        help='Shard size limit (only for tar/zip → barecat)',
    )
    p.add_argument('-f', '--force', action='store_true', help='Overwrite existing output')
    p.add_argument(
        '--stdin',
        action='store_true',
        help='Read input from stdin (input arg specifies format: tar, tar.gz, tar.bz2, tar.xz)',
    )
    p.add_argument(
        '--stdout',
        action='store_true',
        help='Write output to stdout (output arg specifies format: tar, tar.gz, tar.bz2, tar.xz)',
    )
    p.add_argument(
        '--root-dir',
        type=str,
        metavar='NAME',
        help='Wrap all files in a root directory (barecat → tar only)',
    )
    p.add_argument(
        '--wrap',
        action='store_true',
        help='Zero-copy: create index over existing tar/zip (symlinks to original, uncompressed only)',
    )

    # upgrade
    p = subparsers.add_parser('upgrade', help='Upgrade archive to new schema version')
    p.add_argument('archive', type=str, help='Path to the barecat')
    p.add_argument('-j', '--workers', type=int, default=8)
    p.add_argument(
        '--preserve-backup',
        action=misc.BoolAction,
        default=True,
        help='Keep the .old backup file after upgrade (default: True)',
    )

    # completion-script
    p = subparsers.add_parser('completion-script', help='Print shell completion script path')
    p.add_argument('shell', choices=['bash', 'zsh'], help='Shell type')

    # mount - FUSE filesystem
    p = subparsers.add_parser(
        'mount',
        help='Mount archive as FUSE filesystem (requires barecat[mount])',
        description='Mount a barecat archive as a FUSE filesystem. '
        'Use -o for mount options (like mount/sshfs).',
    )
    p.add_argument('archive', type=str, help='Barecat archive path')
    p.add_argument('mountpoint', type=str, help='Mount point')
    p.add_argument(
        '-o',
        '--options',
        type=str,
        metavar='OPTIONS',
        help='Comma-separated mount options: ro (default), rw, fg/foreground, '
        'mmap, defrag, overwrite, append_only, shard_size_limit=SIZE',
    )

    # rsync - rsync-like interface
    p = subparsers.add_parser(
        'rsync',
        help='rsync-like sync between local and archive',
        description='Sync files between local filesystem and barecat archives. '
        'Use archive.barecat:path/ syntax for archive paths.',
    )
    p.add_argument('paths', nargs='+', help='Source(s) and destination (last is dest)')
    p.add_argument('-n', '--dry-run', action='store_true', help='Show what would be done')
    p.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    p.add_argument('--progress', action='store_true', help='Show progress')
    p.add_argument('--delete', action='store_true', help='Delete extraneous files from dest')
    p.add_argument(
        '-c', '--checksum', action='store_true', help='Compare by checksum not size/mtime'
    )
    p.add_argument('-u', '--update', action='store_true', help='Skip newer files on dest')
    p.add_argument('--include', action='append', default=[], metavar='PAT', help='Include pattern')
    p.add_argument('--exclude', action='append', default=[], metavar='PAT', help='Exclude pattern')

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    # Dispatch to handlers
    if args.command in ('create', 'c'):
        # create: error if exists (unless -f)
        _handle_add(args, overwrite=args.force, exist_ok=args.force)

    elif args.command in ('add', 'a'):
        # add: error if doesn't exist (unless -c)
        if not barecat.util.misc.exists(args.archive) and not args.create:
            print(
                f"Error: Archive '{args.archive}' does not exist. "
                "Use 'barecat create' or 'barecat add -c'.",
                file=sys.stderr,
            )
            sys.exit(1)
        _handle_add(args, overwrite=False, exist_ok=True)

    elif args.command in ('extract', 'x'):
        _handle_extract(args)

    elif args.command in ('list', 'ls', 'l', 't'):
        _handle_list(args)

    elif args.command == 'cat':
        with barecat.Barecat(args.archive) as bc:
            sys.stdout.buffer.write(bc[args.path])

    elif args.command == 'find':
        _handle_find(args)

    elif args.command == 'shell':
        from .shell import BarecatShell

        shell = BarecatShell(args.archive, readonly=not args.write)
        if args.shell_cmd:
            shell.onecmd(args.shell_cmd)
        else:
            shell.cmdloop()
        shell.close()

    elif args.command == 'browse':
        import curses
        from ..tui.browse import BarecatBrowser

        browser = BarecatBrowser(args.archive)
        curses.wrapper(browser.run)

    elif args.command == 'du':
        _handle_du(args)

    elif args.command == 'ncdu':
        import curses
        from ..tui.ncdu import BarecatDu

        du = BarecatDu(args.archive)
        curses.wrapper(du.run)

    elif args.command == 'tree':
        _handle_tree(args)

    elif args.command == 'verify':
        from .legacy import verify_crc_parallel

        with barecat.Barecat(args.archive, threadsafe=True) as bc:
            if args.quick:
                if not bc.verify_integrity(quick=True):
                    print('Integrity verification failed.', file=sys.stderr)
                    sys.exit(1)
            else:
                if not verify_crc_parallel(bc, workers=1):
                    print('CRC32C verification failed.', file=sys.stderr)
                    sys.exit(1)
                if not bc.index.verify_integrity():
                    print('Index integrity errors were found.', file=sys.stderr)
                    sys.exit(1)

    elif args.command == 'defrag':
        readonly = args.dry_run
        with barecat.Barecat(args.archive, readonly=readonly, append_only=False) as bc:
            defragger = BarecatDefragger(bc)
            if args.dry_run:
                stats = defragger.get_gap_stats()
                _print_defrag_stats(stats)
            elif defragger.needs_defrag():
                if args.quick:
                    freed = defragger.defrag_quick(time_max_seconds=args.max_seconds)
                elif args.smart:
                    freed = defragger.defrag_smart()
                else:
                    freed = defragger.defrag()
                print(f'Freed {_format_size(freed, human_readable=True)}')

    elif args.command == 'reshard':
        from ..maintenance.reshard import reshard

        with barecat.Barecat(args.archive, readonly=False, append_only=False) as bc:
            reshard(bc, target_shard_size_limit=parse_size(args.shard_size_limit))

    elif args.command == 'subset':
        if barecat.util.misc.exists(args.output) and not args.force:
            print(
                f"Error: Output '{args.output}' already exists. Use -f to overwrite.",
                file=sys.stderr,
            )
            sys.exit(1)

        if args.pattern and (args.include or args.exclude):
            print('Error: --pattern is incompatible with --include/--exclude', file=sys.stderr)
            sys.exit(1)

        filter_rules = _build_filter_rules_from_argv() if (args.include or args.exclude) else None
        pattern = _normalize_pattern(args.pattern) if args.pattern else None

        # Use merge into empty archive
        impl.merge(
            source_paths=[args.archive],
            target_path=args.output,
            shard_size_limit=parse_shard_size(args.shard_size_limit),
            overwrite=args.force,
            ignore_duplicates=False,
            as_subdirs=False,
            prefix=None,
            pattern=pattern,
            filter_rules=filter_rules,
        )

    elif args.command == 'merge':
        output_exists = barecat.util.misc.exists(args.output)

        # Validate flag combinations
        if args.symlink and args.append:
            print('Error: --append is not supported with --symlink', file=sys.stderr)
            sys.exit(1)

        if args.pattern and (args.include or args.exclude):
            print('Error: --pattern is incompatible with --include/--exclude', file=sys.stderr)
            sys.exit(1)

        # Check if any inputs are tar/zip (not supported with --symlink)
        has_traditional_archives = any(impl.is_traditional_archive(a) for a in args.archives)
        if args.symlink and has_traditional_archives:
            print('Error: --symlink is not supported with tar/zip inputs', file=sys.stderr)
            sys.exit(1)

        if output_exists and not args.force and not args.append:
            print(
                f"Error: Output '{args.output}' already exists. "
                'Use -f to overwrite or -a to append.',
                file=sys.stderr,
            )
            sys.exit(1)

        # Build filter rules for rsync-style filtering
        filter_rules = _build_filter_rules_from_argv() if (args.include or args.exclude) else None

        pattern = _normalize_pattern(args.pattern) if args.pattern else None

        if args.symlink:
            impl.merge_symlink(
                source_paths=args.archives,
                target_path=args.output,
                overwrite=args.force,
                ignore_duplicates=args.ignore_duplicates,
                as_subdirs=args.as_subdirs,
                prefix=args.prefix,
                pattern=pattern,
                filter_rules=filter_rules,
            )
        else:
            # For append mode, we don't overwrite but allow existing
            impl.merge(
                source_paths=args.archives,
                target_path=args.output,
                shard_size_limit=parse_shard_size(args.shard_size_limit),
                overwrite=args.force,
                ignore_duplicates=args.ignore_duplicates or args.append,
                as_subdirs=args.as_subdirs,
                prefix=args.prefix,
                pattern=pattern,
                filter_rules=filter_rules,
            )

    elif args.command == 'index-to-csv':
        writer = csv.writer(sys.stdout, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(['path', 'shard', 'offset', 'size', 'crc32c'])
        with barecat.Index(resolve_index_path(args.archive)) as index:
            for f in index.iter_all_fileinfos(order=Order.PATH):
                writer.writerow([f.path, f.shard, f.offset, f.size, f.crc32c])

    elif args.command == 'to-ncdu-json':
        impl.print_ncdu_json(resolve_index_path(args.archive))

    elif args.command == 'convert':
        tar_formats = ('tar', 'tar.gz', 'tar.bz2', 'tar.xz')

        # With --stdin, input arg is format; with --stdout, output arg is format
        if args.stdin:
            if args.input not in tar_formats:
                print(
                    f"Error: With --stdin, input must be a tar format: {', '.join(tar_formats)}",
                    file=sys.stderr,
                )
                sys.exit(1)
            input_is_archive = True
        else:
            input_is_archive = impl.is_traditional_archive(args.input)

        if args.stdout:
            if args.output not in tar_formats:
                print(
                    f"Error: With --stdout, output must be a tar format: {', '.join(tar_formats)}",
                    file=sys.stderr,
                )
                sys.exit(1)
            output_is_archive = True
        else:
            output_is_archive = impl.is_traditional_archive(args.output)

        # Validate conversion direction
        if input_is_archive and output_is_archive:
            print(
                'Error: Both input and output are tar/zip. One must be a barecat path.',
                file=sys.stderr,
            )
            sys.exit(1)
        if not input_is_archive and not output_is_archive:
            print(
                'Error: Neither input nor output is tar/zip. '
                'One must have extension like .tar, .tar.gz, .zip (or use --stdin/--stdout)',
                file=sys.stderr,
            )
            sys.exit(1)

        if input_is_archive:
            # tar/zip → barecat
            if barecat.util.misc.exists(args.output) and not args.force:
                print(
                    f"Error: Output '{args.output}' already exists. Use -f to overwrite.",
                    file=sys.stderr,
                )
                sys.exit(1)
            if args.wrap:
                # Zero-copy: create index pointing into original archive
                compressed_exts = ('.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz')
                if args.input.lower().endswith(compressed_exts):
                    print(
                        'Error: --wrap only works with uncompressed .tar or .zip files',
                        file=sys.stderr,
                    )
                    sys.exit(1)
                if args.stdin:
                    print('Error: --wrap cannot be used with --stdin', file=sys.stderr)
                    sys.exit(1)
                try:
                    impl.wrap_archive(
                        src_path=args.input,
                        target_path=args.output,
                        overwrite=args.force,
                    )
                except ValueError as e:
                    print(f'Error: {e}', file=sys.stderr)
                    sys.exit(1)
            elif args.stdin:
                impl.stdin_tar2barecat(
                    tar_format=args.input,
                    target_path=args.output,
                    shard_size_limit=parse_shard_size(args.shard_size_limit),
                    overwrite=args.force,
                )
            else:
                impl.archive2barecat(
                    src_path=args.input,
                    target_path=args.output,
                    shard_size_limit=parse_shard_size(args.shard_size_limit),
                    overwrite=args.force,
                )
        else:
            # barecat → tar/zip
            import os.path as osp

            if not args.stdout and osp.exists(args.output) and not args.force:
                print(
                    f"Error: Output '{args.output}' already exists. Use -f to overwrite.",
                    file=sys.stderr,
                )
                sys.exit(1)
            if args.stdout:
                impl.barecat2stdout_tar(
                    src_path=args.input, tar_format=args.output, root_dir=args.root_dir
                )
            else:
                impl.barecat2archive(
                    src_path=args.input, target_path=args.output, root_dir=args.root_dir
                )

    elif args.command == 'upgrade':
        from ..maintenance.upgrade_database import upgrade

        upgrade(args.archive, workers=args.workers, preserve_backup=args.preserve_backup)

    elif args.command == 'completion-script':
        print(barecat.get_completion_script(args.shell))

    elif args.command == 'mount':
        _handle_mount(args)

    elif args.command == 'rsync':
        from ..maintenance.rsync import rsync, RsyncOptions

        if len(args.paths) < 2:
            print('Error: rsync requires at least source and destination', file=sys.stderr)
            sys.exit(1)

        sources = args.paths[:-1]
        dest = args.paths[-1]
        options = RsyncOptions(
            delete=args.delete,
            dry_run=args.dry_run,
            verbose=args.verbose,
            progress=args.progress,
            checksum=args.checksum,
            update=args.update,
            include=args.include,
            exclude=args.exclude,
        )
        rsync(sources, dest, options)


def _normalize_pattern(pattern):
    """Normalize pattern: trailing / becomes /** (rsync convention)."""
    if pattern.endswith('/'):
        return pattern + '**'
    return pattern


def _build_filter_rules_from_argv():
    """Build filter rules from sys.argv, preserving order of -i/-x flags.

    We parse sys.argv directly because argparse's append action doesn't preserve
    interleaving order between -i/--include and -x/--exclude flags, which matters
    for first-match-wins rsync-style filtering.
    """
    filter_rules = []
    for i, arg in enumerate(sys.argv):
        if arg in ('-i', '--include') and i + 1 < len(sys.argv):
            filter_rules.append(('+', _normalize_pattern(sys.argv[i + 1])))
        elif arg.startswith('--include='):
            filter_rules.append(('+', _normalize_pattern(arg.split('=', 1)[1])))
        elif arg in ('-x', '--exclude') and i + 1 < len(sys.argv):
            filter_rules.append(('-', _normalize_pattern(sys.argv[i + 1])))
        elif arg.startswith('--exclude='):
            filter_rules.append(('-', _normalize_pattern(arg.split('=', 1)[1])))
    return filter_rules


def _handle_add(args, overwrite=False, exist_ok=True):
    """Handle create/add commands."""
    import itertools
    import os
    import os.path as osp

    from ..util.progbar import progressbar

    # Change directory if -C specified (like tar)
    if args.directory:
        os.chdir(args.directory)

    def matches_patterns(path, patterns, compiled_patterns={}):
        """Check if path matches any of the patterns.

        Uses Python glob syntax with ** support:
        - '*.pyc' matches only at root level
        - '**/*.pyc' matches .pyc files at any depth
        - 'tests/**' matches everything under tests/
        """
        if not patterns:
            return True
        for p in patterns:
            if p not in compiled_patterns:
                # Patterns with ** need recursive=True
                recursive = '**' in p
                compiled_patterns[p] = re.compile(
                    glob_to_regex(p, recursive=recursive, include_hidden=True)
                )
            if compiled_patterns[p].match(path):
                return True
        return False

    def should_include(path):
        """Check if path should be included based on include/exclude patterns."""
        # If exclude patterns given and path matches, exclude it
        if args.exclude and matches_patterns(path, args.exclude):
            return False
        # If include patterns given, path must match one of them
        if args.include and not matches_patterns(path, args.include):
            return False
        return True

    def generate_paths():
        """Generate (filesystem_path, store_path) pairs."""
        # From positional arguments (files/directories)
        if args.paths:
            for root in args.paths:
                if osp.isfile(root):
                    if should_include(root):
                        yield root, root
                elif osp.isdir(root):
                    # Add the directory itself
                    if should_include(root):
                        yield root, root

                    for dirpath, subdirnames, filenames in os.walk(root):
                        # Filter subdirs in-place to prevent descending into excluded dirs
                        subdirnames[:] = [
                            d for d in subdirnames if should_include(osp.join(dirpath, d))
                        ]

                        for entryname in itertools.chain(filenames, subdirnames):
                            full_path = osp.join(dirpath, entryname)
                            if not should_include(full_path):
                                continue
                            # Store path same as filesystem path (like tar)
                            yield full_path, full_path

        # From file list (-T FILE or -T -)
        if args.files_from:
            if args.files_from == '-':
                # Read from stdin
                if args.null:
                    for path in impl.iterate_zero_terminated(sys.stdin.buffer):
                        if should_include(path):
                            yield path, path
                else:
                    for line in sys.stdin:
                        path = line.rstrip('\n')
                        if path and should_include(path):
                            yield path, path
            else:
                # Read from file
                if args.null:
                    with open(args.files_from, 'rb') as f:
                        for path in impl.iterate_zero_terminated(f):
                            if should_include(path):
                                yield path, path
                else:
                    with open(args.files_from) as f:
                        for line in f:
                            path = line.rstrip('\n')
                            if path and should_include(path):
                                yield path, path

    # Validate: must have some input
    if not args.paths and not args.files_from:
        print('Error: No input specified. Provide paths or -T FILE.', file=sys.stderr)
        sys.exit(1)

    iterator = generate_paths()

    if args.physical_order:
        iterator = iter(
            sorted(iterator, key=lambda x: barecat_physical_order.get_physical_offset(x[0]))
        )

    iterator = progressbar(iterator, desc='Packing files', unit=' files')

    shard_size = getattr(args, 'shard_size_limit', None)
    impl.create(
        iterator,
        target_path=args.archive,
        shard_size_limit=parse_shard_size(shard_size),
        overwrite=overwrite,
        exist_ok=exist_ok,
        workers=args.workers,
    )


def _handle_extract(args):
    """Handle extract command."""
    import os
    import os.path as osp
    from ..io.copyfile import copy
    from ..util.progbar import progressbar

    if args.pattern and (args.include or args.exclude):
        print('Error: --pattern is incompatible with --include/--exclude', file=sys.stderr)
        sys.exit(1)

    def extract_file(bc, fpath):
        target = osp.join(args.directory, fpath)
        os.makedirs(osp.dirname(target), exist_ok=True)
        with open(target, 'wb') as out:
            copy(bc.open(fpath, 'rb'), out)

    with barecat.Barecat(args.archive) as bc:
        if args.paths:
            # Extract specific paths (filtering not applied to explicit paths)
            for path in args.paths:
                if path in bc:
                    sys.stdout.buffer.write(bc[path])
                elif bc.index.isdir(path):
                    for dirpath, dirnames, filenames in bc.walk(path):
                        for fname in progressbar(filenames):
                            fpath = osp.join(dirpath, fname) if dirpath else fname
                            extract_file(bc, fpath)
                else:
                    print(f'Error: {path} not found in archive', file=sys.stderr)
                    sys.exit(1)
        elif args.pattern:
            # Use optimized glob path
            pattern = _normalize_pattern(args.pattern)
            recursive = '**' in pattern
            for info in progressbar(
                bc.index.iterglob_infos(pattern, recursive=recursive, only_files=True),
                desc='Extracting',
                unit=' files',
            ):
                extract_file(bc, info.path)
        elif args.include or args.exclude:
            # Build filter rules preserving order
            filter_rules = _build_filter_rules_from_argv()
            for info in progressbar(
                bc.index.iterglob_infos_incl_excl(filter_rules, only_files=True),
                desc='Extracting',
                unit=' files',
            ):
                extract_file(bc, info.path)
        else:
            # Extract everything
            for fpath in progressbar(bc, desc='Extracting', unit=' files'):
                extract_file(bc, fpath)


def _format_mode(mode):
    """Format mode bits as ls-style string (e.g., -rw-r--r-- or drwxr-xr-x)."""
    import stat

    if mode is None:
        return '----------'

    # File type
    if stat.S_ISDIR(mode):
        s = 'd'
    elif stat.S_ISLNK(mode):
        s = 'l'
    else:
        s = '-'

    # Owner permissions
    s += 'r' if mode & stat.S_IRUSR else '-'
    s += 'w' if mode & stat.S_IWUSR else '-'
    s += 'x' if mode & stat.S_IXUSR else '-'

    # Group permissions
    s += 'r' if mode & stat.S_IRGRP else '-'
    s += 'w' if mode & stat.S_IWGRP else '-'
    s += 'x' if mode & stat.S_IXGRP else '-'

    # Other permissions
    s += 'r' if mode & stat.S_IROTH else '-'
    s += 'w' if mode & stat.S_IWOTH else '-'
    s += 'x' if mode & stat.S_IXOTH else '-'

    return s


def _format_time(mtime_ns):
    """Format mtime like ls (e.g., 'Dec 23 14:30' or 'Dec 23  2023')."""
    from datetime import datetime

    if mtime_ns is None:
        return '            '

    dt = datetime.fromtimestamp(mtime_ns / 1e9)
    now = datetime.now()

    # If within last 6 months, show time; otherwise show year
    six_months_ago = now.timestamp() - 180 * 24 * 3600
    if dt.timestamp() > six_months_ago:
        return dt.strftime('%b %e %H:%M')
    else:
        return dt.strftime('%b %e  %Y')


def _get_user_group(uid, gid, cache={}):
    """Get user and group names from uid/gid, with caching."""
    import pwd
    import grp

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


def _handle_list(args):
    """Handle list command."""
    from .commands import list_entries

    with barecat.Barecat(args.archive) as bc:
        list_entries(
            bc.index,
            paths=args.paths,
            long_format=args.long,
            recursive=args.recursive,
            jsonl=getattr(args, 'jsonl', False),
        )


def _handle_find(args):
    """Handle find command."""
    from .commands import find_entries

    with barecat.Barecat(args.archive) as bc:
        find_entries(
            bc.index,
            path=args.path,
            name=args.name,
            pathpattern=args.pathpattern,
            ftype=args.ftype,
            size=args.size,
            maxdepth=args.maxdepth,
            print0=args.print0,
        )


def _format_size(size, human_readable=False):
    """Format size for du output."""
    if not human_readable:
        return str(size)

    for unit in ['', 'K', 'M', 'G', 'T']:
        if abs(size) < 1024:
            if unit == '':
                return str(int(round(size)))
            return f'{size:.1f}{unit}'
        size /= 1024
    return f'{size:.1f}P'


def _handle_du(args):
    """Handle du command."""
    from .commands import du_entries

    with barecat.Barecat(args.archive) as bc:
        du_entries(
            bc.index,
            path=args.path,
            all_files=args.all,
            summarize=args.summarize,
            human_readable=args.human_readable,
            max_depth=args.max_depth,
        )


def _handle_tree(args):
    """Handle tree command."""
    from .commands import tree_entries

    with barecat.Barecat(args.archive) as bc:
        tree_entries(
            bc.index,
            path=args.path,
            level=args.level,
            dirs_only=args.dirs_only,
        )


def _handle_mount(args):
    """Handle mount command with standard mount-style -o options."""
    try:
        from barecat_mount import mount
    except ImportError:
        print(
            'Error: Mount support not installed. Install with: pip install barecat[mount]',
            file=sys.stderr,
        )
        sys.exit(1)

    # Parse -o options (comma-separated, like mount/sshfs)
    # Defaults
    readonly = True
    foreground = False
    mmap = False
    overwrite = False
    append_only = False
    enable_defrag = False
    shard_size_limit = None

    if args.options:
        for opt in args.options.split(','):
            opt = opt.strip()
            if opt == 'ro':
                readonly = True
            elif opt == 'rw':
                readonly = False
            elif opt in ('fg', 'foreground'):
                foreground = True
            elif opt == 'mmap':
                mmap = True
            elif opt == 'overwrite':
                overwrite = True
            elif opt == 'append_only':
                append_only = True
            elif opt == 'defrag':
                enable_defrag = True
            elif opt.startswith('shard_size_limit='):
                shard_size_limit = opt.split('=', 1)[1]
            elif opt == '':
                pass  # ignore empty
            else:
                print(f'Error: Unknown mount option: {opt}', file=sys.stderr)
                print(
                    'Valid options: ro, rw, fg/foreground, mmap, defrag, overwrite, '
                    'append_only, shard_size_limit=SIZE',
                    file=sys.stderr,
                )
                sys.exit(1)

    mount(
        barecat_path=args.archive,
        mountpoint=args.mountpoint,
        readonly=readonly,
        foreground=foreground,
        mmap=mmap,
        overwrite=overwrite,
        append_only=append_only,
        enable_defrag=enable_defrag,
        shard_size_limit=shard_size_limit,
    )


if __name__ == '__main__':
    main()
