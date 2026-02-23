#compdef barecat

# Zsh completion for barecat

_barecat() {
    local -a commands
    commands=(
        'create:Create new archive'
        'c:Create new archive (alias)'
        'add:Add files to existing archive'
        'a:Add files (alias)'
        'extract:Extract files from archive'
        'x:Extract files (alias)'
        'list:List archive contents'
        'ls:List contents (alias)'
        'l:List contents (alias)'
        't:List contents (alias)'
        'cat:Print file contents to stdout'
        'find:Search for files in archive'
        'shell:Interactive shell'
        'browse:Ranger-like file browser'
        'du:Show disk usage'
        'ncdu:ncdu-like disk usage viewer (TUI)'
        'tree:Display directory tree'
        'verify:Verify archive integrity'
        'defrag:Defragment archive'
        'reshard:Reshard archive with new shard size'
        'subset:Create new archive with subset of files'
        'merge:Merge multiple archives into one'
        'index-to-csv:Dump index as CSV'
        'to-ncdu-json:Print as ncdu JSON'
        'convert:Convert between barecat and tar/zip'
        'upgrade:Upgrade index to new schema'
        'completion-script:Print shell completion script path'
        'mount:Mount archive as FUSE filesystem'
        'rsync:rsync-like sync between local and archive'
    )

    _arguments -C \
        '1:command:->command' \
        '*::arg:->args'

    case "$state" in
        command)
            _describe -t commands 'barecat commands' commands
            ;;
        args)
            case "$words[1]" in
                create|c)
                    _arguments \
                        '-C[Change to directory before adding]:directory:_files -/' \
                        '--directory[Change to directory before adding]:directory:_files -/' \
                        '-T[Read paths from file (use - for stdin)]:file:_files' \
                        '--files-from[Read paths from file (use - for stdin)]:file:_files' \
                        '-0[Paths are null-separated]' \
                        '--null[Paths are null-separated]' \
                        '*--exclude[Exclude pattern]:pattern:' \
                        '*-i[Include pattern]:pattern:' \
                        '*--include[Include pattern]:pattern:' \
                        '-j[Worker threads]:workers:(1 2 4 8 16)' \
                        '--workers[Worker threads]:workers:(1 2 4 8 16)' \
                        '-f[Overwrite existing archive]' \
                        '--force[Overwrite existing archive]' \
                        '-s[Shard size limit]:size:(100M 500M 1G 2G 4G)' \
                        '--shard-size-limit[Shard size limit]:size:(100M 500M 1G 2G 4G)' \
                        '--physical-order[Add files in physical disk order]' \
                        '1:archive:_files' \
                        '*:paths:_files'
                    ;;
                add|a)
                    _arguments \
                        '-C[Change to directory before adding]:directory:_files -/' \
                        '--directory[Change to directory before adding]:directory:_files -/' \
                        '-T[Read paths from file (use - for stdin)]:file:_files' \
                        '--files-from[Read paths from file (use - for stdin)]:file:_files' \
                        '-0[Paths are null-separated]' \
                        '--null[Paths are null-separated]' \
                        '*--exclude[Exclude pattern]:pattern:' \
                        '*-i[Include pattern]:pattern:' \
                        '*--include[Include pattern]:pattern:' \
                        '-j[Worker threads]:workers:(1 2 4 8 16)' \
                        '--workers[Worker threads]:workers:(1 2 4 8 16)' \
                        '-c[Create archive if not exists]' \
                        '--create[Create archive if not exists]' \
                        '-s[Shard size limit]:size:(100M 500M 1G 2G 4G)' \
                        '--shard-size-limit[Shard size limit]:size:(100M 500M 1G 2G 4G)' \
                        '--physical-order[Add files in physical disk order]' \
                        '1:archive:_files' \
                        '*:paths:_files'
                    ;;
                extract|x)
                    _arguments \
                        '-C[Extract to directory]:directory:_files -/' \
                        '--directory[Extract to directory]:directory:_files -/' \
                        '--pattern[Glob pattern]:pattern:' \
                        '*-x[Exclude pattern]:pattern:' \
                        '*--exclude[Exclude pattern]:pattern:' \
                        '*-i[Include pattern]:pattern:' \
                        '*--include[Include pattern]:pattern:' \
                        '1:archive:_files' \
                        '*:paths:'
                    ;;
                list|ls|l|t)
                    _arguments \
                        '-l[Long listing with sizes]' \
                        '--long[Long listing with sizes]' \
                        '-R[List recursively]' \
                        '--recursive[List recursively]' \
                        '--jsonl[Output as JSON lines]' \
                        '1:archive:_files' \
                        '*:paths:'
                    ;;
                find)
                    _arguments \
                        '-name[Match basename]:pattern:' \
                        '-path[Match full path]:pattern:' \
                        '-type[File type]:type:(f d)' \
                        '-size[Size filter]:size:' \
                        '-maxdepth[Maximum depth]:depth:' \
                        '-print0[Null-separated output]' \
                        '1:archive:_files' \
                        '2:path:'
                    ;;
                subset)
                    _arguments \
                        '-o[Output archive]:output:_files' \
                        '--output[Output archive]:output:_files' \
                        '-s[Shard size limit]:size:(100M 500M 1G 2G 4G)' \
                        '--shard-size-limit[Shard size limit]:size:(100M 500M 1G 2G 4G)' \
                        '-f[Overwrite if exists]' \
                        '--force[Overwrite if exists]' \
                        '--pattern[Glob pattern]:pattern:' \
                        '*-i[Include pattern]:pattern:' \
                        '*--include[Include pattern]:pattern:' \
                        '*-x[Exclude pattern]:pattern:' \
                        '*--exclude[Exclude pattern]:pattern:' \
                        '1:archive:_files'
                    ;;
                cat)
                    _arguments \
                        '1:archive:_files' \
                        '2:path:'
                    ;;
                shell)
                    _arguments \
                        '-c[Execute command]:command:' \
                        '--cmd[Execute command]:command:' \
                        '-w[Open in write mode]' \
                        '--write[Open in write mode]' \
                        '1:archive:_files'
                    ;;
                browse|ncdu|to-ncdu-json|index-to-csv)
                    _arguments '1:archive:_files'
                    ;;
                du)
                    _arguments \
                        '-a[Show all files]' \
                        '--all[Show all files]' \
                        '-s[Show only total]' \
                        '--summarize[Show only total]' \
                        '-H[Human readable sizes]' \
                        '--human-readable[Human readable sizes]' \
                        '-d[Max depth]:depth:' \
                        '--max-depth[Max depth]:depth:' \
                        '1:archive:_files' \
                        '2:path:'
                    ;;
                tree)
                    _arguments \
                        '-L[Limit depth]:level:' \
                        '--level[Limit depth]:level:' \
                        '-d[Directories only]' \
                        '--dirs-only[Directories only]' \
                        '1:archive:_files' \
                        '2:path:'
                    ;;
                verify)
                    _arguments \
                        '--quick[Quick check]' \
                        '1:archive:_files'
                    ;;
                defrag)
                    _arguments \
                        '--quick[Quick defrag (time-limited)]' \
                        '--max-seconds[Time limit for --quick]:seconds:' \
                        '--smart[Smart defrag (copy contiguous chunks)]' \
                        '-n[Dry run - show stats only]' \
                        '--dry-run[Dry run - show stats only]' \
                        '1:archive:_files'
                    ;;
                reshard)
                    _arguments \
                        '-s[New shard size]:size:(100M 500M 1G 2G 4G)' \
                        '--shard-size-limit[New shard size]:size:(100M 500M 1G 2G 4G)' \
                        '1:archive:_files'
                    ;;
                merge)
                    _arguments \
                        '-o[Output archive]:output:_files' \
                        '--output[Output archive]:output:_files' \
                        '-s[Shard size limit]:size:(100M 500M 1G 2G 4G)' \
                        '--shard-size-limit[Shard size limit]:size:(100M 500M 1G 2G 4G)' \
                        '-f[Overwrite existing]' \
                        '--force[Overwrite existing]' \
                        '-a[Append to existing output]' \
                        '--append[Append to existing output]' \
                        '--symlink[Create symlinks instead of copying]' \
                        '--ignore-duplicates[Ignore duplicate files]' \
                        '--as-subdirs[Merge each archive into its own subdirectory]' \
                        '--prefix[Prefix for merged paths]:prefix:' \
                        '--pattern[Glob pattern to filter files]:pattern:' \
                        '-i[Include pattern]:pattern:' \
                        '--include[Include pattern]:pattern:' \
                        '-x[Exclude pattern]:pattern:' \
                        '--exclude[Exclude pattern]:pattern:' \
                        '*:archives:_files'
                    ;;
                convert)
                    _arguments \
                        '-s[Shard size limit]:size:(100M 500M 1G 2G 4G)' \
                        '--shard-size-limit[Shard size limit]:size:(100M 500M 1G 2G 4G)' \
                        '-f[Overwrite existing]' \
                        '--force[Overwrite existing]' \
                        '--stdin[Read tar from stdin]' \
                        '--stdout[Write tar to stdout]' \
                        '--root-dir[Wrap files in root directory]:name:' \
                        '--wrap[Zero-copy index over existing tar/zip]' \
                        '1:input:_files' \
                        '2:output:_files'
                    ;;
                upgrade)
                    _arguments \
                        '-j[Worker threads]:workers:(1 2 4 8 16)' \
                        '--workers[Worker threads]:workers:(1 2 4 8 16)' \
                        '1:archive:_files'
                    ;;
                completion-script)
                    _arguments '1:shell:(bash zsh)'
                    ;;
                mount)
                    _arguments \
                        '-o[Mount options]:options:' \
                        '--options[Mount options]:options:' \
                        '1:archive:_files' \
                        '2:mountpoint:_files -/'
                    ;;
                rsync)
                    _arguments \
                        '-n[Dry run]' \
                        '--dry-run[Dry run]' \
                        '-v[Verbose output]' \
                        '--verbose[Verbose output]' \
                        '--progress[Show progress]' \
                        '--delete[Delete extraneous files from dest]' \
                        '-c[Compare by checksum]' \
                        '--checksum[Compare by checksum]' \
                        '-u[Skip newer files on dest]' \
                        '--update[Skip newer files on dest]' \
                        '*--include[Include pattern]:pattern:' \
                        '*--exclude[Exclude pattern]:pattern:' \
                        '*:paths:_files'
                    ;;
            esac
            ;;
    esac
}

_barecat "$@"
