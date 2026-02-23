# Bash completion for barecat
# Source this file or add to /etc/bash_completion.d/

_barecat() {
    local cur prev words cword
    _init_completion || return

    local commands="create add extract list cat find shell browse du ncdu tree verify defrag reshard subset merge index-to-csv to-ncdu-json convert upgrade completion-script mount rsync"
    local command_aliases="c a x ls l t"

    # Find the subcommand
    local cmd=""
    for ((i=1; i < cword; i++)); do
        case "${words[i]}" in
            create|c|add|a|extract|x|list|ls|l|t|cat|find|shell|browse|du|ncdu|tree|verify|defrag|reshard|subset|merge|index-to-csv|to-ncdu-json|convert|upgrade|completion-script|mount|rsync)
                cmd="${words[i]}"
                break
                ;;
        esac
    done

    # Complete subcommands
    if [[ -z "$cmd" ]]; then
        COMPREPLY=($(compgen -W "$commands" -- "$cur"))
        return
    fi

    # Complete options and arguments based on subcommand
    case "$cmd" in
        create|c)
            case "$prev" in
                -C|--directory)
                    _filedir -d
                    return
                    ;;
                -T|--files-from|-s|--shard-size-limit)
                    _filedir
                    return
                    ;;
                -j|--workers)
                    COMPREPLY=($(compgen -W "1 2 4 8 16" -- "$cur"))
                    return
                    ;;
            esac
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "-C --directory -T --files-from -0 --null --exclude -i --include -j --workers -f --force -s --shard-size-limit --physical-order" -- "$cur"))
            else
                _filedir
            fi
            ;;
        add|a)
            case "$prev" in
                -C|--directory)
                    _filedir -d
                    return
                    ;;
                -T|--files-from|-s|--shard-size-limit)
                    _filedir
                    return
                    ;;
                -j|--workers)
                    COMPREPLY=($(compgen -W "1 2 4 8 16" -- "$cur"))
                    return
                    ;;
            esac
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "-C --directory -T --files-from -0 --null --exclude -i --include -j --workers -c --create -s --shard-size-limit --physical-order" -- "$cur"))
            else
                _filedir
            fi
            ;;
        extract|x)
            case "$prev" in
                -C|--directory)
                    _filedir -d
                    return
                    ;;
            esac
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "-C --directory --pattern -x --exclude -i --include" -- "$cur"))
            else
                _filedir
            fi
            ;;
        list|ls|l|t)
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "-l --long -R --recursive --jsonl" -- "$cur"))
            else
                _filedir
            fi
            ;;
        find)
            case "$prev" in
                -name|-path|-wholename)
                    return
                    ;;
                -type)
                    COMPREPLY=($(compgen -W "f d" -- "$cur"))
                    return
                    ;;
                -size|-maxdepth)
                    return
                    ;;
            esac
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "-name -path -type -size -maxdepth -print0" -- "$cur"))
            else
                _filedir
            fi
            ;;
        subset)
            case "$prev" in
                -o|--output)
                    _filedir
                    return
                    ;;
                -s|--shard-size-limit)
                    COMPREPLY=($(compgen -W "100M 500M 1G 2G 4G" -- "$cur"))
                    return
                    ;;
            esac
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "-o --output -s --shard-size-limit -f --force --pattern -i --include -x --exclude" -- "$cur"))
            else
                _filedir
            fi
            ;;
        cat|shell|browse|du|ncdu|tree|verify|defrag|to-ncdu-json|index-to-csv|upgrade)
            case "$cmd" in
                shell)
                    if [[ "$cur" == -* ]]; then
                        COMPREPLY=($(compgen -W "-c --cmd -w --write" -- "$cur"))
                        return
                    fi
                    ;;
                du)
                    if [[ "$cur" == -* ]]; then
                        COMPREPLY=($(compgen -W "-a --all -s --summarize -H --human-readable -d --max-depth" -- "$cur"))
                        return
                    fi
                    ;;
                tree)
                    if [[ "$cur" == -* ]]; then
                        COMPREPLY=($(compgen -W "-L --level -d --dirs-only" -- "$cur"))
                        return
                    fi
                    ;;
                verify)
                    if [[ "$cur" == -* ]]; then
                        COMPREPLY=($(compgen -W "--quick" -- "$cur"))
                        return
                    fi
                    ;;
                defrag)
                    case "$prev" in
                        --max-seconds)
                            return
                            ;;
                    esac
                    if [[ "$cur" == -* ]]; then
                        COMPREPLY=($(compgen -W "--quick --max-seconds --smart -n --dry-run" -- "$cur"))
                        return
                    fi
                    ;;
                upgrade)
                    if [[ "$cur" == -* ]]; then
                        COMPREPLY=($(compgen -W "-j --workers" -- "$cur"))
                        return
                    fi
                    ;;
            esac
            _filedir
            ;;
        reshard)
            case "$prev" in
                -s|--shard-size-limit)
                    COMPREPLY=($(compgen -W "100M 500M 1G 2G 4G" -- "$cur"))
                    return
                    ;;
            esac
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "-s --shard-size-limit" -- "$cur"))
            else
                _filedir
            fi
            ;;
        merge)
            case "$prev" in
                -o|--output)
                    _filedir
                    return
                    ;;
                -s|--shard-size-limit)
                    COMPREPLY=($(compgen -W "100M 500M 1G 2G 4G" -- "$cur"))
                    return
                    ;;
            esac
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "-o --output -s --shard-size-limit -f --force -a --append --symlink --ignore-duplicates --as-subdirs --prefix --pattern -i --include -x --exclude" -- "$cur"))
            else
                _filedir
            fi
            ;;
        convert)
            case "$prev" in
                -s|--shard-size-limit)
                    COMPREPLY=($(compgen -W "100M 500M 1G 2G 4G" -- "$cur"))
                    return
                    ;;
                --root-dir)
                    return
                    ;;
            esac
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "-s --shard-size-limit -f --force --stdin --stdout --root-dir --wrap" -- "$cur"))
            else
                _filedir
            fi
            ;;
        completion-script)
            COMPREPLY=($(compgen -W "bash zsh" -- "$cur"))
            ;;
        mount)
            case "$prev" in
                -o|--options)
                    COMPREPLY=($(compgen -W "ro rw fg foreground mmap defrag overwrite append_only" -- "$cur"))
                    return
                    ;;
            esac
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "-o --options" -- "$cur"))
            else
                _filedir
            fi
            ;;
        rsync)
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "-n --dry-run -v --verbose --delete -c --checksum -u --update --exclude --include --progress" -- "$cur"))
            else
                _filedir
            fi
            ;;
    esac
}

complete -F _barecat barecat
