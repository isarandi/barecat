# This is copied from CPython main branch as of 2024-12-07.
import re
import os.path
import functools

_re_setops_sub = re.compile(r'([&~|])').sub
_re_escape = functools.lru_cache(maxsize=512)(re.escape)


def glob_to_regex(pat, *, recursive=False, include_hidden=False, seps=None):
    """Translate a pathname with shell wildcards to a regular expression.

    If `recursive` is true, the pattern segment '**' will match any number of
    path segments.

    If `include_hidden` is true, wildcards can match path segments beginning
    with a dot ('.').

    If a sequence of separator characters is given to `seps`, they will be
    used to split the pattern into segments and match path separators. If not
    given, os.path.sep and os.path.altsep (where available) are used.
    """
    if not seps:
        if os.path.altsep:
            seps = (os.path.sep, os.path.altsep)
        else:
            seps = os.path.sep
    escaped_seps = ''.join(map(re.escape, seps))
    any_sep = f'[{escaped_seps}]' if len(seps) > 1 else escaped_seps
    not_sep = f'[^{escaped_seps}]'
    if include_hidden:
        one_last_segment = f'{not_sep}+'
        one_segment = f'{one_last_segment}{any_sep}'
        any_segments = f'(?:.+{any_sep})?'
        any_last_segments = '.*'
    else:
        one_last_segment = f'[^{escaped_seps}.]{not_sep}*'
        one_segment = f'{one_last_segment}{any_sep}'
        any_segments = f'(?:{one_segment})*'
        any_last_segments = f'{any_segments}(?:{one_last_segment})?'

    results = []
    parts = re.split(any_sep, pat)
    last_part_idx = len(parts) - 1
    for idx, part in enumerate(parts):
        if part == '*':
            results.append(one_segment if idx < last_part_idx else one_last_segment)
        elif recursive and part == '**':
            if idx < last_part_idx:
                if parts[idx + 1] != '**':
                    results.append(any_segments)
            else:
                results.append(any_last_segments)
        else:
            if part:
                if not include_hidden and part[0] in '*?':
                    results.append(r'(?!\.)')
                results.extend(_translate(part, f'{not_sep}*', not_sep)[0])
            if idx < last_part_idx:
                results.append(any_sep)
    res = ''.join(results)
    return fr'(?s:{res})\Z'


def _translate(pat, star, question_mark):
    res = []
    add = res.append
    star_indices = []

    i, n = 0, len(pat)
    while i < n:
        c = pat[i]
        i = i + 1
        if c == '*':
            # store the position of the wildcard
            star_indices.append(len(res))
            add(star)
            # compress consecutive `*` into one
            while i < n and pat[i] == '*':
                i += 1
        elif c == '?':
            add(question_mark)
        elif c == '[':
            j = i
            if j < n and pat[j] == '!':
                j = j + 1
            if j < n and pat[j] == ']':
                j = j + 1
            while j < n and pat[j] != ']':
                j = j + 1
            if j >= n:
                add('\\[')
            else:
                stuff = pat[i:j]
                if '-' not in stuff:
                    stuff = stuff.replace('\\', r'\\')
                else:
                    chunks = []
                    k = i + 2 if pat[i] == '!' else i + 1
                    while True:
                        k = pat.find('-', k, j)
                        if k < 0:
                            break
                        chunks.append(pat[i:k])
                        i = k + 1
                        k = k + 3
                    chunk = pat[i:j]
                    if chunk:
                        chunks.append(chunk)
                    else:
                        chunks[-1] += '-'
                    # Remove empty ranges -- invalid in RE.
                    for k in range(len(chunks) - 1, 0, -1):
                        if chunks[k - 1][-1] > chunks[k][0]:
                            chunks[k - 1] = chunks[k - 1][:-1] + chunks[k][1:]
                            del chunks[k]
                    # Escape backslashes and hyphens for set difference (--).
                    # Hyphens that create ranges shouldn't be escaped.
                    stuff = '-'.join(s.replace('\\', r'\\').replace('-', r'\-') for s in chunks)
                i = j + 1
                if not stuff:
                    # Empty range: never match.
                    add('(?!)')
                elif stuff == '!':
                    # Negated empty range: match any character.
                    add('.')
                else:
                    # Escape set operations (&&, ~~ and ||).
                    stuff = _re_setops_sub(r'\\\1', stuff)
                    if stuff[0] == '!':
                        stuff = '^' + stuff[1:]
                    elif stuff[0] in ('^', '['):
                        stuff = '\\' + stuff
                    add(f'[{stuff}]')
        else:
            add(_re_escape(c))
    assert i == n
    return res, star_indices


def expand_doublestar(pattern, recursive=True):
    """Expand ** for SQLite GLOB. Returns list of patterns.

    Since SQLite GLOB's * matches any character including /, we need to expand
    ** patterns into OR'd alternatives to handle the zero-segment case.

    Examples:
        '**/foo.txt' -> ['foo.txt', '*/foo.txt']
        'a/**/b' -> ['a/b', 'a/*/b']
        'a/**' -> ['a', 'a/*']
    """
    if not recursive:
        return [pattern]

    # Count only "proper" ** segments (complete path segments)
    count = 0
    if pattern.startswith('**/'):
        count += 1
    if pattern.endswith('/**'):
        count += 1
    count += pattern.count('/**/')
    if pattern == '**':
        count = 1

    if count == 0:
        return [pattern]

    if count > 2:
        # Too many - collapse all ** to single * (broad but filtered by regex)
        result = pattern
        result = result.replace('/**/', '*')
        if result.startswith('**/'):
            result = '*' + result[3:]
        if result.endswith('/**'):
            result = result[:-3] + '*'
        if result == '**':
            result = '*'
        return [result]

    variants = [pattern]

    # **/X at start → X or */X
    while any(v.startswith('**/') for v in variants):
        new = []
        for v in variants:
            if v.startswith('**/'):
                new.append(v[3:])
                new.append('*/' + v[3:])
            else:
                new.append(v)
        variants = new

    # X/**/ in middle → X/ or X/*/
    while any('/**/' in v for v in variants):
        new = []
        for v in variants:
            if '/**/' in v:
                new.append(v.replace('/**/', '/', 1))
                new.append(v.replace('/**/', '/*/', 1))
            else:
                new.append(v)
        variants = new

    # X/** at end → X or X/*
    while any(v.endswith('/**') for v in variants):
        new = []
        for v in variants:
            if v.endswith('/**'):
                new.append(v[:-3])
                new.append(v[:-3] + '/*')
            else:
                new.append(v)
        variants = new

    # Handle bare ** → just *
    variants = ['*' if v == '**' else v for v in variants]

    # Dedupe
    seen = set()
    result = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            result.append(v)

    # Remove redundant: if '*' is present, it covers everything
    if '*' in result:
        return ['*']

    # If P ends with *, then P/*... is redundant
    final = []
    for v in result:
        dominated = False
        for other in result:
            if other == v:
                continue
            if other.endswith('*') and v.startswith(other) and v[len(other):].startswith('/'):
                dominated = True
                break
        if not dominated:
            final.append(v)

    return final


def pattern_to_sql_exclude(pattern):
    """Convert exclude pattern to SQL condition, if possible.

    Returns (sql_expr, params) tuple, or None if pattern can't be efficiently
    expressed in SQL and should be handled by Python.

    Handles common patterns:
    - '*.ext' (root only) -> parent = '' AND path GLOB '*.ext'
    - 'dir/*.ext' (direct children) -> parent = 'dir' AND path GLOB 'dir/*.ext'
    - '**/*.ext' (any depth) -> path GLOB '*.ext'
    - 'dir/**' (entire subtree) -> path GLOB 'dir/*'
    - '**/dir/**' (dir at any depth) -> path GLOB '*/dir/*' OR path GLOB 'dir/*'
    """
    # Case 1: **/*.ext or **/* - any depth with suffix
    if pattern.startswith('**/') and '/' not in pattern[3:]:
        suffix = pattern[3:]  # e.g., '*.ext' or '*'
        return f"path GLOB :p", {'p': glob_to_sqlite(suffix)}

    # Case 2: dir/** - entire subtree
    if pattern.endswith('/**') and '**' not in pattern[:-3] and '*' not in pattern[:-3]:
        prefix = pattern[:-3]  # e.g., 'thumbs'
        return f"path GLOB :p", {'p': glob_to_sqlite(prefix + '/*')}

    # Case 3: dir/*.ext or *.ext - direct children only, no ** anywhere
    if '**' not in pattern:
        if '/' in pattern:
            parent = pattern.rsplit('/', 1)[0]
            # Check parent has no wildcards
            if '*' not in parent and '?' not in parent and '[' not in parent:
                return f"parent = :parent AND path GLOB :p", {
                    'parent': parent,
                    'p': glob_to_sqlite(pattern)
                }
        else:
            # Root level pattern like *.ext
            return f"parent = '' AND path GLOB :p", {'p': glob_to_sqlite(pattern)}

    # Complex pattern - can't handle in SQL
    return None


def glob_to_sqlite(pat):
    """Convert Python glob pattern to SQLite GLOB pattern.

    Handles the differences between Python glob and SQLite GLOB:
    - Python uses [!...] for negation, SQLite uses [^...]
    - Python treats [^...] as a bracket with literal ^, SQLite treats it as negation

    Transformations:
    - [!] (invalid, literal) -> [[]!]
    - [!X...] (negation) -> [^X...]
    - [^] (bracket with ^) -> ^
    - [^^^...] (only carets) -> ^
    - [^^X...] (leading carets) -> [X...^^]
    - Unclosed [ -> [[]
    """
    # Fast path: if no opening bracket, no conversion needed
    if '[' not in pat:
        return pat

    res = []
    i, n = 0, len(pat)
    while i < n:
        c = pat[i]
        i += 1
        if c == '[':
            j = i
            negate = False
            if j < n and pat[j] == '!':
                negate = True
                j += 1
            content_start = j
            # First char after [! can be ] and it's literal content
            if j < n and pat[j] == ']':
                j += 1
            while j < n and pat[j] != ']':
                j += 1
            if j >= n:
                # Unclosed bracket - literal [
                res.append('[[]')
            else:
                content = pat[content_start:j]
                if negate:
                    if content == '':
                        # [!] - nothing to negate, literal [!]
                        res.append('[[]!]')
                    else:
                        # [!X...] -> [^X...]
                        res.append('[^' + content + ']')
                else:
                    # Count leading ^ chars
                    num_carets = 0
                    while num_carets < len(content) and content[num_carets] == '^':
                        num_carets += 1
                    if num_carets == len(content) and content:
                        # Content is ONLY ^ chars -> single ^
                        res.append('^')
                    elif num_carets > 0:
                        # Move all leading ^ to end: [^^a] -> [a^^]
                        rest = content[num_carets:]
                        carets = '^' * num_carets
                        res.append('[' + rest + carets + ']')
                    else:
                        res.append('[' + content + ']')
                i = j + 1
        else:
            res.append(c)
    return ''.join(res)
