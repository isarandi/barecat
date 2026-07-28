"""Per-worker writer for parallel ingestion via PostgreSQL coordination.

PostgreSQL handles shard/offset allocation via a BEFORE INSERT trigger.
Workers INSERT metadata (with size), get back the allocated shard and offset
via RETURNING, and pwrite data directly to shard files. Multiple workers
can safely pwrite to the same shard at different offsets.

No custom server needed — PostgreSQL itself handles coordination.
"""

import os
from collections.abc import Iterator, MutableMapping
from typing import Optional, Union

import crc32c as crc32c_lib
import psycopg

from ..core.paths import normalize_path


class BarecatPostgresWriter(MutableMapping):
    """Writer for parallel barecat ingestion via PostgreSQL.

    Each writer buffers file data and metadata, then flushes in batches.
    On flush, metadata is INSERTed into PostgreSQL (a trigger allocates
    shard/offset), and data is pwritten to shard files.

    Multiple writers can run concurrently — they share shards safely
    because pwrite at non-overlapping offsets is atomic. The PostgreSQL
    connection is opened when needed and closed after every flush, so
    hundreds of workers idling between batches do not hold hundreds of
    server slots (PostgreSQL's default ``max_connections`` is only 100).

    Crash behavior: metadata is committed before the data is written, so a
    worker killed mid-flush can leave index rows whose bytes are absent or
    partial. This is deliberate (one round-trip per batch); such rows are
    detectable via their crc32c and are flagged/dropped by the verification
    pass in :func:`barecat.postgres.export_to_sqlite`.

    Args:
        conninfo: PostgreSQL connection string (e.g., 'postgresql:///mydb').
        archive_path: Path to the barecat archive on shared storage.
            Shard files will be created as '{archive_path}-shard-XXXXX'.
        batch_size: Max number of file entries to buffer before flushing.
        batch_bytes: Max total bytes of file data to buffer before flushing.
            Defaults to 256 MB.
        ignore_duplicates: If True, silently skip files whose path already
            exists in the index. If False (default), raise on duplicates.
    """

    def __init__(
        self,
        conninfo: str,
        archive_path: Union[str, os.PathLike],
        batch_size: int = 1000,
        batch_bytes: int = 256 * 1024 * 1024,
        ignore_duplicates: bool = False,
    ):
        self._conninfo = conninfo
        self._archive_path = os.fspath(archive_path)
        self._batch_size = batch_size
        self._batch_bytes = batch_bytes
        self._ignore_duplicates = ignore_duplicates
        self._conn: Optional[psycopg.Connection] = None
        self._pending = []  # [(path, data, size, crc32c, mode, uid, gid, mtime_ns), ...]
        self._pending_bytes = 0
        # Rows whose metadata is already committed but whose bytes still need
        # pwriting (a previous flush failed partway): [(path, data, shard, offset)]
        self._pwrite_backlog = []
        self._shard_fds = {}  # shard_num -> fd
        self._closed = False

    def _get_conn(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._conninfo)
        return self._conn

    def _release_conn(self):
        """Close the connection so idle workers don't occupy a server slot."""
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None

    def _get_shard_fd(self, shard):
        """Get file descriptor for a shard, creating the file if needed."""
        if shard not in self._shard_fds:
            path = f'{self._archive_path}-shard-{shard:05d}'
            self._shard_fds[shard] = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        return self._shard_fds[shard]

    def add(
        self,
        path: str,
        data: bytes,
        mode: Optional[int] = None,
        uid: Optional[int] = None,
        gid: Optional[int] = None,
        mtime_ns: Optional[int] = None,
    ):
        """Add a file from bytes.

        Args:
            path: Path within the archive. Normalized on entry; paths escaping
                the archive root raise :class:`PathEscapeBarecatError`.
            data: File contents.
            mode: File mode (permissions).
            uid: User ID.
            gid: Group ID.
            mtime_ns: Modification time in nanoseconds since epoch.
        """
        # Normalize here, not at export: the index must never hold paths that the
        # normalized lookups of the final SQLite archive cannot reach, and '..'
        # escapes must be rejected outright (audit item 2 applies here too).
        path = normalize_path(path)
        crc32c = crc32c_lib.crc32c(data)
        self._pending.append((path, data, len(data), crc32c, mode, uid, gid, mtime_ns))
        self._pending_bytes += len(data)
        if len(self._pending) >= self._batch_size or self._pending_bytes >= self._batch_bytes:
            self.flush()

    def add_from_file(
        self,
        archive_path: str,
        filesystem_path: str,
        preserve_metadata: bool = True,
    ):
        """Add a file from the filesystem.

        Args:
            archive_path: Path within the archive.
            filesystem_path: Path to the file on disk.
            preserve_metadata: If True, preserve mode/uid/gid/mtime from stat.
        """
        s = os.stat(filesystem_path)
        with open(filesystem_path, 'rb') as f:
            data = f.read()

        if preserve_metadata:
            self.add(
                archive_path,
                data,
                mode=s.st_mode,
                uid=s.st_uid,
                gid=s.st_gid,
                mtime_ns=s.st_mtime_ns,
            )
        else:
            self.add(archive_path, data)

    def flush(self):
        """Flush buffered entries: INSERT metadata to PG, pwrite data to shards.

        If a previous flush failed after committing its INSERT but before all
        pwrites completed, those writes are retried first — without
        re-INSERTing, which would only hit unique violations.
        """
        self._drain_pwrite_backlog()

        if not self._pending:
            self._release_conn()
            return

        # Deduplicate within the batch, first occurrence winning, to match ON
        # CONFLICT DO NOTHING semantics. Without this, two same-path entries in
        # one batch map to a single allocation and both payloads get pwritten to
        # it — final bytes from the second, stored CRC from the first: a
        # self-corrupting entry.
        seen = set()
        entries = []
        for entry in self._pending:
            if entry[0] in seen:
                if not self._ignore_duplicates:
                    raise ValueError(f'Duplicate path within one batch: {entry[0]!r}')
                continue
            seen.add(entry[0])
            entries.append(entry)

        n = len(entries)
        params = []
        for path, _data, size, crc32c, mode, uid, gid, mtime_ns in entries:
            params.extend([path, size, crc32c, mode, uid, gid, mtime_ns])

        placeholders = ','.join(['(%s, %s, %s, %s, %s, %s, %s)'] * n)
        maybe_skip_dupes = 'ON CONFLICT (path) DO NOTHING ' if self._ignore_duplicates else ''
        # RETURNING includes path so allocations are matched by key. Do NOT zip
        # RETURNING rows with the VALUES order: PostgreSQL does not guarantee
        # that they correspond, and a mismatch would pwrite payloads to wrong
        # offsets — cross-file corruption.
        query = (
            f'INSERT INTO files (path, size, crc32c, mode, uid, gid, mtime_ns) '
            f'VALUES {placeholders} '
            f'{maybe_skip_dupes}'
            f'RETURNING path, shard, "offset"'
        )
        conn = self._get_conn()
        rows = conn.execute(query, params).fetchall()
        conn.commit()

        alloc = {row[0]: (row[1], row[2]) for row in rows}

        # Metadata is committed; from here on the remaining work is byte
        # delivery, tracked in the backlog so that a failed pwrite is completed
        # on the next flush instead of re-INSERTed.
        self._pwrite_backlog = [
            (path, data, *alloc[path]) for path, data, *_ in entries if path in alloc
        ]
        self._pending.clear()
        self._pending_bytes = 0
        try:
            self._drain_pwrite_backlog()
        finally:
            self._release_conn()

    def _drain_pwrite_backlog(self):
        while self._pwrite_backlog:
            path, data, shard, offset = self._pwrite_backlog[-1]
            os.pwrite(self._get_shard_fd(shard), data, offset)
            self._pwrite_backlog.pop()

    def close(self):
        """Flush remaining data, close shard fds and PG connection."""
        if self._closed:
            return
        self._closed = True

        try:
            self.flush()
        finally:
            for fd in self._shard_fds.values():
                os.close(fd)
            self._shard_fds.clear()
            self._release_conn()

    # Read helpers

    def _lookup(self, path: str):
        """Look up a file, checking pending buffer first, then PG."""
        path = normalize_path(path)
        for entry in reversed(self._pending):
            if entry[0] == path:
                return entry[1]  # return buffered data directly

        row = (
            self._get_conn()
            .execute('SELECT shard, "offset", size FROM files WHERE path = %s', (path,))
            .fetchone()
        )
        if row is None:
            raise KeyError(path)
        shard, offset, size = row
        return os.pread(self._get_shard_fd(shard), size, offset)

    # MutableMapping interface

    def __setitem__(self, path: str, data: bytes):
        self.add(path, data)

    def __getitem__(self, path: str) -> bytes:
        return self._lookup(path)

    def __delitem__(self, path: str):
        self.flush()
        conn = self._get_conn()
        conn.execute('DELETE FROM files WHERE path = %s', (normalize_path(path),))
        conn.commit()

    def __contains__(self, path) -> bool:
        path = normalize_path(path)
        for entry in self._pending:
            if entry[0] == path:
                return True
        row = self._get_conn().execute('SELECT 1 FROM files WHERE path = %s', (path,)).fetchone()
        return row is not None

    def __iter__(self) -> Iterator[str]:
        self.flush()
        with self._get_conn().cursor(name='iter_paths') as cur:
            cur.execute('SELECT path FROM files')
            while rows := cur.fetchmany(1000):
                for row in rows:
                    yield row[0]

    def __len__(self) -> int:
        self.flush()
        row = self._get_conn().execute('SELECT COUNT(*) FROM files').fetchone()
        return row[0]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
