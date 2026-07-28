# Parallel Ingestion via PostgreSQL

> **Status: experimental.** The API and CLI may change; the backend has an
> end-to-end test suite but has not yet been through a full production dataset
> build. The resulting archives are standard barecat archives either way — the
> experimental surface is the ingestion workflow, not the output format.

When building a large barecat archive from many machines or Slurm jobs, you can use
PostgreSQL to coordinate shard allocation across parallel workers. Each worker INSERTs
file metadata into PG (a trigger atomically allocates shard/offset), then pwrites data
directly to shared shard files. After all workers finish, export the PG index to SQLite
for a standard barecat archive.

## Prerequisites

```bash
pip install barecat[postgres]
# or: pip install psycopg crc32c
```

You need a running PostgreSQL instance. For a user-local instance (no root):

```bash
initdb -D /var/tmp/pgdata
pg_ctl -D /var/tmp/pgdata -l /var/tmp/pgdata/logfile start
```

## Workflow

### 1. Initialize

Create the database and schema:

```bash
python -m barecat.postgres init mydb -H dbhost -s 4G
```

This creates a PostgreSQL database `mydb` with the barecat schema and a 4 GB shard size
limit. The `init` command is idempotent.

All commands accept the server location as `-H HOST`, `-H HOST:PORT`, or `-H` with the
path of an endpoint file containing `host:port` (as written by a coordinator SLURM job);
alternatively `--conninfo` for a full libpq string, or the `BARECAT_PG` environment
variable. Without any of these, the local default socket is used — beware that this
silently talks to whichever PostgreSQL instance owns that socket.

### 2. Run workers

Each Slurm job (or any parallel process) writes to the same archive path on shared
storage:

```python
from barecat.postgres import BarecatPostgresWriter

conninfo = 'host=dbserver dbname=mydb'
archive_path = '/shared/data/myarchive.barecat'

with BarecatPostgresWriter(conninfo, archive_path) as w:
    w['train/img001.jpg'] = jpeg_bytes
    w.add_from_file('train/img002.jpg', '/data/img002.jpg')
```

Workers batch writes internally (default: 1000 files or 256 MB) and flush
automatically. Multiple workers can safely write to the same shard files
because `pwrite` at non-overlapping offsets is atomic.

### 3. Monitor progress

```bash
python -m barecat.postgres stats mydb -H dbhost
```

### 4. Export to SQLite

After all workers are done:

```bash
python -m barecat.postgres export mydb /shared/data/myarchive.barecat -H dbhost
```

This creates the SQLite index, builds directory entries and tree statistics.
The resulting archive is a standard barecat archive, ready for reading, browsing,
mounting, etc.

Export verifies the shard bytes: by default (`--verify length`) rows extending past
the physical end of their shard are excluded and reported — this catches the
unwritten tail batch of a worker that was killed between committing its metadata and
writing its data. `--verify full` additionally re-reads every byte and checks the
crc32c (a full data scan; use for final production archives). Re-exporting over an
existing index requires `-f/--force` (shard files are never touched).

### 5. Stop PG (if user-local)

```bash
pg_ctl -D /var/tmp/pgdata stop
```

## Continuing an existing archive

To add more files to an existing barecat archive using parallel workers, first import
its index into PostgreSQL:

```bash
python -m barecat.postgres init mydb -s 4G
python -m barecat.postgres import mydb /shared/data/myarchive.barecat
```

Then run workers as in step 2 above. The new files will be appended after the existing
data. When done, export again (to a new path or overwrite the old index).

## How it works

- **Shard allocation**: A PostgreSQL `BEFORE INSERT` trigger locks the `shard_state`
  row, assigns the next available (shard, offset) to the new file, and advances the
  cursor. This serializes allocation while data writes happen in parallel.

- **Data writes**: Workers `pwrite()` file data directly to shard files at the
  allocated offset. Since offsets don't overlap, concurrent pwrites are safe.

- **Import**: When importing an existing archive, rows are inserted with explicit
  shard/offset values. The trigger has a `WHEN (NEW.shard IS NULL)` guard, so it
  skips already-allocated rows.

## Python API

```python
from barecat.postgres import (
    setup_schema,         # Create PG schema (idempotent)
    BarecatPostgresWriter,  # Per-worker writer (MutableMapping)
    export_to_sqlite,     # PG -> SQLite index
    import_from_sqlite,   # SQLite index -> PG
    get_stats,            # {num_files, total_size, num_shards}
)
```

## CLI reference

```
python -m barecat.postgres init DBNAME [-H HOST] [-s SHARD_SIZE]      # Create DB + schema
python -m barecat.postgres import DBNAME ARCHIVE [-H HOST]            # Import existing archive
python -m barecat.postgres export DBNAME ARCHIVE [-H HOST]
    [--verify none|length|full] [-f]                                  # Export to SQLite
python -m barecat.postgres stats DBNAME [-H HOST]                     # Show progress
```
