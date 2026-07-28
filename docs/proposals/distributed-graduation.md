# Distributed barecat: graduation plan

Status: decided 2026-07-27 (architecture, crash policy, coordinator placement);
hardening backlog B1-B8 implemented and tested 2026-07-28 (11 tests in
`tests/test_postgres.py`, run against a throwaway PostgreSQL instance;
suite skips where PG binaries are absent). Awaiting commit/tracking approval. Supersedes the planning
notes in `FUTURE.md` § "PostgreSQL Backend for Concurrent Writes".

## Goal

Dataset-creation scripts run as SLURM jobs on compute nodes sharing a network
filesystem. All jobs write into **one** archive; the only thing they need is a
coordination mechanism for space allocation. This replaces the status quo of
per-job mini-barecats plus a later `barecat merge`.

## Inventory at decision time

| where | what | state |
|---|---|---|
| dev branch `src/barecat/distributed/local.py` | per-file reservation server, clients pwrite to shared FS | tested; used by `posepile/util/preproc_for_efficiency2.py` |
| dev `local_chunked.py` | chunk leasing (~100 MB), fire-and-forget metadata | best-tested; used by `posepile/ds/mvhumannet/tar_process.py` |
| dev `local_optimistic.py` | optimistic allocation, defrag collisions later | **no tests** |
| dev `remote.py` | data-through-server TCP (no shared FS needed), unified read/write | tested |
| dev `remote_reader.py`/`remote_writer.py` | older split servers | superseded by `remote.py` |
| dev `ssh.py`, `sftp_server.py` | read access over SSH/SFTP; the backing `maintenance/rsync.py`'s remote imports expect | — |
| working tree `src/barecat/postgres/` | PG trigger-allocated offsets, workers pwrite directly, export to SQLite | untracked; measured at 21M files (~85K inserts/s); **live-verified 2026-07-27** |

## Decisions

1. **The PostgreSQL path graduates** as the cluster-creation mechanism.
   Rationale: on a preemptable cluster the discriminator is failure behavior,
   not throughput. PG inherits crash-safety (WAL), restart recovery, concurrent
   allocation correctness, and loud duplicate rejection (`UNIQUE(path)`);
   the socket servers would need all of that hand-built (the chunked server's
   fire-and-forget metadata queue loses files on a server crash; client leases
   have no timeout/reclaim). The socket-server zoo stays dev-branch
   experimental.
2. **Crash-ordering policy: accept metadata-before-data, verify at export.**
   The writer's flush commits the INSERT before pwriting; a killed worker
   leaves committed rows whose bytes are absent or partial — detectable via
   per-row crc32c. This matches the archive-wide crash policy ("inconsistent
   is acceptable if it is easy to tell"). `export` gains a verification pass
   (see backlog) so the final SQLite archive is guaranteed clean. The
   two-phase and per-batch-fsync alternatives were considered and declined
   (extra round-trips / NFS fsync per batch).
3. **Coordinator placement: a dedicated SLURM job hosts PG**, publishing its
   endpoint to a file on the shared FS (`/shared/<dataset>/.pg_endpoint`);
   workers read it. Caveats acknowledged: coordinator preemption ends the run
   unless pgdata is on shared FS (slower) or the job is requeue-protected.
4. **Read satellites** (`remote.py` read path, `ssh.py`, `sftp_server.py`)
   stay on dev as the eventual backing for rsync's remote syntax — separate
   feature, separate graduation decision.
5. **`preproc_for_efficiency3`'s writer-agnostic API is the integration
   contract**: posepile scripts take a writer (MutableMapping with
   `add_from_file`/`flush`/`close`) and don't care which coordinator backs it.

## Live verification (2026-07-27)

Throwaway PG 18 instance, 4 concurrent writer processes, 1 MB shard limit to
force rollover, deliberate cross-worker duplicate path with
`ignore_duplicates=True`:

- 1,601 files across 4 shards; export to SQLite; `verify_integrity()` green;
  every file byte-exact against a re-derived reference; the duplicate stored
  exactly one writer's content.
- Probe: `ON CONFLICT DO NOTHING` with intra-statement duplicate rows does
  **not** error in PG (first occurrence wins, second skipped) — relevant to
  backlog item B2.
- Probe: `INSERT … VALUES … RETURNING` preserved VALUES order over 500 rows
  in this run — but PostgreSQL does not document this as a guarantee, hence
  backlog item B1.

## Hardening backlog (small fixes, no rearchitecting) — ALL IMPLEMENTED 2026-07-28

- **B1 — stop relying on RETURNING order.** `flush()`'s non-duplicate branch
  zips `RETURNING shard, "offset"` rows with the pending list positionally.
  Undocumented ordering; if it ever differs, data is pwritten to *wrong
  offsets* (cross-file corruption, CRC-detectable but disastrous). Fix:
  `RETURNING path, shard, "offset"` and map by path — the duplicates branch
  already does this.
- **B2 — dedupe within a batch.** With `ignore_duplicates=True`, the same
  path twice in one batch maps to one allocation, and the pwrite loop writes
  *both* payloads to that offset: final bytes are the second's, stored CRC is
  the first's → a self-corrupting entry. Fix: drop later same-path entries at
  flush time (first-wins, matching ON CONFLICT semantics).
- **B3 — normalize and validate paths at `add()`.** Paths currently enter PG
  verbatim, and `export_to_sqlite` builds `BarecatFileInfo` via `__new__`,
  bypassing `normalize_path` — so `a//b`, leading `/`, even escaping `..`
  paths (audit item 2!) flow into the final index unchecked. Fix: call
  `barecat.normalize_path` (which now raises `PathEscapeBarecatError`) in
  `add()`/`add_from_file`.
- **B4 — CLI needs a conninfo.** `python -m barecat.postgres` hardcodes
  `postgresql:///{dbname}`: it can only reach a default-socket local server —
  unusable with the decided SLURM-job coordinator, and (live-demonstrated) it
  silently talks to whatever instance owns the default socket. Fix: accept a
  full conninfo / `--host`/`--port`, or a `.pg_endpoint` file path.
- **B5 — connection count at array scale.** Each writer holds one PG
  connection for the whole job; PG's default `max_connections` is 100, a
  500-task array exhausts it. Fix: connect-per-flush (batches are minutes
  apart; connection cost is irrelevant), or document `max_connections` in the
  coordinator runbook.
- **B6 — export verification pass (decision 2).** After the bulk insert,
  re-read each shard span and compare crc32c (opt-out for the fast path or
  sampling flag); report and quarantine failing rows; also compare each
  shard's physical size against the maximum allocated end, which catches a
  dead worker's unwritten tail batch wholesale.
- **B7 — export overwrite semantics.** `export_to_sqlite` onto an existing
  index appends/conflicts; the HOWTO's "export again" flow needs an
  `overwrite` flag or documented fresh-path requirement.
- **B8 — flush() partial-failure retry.** If a pwrite fails mid-batch after
  the INSERT committed, `_pending` is not cleared; a retry re-INSERTs and
  hits unique violations. Either clear-and-report or make flush idempotent.

Performance note (not backlog): the allocation trigger runs per row and
updates the single `shard_state` row per insert — 1000 dead tuples per batch
on a hot one-row table. Autovacuum copes and 85K inserts/s was measured
anyway; a per-batch span reservation would be the optimization if allocation
ever shows up in profiles.

## Coordinator runbook sketch (SLURM)

```bash
# coordinator.sh (one job, submitted before the array)
initdb -D "$TMPDIR/pgdata" -A trust          # node-local; shared-FS pgdata if
pg_ctl -D "$TMPDIR/pgdata" \                 # the run must survive preemption
    -o "-c listen_addresses='*' -c max_connections=<array size + margin>" start
createdb mydb
python -m barecat.postgres init mydb -s 4G   # after B4: with explicit conninfo
echo "$(hostname):5432" > /shared/dataset/.pg_endpoint
sleep infinity
```

Workers read `.pg_endpoint`, construct conninfo, and use
`BarecatPostgresWriter`. After the array completes: `export` (with B6
verification), stop the coordinator job. Open items: auth (trust within the
cluster vs. password), and per-cluster policy checks (ports, daemon rules).

## Migration steps

1. Land the backlog fixes; track `src/barecat/postgres/` (new-file approval).
2. posepile: pass `BarecatPostgresWriter` into `preproc_for_efficiency3`;
   migrate or retire `preproc_for_efficiency2` (uses dev `BarecatLocalClient`)
   and `mvhumannet/tar_process` (uses dev `BarecatChunkedClient`) so no
   posepile script imports dev-branch barecat modules.
3. dev branch: mark `local_optimistic.py` (untested) and
   `remote_reader.py`/`remote_writer.py` (superseded) as retired; keep
   `local.py`/`local_chunked.py` as experimental alternatives; keep the read
   satellites for the rsync-remote feature.
