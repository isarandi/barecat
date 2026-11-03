# Proposal: reserve/write/commit API for incremental appends

Status: draft for review (FIXME item 11). No code exists yet.

## Problem

A filesystem layer needs to create a file whose size is unknown up front: reserve
space at the end of a shard, write into it incrementally as the application writes,
and commit the metadata when the file is closed — or walk away and leave no trace.
Barecat has no public way to do that, so `barecat_mount` reaches into internals:

| mount operation | internals it touches today |
|---|---|
| `create` | `sharder.shard_files[-1].seek(0, SEEK_END)` to claim the tail offset |
| `write` | `shard_files[-1].seek(...)` + `write()` at the pending offset; hand-rolled incremental CRC while writes stay sequential; manual rollover via `sharder.start_new_shard_and_transfer_last_file` when the shard limit is hit |
| `release` | CRC backfill scan, then `index.add_file(pending_finfo)` |
| other ops | raw SQL on `index.cursor` |

Three audit findings were downstream of this one gap: the `row_factory` poisoning
(FIXME 5, fixed), `defrag(quick=True)` destroying the in-flight region because it is
invisible to the index (FIXME 7, currently only documented), and the mount's own
buffered-write bug. Fixing them individually leaves the class of bug reachable; a
public reserve/commit API makes it unreachable.

## Relationship to the existing `Sharder.reserve(size)`

Barecat already has a reservation primitive, but for a different shape of problem:
`Sharder.reserve(size)` (used by `create_with_workers`) claims a **fixed-size**
extent — it seeks to the tail, pre-rolls to a new shard if the extent would not
fit, fallocates, and returns `(shard, offset)`. Because the size is known up
front, any number of reservations can be live at once (disjoint extents), which is
what lets worker threads write in parallel while the main thread inserts index
rows.

That primitive cannot serve the mount, whose defining constraint is that the size
is unknown until the application closes the file: there is no extent to fallocate.
The proposal below is the **unknown-size sibling** — a growing, exclusive claim on
the tail, with a commit/abandon lifecycle, internal CRC tracking and mid-write
rollover, none of which the fixed-size path needs.

| | `Sharder.reserve(size)` | proposed `reserve_append()` |
|---|---|---|
| size | known up front | unknown, grows as written |
| concurrency | many at once (disjoint extents) | one (it *is* the tail) |
| lifecycle | none — caller writes, then `add_file` | `commit()` / `abandon()` |
| shard rollover | pre-rolled at reserve time | relocation mid-write |
| CRC | caller's concern | tracked internally |
| defrag awareness | none | live reservation blocks/skips defrag |

Note that the fixed-size path has the item-7 hole too, in principle: a
fallocated-but-uncommitted extent is just as invisible to defrag. It has never
bitten because `create_with_workers` runs in one process and nothing defragments
mid-ingest — but if the reservation registry proposed below lands, registering
fixed-size reservations in it as well would close that hole for both paths (open
question 5).

## Proposed API

```python
res = bc.reserve_append()  # claim the tail of the last shard
res.pwrite(buf, offset)  # positional write within the region
res.pread(buf, offset)  # read back what was written so far
res.truncate(length)  # grow (zero-fill) or shrink (real ftruncate)
res.size  # high-water mark / truncated size
res.shard, res.offset  # current placement (may change on rollover)
res.commit(mode=..., uid=..., gid=..., mtime_ns=...)  # insert the index row
res.abandon()  # forget it; the bytes become a gap
```

Also usable as a context manager: `with bc.reserve_append() as res: ...` commits on
clean exit only if `res.commit(...)` was called; otherwise abandons (explicit commit,
so a half-written file is never committed by accident).

### Semantics

- **The region is the tail.** A reservation claims `[logical_end_of_last_shard, ∞)`
  of the last shard. It grows as it is written; nothing else may append while it is
  live.
- **One live reservation per archive.** A second `reserve_append()` raises. This
  matches the mount's existing `EBUSY` discipline and avoids inventing extent
  bookkeeping for concurrent tail writers. (Open question 1 below.)
- **Rollover is internal.** When the region outgrows `shard_size_limit`, the
  reservation relocates itself using the existing
  `start_new_shard_and_transfer_last_file` and updates `res.shard`/`res.offset`.
  Callers never track placement; the mount's hand-rolled rollover block is deleted.
- **CRC is internal.** The reservation keeps a running CRC32C while writes are
  sequential (`offset == size`); the first out-of-order write voids it, and
  `commit()` backfills with one scan of the region — exactly the mount's current
  logic, moved behind the API.
- **`commit()` = insert-and-done.** Builds the `BarecatFileInfo` from the
  reservation's placement plus the caller's metadata and calls `index.add_file`.
  Whether the row is committed to disk stays governed by the normal transaction
  policy (one big transaction; `Barecat.flush()` when a consumer needs a durability
  point). `commit()` itself does not fsync.
- **`abandon()` = nothing happened.** The bytes past the logical end have no index
  row, which is already the archive's well-defined "gap" state: invisible to reads,
  flagged by nothing, reclaimed by the next defrag.

### Requirements surfaced by reading barecat_mount (2026-07-26)

Reading the mount's pending-file machinery end to end adds three methods the first
draft lacked, plus one contract:

- **`pread(buf, offset)`** — a pending file must be readable while open: FUSE
  serves reads on the creating handle, and the mount's audit item N7 wants other
  descriptors to read it too. Today the mount passes its uncommitted
  `BarecatFileInfo` into `bc.readinto`, which only works by structural accident.
- **`truncate(length)`** — POSIX truncate on the open file. Grow = chunked
  zero-fill (the mount hand-rolls this, audit N12); shrink can be a **real
  `ftruncate`**, because the reservation is the shard tail — which also fixes the
  mount's N5c (shrunk-away tail bytes currently leak as a permanent gap that only
  defrag reclaims). Either direction voids the running CRC (the mount's B2 fix,
  moved inside).
- **`size`** — `getattr` needs the authoritative size of the pending file.
- **Check-before-write contract**: `pwrite` must validate the shard-limit outcome
  *before* writing anything, so a failed write writes nothing (the mount's N2 fix,
  moved inside). On rollover, relocation happens first, then the write.

The division of labor that falls out: the reservation owns **placement, size, CRC
and bytes**; the caller keeps **naming and metadata** (path, mode, uid, gid,
mtime) until `commit()`. The mount's pending-rename and pending-chmod/chown then
stay what they are — edits to its own staging struct — and need nothing from
barecat.

### Interaction with maintenance

`defrag()`, `defrag_quick()` and `truncate_all_to_logical_size()` currently assume
"past the logical end = garbage". With a reservation registry that assumption gets an
exception:

- **Explicit maintenance refuses:** `defrag*()` raises while a reservation is live
  ("commit or abandon it first"). Defragmentation is an explicit, infrequent
  operation; refusing is simpler and safer than relocating a moving target.
- **Auto-defrag skips:** the mount's delete-triggered auto-defrag should skip the
  cycle instead of raising; expose the condition as `bc.has_live_reservation` so the
  caller chooses.

### Crash story

Unchanged from the archive's best-effort policy, and that is the point: a crash with
a live reservation leaves bytes past the logical end and no index row — a gap. The
archive opens cleanly, reads correctly, `verify_integrity()` passes, defrag reclaims
the space. Nothing to repair, nothing silently wrong.

### What this does not cover

- **Rewriting committed files** — `update_file`/`open('r+b')` already exist.
- **Concurrent multi-writer ingest** — `create_with_workers` already has the
  fixed-size `Sharder.reserve()` path; this proposal is specifically for
  unknown-size, single-writer, incremental appends.
- **Per-chunk durability** — `Barecat.flush()` (added with FIXME 4) is the
  durability point; the mount's `fsync` handler maps to it.

## Migration sketch for barecat_mount

| today (audit marker) | after |
|---|---|
| `create_raw`: `shard_files[-1].seek(0, SEEK_END)`, build `pending_finfo` by hand | `self.res = bc.reserve_append()` + a small metadata staging struct |
| `write`: limit pre-check (N2), manual rollover, raw-fd `pwrite` (H2), CRC bookkeeping | `self.res.pwrite(buf, offset)` |
| `read` of pending: uncommitted finfo passed into `bc.readinto` | `self.res.pread(buf, offset)` |
| `truncate` of pending: chunked zero-fill (N12), CRC voiding (B2), shrink leaks tail (N5c) | `self.res.truncate(length)` — shrink actually reclaims |
| `getattr` of pending: `pending_finfo.size` | `self.res.size` |
| `release`: CRC backfill with the seek-discards-stale-buffer subtlety + `index.add_file` | `self.res.commit(mode=..., uid=..., gid=..., mtime_ns=...)` |
| `unlink` of pending: ENOENT (no way to cancel) | `self.res.abandon()` — POSIX unlink-while-open |
| `fsync`: delegates to `Barecat.flush()` (already migrated) | unchanged |
| auto-defrag guard: `if self.pending_finfo is None` (M2 stopgap) | `if not bc.has_live_reservation: bc.defrag(quick=True)` — and defrag refuses regardless |
| raw SQL on `index.cursor` | unrelated to this API; safe since the row_factory fix, should move to public lookups separately |

Of the mount audit's confirmed pending-file bugs, five (H2, B2, N2, N12, N5c) were
defects in the mount's reimplementation of exactly what the reservation would own,
one (M2) becomes structurally impossible instead of guarded, and the audit's own
N15 names this API gap as the root cause: "the real direction is using barecat's
API... worth recording in barecat's FUTURE.md if the API needs extending".

## Open questions

1. **Single vs. multiple live reservations.** Single is proposed. Multiple would
   require reservations to be extents (not "the tail"), gap-allocating later ones —
   machinery with no current consumer.
2. **Where the registry lives.** `Barecat` (simplest, proposed) vs. `Index` (would
   survive a `Barecat`-level reopen, but reservations should not outlive the process
   anyway — they are not crash-durable by design).
3. **Should `commit()` accept a ready `BarecatFileInfo`** instead of metadata
   kwargs? Kwargs proposed, so placement fields can never disagree with where the
   bytes actually are.
4. **Naming.** `reserve_append` avoids colliding with the existing fixed-size
   `Sharder.reserve()`; alternatives: `begin_file`, `create_incremental`.
5. **Should fixed-size `Sharder.reserve()` extents register in the same
   reservation registry?** That would close the (currently unexercised) item-7
   hole for the parallel-ingest path too — defrag would refuse or skip while any
   reservation, fixed or growing, is live. Cost: `create_with_workers` would hold
   many live registrations; the check is only consulted by defrag, so this is
   bookkeeping, not hot-path work.
