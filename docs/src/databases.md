# Databases (pgdb://)

`pgdb://` stores a native DuckDB database in PostgreSQL: the same file DuckDB
would write to disk, with ART indexes, primary and foreign keys, `CHECK`
constraints, sequences, views, macros and the WAL. One process writes; any
number of DuckDB processes read it, on the primary or on streaming standbys, and
follow new commits by themselves. Nothing else runs beside PostgreSQL.

```sql
LOAD pgvfs;
CREATE SECRET (TYPE postgres, HOST 'localhost', PORT 54329,
               DATABASE 'lake', USER 'lake', PASSWORD 'lake');

-- the one writer
ATTACH 'pgdb://lake/sales.duckdb' AS sales;
USE sales;
CREATE TABLE customers (id BIGINT PRIMARY KEY, email VARCHAR UNIQUE NOT NULL);
CREATE TABLE orders (
    id          BIGINT PRIMARY KEY,
    customer_id BIGINT REFERENCES customers(id),
    amount      DECIMAL(12, 2) CHECK (amount >= 0));
INSERT INTO customers VALUES (1, 'ada@example.com');
INSERT INTO orders VALUES (10, 1, 42.00);
```

```sql
-- any other process, any number of them
ATTACH 'pgdb://lake/sales.duckdb' AS sales (READ_ONLY);
SELECT c.email, sum(o.amount) FROM sales.orders o JOIN sales.customers c
    ON o.customer_id = c.id GROUP BY ALL;
```

Paths are `pgdb://<volume>/<name>`; credentials and volumes work as for
`pgvfs://` ([credentials](how-it-works.md#credentials)). It is the same
PostgreSQL database that can hold a DuckLake on `pgvfs://` too: a session can
attach both and join across them.

## One writer, many readers

- **One writer per database.** A second read-write `ATTACH` anywhere fails at
  once ("attached for writing by another process", or "in this process"). A
  read-only `ATTACH` in the writer's own process reads published versions, as
  any reader does. The writer holds a
  PostgreSQL advisory lock for as long as the database is attached, and every
  write it makes checks that it still owns the database. A writer that loses
  its connection fails rather than diverging.
- **Every commit is durable in PostgreSQL** before DuckDB reports it: DuckDB's
  fsync publishes everything written since the last one as a new **version**,
  in one statement. A commit costs one round trip to PostgreSQL (about 1.5 ms
  on a local server), so batch small writes as you would on any remote disk.
- **Readers see a version, never a mix.** Attaching resolves the path to the
  newest version (`duckdb_databases()` shows `pgdb://lake/sales.duckdb@42~3`:
  version 42, and a token telling this attach from others in the process), and
  every read is of that version. A version is a set of the writer's fsyncs:
  exactly a state DuckDB's own crash recovery would accept, so a reader replays
  the WAL as it would after a crash.
- **Readers follow new commits.** Every `pgdb_refresh_interval` seconds (10 by
  default, `0` off), each database attached read-only by its plain path is
  attached again at the newest version, in place (`ATTACH OR REPLACE`). A
  running transaction keeps the version it started with; the next one sees the
  new version. So readers lag by at most the interval, and are never
  inconsistent.
- **Pin a version** by attaching `pgdb://lake/sales.duckdb@42`: it is never
  refreshed. Workers of one distributed query can attach the same version.
- **Old versions are reaped** once they have been superseded for the reap grace
  (`PGVFS_REAP_GRACE_S`, 10 minutes by default). A reader of a version older than
  that gets an error telling it to attach again, never wrong bytes. Refreshing
  readers never get there; only a single transaction longer than the grace on a
  superseded version can.
- **A writer that dies** leaves the versions it published. The next writer
  removes anything it wrote but never published, then DuckDB replays the WAL
  as after any crash.

## Performance

Readers cache through DuckDB's external file cache (counted in `memory_limit`,
visible in `duckdb_external_file_cache()`, off with `SET
enable_external_file_cache = false`). A cached range is keyed by the version
that last wrote it, so a refresh keeps every unchanged range cached: moving to a
new version costs the bytes that changed, plus reading the catalog and replaying
the WAL written since the last checkpoint. The external cache does not survive
a restart; PostgreSQL's own buffers are the next tier.

DuckDB batches the reads of adjacent blocks, and pgvfs splits large reads into
8 MB range queries that run in parallel.

`just bench-db` compares a `pgdb://` database with the same database in a local
file: 30M rows (about 1 GB), PostgreSQL 18 in a container on 3 of a 16-core
laptop's cores, 13 single-threaded readers (2 GiB `memory_limit` each) on the
rest, three runs (medians). Cold passes restart PostgreSQL and evict its files
from the OS page cache, and evict the local file.

| | local file | pgdb:// |
| --- | ---: | ---: |
| `CREATE TABLE AS` 30M rows | 6.9 s | 8.6 s (1.24×) |
| Primary key and an index | 7.6 s | 10.6 s (1.40×) |
| Commit (one-row insert) | 0.6 ms | 2.2 ms |
| 13 readers, warm: queries/s | 74.5 | 69.1 (93%) |
| ... point lookup p50 | 1.7 ms | 11.8 ms |
| ... aggregate over the table p50 | 249 ms | 263 ms |
| 13 readers, first pass: queries/s | 71.0 | 51.7 (73%) |
| Cold point lookup | 3.8 ms | 21.6 ms |
| Cold lookup of 300 scattered rows | 82 ms | 512 ms |
| Cold scan of an incompressible column | 118 ms | 375 ms |
| A reader's refresh, then one query of each kind | | 150 ms, 2 MB from PostgreSQL |
| ... without the external file cache | | 433 ms, 337 MB from PostgreSQL |

Where it costs: each block a query misses in memory is a round trip to
PostgreSQL, so scattered lookups on a cold or larger-than-memory working set
are several times a local SSD's; each reader process caches for itself, where
local readers share the OS page cache. Scans and aggregates are close to local.
Writes go to PostgreSQL by binary `COPY` (blocks new to a version) and upserts
(blocks rewritten in one version), on one connection.

Readers hold a block twice while it is hot: in DuckDB's buffer pool and in the
external file cache, which share one eviction queue. Set `memory_limit` per
reader process (several readers on one host each default to 80% of its memory).

Large reads need some PostgreSQL CPU, as for `pgvfs://`
([sizing](performance.md#sizing)). Every block written is new rows plus
PostgreSQL WAL, and superseded versions stay until reaped: a checkpoint that
rewrites X bytes holds about 2X for the reap grace.

## Limits

- One writer per database. A read-only attach in the writer's own process
  reads published versions, like any other reader.
- A superseded version stays readable for the reap grace only: a single
  transaction longer than that on an old version gets an error (attach again).
- Spill files stay local: `pgdb://` holds databases and their WAL files only,
  so a `temp_directory` under it fails.
- No listing or globbing of `pgdb://` paths. To copy a database in or out, use
  `COPY FROM DATABASE`.
- Attaching needs a session-pooled connection to PostgreSQL (the writer's lock
  is a session advisory lock); PgBouncer in transaction mode is unsupported for
  databases. `pgvfs://` is unaffected.
- The `pgdb_refresh_interval` setting is database-wide (`SET` applies to every
  connection of that DuckDB).

## Storage

Five tables beside the `pgvfs://` layout, created by the first database writer
(`dbschema.sql`); the layout version does not change. Reader roles need
`SELECT` on them, writer roles `INSERT`, `UPDATE` and `DELETE` and the
`pgvfs.db_ids` and `pgvfs.file_ids` sequences.

- `pgvfs.dbchunks`: 8,112-byte rows keyed by `(file_id, chunk, version)`,
  one per 8 KB page. A write inserts a new version; nothing is updated in
  place, so readers of older versions are unaffected.
- `pgvfs.dbs`: one row per database: the newest version, the writer epoch
  (fencing) and the reap horizon.
- `pgvfs.dbfiles`: each file's chunk set and size per version (the database
  and its WAL files).
- `pgvfs.dbgens`: one row per version: when, and which chunks it wrote.
  Readers key their cache by it; the reaper walks it.
- `pgvfs.dbfileids`: every file a database has had, for reaping and recovery.

Every read checks the reap horizon in the same statement as its rows, so a
read races no reap.

## Design

A `pgdb://` database is the file DuckDB would write to disk, so DuckDB's own
storage, recovery and caching do almost all the work. pgvfs supplies what a
local disk gives DuckDB, out of PostgreSQL primitives: durable, ordered fsyncs,
one writer, and readers that each see one consistent state.

**Why a version is safe to read.** A version is everything written up to one of
the writer's fsyncs: exactly a state a crash could leave on a local disk, and
DuckDB recovers from every such state, including "checkpointed, WAL not yet
truncated". DuckDB replays the WAL on read-only attach too. Within a version
DuckDB writes new blocks only to free block ids, and frees modified blocks only
after the new header is written, so a reader's blocks stay put until it moves
to a newer version. Per-statement MVCC alone would not do: a reader reads the
header, the writer's checkpoint then reuses those block ids, and block
checksums verify content, not identity.

**The writer.** Attaching read-write takes `pg_try_advisory_lock` on a
dedicated connection (a second writer fails at once), bumps the database's
epoch and, if the last writer died, deletes what it wrote but never published.
Writes go to that connection in ordered batches of about 8 MB: binary `COPY`
for chunks new to the version (about 2.5x an upsert's rate), upserts for chunks
rewritten in it. A chunk written in part stays in memory until it is complete
or the next fsync. Every fsync publishes in one statement: the last chunks,
file sizes, the chunks the version changed, and the new version number, all
conditional on the epoch. A writer whose lock was lost (its connection
dropped, a pooler in between) fails on its next write rather than diverging.

**Readers and paths.** Attaching the plain path resolves it to the newest
version: `pgdb://lake/sales.duckdb@42~3` (DuckDB derives the WAL path from it,
so the WAL is read as of the same version). The token after `~` is unique in the
process: DuckDB refuses a read-write and a read-only attach of the same path,
and only the writer's own attach may read its unpublished files. A path with an
explicit `@<version>` is pinned.

**Refresh** relies on DuckDB behaviour that is public but not a documented
contract, so the end-to-end test asserts it for every DuckDB build: `ATTACH OR REPLACE` swaps an attached
database atomically and is a no-op for the same path (hence versions in the
path); a running transaction keeps the database it first touched, so it
finishes on its version; a replaced database closes when its last user goes;
prepared statements rebind. The refresher is a thread holding only a weak
reference to the DuckDB instance; a swap that fails stops following that
database rather than retrying in a loop.

**Cache keys that never go stale.** Readers read through DuckDB's
`CachingFileSystem` under the key `<file_id>@<version that last wrote this
range>`, found from the change lists in `pgvfs.dbgens`. The bytes under a key
never change, so nothing is invalidated, the cache needs no validation, old
keys age out with ordinary eviction, and the cache counts against
`memory_limit` and shares DuckDB's eviction queue with table data. A miss reads
the key's version, with the same reap check as any read.

**Reaping.** The writer deletes chunk versions superseded for longer than the
reap grace, incrementally from the change lists, in batches. Every read checks
the database's reap horizon in the same statement, so a reader of an expired
version gets an error, never wrong bytes. The grace protects standby readers
too, without the primary knowing about them.

**Chosen against:**

| Alternative | Why not |
| --- | --- |
| Mutable rows in `pgvfs.files` | Breaks the rule that a `file_id` names the same bytes forever, which the `pgvfs://` caches and reads rely on. Hence a separate scheme and tables. |
| A PostgreSQL snapshot per reader (`pg_export_snapshot`) | Holds back vacuum cluster-wide for each attach's lifetime, idle-in-transaction connections per reader, recovery conflicts on standbys. |
| The advisory lock as the only writer guard | Silently released if its connection drops; meaningless behind a transaction pooler. The epoch check makes every publish safe regardless. |
| Reader pins as advisory locks, read by the reaper | Invisible across standbys, 32-bit keys, a pin session per process. The grace plus the per-read horizon check covers primary and standby alike. |
| `LISTEN/NOTIFY` for refresh | `NOTIFY` does not reach standbys; one cheap poll per interval works everywhere. |
| Merging partial chunks in SQL (`overlay()`) | Opaque SQL; merging in the writer is plain code, covered by the model test. |
| Chunks aligned to DuckDB's block size | Block sizes vary (16 to 256 KiB); 8,112 bytes is the largest payload that keeps a row and its version inline in one page. |

## Not yet

- **Strong consistency** (each transaction sees the newest version, at one
  round trip per transaction): readers lag by up to `pgdb_refresh_interval`.
- **Faster commits by `synchronous_commit = off` on the writer**: PostgreSQL
  would lose only a suffix of commits, never reorder them, so every crash state
  is still a valid version; the last commits DuckDB reported may be lost.
- **One log instead of two**: every commit is in DuckDB's WAL and PostgreSQL's.
- **Long-lived pinned versions** beyond the reap grace (would need pins).
- **Prefetching scattered blocks**: a cold index lookup reads its blocks one
  round trip at a time.
