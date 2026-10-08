# How it works

## Storage

A file is a sequence of 8,120-byte rows in one table, `pgvfs.chunks`, keyed by
`(file_id, row number)`. Each row fills one 8 KB page, stored inline without
TOAST. `pgvfs.files` maps `(volume, path)` to the file's `file_id` and size.
Paths are `pgvfs://<volume>/<path>`: a volume (`[a-z0-9][a-z0-9._-]{0,62}`) is
a namespace, so one database can hold several lakes. The layout is
`schema.sql` and `functions.sql`, which writers keep current. It needs no
PostgreSQL extensions; [install](install.md) lists the supported versions.

- **Files are immutable.** A write streams rows under a new `file_id` with
  one binary `COPY`, and publishes the path in the same transaction when the
  file is closed. Overwriting a path publishes a new `file_id`. DuckLake
  itself never rewrites a path.
- **Reads** are primary-key range queries, up to 8 MB each; bigger reads run
  their pieces in parallel on pooled connections. Rows stream straight into
  DuckDB's buffer.
- **Deletes** unpublish the path and queue the old `file_id`. Its rows stay for
  the reap grace (`PGVFS_REAP_GRACE_S`, 10 minutes by default) so queries that
  already opened it can finish, then a writer removes them in the background.

### Storage layout versions

The layout has a version, stored in `pgvfs.layout`; this release reads and
writes **v2**. pgvfs refuses a database with any other version rather than
misread it. Layout changes are listed in the [changelog](https://github.com/adonm/pgvfs/blob/main/CHANGELOG.md)
and, during 0.x, bump the minor version. There is no in-place migration yet: to
move a lake to a new layout, copy its tables with DuckDB into a lake in a new
database (`CREATE TABLE new.t AS FROM old.t`).

## Writers and readers

- **Any process whose role can write may write.** There is no writer lock.
  Each file is written in one transaction, so its rows and its path are
  published together when the file is closed, or not at all. Writers in
  different processes and connections run in parallel.
- **Two writers of one path:** the second to publish fails with "published by
  another writer at the same time; this write was rolled back", and nothing of
  its file is kept. DuckLake never rewrites a path, so a lake does not hit this.
- **Readers need only `SELECT`.** They take no locks and run no DDL, so they
  work with `SELECT`-only roles and on read replicas. Before the schema exists
  they see an empty store.
- **Roles decide who writes.** A `SELECT`-only role's write, drop or reap fails
  with PostgreSQL's permission error. See [roles](#roles).
- **Each process's first write to a database** installs the schema if it is
  missing (for a role that may create it), and replaces the functions when
  their version changed and the role owns them. A transaction-scoped advisory
  lock serialises this, so concurrent first writes do the work once. A standby
  refuses writes.

## Operations

- **Dropping a volume.** From any writer, `SELECT pgvfs_drop_volume('lake-fts');`
  unpublishes all files in exactly that volume and returns their count. It
  invalidates that database's file and search caches; rows are reaped after
  the usual grace. This is destructive: DuckLake's catalog is not removed,
  so do not drop an active lake's data volume.
- **Reaping and grace.** Each writer reaps at most once a minute while it
  writes, and the first time it connects. A try-lock serialises reaps across
  processes, so a reap that finds another running skips. Each reap removes up
  to 65,536 rows (about 512 MB) and the rest waits for the next. A deleted
  file's rows stay readable for `PGVFS_REAP_GRACE_S` seconds (600 by default).
- **Grace bounds reads.** A read still running when its file's rows are reaped
  fails, with a message that names `PGVFS_REAP_GRACE_S`. In a run that paused a
  reader after an overwrite, a reader that resumed at 9 minutes (grace 10) read
  on, and one that resumed at 11 minutes failed on its next batch.
- **Reaping by hand.** If writes stop for a long time, reap from cron or a
  scheduler, as a role that can write: `SELECT pgvfs.reap();`. It uses a grace
  of 10 minutes; pass another, for example `SELECT pgvfs.reap(interval '1 hour');`,
  to match `PGVFS_REAP_GRACE_S`.
- **Vacuum.** Reaping deletes whole files' rows, so new databases set
  `pgvfs.chunks` to autovacuum at 1% dead rows instead of 20%. Databases
  created before this setting existed can apply it themselves:
  `ALTER TABLE pgvfs.chunks SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_threshold = 1000);`
- **Read replicas.** Readers (pgvfs and DuckLake's catalog) work on streaming
  standbys; attach with `READ_ONLY`. Catalog and data share one database, so
  a replica never sees a snapshot that names files it doesn't have yet. When
  the primary reaps, a long query on a standby can be cancelled by a recovery
  conflict: set `hot_standby_feedback = on` on the standby. A writer that
  connects to a standby is refused.
- **Connection loss.** A read that hits a connection which died (a PostgreSQL
  restart, a network drop) retries once on a fresh one. A write that loses its
  connection fails and publishes nothing; the next write connects again.
- **Poolers.** Connect directly, or through a pooler in session mode. Only
  direct connections are tested.

## Storage costs

Measured on PostgreSQL 18, writing, dropping and reaping 256 MB files in a
loop, with four live at a time:

- **WAL on write: about 1x the bytes.** A 256 MB file wrote 262 MB of WAL.
- **WAL on reap: about 1x, once a checkpoint has passed.** PostgreSQL writes a
  page's full image the first time it changes after a checkpoint, and reaping
  changes the pages a write filled. Reaping 256 MB after a checkpoint wrote
  262 MB of WAL; with no checkpoint in between, it wrote 2 MB.
- **Vacuum adds full-page images.** Vacuum writes a full page the first time it
  touches a page after a checkpoint, so its WAL varied from 1 MB to 525 MB per
  cycle. Checkpoints set how often that happens: `max_wal_size` and
  `checkpoint_timeout`. On Aurora, which bills I/O operations, the same writes
  are billed too (not priced here).
- **Peak heap is live data, plus the files dropped within the grace, plus
  space that a long transaction holds.** Freed pages are reused: with the grace
  set to zero, the heap stopped growing after a few cycles, at 1.25x the live
  data. A nonzero grace keeps dropped files' rows until it ends, so the heap
  holds them meanwhile. A transaction that stays open (a long write, or a reader
  holding an old snapshot) stops vacuum from freeing the pages of files dropped
  during it. They stay in the heap afterwards, since plain `VACUUM` does not
  return space to the operating system.

## Caching

- **DuckDB's file cache.** `file_id` is the cache's version tag, so DuckDB
  keeps file bytes in memory across queries and they can never be stale.
  Warm queries don't reach PostgreSQL for data at all.
- **Parquet footers.** Loading pgvfs turns on DuckDB's `parquet_metadata_cache`
  (off by default), so footers aren't re-parsed on every query.
- **File opens.** pgvfs remembers each path's file for 10 s per DuckDB
  database. DuckDB re-opens files to check them (DuckLake's delete files on
  every query), and this answers those checks without a round trip. This
  process's own writes drop the entry at once; another process's rewrite of a
  path shows up within 10 s.

## Credentials

pgvfs finds PostgreSQL credentials the same way DuckDB's `postgres` extension,
and therefore DuckLake's catalog, does. One secret can serve both. For each
path, pgvfs uses the first of these that exists:

1. A `postgres` secret named `pgvfs_<volume>`, which gives that volume its own
   database: `CREATE SECRET pgvfs_lake (TYPE postgres, ...)` for `pgvfs://lake/`.
   DuckDB's postgres secrets keep no `SCOPE`, so the name is what routes a path.
2. The `postgres` secret named by `SET pgvfs_secret = 'name'`. Pair it with
   `ATTACH ... (META_SECRET 'name')`.
3. Otherwise the `PGVFS_URL` environment variable (a URL or `key=value` string).
4. Otherwise the unnamed default `postgres` secret, which DuckLake also uses.

Each database gets its own pool, so one DuckDB process can read and write
volumes in several databases. Secrets keep the password redacted, and no
setting holds one. Connection options pgvfs's client doesn't support
(`passfile`, `sslrootcert`, `service`, RDS IAM) are rejected rather than
ignored.

TLS: a remote server needs `sslmode=require`. Certificates are checked against
the system CAs (on Alpine, install `ca-certificates`) plus
`PGVFS_DB_CA_FILE`. `PGVFS_DB_ALLOW_PLAINTEXT=true` allows plaintext on an
isolated network; local servers may always use it.

## Roles

- **Readers:** `USAGE` on schema `pgvfs` and `SELECT` on its tables.
  `ALTER DEFAULT PRIVILEGES` can grant these before the schema exists.
- **Writers:** the same, plus `INSERT`, `UPDATE` and `DELETE` on its tables and
  `USAGE` on its sequences. The first write also needs `CREATE` on the database
  to install the schema, after which the role that installed it owns the functions
  and replaces them on upgrades. A writer that does not own them uses the ones
  installed. For a writer role that is not the owner:

```sql
GRANT USAGE ON SCHEMA pgvfs TO etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA pgvfs TO etl;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA pgvfs TO etl;
```

pgvfs never shares a database with the pgvs3 S3 gateway: each refuses a
database holding the other's schema.

## Configuration

| Setting | Default | |
| --- | --- | --- |
| `pgvfs_<volume>` postgres secret | none | credentials for one volume's paths |
| `SET pgvfs_secret = 'name'` | the default `postgres` secret | credentials for all other paths |
| `PGVFS_URL` | | credentials, if no secret is named |
| `PGVFS_REAP_GRACE_S` | 600 | seconds a deleted file's rows stay readable |
| `PGVFS_POOL_MAX` | max(8, 2 × DuckDB `threads`) | PostgreSQL connections per DuckDB database |
| `PGVFS_POOL_MIN` | 4 | connections kept open |
| `PGVFS_IO_THREADS` | DuckDB `threads` | I/O threads per DuckDB database |
| `PGVFS_OPEN_CACHE_S` | 10 | seconds to remember a path's file (0 off) |
| `PGVFS_DB_CA_FILE` | | extra CA certificates (PEM) |
| `PGVFS_DB_ALLOW_PLAINTEXT` | false | allow plaintext to a remote server |

`SELECT pgvfs_stats()` returns this process's counters as JSON: opens and
cache hits, reads, bytes, time spent in pgvfs, and range queries sent. They
are cumulative, so diff two samples.
