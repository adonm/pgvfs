# How it works

## Storage

A file is a sequence of 8,120-byte rows in one table, `pgvfs.chunks`, keyed by
`(file_id, row number)`. Each row fills one 8 KB page, stored inline without
TOAST. `pgvfs.files` maps `(volume, path)` to the file's `file_id` and size.
Paths are `pgvfs://<volume>/<path>`: a volume (`[a-z0-9][a-z0-9._-]{0,62}`) is
a namespace, so one database can hold several lakes. The layout is
`schema.sql`; it needs PostgreSQL 11+ and no extensions.

- **Files are immutable.** A write streams rows under a new `file_id` with
  one binary `COPY`, and publishes the path in the same transaction when the
  file is closed. Overwriting a path publishes a new `file_id`. DuckLake
  itself never rewrites a path.
- **Reads** are primary-key range queries, up to 8 MB each; bigger reads run
  their pieces in parallel on pooled connections. Rows stream straight into
  DuckDB's buffer.
- **Deletes** unpublish the path and queue the old `file_id`. Its rows stay
  for 10 minutes so queries that already opened it can finish, then the
  writer removes them in the background.

### Storage layout versions

The layout has a version, stored in `pgvfs.layout`; this release reads and
writes **v2**. pgvfs refuses a database with any other version rather than
misread it. Layout changes are listed in the [changelog](https://github.com/adonm/pgvfs/blob/main/CHANGELOG.md)
and, during 0.x, bump the minor version. There is no in-place migration yet: to
move a lake to a new layout, copy its tables with DuckDB into a lake in a new
database (`CREATE TABLE new.t AS FROM old.t`).

## One writer, many readers

- **The writer** is whichever process writes first (creates, deletes or
  renames a file). It takes a PostgreSQL advisory lock on a dedicated
  connection and holds it while connected. A second writer fails at once, and
  a crashed writer's lock is released with its connection. The first write
  to a new database also installs the schema.
- **Readers** take no locks and run no DDL, so they work with `SELECT`-only
  roles and on read replicas. Before the schema exists they see an empty
  store.

## Operations

- **Reaping.** Deleted files' rows are removed by the writer, at most once a
  minute while it writes and when it first connects. If writes stop for a
  long time, reap from cron or a scheduler, as the writer's role:
  `SELECT pgvfs.reap();` (up to 65,536 rows, about 512 MB, per call; files
  deleted in the last 10 minutes are kept for readers that still have them
  open).
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
- **Connection loss.** A read that hits a connection which died (a
  PostgreSQL restart, a network drop) retries once on a fresh one. A write
  that loses its connection fails and publishes nothing, and a writer that
  loses its lock stops writing; reconnect to write again.

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
and therefore DuckLake's catalog, does. One secret can serve both:

1. The `postgres` secret named by `SET pgvfs_secret = 'name'`. Pair it with
   `ATTACH ... (META_SECRET 'name')`.
2. Otherwise the `PGVFS_URL` environment variable (a URL or `key=value` string).
3. Otherwise the unnamed default `postgres` secret, which DuckLake also uses.

Secrets keep the password redacted, and no setting holds one. Connection
options pgvfs's client doesn't support (`passfile`, `sslrootcert`,
`service`, RDS IAM) are rejected rather than ignored.

TLS: a remote server needs `sslmode=require`. Certificates are checked against
the system CAs (on Alpine, install `ca-certificates`) plus
`PGVFS_DB_CA_FILE`. `PGVFS_DB_ALLOW_PLAINTEXT=true` allows
plaintext on an isolated network; local servers may always use it.

## Roles

- **Readers:** `USAGE` on schema `pgvfs` and `SELECT` on its tables.
  `ALTER DEFAULT PRIVILEGES` can grant these before the schema exists.
- **The writer:** `CREATE` on the database the first time, to install the
  schema, then ownership of it.

pgvfs never shares a database with the pgvs3 S3 gateway: each refuses a
database holding the other's schema.

## Configuration

| Setting | Default | |
| --- | --- | --- |
| `SET pgvfs_secret = 'name'` | the default `postgres` secret | credentials to use |
| `PGVFS_URL` | | credentials, if no secret is named |
| `PGVFS_POOL_MAX` | max(8, 2 × DuckDB `threads`) | PostgreSQL connections per DuckDB database |
| `PGVFS_POOL_MIN` | 4 | connections kept open |
| `PGVFS_IO_THREADS` | DuckDB `threads` | I/O threads per DuckDB database |
| `PGVFS_OPEN_CACHE_S` | 10 | seconds to remember a path's file (0 off) |
| `PGVFS_DB_CA_FILE` | | extra CA certificates (PEM) |
| `PGVFS_DB_ALLOW_PLAINTEXT` | false | allow plaintext to a remote server |

`SELECT pgvfs_stats()` returns this process's counters as JSON: opens and
cache hits, reads, bytes, time spent in pgvfs, and range queries sent. They
are cumulative, so diff two samples.
