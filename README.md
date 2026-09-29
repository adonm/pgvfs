# pgvfs: DuckLake on PostgreSQL, many readers

`pgvfs://` is a DuckDB filesystem that keeps DuckLake's data files as rows in
PostgreSQL. With the DuckLake catalog in the same database, one PostgreSQL
is the whole lake: one secret, one backup, one set of roles. Any number of
DuckDB readers query it directly, with no object store, gateway or HTTP.

```sql
LOAD 'pgvfs.duckdb_extension';            -- allow_unsigned_extensions = true
CREATE SECRET (TYPE postgres, HOST 'db', USER 'lake', PASSWORD '...', DATABASE 'lake');
ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 'pgvfs://lake/');
```

Paths are `pgvfs://<volume>/<path>`. A volume (`[a-z0-9][a-z0-9._-]{0,62}`)
is a namespace, so one database can hold several lakes.

## Model

- **One writer, any number of readers.** The first write (a file create,
  delete or rename) takes a session advisory lock on a dedicated connection.
  A second writer fails at once, and a crashed writer's lock goes with its
  connection. Readers take no locks.
- **Readers need `SELECT` only.** They run no DDL, work on hot standbys, and
  see an empty store until the writer has installed the layout.
- **Files are immutable.** A write streams rows under a fresh `file_id` and
  publishes `(volume, path) → file_id` in the same transaction when the file
  closes. `file_id` is DuckDB's cache version tag, so the external file cache
  never serves stale bytes.
- **Reads** are one primary-key range query per 8 MiB piece. A large read
  fetches its pieces in parallel on pooled connections, streaming rows
  straight into DuckDB's buffer.
- **Deletes** queue the old `file_id`. Its rows outlive the delete by 10
  minutes, so queries that already opened the file can finish it. The writer
  reaps in the background, at most once a minute.

Storage (`schema.sql`) is 8120-byte inline rows (one tuple per 8 KB page, no
TOAST) in 32 hash partitions. It needs **PostgreSQL 11+** and **no
extensions**: no pg_cron, no superuser. The contract tests run on 11, 13, 15,
17 and 18.

## Credentials

pgvfs finds its credentials the same way DuckDB's `postgres` extension does,
and so the same way DuckLake's catalog does. One secret can serve both:

1. The `postgres` secret named by `SET pgvfs_secret = 'name'`. Pair it with
   `ATTACH ... (META_SECRET 'name')`.
2. Otherwise the `PGVFS_URL` environment variable (a URL or `key=value` string).
3. Otherwise the unnamed default `postgres` secret, which is also
   DuckLake's default.

Secrets redact the password, and no setting holds one. Options the client
does not support (`passfile`, `sslrootcert`, `service`, RDS IAM) are rejected
rather than ignored. TLS: remote servers need `sslmode=require` (certificates
are checked against the system CAs, plus `PGVFS_DB_CA_FILE`), and
`PGVFS_DB_ALLOW_PLAINTEXT=true` allows plaintext on an isolated network.

Roles:
- **Readers:** `USAGE` on schema `pgvfs` and `SELECT` on its tables
  (`ALTER DEFAULT PRIVILEGES` can grant these ahead of the schema).
- **The writer:** `CREATE` on the database the first time, to install the
  schema, then ownership of it.

## Performance

**Against an S3 gateway.** Full ClickBench (100M rows), DuckDB 1.5.6, local
kind. The same DuckLake read through DuckDB httpfs and a PostgreSQL-backed S3
gateway ([pgvs3](../pgvs3)) was compared with pgvfs. Fresh DuckDB per query:

| | S3 gateway | pgvfs |
| --- | ---: | ---: |
| Geomean | 430 ms | **371 ms** (0.86×) |
| Warm geomean | 229 ms | 229 ms |

Short queries gain most, because the HTTP hop and HEAD revalidation are gone.
The heaviest string scans remain 2–7% slower. The run is from pgvs3 `abcdb09`,
before the split. It used identical data (checked by whole-table checksum),
the two stacks alternated run by run, and each figure is the median of 5.
Records: [`docs/results/vs-s3-gateway.jsonl`](docs/results/vs-s3-gateway.jsonl).

**Concurrent readers** (`just bench --parts 20 --readers 1,2,4,8,16`). 20M
rows, PostgreSQL 18 and every DuckDB reader on one 16-core machine, each
reader with `cores / readers` threads, all 43 queries per reader from a cold
cache:

| Readers | Queries/s | p50 | p95 | geomean |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 5.5 | 79 ms | 433 ms | 78 ms |
| 2 | 6.0 | 144 ms | 873 ms | 154 ms |
| 4 | 6.6 | 263 ms | 1.8 s | 266 ms |
| 8 | 6.3 | 621 ms | 3.8 s | 520 ms |
| 16 | 6.9 | 1.0 s | 6.5 s | 873 ms |

Throughput holds as readers are added, and latency follows each reader's CPU
share. On one machine the cores are the limit, not the storage layer. Readers
on separate machines scale until PostgreSQL's I/O or CPU saturates.

## Layout

- `schema.sql`: storage layout.
- `src/store.rs`: reads, the `COPY` writer, the writer lease and reaping.
- `src/pg.rs`: the connection pool (most recently used first) and TLS.
- `src/lib.rs`: the C ABI (`extension/src/include/pgvfs.h`).
- `extension/`: the thin C++ DuckDB `FileSystem` adapter. The adapter has
  to be C++ because DuckDB's stable C API can use filesystems but cannot
  register one.
- `bench/`: the concurrent-readers benchmark.
- `scripts/`: runners using disposable PostgreSQL containers.

## Build and test

```sh
just check            # fmt, clippy, unit tests
just ext              # container build -> target/ext/pgvfs.duckdb_extension
just contract         # storage contract on a disposable PostgreSQL (PG_IMAGE=postgres:11..18)
just compat           # the contract on every supported major
just e2e              # contract + DuckDB end-to-end through the built extension
just bench --parts 20 --readers 1,4,16
```

The extension is statically linked against `duckdb_static` of the exact
DuckDB release that loads it (v1.5.6). Like DuckDB's own extensions, it
needs that because Python loads DuckDB with `RTLD_LOCAL`. The link uses the
release's prebuilt static libraries and source headers, both pinned by
SHA-256 in `extension/Containerfile`, so DuckDB is never compiled. To change
DuckDB versions, update the version and both digests together.

Pool sizing: `PGVFS_POOL_MIN` (default 4) and `PGVFS_POOL_MAX` (default 32)
per DuckDB database. `PGVFS_IO_THREADS` defaults to one per core.

Alpha: layout changes require a fresh database, and there are no releases.
