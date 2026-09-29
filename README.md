# pgvfs: DuckLake on PostgreSQL, many readers

<!-- ANCHOR: intro -->
`pgvfs://` is a DuckDB filesystem that keeps DuckLake's data files as rows in
PostgreSQL. With the DuckLake catalog in the same database, one PostgreSQL
is the whole lake: one secret, one backup, one set of roles. Any number of
DuckDB readers query it directly, with no object store, gateway or HTTP.
<!-- ANCHOR_END: intro -->

## Install
<!-- ANCHOR: install -->

Builds are published weekly for the stable DuckDB release and recent 2.0 dev
builds (linux_amd64). Each is a GitHub pre-release, `duckdb-<version>`, and
<https://pgvfs.adonm.dev> serves them as a DuckDB extension repository, so
DuckDB fetches the build that matches its own version:

```sql
-- start DuckDB with allow_unsigned_extensions = true (CLI: duckdb -unsigned)
INSTALL pgvfs FROM 'https://pgvfs.adonm.dev';
LOAD pgvfs;

-- one secret for the DuckLake catalog and the pgvfs data
CREATE SECRET (TYPE postgres, HOST 'db', USER 'lake', PASSWORD '...', DATABASE 'lake');
ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 'pgvfs://lake/');
-- once per lake (persisted in the catalog): small row groups for low latency
CALL lake.set_option('parquet_row_group_size', 8192);
```

A lookup reads whole row groups, so their size sets its latency. DuckDB's
default of 122,880 rows made a 1,000-row lookup take 41 ms; with 8,192 rows it
took 10 ms (2,048 rows was slower again, at 12.5 ms). DuckLake takes no default
from extensions, so the option has to be set on the lake. Every tool in this
repository sets it.

In Python: `duckdb.connect(config={"allow_unsigned_extensions": "true"})`.
The builds and their matching wheels are listed at
<https://pgvfs.adonm.dev/install.html>. You can also install one straight
from its release:
`INSTALL 'https://github.com/adonm/pgvfs/releases/download/duckdb-v1.5.6/pgvfs.duckdb_extension'`.

Paths are `pgvfs://<volume>/<path>`. A volume (`[a-z0-9][a-z0-9._-]{0,62}`)
is a namespace, so one database can hold several lakes.
<!-- ANCHOR_END: install -->

## Model
<!-- ANCHOR: model -->

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

Storage (`schema.sql`) is one table of 8120-byte inline rows (one tuple per
8 KB page, no TOAST). It needs **PostgreSQL 11+** and **no
extensions**: no pg_cron, no superuser. The contract tests run on 11, 13, 15,
17 and 18.
<!-- ANCHOR_END: model -->

## Credentials
<!-- ANCHOR: credentials -->

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
<!-- ANCHOR_END: credentials -->

## Performance
<!-- ANCHOR: performance -->

**Against an S3 gateway.** Full ClickBench (100M rows), DuckDB 1.5.6, local
kind. The same DuckLake read through DuckDB httpfs and a PostgreSQL-backed S3
gateway ([pgvs3](https://github.com/adonm/pgvs3)) was compared with pgvfs. Fresh DuckDB per query:

| | S3 gateway | pgvfs |
| --- | ---: | ---: |
| Geomean | 430 ms | **371 ms** (0.86×) |
| Warm geomean | 229 ms | 229 ms |

Short queries gain most, because the HTTP hop and HEAD revalidation are gone.
The heaviest string scans remain 2–7% slower. The run is from pgvs3 `abcdb09`,
before the split. It used identical data (checked by whole-table checksum),
the two stacks alternated run by run, and each figure is the median of 5.
Records: [`docs/results/vs-s3-gateway.jsonl`](https://github.com/adonm/pgvfs/blob/main/docs/results/vs-s3-gateway.jsonl).

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
<!-- ANCHOR_END: performance -->

## Layout
<!-- ANCHOR: layout -->

- `schema.sql`: storage layout.
- `src/store.rs`: reads, the `COPY` writer, the writer lease and reaping.
- `src/pg.rs`: the connection pool (deadpool-postgres, most recently used
  first) and TLS.
- `src/lib.rs`: the C ABI (`extension/src/include/pgvfs.h`).
- `extension/`: the thin C++ DuckDB `FileSystem` adapter. The adapter has
  to be C++ because DuckDB's stable C API can use filesystems but cannot
  register one.
- `bench/`: the concurrent-readers benchmark.
- `scripts/`: DuckDB input fetching, runners using disposable PostgreSQL
  containers, and the Pages site builder.
- `docs/`: the mdbook site, built from this README plus generated build and
  benchmark tables.
<!-- ANCHOR_END: layout -->

## Build and test
<!-- ANCHOR: build -->

```sh
just check                   # fmt, clippy, unit tests
just ext                     # stable DuckDB (1.5.6) -> target/ext/release/pgvfs.duckdb_extension
just ext nightly [WHEEL]     # DuckDB 2.0 dev wheel (default: newest on PyPI) -> target/ext/nightly/
just contract                # storage contract on a disposable PostgreSQL (PG_IMAGE=postgres:11..18)
just compat                  # the contract on every supported major
just e2e [release|nightly]   # contract + DuckDB end-to-end through the built extension
just bench --parts 20 --readers 1,4,16
```

A C++ DuckDB extension must be statically linked against `duckdb_static` of
the exact DuckDB build that loads it. Python loads DuckDB with `RTLD_LOCAL`,
so the host's symbols are out of reach, and DuckDB's own extensions are built
the same way. DuckDB is never compiled here. `scripts/duckdb.sh` fetches its
headers and prebuilt static libraries, and the container only links
(`extension/build.sh`, a few seconds):

- **release:** the release's `static-libs-linux-amd64.zip` and source
  tarball, pinned by SHA-256 in `scripts/duckdb.sh`. To change versions,
  update the version and both digests together.
- **nightly:** each 2.0 dev wheel is cut by a manual run of DuckDB's `Main`
  CI on its exact commit (`PRAGMA version`). That run keeps
  `duckdb-static-libs-linux-amd64.tar.gz` for 90 days, and the download is
  checked against GitHub's recorded digest. Fetching it needs a GitHub token
  (`gh auth login` or `GH_TOKEN`). The extension footer carries DuckDB's
  version tag, or the commit id for `-dev` builds.

`target/ext/<target>/DUCKDB_PY` and `DUCKDB_VERSION` record the wheel each
build loads into and its extension-repository directory.

CI (`.github/workflows/`):
- **Each push:** fmt, clippy, unit tests, then the contract on PostgreSQL 18
  and e2e for the stable build.
- **Weekly (Monday), or by hand:**
  - the contract on PostgreSQL 11;
  - e2e for the stable and newest 2.0 dev builds;
  - publishing each build as the pre-release `duckdb-<version>`
    (`scripts/release.sh`, keeping every stable build and the last 4 dev
    builds);
  - a 10M-row, 1–4-reader benchmark appended to the `bench` pre-release's
    `history.jsonl`.
- **Pages** (`pages.yml`: weekly and on docs changes): <https://pgvfs.adonm.dev>
  is assembled from the releases by `scripts/site.py`. It is an mdbook
  (`docs/`) whose pages include this README's sections, plus the
  `INSTALL ... FROM` tree. Preview it with `just site`.

<!-- ANCHOR_END: build -->
