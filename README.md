# pgvfs: DuckLake on PostgreSQL

`pgvfs://` is a DuckDB filesystem that stores DuckLake's data files as rows in
PostgreSQL. Put the DuckLake catalog in the same database and one PostgreSQL
is the whole lake: one secret, one backup, one set of roles. Any number of
DuckDB readers query it directly, with no object store, gateway or HTTP, so
lookups take milliseconds. One process writes. Works on any PostgreSQL 11+,
including Amazon Aurora.

Documentation: **<https://pgvfs.adonm.dev>**

## Install

```sql
INSTALL pgvfs FROM community;
LOAD pgvfs;
```

Signed builds for DuckDB **1.5.6** on Linux (x86-64 and ARM64, glibc and
musl), macOS (Intel and Apple Silicon), and Windows (x86-64). No `-unsigned`
flag needed. [Install](https://pgvfs.adonm.dev/install.html) covers Python,
updating an older installation, and 2.0 dev builds.

## Quick start

Use an existing PostgreSQL database and a writer role that can create
schemas and catalog tables, or start the
[local demo](https://pgvfs.adonm.dev/install.html#quick-start).
The credentials below are for that demo; replace them for your own server.

```sql
INSTALL ducklake;
INSTALL postgres;
LOAD ducklake;
LOAD postgres;

-- one secret serves the DuckLake catalog and the pgvfs data
CREATE SECRET (
    TYPE postgres, HOST 'localhost', PORT 54329,
    DATABASE 'lake', USER 'lake', PASSWORD 'lake'
);
ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 'pgvfs://lake/');

-- once per lake, before the first insert: the fast layout
CALL lake.set_option('parquet_compression', 'zstd');
CALL lake.set_option('parquet_version', 2);
CALL lake.set_option('parquet_row_group_size', 8192);
CALL lake.set_option('target_file_size', '64MB');

CREATE TABLE lake.events (site_id INTEGER, day DATE, value DOUBLE);
ALTER TABLE lake.events SET SORTED BY (site_id, day);
INSERT INTO lake.events VALUES
    (42, DATE '2026-09-01', 1.0),
    (42, DATE '2026-09-01', 2.0),
    (7,  DATE '2026-09-01', 3.0);

SELECT count(*) AS events FROM lake.events
WHERE site_id = 42 AND day = DATE '2026-09-01'; -- 2
```

Run initialization once. In later sessions, load the extensions, recreate
the secret, and attach the existing lake. Readers use `ATTACH
'ducklake:postgres:' AS lake (READ_ONLY)` with a role that can read the catalog
and pgvfs tables. Only one process writes. For remote PostgreSQL, add
`SSLMODE 'require'` to the secret; see [credentials](https://pgvfs.adonm.dev/how-it-works.html#credentials).

## Documentation

- [Install](https://pgvfs.adonm.dev/install.html): builds, versions, Python.
- [Loading data](https://pgvfs.adonm.dev/loading.html): lay out a lake for
  fast reads, load at scale, and keep it fast.
- [Full-text search](https://pgvfs.adonm.dev/search.html): tantivy indexes
  built and searched from SQL, on pgvfs or any DuckDB filesystem.
- [Vector search](https://pgvfs.adonm.dev/vectors.html): nearest neighbours
  over a clustered lake, in plain SQL.
- [How it works](https://pgvfs.adonm.dev/how-it-works.html): storage, one
  writer and many readers, credentials, configuration.
- [Performance](https://pgvfs.adonm.dev/performance.html): benchmarks and
  sizing.
- [Development](https://pgvfs.adonm.dev/development.html): build, test,
  benchmark, release.

## Development

```sh
mise install          # toolchain: rust, just, uv, python, mdbook
just check            # fmt, clippy, unit tests
just e2e              # build the extension in a container, then contract + end-to-end tests
just bench city       # benchmark: Overture Houston, about a minute
```

Apache-2.0.
