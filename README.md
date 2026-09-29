# pgvfs: DuckLake on PostgreSQL

`pgvfs://` is a DuckDB filesystem that stores DuckLake's data files as rows in
PostgreSQL. Put the DuckLake catalog in the same database and one PostgreSQL
is the whole lake: one secret, one backup, one set of roles. Any number of
DuckDB readers query it directly, with no object store, gateway or HTTP. One
process writes.

Documentation: **<https://pgvfs.adonm.dev>**

## Install

```sql
-- DuckDB 1.5.6 or a recent 2.0 dev build, linux_amd64, started with
-- allow_unsigned_extensions = true (CLI: duckdb -unsigned)
INSTALL pgvfs FROM 'https://pgvfs.adonm.dev';
LOAD pgvfs;
```

## Quick start

```sql
-- one secret serves the DuckLake catalog and the pgvfs data
CREATE SECRET (TYPE postgres, HOST 'db', USER 'lake', PASSWORD '...', DATABASE 'lake');
ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 'pgvfs://lake/');

-- once per lake, before the first insert: the fast layout
CALL lake.set_option('parquet_row_group_size', 8192);
CALL lake.set_option('parquet_compression', 'lz4');
CALL lake.set_option('target_file_size', '64MB');

-- declare the order your queries filter by, then load
CREATE TABLE lake.events AS FROM 'events.parquet' LIMIT 0;
ALTER TABLE lake.events SET SORTED BY (site_id, day);
INSERT INTO lake.events FROM 'events.parquet';

SELECT count(*) FROM lake.events WHERE site_id = 42 AND day = DATE '2026-09-01';
```

Readers need only `SELECT` on the database and take no locks.

## Documentation

- [Install](https://pgvfs.adonm.dev/install.html): builds, versions, Python.
- [Loading data](https://pgvfs.adonm.dev/loading.html): lay out a lake for
  fast reads, and keep it fast.
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
