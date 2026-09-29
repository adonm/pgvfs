# Install

pgvfs is published weekly for the stable DuckDB release and recent 2.0 dev
builds, on linux_amd64. <https://pgvfs.adonm.dev> is a DuckDB extension
repository, so DuckDB fetches the build that matches its own version:

```sql
-- start DuckDB with allow_unsigned_extensions = true (CLI: duckdb -unsigned)
INSTALL pgvfs FROM 'https://pgvfs.adonm.dev';
LOAD pgvfs;
```

From Python:

```python
import duckdb
con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute("INSTALL pgvfs FROM 'https://pgvfs.adonm.dev'")
con.execute("LOAD pgvfs")
```

Each build is also a GitHub pre-release, `duckdb-<version>`, which can be
installed directly:

```sql
INSTALL 'https://github.com/adonm/pgvfs/releases/download/duckdb-v1.5.6/pgvfs.duckdb_extension';
```

## Quick start

```sql
-- one secret serves the DuckLake catalog and the pgvfs data
CREATE SECRET (TYPE postgres, HOST 'db', USER 'lake', PASSWORD '...', DATABASE 'lake');
ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 'pgvfs://lake/');

-- once per lake, before the first insert (see Loading data)
CALL lake.set_option('parquet_row_group_size', 8192);
CALL lake.set_option('parquet_compression', 'lz4');
CALL lake.set_option('target_file_size', '64MB');

CREATE TABLE lake.events AS FROM 'events.parquet' LIMIT 0;
ALTER TABLE lake.events SET SORTED BY (site_id, day);
INSERT INTO lake.events FROM 'events.parquet';
```

The first write installs pgvfs's schema in the database (it needs `CREATE`
there once). Readers only need `SELECT`; see
[How it works](how-it-works.md#roles).

## Published builds

{{#include ../generated/builds.md}}
