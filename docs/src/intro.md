# pgvfs

`pgvfs://` is a DuckDB filesystem that stores DuckLake's data files as rows in
PostgreSQL. Put the DuckLake catalog in the same database and one PostgreSQL
is the whole lake:

- **One secret, one backup, one set of roles.** No object store, no S3
  credentials, no gateway.
- **Many readers, one writer.** Readers need only `SELECT`, take no locks and
  can run on read replicas. One process writes at a time.
- **Fast reads.** Files are immutable, so DuckDB caches them without
  revalidating. Reads are primary-key range queries straight into DuckDB's
  buffers.
- **Plain PostgreSQL 11+.** No extensions and no superuser needed.

It suits lakes that fit comfortably in one PostgreSQL (tested to 100M rows,
14 GB) with many concurrent readers doing lookups and dashboard-style
queries: map layers, per-customer analytics, APIs over DuckLake.

```sql
INSTALL pgvfs FROM 'https://pgvfs.adonm.dev';  -- allow_unsigned_extensions
LOAD pgvfs;
CREATE SECRET (TYPE postgres, HOST 'db', USER 'lake', PASSWORD '...', DATABASE 'lake');
ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 'pgvfs://lake/');
```

Start with [Install](install.md), then [Loading data](loading.md): how a lake
is laid out decides most of its read speed.

Source: <https://github.com/adonm/pgvfs> (Apache-2.0). Alpha: layout changes
need a fresh database, and there are no versioned releases yet.
