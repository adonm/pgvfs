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
INSTALL pgvfs FROM community;
LOAD pgvfs;
```

Signed builds need no `-unsigned` flag. Start with [Install](install.md) for
a runnable DuckLake example, then [Loading data](loading.md): how a lake is
laid out decides most of its read speed.

Source: <https://github.com/adonm/pgvfs> (Apache-2.0). Beta, storage layout v2;
see the [changelog](https://github.com/adonm/pgvfs/blob/main/CHANGELOG.md) and
[layout compatibility policy](how-it-works.md#storage-layout-versions).
