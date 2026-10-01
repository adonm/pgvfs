# pgvfs

`pgvfs://` is a DuckDB filesystem that stores DuckLake's data files as rows in
PostgreSQL. Put the DuckLake catalog in the same database and one PostgreSQL
is the whole lake:

- **One secret, one backup, one set of roles.** No object store, no S3
  credentials, no gateway.
- **Many readers, one writer.** Readers need only `SELECT`, take no locks and
  can run on read replicas. One process writes at a time.
- **Millisecond reads.** Files are immutable, so DuckDB caches them without
  revalidating. Reads are primary-key range queries straight into DuckDB's
  buffers: a block read takes about 0.1–0.2 ms, and lookups on a well-laid-out
  lake take single-digit milliseconds (see [Performance](performance.md)).
- **Plain PostgreSQL 11+.** No extensions and no superuser needed, so managed
  services work too; tested on Amazon Aurora PostgreSQL (with password
  authentication; RDS IAM tokens aren't supported).

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

## Related projects

pgvfs keeps the lake inside PostgreSQL and reads it from DuckDB. To go the
other way, querying Parquet or Iceberg files in object storage from
PostgreSQL clients, use one of these instead:

- [Aurora PostgreSQL direct querying](https://aws.amazon.com/blogs/aws/amazon-aurora-postgresql-now-supports-direct-querying-of-apache-iceberg-and-parquet-data-in-your-data-lake/)
  (`aurora_analytics`): DuckDB inside Aurora, reading S3, S3 Tables and Glue
  or Iceberg REST catalogs as foreign tables.
- [pg_duckdb](https://github.com/duckdb/pg_duckdb): DuckDB inside any
  PostgreSQL, reading Parquet, Iceberg and Delta files.
- [pg_lake](https://github.com/Snowflake-Labs/pg_lake): Iceberg tables and
  data lake files in PostgreSQL, using DuckDB to execute queries.

They suit scans and analytics over large lakes, but not millisecond lookups.
In our testing, only pgvfs served lake queries in milliseconds: the others read
from object storage, where each request takes tens of milliseconds. Aurora's
announcement likewise recommends copying lake data into native tables when a
query needs single-digit-millisecond latency.

Source: <https://github.com/adonm/pgvfs> (Apache-2.0). Beta, storage layout v2;
see the [changelog](https://github.com/adonm/pgvfs/blob/main/CHANGELOG.md) and
[layout compatibility policy](how-it-works.md#storage-layout-versions).
