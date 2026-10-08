# Install

For **DuckDB 1.5.6**, install the signed community extension. No `-unsigned`
flag or `allow_unsigned_extensions` setting is needed:

```sql
INSTALL pgvfs FROM community;
LOAD pgvfs;
```

DuckDB selects the build matching its version and platform:

| Platform | Architectures |
| --- | --- |
| Linux, glibc or musl (Alpine) | x86-64, ARM64 |
| macOS | Intel, Apple Silicon |
| Windows | x86-64 |

WebAssembly and MinGW/rtools are not supported. Storage needs PostgreSQL
**14 or later**; the contract also passes on 11 to 13, which are end-of-life
upstream. No PostgreSQL extensions are required. pgvfs is in beta, on
[storage layout v2](how-it-works.md#storage-layout-versions).

## Python

Install the matching DuckDB package (`python -m pip install duckdb==1.5.6`),
then use a normal connection:

```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL pgvfs FROM community")
con.execute("LOAD pgvfs")
```

Run the same quick-start SQL below with `con.execute(...)`.

## Quick start

The PostgreSQL database must already exist. Use a writer role that can
create schemas and DuckLake catalog tables there; a database owner role
suffices. The first write creates pgvfs's schema automatically; see
[roles](how-it-works.md#roles) for production permissions.

For an optional **throwaway local demo**, start PostgreSQL with Docker:

```sh
docker run --name pgvfs-demo --rm -d -p 127.0.0.1:54329:5432 \
  -e POSTGRES_DB=lake -e POSTGRES_USER=lake -e POSTGRES_PASSWORD=lake postgres:18
```

Wait for PostgreSQL to be ready (`docker exec pgvfs-demo pg_isready -h 127.0.0.1 -U lake -d lake`; without `-h` it can report ready before TCP connections work).
This demo uses local-only credentials and loses its data when stopped.
For your own server, replace the host, port, database, user and password below.

After installing and loading pgvfs, run this once to initialize a new lake:

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

No local data file is needed. To load your own Parquet files instead, follow
[Loading data](loading.md), keeping the options and sort order set before
the first insert. For a remote PostgreSQL server, add `SSLMODE 'require'`
to the secret. Certificates are verified; private CAs and Alpine's
`ca-certificates` package are covered under [Credentials](how-it-works.md#credentials).

## Reconnect or read from another process

Installation is cached, but `LOAD` and the temporary `CREATE SECRET` must
be repeated in each new DuckDB session. Do not recreate the table or run
the initialization again. The data path and lake options are stored in the
catalog, so an existing lake needs no `DATA_PATH` on attach.

After loading the three extensions and creating the secret, readers use:

```sql
ATTACH 'ducklake:postgres:' AS lake (READ_ONLY);
SELECT count(*) FROM lake.events; -- 3
```

The reader role needs `USAGE` on both the DuckLake catalog and pgvfs schemas,
and `SELECT` on their tables. The same attach works on a streaming read
replica. A process whose role can write attaches without `READ_ONLY`; several
such processes may write at once, and [roles](how-it-works.md#roles) say which
roles those are.

## Switch from an unsigned installation

`INSTALL` alone does not replace a cached extension. In a fresh DuckDB
process, switch an older installation from this site's repository to the
signed community build:

```sql
FORCE INSTALL pgvfs FROM community;
LOAD pgvfs;
```

Start DuckDB normally, without `-unsigned`. This changes the installed
extension, not the data in PostgreSQL. Check the [layout compatibility
policy](how-it-works.md#storage-layout-versions) if upgrading an older lake.

To check the installed source:

```sql
SELECT extension_version, installed_from
FROM duckdb_extensions() WHERE extension_name = 'pgvfs';
```

`installed_from` should be `community`. Its version may be displayed as the
source commit rather than a release tag.

## DuckDB 2.0 dev builds

Until community builds are available for 2.0, recent dev versions use the
**unsigned, Linux x86-64 glibc-only** builds at <https://pgvfs.adonm.dev>.
These are published weekly and must match the exact DuckDB build:

```sql
-- only for dev builds: start with duckdb -unsigned
INSTALL pgvfs FROM 'https://pgvfs.adonm.dev';
LOAD pgvfs;
```

In Python, only these unsigned builds need
`duckdb.connect(config={"allow_unsigned_extensions": True})`.

### Available unsigned builds

This table lists this site's builds, including older stable builds, not the
signed community binaries. Prefer `FROM community` for stable DuckDB.

{{#include ../generated/builds.md}}

## Troubleshooting

- **Extension not found:** check your DuckDB version and platform against
  the supported builds above. A 2.0 dev wheel needs its exact matching build.
- **Missing PostgreSQL credentials:** load `postgres` and create its secret
  before attaching the lake. Secrets are temporary unless made persistent.
- **Permission denied on a write:** the role has no write privileges on
  pgvfs's tables. Attach with `READ_ONLY` for a reader, or grant the writer
  privileges as shown in [roles](how-it-works.md#roles).
