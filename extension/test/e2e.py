"""End-to-end: DuckDB + the pgvfs extension against a real PostgreSQL.

    PGVFS_TEST_URL=postgres://... python e2e.py path/to/pgvfs.duckdb_extension

PGVFS_TEST_URL names the database under test. It becomes
one default postgres secret that serves both pgvfs and the DuckLake catalog
(its own tables in the same database; no s3p schema). Each run uses a fresh
volume, so reruns never collide.
"""

import json
import os
import sys
import time
from urllib.parse import unquote, urlsplit

import duckdb

ext = sys.argv[1]
url = urlsplit(os.environ["PGVFS_TEST_URL"])
os.environ.pop("PGVFS_URL", None)  # exercise the secret, not the env fallback
vol = f"e2e-{os.getpid()}-{int(time.time())}"
root = f"pgvfs://{vol}"


def sql_text(value):
    return "'" + str(value).replace("'", "''") + "'"


def connect():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{ext}'")
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(
        f"CREATE SECRET (TYPE postgres, HOST {sql_text(url.hostname)}, PORT {url.port or 5432}, "
        f"USER {sql_text(unquote(url.username))}, PASSWORD {sql_text(unquote(url.password))}, "
        f"DATABASE {sql_text(url.path.lstrip('/'))})"
    )
    return con


def one(con, sql, *args):
    return con.execute(sql, args).fetchone()


con = connect()
assert one(con, "SELECT current_setting('parquet_metadata_cache')") == (True,), "footer cache not on"
stats0 = json.loads(one(con, "SELECT pgvfs_stats()")[0])

# Parquet round trip, including a multi-row-group file larger than a read piece.
con.execute(
    f"COPY (SELECT i, i * 2 AS j, repeat('x', i % 50) AS s FROM range(2000000) t(i)) "
    f"TO '{root}/p/big.parquet' (ROW_GROUP_SIZE 100000)"
)
con.execute(f"COPY (SELECT 1 AS i) TO '{root}/p/small.parquet'")
assert one(con, f"SELECT count(*), sum(j) FROM '{root}/p/big.parquet'") == (
    2000000,
    2 * sum(range(2000000)),
)
assert one(con, f"SELECT count(*) FROM read_parquet('{root}/p/*.parquet')") == (2000001,)
assert one(con, f"SELECT count(*) FROM glob('{root}/**')") == (2,)
assert one(con, f"SELECT count(*) FROM glob('{root}/q/*')") == (0,)

# Overwrite replaces the file; a new connection must see the new bytes.
con.execute(f"COPY (SELECT 42 AS i) TO '{root}/p/small.parquet'")
assert one(connect(), f"SELECT i FROM '{root}/p/small.parquet'") == (42,)

# Missing files fail cleanly.
try:
    con.execute(f"SELECT * FROM '{root}/p/missing.parquet'")
    raise AssertionError("missing file read succeeded")
except duckdb.IOException:
    pass

# DuckLake with its data on pgvfs.
con.execute("INSTALL ducklake")
schema = "dl_" + vol.replace("-", "_")
con.execute(
    f"ATTACH 'ducklake:postgres:' AS lake "
    f"(DATA_PATH '{root}/lake/', METADATA_SCHEMA '{schema}')"
)
con.execute("CALL lake.set_option('parquet_row_group_size', 8192)")
con.execute("CALL lake.set_option('parquet_compression', 'lz4')")
con.execute("CREATE TABLE lake.t AS SELECT i, i % 7 AS k FROM range(100000) t(i)")
con.execute("INSERT INTO lake.t SELECT i, i % 7 FROM range(100000, 150000) t(i)")
assert one(con, "SELECT count(*), sum(k) FROM lake.t") == (
    150000,
    sum(i % 7 for i in range(150000)),
)
con.execute("DELETE FROM lake.t WHERE k = 0")
assert one(con, "SELECT count(*) FROM lake.t WHERE k = 0") == (0,)
files = one(con, f"SELECT count(*) FROM glob('{root}/lake/**')")[0]
assert files >= 3, files

# Dropped data is removed from pgvfs by DuckLake's cleanup.
con.execute("CREATE TABLE lake.gone AS SELECT range AS i FROM range(1000)")
assert one(con, f"SELECT count(*) FROM glob('{root}/lake/main/gone/*')")[0] == 1
con.execute("DROP TABLE lake.gone")
con.execute("CALL ducklake_expire_snapshots('lake', older_than => now())")
con.execute("CALL ducklake_cleanup_old_files('lake', cleanup_all => true)")
assert one(con, f"SELECT count(*) FROM glob('{root}/lake/main/gone/*')") == (0,)
after = one(con, f"SELECT count(*) FROM glob('{root}/lake/**')")[0]
assert one(con, "SELECT count(*) FROM lake.t") == (
    150000 - sum(1 for i in range(150000) if i % 7 == 0),
)

# A second process-level connection reads the lake through the cache path.
con2 = connect()
con2.execute(
    f"ATTACH 'ducklake:postgres:' AS lake "
    f"(METADATA_SCHEMA '{schema}')"
)
assert one(con2, "SELECT count(*) FROM lake.t") == one(con, "SELECT count(*) FROM lake.t")

stats = json.loads(one(con, "SELECT pgvfs_stats()")[0])
assert stats["reads"] > stats0["reads"] and stats["read_bytes"] > 0, stats
print(f"pgvfs e2e ok: volume {vol}, lake files {files} -> {after}")
