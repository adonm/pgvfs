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
import tempfile
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


def connect(url=url):
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

# Overwrite replaces the file; a new connection must see the new bytes...
con.execute(f"COPY (SELECT 42 AS i) TO '{root}/p/small.parquet'")
assert one(connect(), f"SELECT i FROM '{root}/p/small.parquet'") == (42,)
# ... and so must the connection that overwrote it (its file cache entry).
assert one(con, f"SELECT i FROM '{root}/p/small.parquet'") == (42,)

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
con.execute("CALL lake.set_option('parquet_compression', 'zstd')")
con.execute("CALL lake.set_option('parquet_version', 2)")
con.execute("CALL lake.set_option('target_file_size', '64MB')")
con.execute("CREATE TABLE lake.t AS SELECT i, i % 7 AS k FROM range(100000) t(i)")
con.execute("INSERT INTO lake.t SELECT i, i % 7 FROM range(100000, 150000) t(i)")
assert one(con, "SELECT count(*), sum(k) FROM lake.t") == (
    150000,
    sum(i % 7 for i in range(150000)),
)

# Files written elsewhere, byte-copied into pgvfs and registered (loading.md).
staged = os.path.join(tempfile.mkdtemp(), "staged.parquet")
con.execute(
    f"COPY (SELECT i, i % 7 AS k FROM range(150000, 160000) t(i)) TO '{staged}' "
    "(FORMAT parquet, COMPRESSION zstd, PARQUET_VERSION V2, ROW_GROUP_SIZE 8192)"
)
copied = f"{root}/lake/main/t/staged.parquet"
con.execute(f"COPY (SELECT content FROM read_blob('{staged}')) TO '{copied}' (FORMAT blob)")
assert one(con, f"SELECT size FROM read_blob('{copied}')") == (os.path.getsize(staged),)
con.execute(f"CALL ducklake_add_data_files('lake', 't', ['{copied}'])")
assert one(con, "SELECT count(*) FROM lake.t WHERE i >= 150000") == (10000,)
con.execute("DELETE FROM lake.t WHERE i >= 150000")
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

# Full-text search: tantivy splits in pgvfs, built by the writer, searched by
# any reader. Outside DATA_PATH, so DuckLake's cleanup leaves them alone.
idx = f"{root}/fts/docs"
con.execute(
    "CREATE TABLE lake.docs AS SELECT * FROM (VALUES "
    "(1, 'The quick brown fox jumps over the lazy dog', 'Mühleisen'), "
    "(2, 'Small cats and big dogs', 'Raasveldt'), "
    "(3, 'A café for cats', NULL)) v(id, body, author)"
)
assert one(con, f"FROM tantivy_create_index('{idx}', 'lake.docs', 'id', ['body', 'author'])") == (3,)


def match(c, query, args=""):
    return [
        r[0]
        for r in c.execute(
            f"SELECT id FROM (SELECT id, tantivy_match_bm25('{idx}', id, {sql_text(query)}{args}) AS score "
            "FROM lake.docs) WHERE score IS NOT NULL ORDER BY id"
        ).fetchall()
    ]


assert match(con, "cat") == [2, 3]  # stemmed
assert match(con, "CAFE") == [3]  # lowercased, accents folded
assert match(con, "the") == []  # a stop word
assert match(con, "muhleisen", ", fields := 'author'") == [1]
assert match(con, "muhleisen", ", fields := 'body'") == []
assert match(con, "cats dogs") == [1, 2, 3]
assert match(con, "cats dogs", ", conjunctive := true") == [2]
# tantivy's query language, best first; top hits joined back to the table.
hits = con.execute(f"SELECT score, doc FROM tantivy_search('{idx}', 'body:fox OR author:raasveldt')").fetchall()
assert sorted(json.loads(d)["_key"] for _, d in hits) == ["1", "2"], hits
assert hits[0][0] >= hits[1][0]
assert one(
    con,
    f"SELECT d.id FROM tantivy_search('{idx}', '\"brown fox\"', '{{\"top_k\": 5}}') s "
    "JOIN lake.docs d ON d.id = (s.doc->>'_key')::INTEGER",
) == (1,)
assert con2.execute(f"SELECT count(*) FROM tantivy_search('{idx}', 'cats')").fetchone() == (2,)
assert match(con2, "cat") == [2, 3]
# Splits are immutable: refresh by dropping and building again.
try:
    con.execute(f"FROM tantivy_create_index('{idx}', 'lake.docs', 'id', ['body'])").fetchall()
    raise AssertionError("rebuilt a split in place")
except duckdb.Error as e:
    assert "already holds" in str(e), e
assert con.execute(f"SELECT tantivy_drop('{idx}')").fetchall() == [(True,)]
assert con.execute(f"SELECT tantivy_drop('{idx}')").fetchall() == [(False,)]
assert one(con, f"SELECT count(*) FROM glob('{idx}/*')") == (0,)
assert one(con, f"FROM tantivy_create_index('{idx}', 'lake.docs', 'id', ['body'], stemmer := 'english')") == (3,)
assert one(connect(), f"SELECT count(*) FROM tantivy_search('{idx}', 'fox')") == (1,)

# Maintenance with DuckLake commits, all SQL: one split per range of
# snapshots, listed in a lake table. A hit counts if its row still exists and
# this split indexed its current version (rowid and snapshot_id are DuckLake's).
splits = f"{root}/fts/splits"
fts_schema = json.dumps([
    {"name": "rowid", "type": "i64", "options": {"stored": True, "indexed": True}},
    {"name": "body", "type": "text", "options": {"indexing": {"record": "position", "tokenizer": "en_stem"}}},
])
con.execute("CREATE TABLE lake.docs_splits (path VARCHAR, snapshot BIGINT, docs BIGINT)")


def split_of(rows, s1, path):
    return f"""SELECT path, {s1} AS snapshot, tantivy_index(path, {sql_text(fts_schema)}, to_json(c)) AS docs
        FROM (SELECT {path} AS path, * FROM ({rows})) c GROUP BY path"""


# The first split indexes the table; later ones, the current version of each
# row changed since (rows deleted since drop out at search time).
CHANGES = """SELECT * FROM lake.table_changes('docs', getvariable('s0') + 1, getvariable('s1'))
    WHERE change_type IN ('insert', 'update_postimage')
    QUALIFY row_number() OVER (PARTITION BY rowid ORDER BY snapshot_id DESC) = 1"""


def index_changes():
    con.execute("SET VARIABLE s0 = (SELECT max(snapshot) FROM lake.docs_splits)")
    con.execute("SET VARIABLE s1 = (SELECT id::BIGINT FROM lake.current_snapshot())")
    first = one(con, "SELECT getvariable('s0') IS NULL")[0]
    rows = "SELECT rowid, snapshot_id, * FROM lake.docs" if first else CHANGES
    con.execute(
        "INSERT INTO lake.docs_splits " + split_of(rows, "getvariable('s1')", f"'{splits}/' || getvariable('s1')")
    )


def search(c, query):
    return sorted(
        r[0]
        for r in c.execute(
            f"""SELECT d.id
            FROM lake.docs_splits s
            CROSS JOIN tantivy_search(s.path, {sql_text(query)}) h
            JOIN lake.docs d ON d.rowid = (h.doc->>'rowid')::BIGINT AND d.snapshot_id <= s.snapshot"""
        ).fetchall()
    )


assert search(con, "fox") == []  # no splits yet
index_changes()
assert search(con, "fox") == [1]
con.execute("INSERT INTO lake.docs VALUES (4, 'Foxes everywhere', NULL)")
con.execute("UPDATE lake.docs SET body = 'no longer' WHERE id = 1")
con.execute("DELETE FROM lake.docs WHERE id = 3")
assert search(con, "fox") == [] and search(con, "cafe") == []  # stale: filtered, not yet indexed
index_changes()
assert search(con, "fox") == [4]
assert search(con, "longer") == [1]
assert search(con2, "fox") == [4]
assert one(con, "SELECT count(*), sum(docs) FROM lake.docs_splits") == (2, 5)
# Compaction: one split for the whole range the others cover replaces them.
con.execute("SET VARIABLE s1 = (SELECT max(snapshot) FROM lake.docs_splits)")
# (The change feed for the range would do too, had this lake not expired
# its early snapshots.)
con.execute(
    "CREATE TEMP TABLE merged AS "
    + split_of(
        "SELECT rowid, snapshot_id, * FROM lake.docs WHERE snapshot_id <= getvariable('s1')",
        "getvariable('s1')",
        f"'{splits}/0-' || getvariable('s1')",
    )
)
con.execute("CREATE TEMP TABLE replaced AS FROM lake.docs_splits")
con.execute("BEGIN")
con.execute("DELETE FROM lake.docs_splits")
con.execute("INSERT INTO lake.docs_splits FROM merged")
con.execute("COMMIT")
assert con.execute("SELECT bool_and(tantivy_drop(path)) FROM replaced").fetchall() == [(True,)]
assert search(con, "fox") == [4] and search(con, "longer") == [1] and search(con, "cats") == [2]
assert one(con, f"SELECT count(*) FROM glob('{splits}/*/*')")[0] > 0
assert one(con, f"SELECT count(DISTINCT parse_dirpath(file)) FROM glob('{splits}/*/*')") == (1,)

# The primitive takes any rows: one split per group, tantivy's own schema.
for sql, msg in [
    (f"FROM tantivy_search('{root}/fts/missing', 'x')", "no tantivy index"),
    (f"FROM tantivy_search('{idx}', 'nosuch:x', '{{\"strict\": true}}')", "nosuch"),
    (f"FROM tantivy_search('{idx}', 'x', '{{\"limit\": 1}}')", "unknown field"),
    (f"SELECT tantivy_index('{root}/fts/x' || id, {sql_text(fts_schema)}, to_json(t)) FROM lake.docs t", "one split per group"),
    (f"SELECT tantivy_index('{root}/fts/x', 'not json', '{{}}')", "schema"),
]:
    try:
        con.execute(sql).fetchall()
        raise AssertionError(f"succeeded: {sql}")
    except duckdb.Error as e:
        assert msg in str(e), (sql, e)
assert one(con, f"SELECT count(*) FROM glob('{root}/fts/x*/*')") == (0,), "failed builds left files"
try:  # only the writer builds
    con2.execute(f"SELECT tantivy_index('{root}/fts/y', {sql_text(fts_schema)}, to_json(t)) FROM lake.docs t")
    raise AssertionError("a second process built an index")
except duckdb.Error as e:
    assert "writer" in str(e), e

stats = json.loads(one(con, "SELECT pgvfs_stats()")[0])
assert stats["reads"] > stats0["reads"] and stats["read_bytes"] > 0, stats
# Readers on a streaming standby: DuckLake attaches read-only, and sees the
# lake once replication catches up (catalog and data share one database, so
# a snapshot never names files the standby lacks).
standby = os.environ.get("PGVFS_TEST_STANDBY_URL")
if standby:
    con3 = connect(urlsplit(standby))
    con3.execute(f"ATTACH 'ducklake:postgres:' AS lake (METADATA_SCHEMA '{schema}', READ_ONLY)")
    want = one(con, "SELECT count(*) FROM lake.t")
    for _ in range(100):
        try:
            if one(con3, "SELECT count(*) FROM lake.t") == want:
                break
        except duckdb.Error:
            pass  # not replicated yet
        time.sleep(0.1)
    assert one(con3, "SELECT count(*) FROM lake.t") == want, "standby never caught up"
    assert one(con3, "SELECT count(*) FROM lake.docs_splits s CROSS JOIN tantivy_search(s.path, 'fox')") == (1,)

print(f"pgvfs e2e ok: volume {vol}, lake files {files} -> {after}")
