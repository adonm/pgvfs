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

from search import check_search

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
with tempfile.TemporaryDirectory() as local_root:
    check_search(con, local_root)

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
con.execute(f"ATTACH 'ducklake:postgres:' AS lake " f"(DATA_PATH '{root}/lake/', METADATA_SCHEMA '{schema}')")
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
assert one(con, "SELECT count(*) FROM lake.t") == (150000 - sum(1 for i in range(150000) if i % 7 == 0),)

# A second process-level connection reads the lake through the cache path.
con2 = connect()
con2.execute(f"ATTACH 'ducklake:postgres:' AS lake " f"(METADATA_SCHEMA '{schema}')")
assert one(con2, "SELECT count(*) FROM lake.t") == one(con, "SELECT count(*) FROM lake.t")

# Tantivy search splits: one immutable file each, through DuckDB's filesystem,
# so pgvfs:// (outside DATA_PATH, which DuckLake's cleanup owns) or anywhere.
check_search(con, f"{root}/advanced-fts")
idx = f"{root}/fts/docs.tantivy"
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
assert (
    one(
        con,
        f"""SELECT d.id FROM tantivy_search('{idx}', '"brown fox"', '{{"top_k": 5}}') s
    JOIN lake.docs d ON d.id = (s.doc->>'_key')::INTEGER""",
    )
    == (1,)
)
assert con2.execute(f"SELECT count(*) FROM tantivy_search('{idx}', 'cats')").fetchone() == (2,)
assert match(con2, "cat") == [2, 3]
# Splits are immutable: refresh by dropping and building again.
try:
    con.execute(f"FROM tantivy_create_index('{idx}', 'lake.docs', 'id', ['body'])").fetchall()
    raise AssertionError("rebuilt a split in place")
except duckdb.Error as e:
    assert "already exists" in str(e), e
assert con.execute(f"SELECT tantivy_drop('{idx}')").fetchall() == [(True,)]
assert con.execute(f"SELECT tantivy_drop('{idx}')").fetchall() == [(False,)]
assert one(con, f"SELECT count(*) FROM glob('{idx}')") == (0,)
assert one(con, f"FROM tantivy_create_index('{idx}', 'lake.docs', 'id', ['body'], stemmer := 'english')") == (3,)
assert one(connect(), f"SELECT count(*) FROM tantivy_search('{idx}', 'fox')") == (1,)

# Any DuckDB filesystem: the same split on local disk.
local = os.path.join(tempfile.mkdtemp(), "docs.tantivy")
assert one(con, f"FROM tantivy_create_index('{local}', 'lake.docs', 'id', ['body'])") == (3,)
assert os.path.getsize(local) > 0
assert one(con, f"SELECT count(*) FROM tantivy_search('{local}', 'cats')") == (2,)
assert one(con, f"SELECT tantivy_drop('{local}')") == (True,) and not os.path.exists(local)

# Maintenance with DuckLake commits, all SQL: a split per range of snapshots,
# listed in a lake table. A hit counts if its row still exists and the split
# indexed its current version (rowid and snapshot_id are DuckLake's).
splits = f"{root}/fts/splits"
fts_schema = json.dumps(
    [
        {"name": "rowid", "type": "i64", "options": {"stored": True, "indexed": True, "fast": True}},
        {"name": "body", "type": "text", "options": {"indexing": {"record": "position", "tokenizer": "en_stem"}}},
    ]
)
con.execute("CREATE TABLE lake.docs_splits (path VARCHAR, snapshot BIGINT, docs BIGINT)")
con.execute("CREATE TABLE lake.docs_fts_dead (rowid BIGINT, snapshot BIGINT)")
con.execute("CREATE TABLE lake.docs_fts_state (snapshot BIGINT)")


def split_of(rows, s1, path):
    return f"""SELECT path, {s1} AS snapshot, tantivy_index(path, {sql_text(fts_schema)}, c) AS docs
        FROM (SELECT {path} AS path, * FROM ({rows})) c GROUP BY path"""


# The first split indexes the table; later ones, the current version of each
# row changed since (rows deleted since drop out at search time).
CHANGES = """SELECT * FROM (
    SELECT * FROM lake.table_changes('docs', getvariable('s0') + 1, getvariable('s1'))
    WHERE change_type IN ('insert', 'update_postimage', 'delete')
    QUALIFY row_number() OVER (PARTITION BY rowid ORDER BY snapshot_id DESC) = 1
) WHERE change_type IN ('insert', 'update_postimage')"""


def index_changes():
    con.execute("BEGIN")
    con.execute("SET VARIABLE s0 = (SELECT snapshot FROM lake.docs_fts_state)")
    con.execute("SET VARIABLE s1 = (SELECT id::BIGINT FROM lake.current_snapshot())")
    first = one(con, "SELECT getvariable('s0') IS NULL")[0]
    if not first:
        con.execute(
            "INSERT INTO lake.docs_fts_dead SELECT DISTINCT rowid, snapshot_id FROM "
            "lake.table_changes('docs', getvariable('s0') + 1, getvariable('s1')) "
            "WHERE change_type IN ('delete', 'update_preimage')"
        )
    rows = "SELECT rowid, snapshot_id, * FROM lake.docs" if first else CHANGES
    path = f"'{splits}/' || getvariable('s1') || '.tantivy'"
    con.execute("INSERT INTO lake.docs_splits " + split_of(rows, "getvariable('s1')", path))
    con.execute("DELETE FROM lake.docs_fts_state")
    con.execute("INSERT INTO lake.docs_fts_state VALUES (getvariable('s1'))")
    con.execute("COMMIT")


def search(c, query):
    return sorted(r[0] for r in c.execute(f"""SELECT d.id
            FROM lake.docs_splits s
            CROSS JOIN tantivy_search(s.path, {sql_text(query)}) h
            JOIN lake.docs d ON d.rowid = (h.doc->>'rowid')::BIGINT AND d.snapshot_id <= s.snapshot""").fetchall())


assert search(con, "fox") == []  # no splits yet
index_changes()
assert search(con, "fox") == [1]
con.execute("INSERT INTO lake.docs VALUES (4, 'Foxes everywhere', NULL)")
con.execute("UPDATE lake.docs SET body = 'no longer' WHERE id = 1")
con.execute("DELETE FROM lake.docs WHERE id = 3")
con.execute("INSERT INTO lake.docs VALUES (5, 'temporary fox', NULL)")
con.execute("DELETE FROM lake.docs WHERE id = 5")  # inserted and deleted inside the same indexed range
assert search(con, "fox") == [] and search(con, "cafe") == []  # stale: filtered, not yet indexed
# Dead versions from the change feed: an invalidation after a split's snapshot
# excludes the old version, but not the same rowid's version in a newer split.
con.execute("SET VARIABLE s0 = (SELECT min(snapshot) FROM lake.docs_splits)")
con.execute("SET VARIABLE s1 = (SELECT id::BIGINT FROM lake.current_snapshot())")
con.execute(
    "CREATE TEMP TABLE fts_dead AS SELECT rowid, snapshot_id FROM "
    "lake.table_changes('docs', getvariable('s0') + 1, getvariable('s1')) "
    "WHERE change_type IN ('delete', 'update_preimage')"
)
dead_ids = "(SELECT list(rowid) FROM fts_dead d WHERE d.snapshot_id > s.snapshot)"
live_options = sql_text('{"exclude_field":"rowid","fast":["rowid"],"top_k":1}')


def live_count(query):
    return one(
        con,
        f"SELECT sum(tantivy_count(s.path, {sql_text(query)}, {live_options}, {dead_ids})) " "FROM lake.docs_splits s",
    )[0]


def live_search(query):
    return sorted(
        r[0]
        for r in con.execute(
            f"SELECT d.id FROM lake.docs_splits s CROSS JOIN "
            f"tantivy_search(s.path, {sql_text(query)}, {live_options}, {dead_ids}) h "
            "JOIN lake.docs d ON d.rowid = (h.doc->>'rowid')::BIGINT"
        ).fetchall()
    )


assert live_count("fox") == 0 and live_count("cafe") == 0
assert live_count("cats") == 1 and live_search("cats") == [2]  # dead hit cannot crowd out top_k
index_changes()
assert search(con, "fox") == [4]
assert search(con, "longer") == [1]
assert live_count("fox") == 1 and live_search("fox") == [4]
assert live_count("longer") == 1 and live_search("longer") == [1]
assert search(con2, "fox") == [4]
assert one(con, "SELECT count(*), sum(docs) FROM lake.docs_splits") == (2, 5)
# Native compaction: prune each source with its own dead versions, then merge
# the live segments. No table rescan or document re-indexing.
con.execute("SET VARIABLE s1 = (SELECT max(snapshot) FROM lake.docs_splits)")
con.execute(
    f"CREATE TEMP TABLE cleaned AS SELECT s.path AS old_path, "
    f"'{splits}/clean/' || s.snapshot || '.tantivy' AS path, "
    f"tantivy_merge([s.path], '{splits}/clean/' || s.snapshot || '.tantivy', "
    f'\'{{"exclude_field":"rowid"}}\', {dead_ids}) AS docs FROM lake.docs_splits s'
)
con.execute(
    f"CREATE TEMP TABLE merged AS SELECT '{splits}/0-' || getvariable('s1') || '.tantivy' AS path, "
    "getvariable('s1') AS snapshot, "
    f"tantivy_merge((SELECT list(path) FROM cleaned), '{splits}/0-' || getvariable('s1') || '.tantivy') AS docs"
)
assert one(con, "SELECT docs FROM merged") == (3,)
con.execute("CREATE TEMP TABLE replaced AS FROM lake.docs_splits")
con.execute("BEGIN")
con.execute("DELETE FROM lake.docs_splits")
con.execute("INSERT INTO lake.docs_splits FROM merged")
con.execute("COMMIT")
assert con.execute("SELECT bool_and(tantivy_drop(path)) FROM replaced").fetchall() == [(True,)]
assert con.execute("SELECT bool_and(tantivy_drop(path)) FROM cleaned").fetchall() == [(True,)]
assert search(con, "fox") == [4] and search(con, "longer") == [1] and search(con, "cats") == [2]
assert live_count("fox") == 1 and live_count("cats") == 1
assert one(con, f"SELECT count(*) FROM glob('{splits}/*')") == (1,)

# A coherent indexed checkpoint, as in search.md: newer lake rows need not be
# indexed yet. Time travel joins the data version that the checkpoint covers.
con.execute("INSERT INTO lake.docs VALUES (99, 'a future fox', NULL)")
con.execute("BEGIN")
con.execute("SET VARIABLE r = (SELECT snapshot FROM lake.docs_fts_state)")
checkpoint = con.execute("""SELECT d.id FROM lake.docs_splits s
    CROSS JOIN tantivy_search(s.path, 'fox',
        '{"top_k":1,"fast":["rowid"],"exclude_field":"rowid"}',
        (SELECT list(rowid) FROM lake.docs_fts_dead x
         WHERE x.snapshot > s.snapshot AND x.snapshot <= getvariable('r'))) h
    JOIN lake.docs d AT (VERSION => getvariable('r')) ON d.rowid = (h.doc->>'rowid')::BIGINT""").fetchall()
assert checkpoint == [(4,)], checkpoint
assert one(con, "SELECT count(*) FROM lake.docs AT (VERSION => getvariable('r'))") == (3,)
con.execute("COMMIT")
con.execute("DELETE FROM lake.docs WHERE id = 99")
previous = one(con, "SELECT snapshot FROM lake.docs_fts_state")[0]
index_changes()  # no surviving postimage: advance the watermark without a split
assert one(con, "SELECT snapshot FROM lake.docs_fts_state")[0] > previous
assert one(con, "SELECT count(*) FROM lake.docs_splits") == (1,)
assert live_count("fox") == 1

for sql, msg in [
    (f"FROM tantivy_search('{root}/fts/missing.tantivy', 'x')", "no tantivy split"),
    (f"""FROM tantivy_search('{idx}', 'nosuch:x', '{{"strict": true}}')""", "nosuch"),
    (f"""FROM tantivy_search('{idx}', 'x', '{{"limit": 1}}')""", "unknown field"),
    (
        f"SELECT tantivy_index('{root}/fts/x' || id || '.tantivy', {sql_text(fts_schema)}, to_json(t)) FROM lake.docs t",
        "one split per group",
    ),
    (f"SELECT tantivy_index('{root}/fts/x.tantivy', 'not json', '{{}}')", "schema"),
]:
    try:
        con.execute(sql).fetchall()
        raise AssertionError(f"succeeded: {sql}")
    except duckdb.Error as e:
        assert msg in str(e), (sql, e)
assert one(con, f"SELECT count(*) FROM glob('{root}/fts/x*')") == (0,), "failed builds left files"
try:  # only the pgvfs writer writes to pgvfs
    con2.execute(
        f"SELECT tantivy_index('{root}/fts/y.tantivy', {sql_text(fts_schema)}, to_json(t)) FROM lake.docs t"
    ).fetchall()
    raise AssertionError("a second process wrote a split")
except duckdb.Error as e:
    assert "writer" in str(e), e
assert one(con, f"SELECT count(*) FROM glob('{root}/fts/y*')") == (0,)

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

# Drop only one volume, through the writer. Cache entries are invalidated at
# once in that database; open readers keep their file IDs for the usual grace.
drop_volume = vol + "-drop"
drop_root = f"pgvfs://{drop_volume}"
drop_index = f"{drop_root}/docs.tantivy"
con.execute(f"COPY (SELECT 1 AS i) TO '{drop_root}/one.parquet'").fetchall()
assert one(con, f"SELECT i FROM '{drop_root}/one.parquet'") == (1,)
assert one(
    con, f"SELECT tantivy_index('{drop_index}', {sql_text(fts_schema)}, " "json_object('rowid', 1, 'body', 'fox'))"
) == (1,)
assert one(con, f"SELECT tantivy_count('{drop_index}', 'fox')") == (1,)
try:
    con2.execute(f"SELECT pgvfs_drop_volume('{drop_volume}')").fetchall()
    raise AssertionError("a reader dropped a volume")
except duckdb.Error as e:
    assert "writer" in str(e), str(e)
assert one(con, "SELECT pgvfs_drop_volume(NULL)") == (None,)
try:
    con.execute("SELECT pgvfs_drop_volume(?)", [drop_volume + "\0suffix"]).fetchall()
    raise AssertionError("a NUL-containing volume name was accepted")
except duckdb.Error as e:
    assert "NUL" in str(e), str(e)
assert one(con, f"SELECT tantivy_count('{drop_index}', 'fox')") == (1,)
assert one(con, f"SELECT pgvfs_drop_volume('{drop_volume}')") == (2,)
assert one(con, f"SELECT pgvfs_drop_volume('{drop_volume}')") == (0,)
assert one(con, f"SELECT count(*) FROM glob('{drop_root}/**')") == (0,)
assert one(con, f"SELECT count(*) FROM glob('{root}/**')")[0] > 0, "other volumes were removed"
try:
    con.execute(f"SELECT tantivy_count('{drop_index}', 'fox')").fetchall()
    raise AssertionError("a dropped volume's split remained cached")
except duckdb.Error as e:
    assert "no tantivy split" in str(e), str(e)

print(f"pgvfs e2e ok: volume {vol}, lake files {files} -> {after}")
