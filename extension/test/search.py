"""Tantivy SQL contract on a small synthetic corpus, on any filesystem.

Standalone (local files): python search.py path/to/pgvfs.duckdb_extension
The PostgreSQL e2e also calls check_search on pgvfs://.
"""

import json
import struct
import sys
import tempfile
from collections import defaultdict

import duckdb


def sql_text(value):
    return "'" + str(value).replace("'", "''") + "'"


def roaring(ids):
    """Portable 32-bit roaring serialization, using small array containers."""
    buckets = defaultdict(list)
    for i in sorted(set(ids)):
        buckets[i >> 16].append(i & 0xFFFF)
    keys = sorted(buckets)
    out = struct.pack("<II", 12346, len(keys))
    for k in keys:
        assert len(buckets[k]) <= 4096
        out += struct.pack("<HH", k, len(buckets[k]) - 1)
    offset = 8 + 8 * len(keys)
    for k in keys:
        out += struct.pack("<I", offset)
        offset += 2 * len(buckets[k])
    for k in keys:
        out += struct.pack(f"<{len(buckets[k])}H", *buckets[k])
    return out


def check_search(con, root):
    def one(sql, *args):
        return con.execute(sql, args).fetchone()

    def fails(sql, message, *args):
        try:
            con.execute(sql, args).fetchall()
            raise AssertionError("succeeded: " + sql)
        except duckdb.Error as e:
            assert message in str(e), (sql, str(e))

    root = root.rstrip("/")
    schema = json.dumps(
        [
            {"name": "id", "type": "i64", "options": {"stored": True, "indexed": True, "fast": True}},
            {"name": "body", "type": "text", "options": {"indexing": {"record": "position", "tokenizer": "en_stem"}}},
            {
                "name": "building",
                "type": "text",
                "options": {"stored": True, "fast": True, "indexing": {"record": "basic", "tokenizer": "raw"}},
            },
        ]
    )
    con.execute("""CREATE OR REPLACE TEMP TABLE fts_documents AS SELECT i AS id,
        CASE i % 3 WHEN 0 THEN 'red roof' WHEN 1 THEN 'blue roof tiles' ELSE 'green garden' END AS body,
        'b' || (i % 5) AS building, i % 2 AS part FROM range(1000) t(i)""").fetchall()
    roof = [i for i in range(1000) if i % 3 != 2]
    red = [i for i in range(1000) if i % 3 == 0]
    paths = [f"{root}/p0.tantivy", f"{root}/p1.tantivy"]
    indexes = "[" + ", ".join(map(sql_text, paths)) + "]"
    exclude = sql_text('{"exclude_field": "id"}')

    # A row as the document, without to_json: one split per group.
    rows = con.execute(
        f"SELECT part, tantivy_index({sql_text(root + '/p')} || part || '.tantivy', ?, t) "
        "FROM fts_documents t GROUP BY part ORDER BY part",
        [schema],
    ).fetchall()
    assert rows == [(0, 500), (1, 500)], rows
    assert one(f"SELECT tantivy_count({indexes}, 'roof')") == (len(roof),)
    assert one(f"SELECT tantivy_count({indexes}, 'red', NULL)") == (len(red),)
    assert one(f"SELECT tantivy_count({indexes}, 'red', NULL, NULL::BLOB)") == (len(red),)
    assert one("SELECT tantivy_count(NULL::VARCHAR, 'red')") == (None,)
    assert one("SELECT tantivy_count([]::VARCHAR[], 'red')") == (0,)
    assert one(f"SELECT tantivy_count({indexes}, 'red', {exclude}, [0, 3, 6])") == (len(red) - 3,)
    dead = roaring([0, 3, 6, 1, 70000])
    assert one(f"SELECT tantivy_count({indexes}, 'red', {exclude}, ?::BLOB)", dead) == (len(red) - 3,)
    # A 64-bit treemap (one bitmap for each high 32-bit key).
    dead64 = struct.pack("<QI", 2, 0) + roaring([0, 3]) + struct.pack("<I", 256) + roaring([0])
    assert one(f"SELECT tantivy_count({indexes}, 'red', {exclude}, ?::BLOB)", dead64) == (len(red) - 2,)
    assert one(f"SELECT tantivy_count({indexes}, 'red', {exclude}, ?::BLOB)", roaring([])) == (len(red),)
    # Paths need no separate API: DuckDB's read_blob uses the same filesystem.
    bitmap = sql_text(f"{root}/dead.roaring")
    con.execute(f"COPY (SELECT ?::BLOB) TO {bitmap} (FORMAT blob)", [dead]).fetchall()
    assert one(f"SELECT tantivy_count({indexes}, 'red', {exclude}, " f"(SELECT content FROM read_blob({bitmap})))") == (
        len(red) - 3,
    )

    # Excluded before top_k; doc comes only from fast fields, with the split path.
    options = '{"exclude_field":"id","top_k":5,"fast":["id","building"]}'
    hits = con.execute(f"FROM tantivy_search({indexes}, 'red', ?, [0, 3, 6, 9, 12])", [options]).fetchall()
    assert len(hits) == 5 and all(json.loads(h[1])["id"] not in (0, 3, 6, 9, 12) for h in hits), hits
    assert all(set(json.loads(h[1])) == {"id", "building"} and h[2] in paths for h in hits), hits
    assert [h[0] for h in hits] == sorted((h[0] for h in hits), reverse=True)
    assert one(f"SELECT count(*) FROM tantivy_search({indexes}, 'red', '{{\"top_k\":0}}')") == (0,)
    assert one(f"SELECT count(*) FROM tantivy_search({indexes}, 'red', " "'{\"top_k\":18446744073709551615}')") == (
        len(red),
    )
    # Best roof per building: grouping and projection stay in SQL.
    best = con.execute(f"""SELECT doc->>'building' AS b, arg_max((doc->>'id')::INT, score)
        FROM tantivy_search({indexes}, 'roof', '{{"fast":["id","building"]}}')
        GROUP BY b ORDER BY b""").fetchall()
    assert [b for b, _ in best] == ["b0", "b1", "b2", "b3", "b4"], best
    con.execute("CREATE OR REPLACE TEMP TABLE fts_paths(path VARCHAR)").fetchall()
    con.executemany("INSERT INTO fts_paths VALUES (?)", [(p,) for p in paths])
    assert one("SELECT count(*) FROM tantivy_search((SELECT list(path) FROM fts_paths), 'roof')") == (len(roof),)
    assert one("SELECT sum(tantivy_count(path, 'roof')) FROM fts_paths") == (len(roof),)

    # Query DSL, including non-scoring filters, works in all collectors.
    dsl = json.dumps({"bool": {"must": {"match": {"body": "roof"}}, "filter": {"term": {"building": "b1"}}}})
    want = sum(1 for i in roof if i % 5 == 1)
    assert one(f"SELECT tantivy_count({indexes}, ?)", dsl) == (want,)
    assert one(f"SELECT count(*) FROM tantivy_search({indexes}, ?)", dsl) == (want,)
    fuzzy = json.dumps({"match": {"body": {"query": "rof", "fuzziness": 1}}})
    assert one(f"SELECT tantivy_count({indexes}, ?)", fuzzy) == (len(roof),)
    aggs = json.dumps({"b": {"terms": {"field": "building", "size": 10}}, "n": {"cardinality": {"field": "id"}}})
    res = json.loads(one(f"SELECT tantivy_aggregate({indexes}, 'roof', ?, NULL)", aggs)[0])
    assert sum(b["doc_count"] for b in res["b"]["buckets"]) == len(roof), res
    assert abs(res["n"]["value"] - len(roof)) / len(roof) < 0.05, res
    res = json.loads(one(f"SELECT tantivy_aggregate({indexes}, ?, ?)", dsl, aggs)[0])
    assert sum(b["doc_count"] for b in res["b"]["buckets"]) == want, res
    res = json.loads(one(f"SELECT tantivy_aggregate({indexes}, 'red', ?, {exclude}, [0, 3])", aggs)[0])
    assert sum(b["doc_count"] for b in res["b"]["buckets"]) == len(red) - 2, res

    # Global BM25 statistics give the scores of one combined index.
    whole = sql_text(f"{root}/all.tantivy")
    assert one(f"SELECT tantivy_index({whole}, ?, t) FROM fts_documents t", schema) == (1000,)
    scores = dict(
        con.execute(f"SELECT doc->>'id', score FROM tantivy_search({whole}, 'red', " '\'{"fast":["id"]}\')').fetchall()
    )
    global_scores = dict(
        con.execute(
            f"SELECT doc->>'id', score FROM tantivy_search({indexes}, 'red', "
            '\'{"fast":["id"],"global_stats":true}\')'
        ).fetchall()
    )
    assert scores.keys() == global_scores.keys()
    assert all(abs(scores[k] - global_scores[k]) < 1e-4 for k in scores)
    merged = sql_text(f"{root}/merged.tantivy")
    assert one(f"SELECT tantivy_merge({indexes}, {merged}, {exclude}, [0, 3, 6])") == (997,)
    assert one(f"SELECT tantivy_count({merged}, 'red')") == (len(red) - 3,)
    fails(f"SELECT tantivy_merge({indexes}, {merged})", "already exists")
    empty = sql_text(f"{root}/empty.tantivy")
    assert one(
        f"SELECT tantivy_merge([{sql_text(paths[0])}], {empty}, {exclude}, "
        "(SELECT list(id) FROM fts_documents WHERE part = 0))"
    ) == (0,)
    assert one(f"SELECT tantivy_count({empty}, '*')") == (0,)

    # Prepared index plans may be reused, and count/aggregate plans must not fold
    # filesystem results into constants. Destruction may be deferred past binding.
    prep = sql_text(f"{root}/prep.tantivy")
    con.execute(f"PREPARE fts_build AS SELECT tantivy_index({prep}, {sql_text(schema)}, t) FROM fts_documents t")
    assert one("EXECUTE fts_build") == (1000,)
    con.execute(f"PREPARE fts_count AS SELECT tantivy_count({prep}, '*')")
    con.execute(f"PREPARE fts_aggs AS SELECT tantivy_aggregate({prep}, '*', {sql_text(aggs)})")
    assert one("EXECUTE fts_count") == (1000,)
    assert one(f"SELECT tantivy_drop({prep})") == (True,)
    assert one(f"SELECT tantivy_index({prep}, ?, t) FROM fts_documents t WHERE id < 2", schema) == (2,)
    assert one("EXECUTE fts_count") == (2,)
    assert sum(b["doc_count"] for b in json.loads(one("EXECUTE fts_aggs")[0])["b"]["buckets"]) == 2
    assert one(f"SELECT tantivy_drop({prep})") == (True,)
    assert one("EXECUTE fts_build") == (1000,)

    bad = sql_text(f"{root}/bad.tantivy")
    con.execute("CREATE OR REPLACE TEMP TABLE fts_input(doc VARCHAR)").fetchall()
    con.execute(
        "INSERT INTO fts_input VALUES (?), (?)", ['{"id":1,"body":"fox"}', '{"id":"bad","body":"cat"}']
    ).fetchall()
    con.execute(f"PREPARE fts_retry AS SELECT tantivy_index({bad}, {sql_text(schema)}, doc) FROM fts_input")
    fails("EXECUTE fts_retry", "id")
    assert one(f"SELECT count(*) FROM glob({bad})") == (0,)
    con.execute("UPDATE fts_input SET doc = ? WHERE doc LIKE ?", ['{"id":2,"body":"cat"}', "%bad%"]).fetchall()
    assert one("EXECUTE fts_retry") == (2,), "failed execution kept a partial document"
    assert one(f"SELECT tantivy_count({bad}, '*')") == (2,)
    for name in ["fts_build", "fts_count", "fts_aggs", "fts_retry"]:
        con.execute(f"DEALLOCATE {name}")

    # One DuckDB database's cached split outlives the connection that opened it.
    cursor = con.cursor()
    assert cursor.execute(f"SELECT tantivy_count({prep}, 'red')").fetchone() == (len(red),)
    cursor.close()
    assert one(f"SELECT tantivy_count({prep}, 'red')") == (len(red),)

    for sql, msg in [
        (f"SELECT tantivy_count({indexes}, 'red', '{{}}', [1])", "exclude_field"),
        (f"SELECT tantivy_count({indexes}, '{{\"nested\":{{}}}}')", "unsupported query type"),
        (f'SELECT tantivy_count({indexes}, \'{{"bool":{{"filters":{{}}}}}}\')', "unsupported bool parameter"),
        (f"SELECT tantivy_count({indexes}, 'red', '{{\"exclude_field\":\"body\"}}', [1])", "fast"),
        (f"SELECT count(*) FROM tantivy_search({indexes}, 'red', '{{\"fast\":[\"absent\"]}}')", "no fast field"),
        (f"SELECT tantivy_aggregate({indexes}, 'red', '{{\"x\":{{\"nope\":{{}}}}}}')", "aggregations"),
        (f"SELECT tantivy_count([{sql_text(root + '/missing.tantivy')}], 'red')", "no tantivy split"),
        (f"SELECT tantivy_count([NULL::VARCHAR], 'red')", "cannot be NULL"),
        (f"SELECT tantivy_merge([]::VARCHAR[], {sql_text(root + '/failed.tantivy')})", "at least one split"),
    ]:
        fails(sql, msg)
    fails(f"SELECT tantivy_count({indexes}, 'red', {exclude}, ?::BLOB)", "trailing bytes", dead + b"x")
    fails(f"SELECT tantivy_count({indexes}, ?)", "NUL", "red\0garden")
    fails("SELECT tantivy_drop(?)", "NUL", paths[0] + "\0suffix")
    assert one(f"SELECT tantivy_count({indexes}, 'red')") == (len(red),)
    assert one(f"SELECT count(*) FROM glob({sql_text(root + '/failed.tantivy')})") == (0,)


if __name__ == "__main__":
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD {sql_text(sys.argv[1])}")
    with tempfile.TemporaryDirectory() as root:
        check_search(con, root)
        # Cached splits still obey DuckDB's access controls.
        con.execute("SET enable_external_access = false")
        for sql in [
            f"SELECT tantivy_count({sql_text(root + '/p0.tantivy')}, 'roof')",
            "SELECT pgvfs_drop_volume('blocked')",
        ]:
            try:
                con.execute(sql).fetchall()
                raise AssertionError("external access was not blocked")
            except duckdb.Error as e:
                assert "disabled" in str(e), str(e)
    print("tantivy SQL tests ok: " + con.execute("SELECT version()").fetchone()[0])
