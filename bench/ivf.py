#!/usr/bin/env python3
"""IVF vector search in a DuckLake on pgvfs, in plain SQL: a synthetic check.

    ivf.py --ext target/ext/release/pgvfs.duckdb_extension --url postgres://...

Clusters synthetic vectors with k-means (in SQL), stores each row's nearest
centroid as `cluster` and sorts the table by it, so DuckLake skips the row
groups of clusters a query doesn't probe. Then, for queries near the data,
compares the nearest neighbours found by probing the NPROBE nearest centroids
with exact search: recall@10, and the bytes each reads from PostgreSQL with
cold caches. No extension code: DuckDB's list_distance and DuckLake's
pruning do the work.
"""

import argparse
import json
import random
import statistics
import time
from urllib.parse import unquote, urlsplit

import duckdb

DIM = 64


def connect(args):
    u = urlsplit(args.url)
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{args.ext}'")
    for ext in ("postgres", "ducklake"):
        con.execute(f"INSTALL {ext}; LOAD {ext}")
    con.execute(
        f"CREATE SECRET (TYPE postgres, HOST '{u.hostname}', PORT {u.port or 5432}, "
        f"DATABASE '{u.path.lstrip('/')}', USER '{unquote(u.username)}', PASSWORD '{unquote(u.password)}')"
    )
    con.execute(f"ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 'pgvfs://{args.volume}/', METADATA_SCHEMA '{args.volume}')")
    return con


def vec(v):
    # DuckLake has no fixed-size ARRAY type: vectors are FLOAT[] lists.
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]::FLOAT[]"


def mean_vector():
    return "[" + ", ".join(f"avg(v[{j + 1}])" for j in range(DIM)) + f"]::FLOAT[{DIM}]"


def load(con, args):
    random.seed(args.seed)
    con.execute(f"SELECT setseed({args.seed / 1e6})")
    # Blobs of uneven size around random centres, noisy enough to overlap.
    con.execute(
        f"CREATE TEMP TABLE centres AS SELECT b, list_transform(range({DIM}), lambda j: random() * 2 - 1)::FLOAT[{DIM}] AS v "
        f"FROM range({args.blobs}) t(b)"
    )
    con.execute(
        f"""CREATE TEMP TABLE data AS
        SELECT i AS id, list_transform(c.v, lambda x: x + (random() - 0.5) * {args.noise})::FLOAT[{DIM}] AS v
        FROM range({args.rows}) t(i) JOIN centres c ON c.b = hash(i) % {args.blobs}"""
    )
    # k-means on a sample, in SQL.
    t0 = time.time()
    con.execute(f"CREATE TEMP TABLE sample AS SELECT id, v FROM data USING SAMPLE {args.sample} ROWS (reservoir, {args.seed})")
    con.execute(
        f"CREATE TEMP TABLE centroids AS SELECT row_number() OVER () - 1 AS c, v "
        f"FROM (FROM sample USING SAMPLE {args.lists} ROWS (reservoir, {args.seed + 1}))"
    )
    for _ in range(args.iterations):
        con.execute(
            f"""CREATE OR REPLACE TEMP TABLE centroids AS
            SELECT c, {mean_vector()} AS v FROM (
                SELECT s.v, arg_min(c.c, array_distance(s.v, c.v)) AS c
                FROM sample s, centroids c GROUP BY s.id, s.v)
            GROUP BY c"""
        )
    trained = time.time() - t0
    # Every row's nearest centroid; the lake sorted by it.
    t0 = time.time()
    for k, v in [("parquet_row_group_size", args.row_group), ("parquet_compression", "'zstd'"), ("parquet_version", 2)]:
        con.execute(f"CALL lake.set_option('{k}', {v})")
    con.execute("CREATE TABLE lake.centroids AS SELECT c::INTEGER AS c, v::FLOAT[] AS v FROM centroids")
    con.execute(
        f"""CREATE TABLE lake.vecs AS
        SELECT d.id, arg_min(c.c, array_distance(d.v, c.v))::INTEGER AS cluster, d.v::FLOAT[] AS v
        FROM data d, centroids c GROUP BY d.id, d.v ORDER BY cluster"""
    )
    con.execute("ALTER TABLE lake.vecs SET SORTED BY (cluster)")
    loaded = time.time() - t0
    sizes = con.execute("SELECT min(n), median(n), max(n) FROM (SELECT count(*) n FROM lake.vecs GROUP BY cluster)").fetchone()
    print(json.dumps({"rows": args.rows, "dim": DIM, "lists": args.lists, "train_s": round(trained, 1),
                      "assign_load_s": round(loaded, 1), "cluster_rows_min_median_max": sizes}))


def exact(q):
    return f"SELECT id FROM lake.vecs ORDER BY list_distance(v, {vec(q)}), id LIMIT 10"


def ivf(q, nprobe):
    # One statement: the probe list, then the rows of those clusters only.
    return f"""SELECT id FROM lake.vecs
        WHERE cluster IN (SELECT c FROM lake.centroids ORDER BY list_distance(v, {vec(q)}) LIMIT {nprobe})
        ORDER BY list_distance(v, {vec(q)}), id LIMIT 10"""


def ivf_literal(con, q, nprobe):
    # Two statements: the probe list first, then a literal IN list.
    probe = con.execute(f"SELECT list(c ORDER BY list_distance(v, {vec(q)}))[1:{nprobe}] FROM lake.centroids").fetchone()[0]
    return f"""SELECT id FROM lake.vecs WHERE cluster IN ({",".join(map(str, probe))})
        ORDER BY list_distance(v, {vec(q)}), id LIMIT 10"""


def cold_bytes(args, sql_for):
    """Bytes read from PostgreSQL by a new DuckDB running one query."""
    con = connect(args)
    sql = sql_for(con)
    before = json.loads(con.execute("SELECT pgvfs_stats()").fetchone()[0])["read_bytes"]
    t0 = time.time()
    rows = [r[0] for r in con.execute(sql).fetchall()]
    ms = (time.time() - t0) * 1000
    after = json.loads(con.execute("SELECT pgvfs_stats()").fetchone()[0])["read_bytes"]
    con.close()
    return rows, after - before, ms


def run(con, args):
    random.seed(args.seed + 2)
    ids = random.sample(range(args.rows), args.queries)
    queries = []
    for i in ids:
        v = con.execute(f"SELECT v FROM lake.vecs WHERE id = {i}").fetchone()[0]
        queries.append([x + random.uniform(-0.05, 0.05) for x in v])
    truth = [[r[0] for r in con.execute(exact(q)).fetchall()] for q in queries]
    _, full_bytes, full_ms = cold_bytes(args, lambda c: exact(queries[0]))
    warm = []
    for q in queries[:10]:
        t0 = time.time()
        con.execute(exact(q)).fetchall()
        warm.append((time.time() - t0) * 1000)
    print(json.dumps({"exact": {"warm_ms": round(statistics.median(warm), 1), "cold_bytes": full_bytes,
                                "cold_ms": round(full_ms)}}))
    for nprobe in args.nprobe:
        recall = statistics.mean(
            len(set(r[0] for r in con.execute(ivf(q, nprobe)).fetchall()) & set(t)) / 10
            for q, t in zip(queries, truth)
        )
        warm = []
        for q in queries[:10]:
            t0 = time.time()
            con.execute(ivf(q, nprobe)).fetchall()
            warm.append((time.time() - t0) * 1000)
        sample = queries[: args.cold]
        one = [cold_bytes(args, lambda c, q=q: ivf(q, nprobe)) for q in sample]
        lit = [cold_bytes(args, lambda c, q=q: ivf_literal(c, q, nprobe)) for q in sample]
        print(json.dumps({
            "nprobe": nprobe,
            "recall_at_10": round(recall, 3),
            "warm_ms": round(statistics.median(warm), 1),
            "cold_bytes_subquery": round(statistics.mean(b for _, b, _ in one)),
            "cold_bytes_literal_in": round(statistics.mean(b for _, b, _ in lit)),
            "fraction_of_exact_literal_in": round(statistics.mean(b for _, b, _ in lit) / full_bytes, 3),
        }))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ext", required=True)
    p.add_argument("--url", required=True)
    p.add_argument("--volume", default="ivf")
    p.add_argument("--rows", type=int, default=200_000)
    p.add_argument("--blobs", type=int, default=256)
    p.add_argument("--noise", type=float, default=0.8)
    p.add_argument("--lists", type=int, default=64)
    p.add_argument("--sample", type=int, default=20_000)
    p.add_argument("--iterations", type=int, default=8)
    p.add_argument("--row-group", type=int, default=2048)
    p.add_argument("--queries", type=int, default=50)
    p.add_argument("--cold", type=int, default=5)
    p.add_argument("--nprobe", type=int, nargs="+", default=[1, 2, 4, 8])
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    con = connect(args)
    if not con.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'vecs'").fetchone()[0]:
        load(con, args)
    run(con, args)


if __name__ == "__main__":
    main()
