#!/usr/bin/env python3
"""Concurrent DuckLake readers on pgvfs.

One writer loads ClickBench `hits` slices into DuckLake on pgvfs; then, for
each reader count R, R separate processes each open a cold DuckDB instance,
attach the lake through one shared postgres secret, and run every query
once (each starting at a different query, so the mix stays even). Reports
per-query latency percentiles and aggregate throughput per R.

    readers.py --ext pgvfs.duckdb_extension --url postgres://... --parts 10 --readers 1,2,4,8
"""

import argparse
import json
import multiprocessing as mp
import os
import statistics
import subprocess
import time
from urllib.parse import unquote, urlsplit

import duckdb

HITS = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet"
# ClickBench's duckdb/load normalization: the queries expect typed columns.
SELECT = """* REPLACE (
    make_date(EventDate) AS EventDate,
    epoch_ms(EventTime * 1000) AS EventTime,
    epoch_ms(ClientEventTime * 1000) AS ClientEventTime,
    epoch_ms(LocalEventTime * 1000) AS LocalEventTime)"""


def queries() -> list[str]:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "clickbench.sql")
    return [ln.strip() for ln in open(path) if ln.strip() and not ln.startswith("--")]


def text(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def connect(args, threads: int | None = None, view: bool = True) -> duckdb.DuckDBPyConnection:
    u = urlsplit(args.url)
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute("SET enable_progress_bar = false")
    if threads:
        con.execute(f"SET threads = {threads}")
    con.execute(f"SET memory_limit = {text(args.memory_limit)}")
    con.execute(f"LOAD {text(args.ext)}")
    for ext in ("postgres", "ducklake"):
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")
    con.execute(
        f"CREATE SECRET (TYPE postgres, HOST {text(u.hostname)}, PORT {u.port or 5432}, "
        f"USER {text(unquote(u.username))}, PASSWORD {text(unquote(u.password or ''))}, "
        f"DATABASE {text(u.path.lstrip('/'))})"
    )
    con.execute(f"ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 'pgvfs://{args.volume}/')")
    if view:
        con.execute("CREATE VIEW hits AS SELECT * FROM lake.hits")
    return con


def load(args) -> None:
    os.makedirs(args.data, exist_ok=True)
    files = []
    for i in range(args.parts):
        dest = os.path.join(args.data, f"hits_{i}.parquet")
        if not os.path.exists(dest) or os.path.getsize(dest) == 0:
            print(f"fetch hits_{i}.parquet", flush=True)
            subprocess.run(["curl", "-fsSL", "-o", dest, HITS.format(i)], check=True)
        files.append(dest)
    con = connect(args, view=False)
    con.execute(f"CALL lake.set_option('parquet_row_group_size', {args.row_group_size})")
    con.execute("CALL lake.set_option('parquet_compression', 'lz4')")
    t0 = time.perf_counter()
    con.execute("DROP TABLE IF EXISTS lake.hits")
    con.execute(f"CREATE TABLE lake.hits AS SELECT {SELECT} "
                f"FROM read_parquet({files!r}, binary_as_string = true)")
    rows = con.execute("SELECT count(*) FROM lake.hits").fetchone()[0]
    print(f"load: {rows} rows in {time.perf_counter() - t0:.1f}s", flush=True)


def reader(args, offset: int, threads: int, start, out) -> None:
    qs = queries()
    order = [(offset + i) % len(qs) for i in range(len(qs))]
    con = connect(args, threads)
    con.execute("SELECT count(*) FROM glob('pgvfs://pgvfs-warm/*')").fetchall()  # open the pool
    start.wait()
    times = {}
    for n in order:
        t0 = time.perf_counter()
        con.execute(qs[n]).fetchall()
        times[f"Q{n + 1}"] = time.perf_counter() - t0
    out.put(times)


def run(args, readers: int) -> dict:
    ctx = mp.get_context("spawn")
    start, out = ctx.Barrier(readers + 1), ctx.Queue()
    threads = max(1, (os.cpu_count() or 1) // readers)
    procs = [ctx.Process(target=reader, args=(args, i * 43 // readers, threads, start, out))
             for i in range(readers)]
    for p in procs:
        p.start()
    start.wait()
    t0 = time.perf_counter()
    results = [out.get() for _ in procs]
    wall = time.perf_counter() - t0
    for p in procs:
        p.join()
        if p.exitcode:
            raise SystemExit(f"reader exited {p.exitcode}")
    latencies = sorted(t for r in results for t in r.values())
    pct = lambda q: latencies[min(len(latencies) - 1, int(q * len(latencies)))]  # noqa: E731
    return {"readers": readers, "threads_each": threads, "queries": len(latencies),
            "wall_s": round(wall, 2), "qps": round(len(latencies) / wall, 2),
            "p50_ms": round(pct(0.50) * 1000), "p95_ms": round(pct(0.95) * 1000),
            "p99_ms": round(pct(0.99) * 1000),
            "geomean_ms": round(statistics.geometric_mean(latencies) * 1000)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ext", required=True)
    ap.add_argument("--url", required=True, help="PostgreSQL URL (becomes a postgres secret)")
    ap.add_argument("--volume", default="bench")
    ap.add_argument("--parts", type=int, default=10, help="ClickBench 1%% slices (100 = full)")
    ap.add_argument("--readers", default="1,2,4,8")
    ap.add_argument("--row-group-size", type=int, default=8192, help="lake parquet_row_group_size")
    ap.add_argument("--memory-limit", default="4GiB", help="per DuckDB process")
    ap.add_argument("--data", default=".tmp/data")
    ap.add_argument("--reuse", action="store_true", help="skip the load")
    args = ap.parse_args()
    if not args.reuse:
        load(args)
    for r in (int(x) for x in args.readers.split(",")):
        print(json.dumps(run(args, r)), flush=True)


if __name__ == "__main__":
    main()
