#!/usr/bin/env python3
"""City-scale DuckLake on pgvfs: Overture buildings and places for central
Houston. READERS processes each run their own fixed batch of random
area-of-interest and attribute queries PASSES times, in lockstep: pass 1 is
cold (new DuckDBs; scripts/city.sh restarts PostgreSQL first), later passes
warm, and a final pass uses new random areas (warm caches, unseen queries).

    city.py download                  Overture -> .tmp/data/houston/ (once, cached)
    city.py load --ext EXT --url URL  into DuckLake on pgvfs
    city.py run  --ext EXT --url URL [--readers N --reader-cpus 3-15 --pg-container C --pg-cpus 0-2]
    city.py profile --ext EXT --url URL   where one warm query of each kind spends its time

--compression (load) overrides the lake's Parquet codec (default lz4);
--arrow fetches results as Arrow tables instead of Python tuples. Loading
pgvfs turns on DuckDB's Parquet footer cache.
"""

import argparse
import json
import multiprocessing as mp
import os
import subprocess
import random
import statistics
import time
from urllib.parse import unquote, urlsplit

import duckdb

RELEASE = "s3://overturemaps-us-west-2/release/2026-09-23.1"
THEMES = {"buildings": "buildings/type=building", "places": "places/type=place"}
XMIN, YMIN, XMAX, YMAX = -95.65, 29.60, -95.15, 29.95  # central Houston
BOX = f"{{'min_x': {XMIN}, 'min_y': {YMIN}, 'max_x': {XMAX}, 'max_y': {YMAX}}}::BOX_2D"
DATA = ".tmp/data/houston"


def connect(args) -> duckdb.DuckDBPyConnection:
    u = urlsplit(args.url)
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute("SET enable_progress_bar = false")
    con.execute(f"LOAD '{args.ext}'")
    for ext in ("postgres", "ducklake", "spatial"):
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")
    con.execute(f"CREATE SECRET (TYPE postgres, HOST '{u.hostname}', PORT {u.port or 5432}, "
                f"USER '{unquote(u.username)}', PASSWORD '{unquote(u.password or '')}', "
                f"DATABASE '{u.path.lstrip('/')}')")
    con.execute("ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 'pgvfs://city/')")
    return con


def within(x1, y1, x2, y2) -> str:
    return f"bbox.xmin <= {x2} AND bbox.xmax >= {x1} AND bbox.ymin <= {y2} AND bbox.ymax >= {y1}"


def download(_args) -> None:
    con = duckdb.connect()
    for ext in ("httpfs", "spatial"):
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")
    con.execute("CREATE SECRET (TYPE s3, PROVIDER config, REGION 'us-west-2')")
    os.makedirs(DATA, exist_ok=True)
    for name, path in THEMES.items():
        dest = f"{DATA}/{name}.parquet"
        if os.path.exists(dest):
            continue
        t0 = time.perf_counter()
        con.execute(f"COPY (SELECT * FROM read_parquet('{RELEASE}/theme={path}/*') "
                    f"WHERE {within(XMIN, YMIN, XMAX, YMAX)}) TO '{dest}.tmp' (FORMAT parquet)")
        con.execute(f"SELECT count(*) FROM read_parquet('{dest}.tmp')")  # a complete file, or fail
        os.rename(f"{dest}.tmp", dest)
        print(f"downloaded {name} in {time.perf_counter() - t0:.0f}s", flush=True)


def load(args) -> None:
    con = connect(args)
    con.execute("CALL lake.set_option('parquet_row_group_size', 8192)")
    con.execute("CALL lake.set_option('parquet_compression', 'lz4')")
    if args.compression:
        con.execute(f"CALL lake.set_option('parquet_compression', '{args.compression}')")
    for name in THEMES:
        t0 = time.perf_counter()
        con.execute(f"DROP TABLE IF EXISTS lake.{name}")
        # Hilbert order: an area query touches few row groups.
        con.execute(f"CREATE TABLE lake.{name} AS SELECT * FROM '{DATA}/{name}.parquet' "
                    f"ORDER BY ST_Hilbert(geometry, {BOX})")
        rows = con.execute(f"SELECT count(*) FROM lake.{name}").fetchone()[0]
        print(json.dumps({"table": name, "rows": rows, "load_s": round(time.perf_counter() - t0, 1)}), flush=True)


def batch(con, n: int, seed: int) -> list[str]:
    rnd = random.Random(seed)
    categories = [r[0] for r in con.execute(
        "SELECT taxonomy.primary FROM lake.places WHERE taxonomy.primary IS NOT NULL "
        "GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 20").fetchall()]

    def window(lo, hi):
        side = rnd.uniform(lo, hi)
        x, y = rnd.uniform(XMIN, XMAX - side), rnd.uniform(YMIN, YMAX - side)
        return within(x, y, x + side, y + side)

    templates = [
        # area of interest (~0.5-2 km): the geometries in view
        lambda: f"SELECT id, class, height, ST_AsWKB(geometry) FROM lake.buildings WHERE {window(.005, .02)}",
        lambda: ("SELECT id, names.primary, taxonomy.primary, confidence, ST_AsWKB(geometry) "
                 f"FROM lake.places WHERE {window(.005, .02)}"),
        # attribute filters, over a district or the whole city
        lambda: ("SELECT id, names.primary, height, num_floors, ST_AsWKB(geometry) FROM lake.buildings "
                 f"WHERE height > {rnd.choice([20, 40, 80])} AND {window(.05, .1)}"),
        lambda: ("SELECT id, names.primary, confidence, ST_AsWKB(geometry) FROM lake.places "
                 f"WHERE taxonomy.primary = '{rnd.choice(categories)}' AND confidence > 0.7"),
        # a summary of an area
        lambda: ("SELECT class, count(*), avg(height) FROM lake.buildings "
                 f"WHERE {window(.02, .05)} GROUP BY class ORDER BY 2 DESC"),
    ]
    return [templates[i % len(templates)]() for i in range(n)]


def fetch(con, sql: str, arrow: bool) -> int:
    if arrow:
        return con.execute(sql).fetch_arrow_table().num_rows
    return len(con.execute(sql).fetchall())


KINDS = ["buildings in view", "places in view", "tall buildings", "places by category", "area summary"]


def profile(args) -> None:
    con = connect(args)
    path = con.execute("SELECT data_file FROM ducklake_list_files('lake', 'buildings') LIMIT 1").fetchone()[0]
    codecs = con.execute(f"SELECT DISTINCT compression FROM parquet_metadata('{path}')").fetchall()
    queries = batch(con, args.queries, args.seed)
    for _ in range(2):  # warm the caches
        for sql in queries:
            fetch(con, sql, False)
    out = f"/tmp/pgvfs-profile-{os.getpid()}.json"
    for kind, sql in zip(KINDS, queries):
        timing = {}
        for mode in ("fetchall", "arrow"):
            samples = []
            for _ in range(5):
                t0 = time.perf_counter()
                fetch(con, sql, mode == "arrow")
                samples.append((time.perf_counter() - t0) * 1000)
            timing[mode] = round(statistics.median(samples), 1)
        con.execute(f"SET enable_profiling = 'json'; SET profiling_output = '{out}'")
        rows = fetch(con, sql, True)
        con.execute("SET enable_profiling = 'no_output'")
        tree = json.load(open(out))
        ops = {}

        def walk(node):
            name = node.get("operator_name") or node.get("operator_type") or "?"
            ops[name] = ops.get(name, 0) + node.get("operator_timing", 0) * 1000
            for child in node.get("children", []):
                walk(child)
        for child in tree.get("children", []):
            walk(child)
        latency = tree.get("latency", 0) * 1000
        print(json.dumps({
            "kind": kind, "rows": rows, "codec": [c[0] for c in codecs],
            "fetchall_ms": timing["fetchall"], "arrow_ms": timing["arrow"],
            "profiled_ms": round(latency, 1),
            # time outside operators: binding and planning, incl. DuckLake's catalog queries
            "plan_ms": round(latency - sum(ops.values()), 1),
            "operators_ms": {k: round(v, 1) for k, v in sorted(ops.items(), key=lambda kv: -kv[1]) if v >= 0.1},
        }), flush=True)


def cpus(spec: str | None) -> list[int]:
    out = []
    for part in (spec or "").split(","):
        if part:
            lo, _, hi = part.partition("-")
            out += range(int(lo), int(hi or lo) + 1)
    return out


def pg_cpu_s(container: str | None) -> float:
    if not container:
        return 0.0
    stat = subprocess.run(["docker", "exec", container, "cat", "/sys/fs/cgroup/cpu.stat"],
                          capture_output=True, text=True, check=True).stdout
    return int(stat.split()[1]) / 1e6  # usage_usec


def reader(args, i: int, threads: int, sync, out) -> None:
    try:
        read(args, i, threads, sync, out)
    except BaseException:
        sync.abort()  # fail the run now, not at the barrier timeout
        raise


def read(args, i: int, threads: int, sync, out) -> None:
    if args.reader_cpus:
        os.sched_setaffinity(0, cpus(args.reader_cpus))
    con = connect(args)
    con.execute(f"SET threads = {threads}")
    seed = args.seed * 1000 + i
    queries = batch(con, args.queries, seed)
    fresh = batch(con, args.queries, seed + 500)  # the new-areas pass
    passes = []
    for p in range(args.passes + 1):
        sync.wait()
        times, rows = [], 0
        for sql in queries if p < args.passes else fresh:
            t0 = time.perf_counter()
            rows += fetch(con, sql, args.arrow)
            times.append((time.perf_counter() - t0) * 1000)
        passes.append((times, rows))
    sync.wait()
    out.put(passes)


def run(args) -> None:
    reader_cores = len(cpus(args.reader_cpus)) or os.cpu_count()
    threads = max(1, reader_cores // args.readers)
    os.environ.update(PGVFS_IO_THREADS=str(threads), PGVFS_POOL_MIN="1",
                      PGVFS_POOL_MAX=str(max(2, 2 * threads)))
    ctx = mp.get_context("spawn")
    # Lockstep: every reader starts each pass together; a dead reader breaks
    # the barrier instead of hanging the run.
    sync, out = ctx.Barrier(args.readers + 1, timeout=600), ctx.Queue()
    procs = [ctx.Process(target=reader, args=(args, i, threads, sync, out)) for i in range(args.readers)]
    for proc in procs:
        proc.start()
    # Each barrier releases a pass (and ends the one before); one more ends the last.
    marks = []
    for _ in range(args.passes + 2):
        sync.wait()
        marks.append((time.perf_counter(), pg_cpu_s(args.pg_container)))
    results = [out.get(timeout=600) for _ in procs]
    for proc in procs:
        proc.join()
    pg_cores = len(cpus(args.pg_cpus))
    for p in range(args.passes + 1):
        times = sorted(t for r in results for t in r[p][0])
        wall = marks[p + 1][0] - marks[p][0]
        kind = "cold" if p == 0 else "new areas" if p == args.passes else "warm"
        print(json.dumps({
            "pass": p + 1, "kind": kind, "readers": args.readers, "threads_each": threads,
            "qps": round(len(times) / wall, 1), "p50_ms": round(statistics.median(times), 1),
            "p95_ms": round(times[int(0.95 * (len(times) - 1))], 1), "max_ms": round(times[-1], 1),
            "rows": sum(r[p][1] for r in results),
            "pg_util": round((marks[p + 1][1] - marks[p][1]) / wall / pg_cores, 2) if pg_cores else None,
        }), flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["download", "load", "run", "profile"])
    ap.add_argument("--ext")
    ap.add_argument("--url", help="PostgreSQL URL (becomes a postgres secret)")
    ap.add_argument("--queries", type=int, default=30)
    ap.add_argument("--passes", type=int, default=10)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--readers", type=int, default=1)
    ap.add_argument("--compression", help="load: override the lake's parquet_compression (default lz4)")
    ap.add_argument("--arrow", action="store_true", help="fetch Arrow tables, not Python tuples")
    ap.add_argument("--reader-cpus", help="pin readers, e.g. 3-15")
    ap.add_argument("--pg-cpus", help="Postgres's cores, for utilisation")
    ap.add_argument("--pg-container", help="Postgres container, for its CPU use")
    args = ap.parse_args()
    {"download": download, "load": load, "run": run, "profile": profile}[args.command](args)


if __name__ == "__main__":
    main()
