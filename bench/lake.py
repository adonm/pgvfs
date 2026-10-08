#!/usr/bin/env python3
"""DuckLake-on-pgvfs benchmark harness. A dataset module (city.py, hits.py)
defines VOLUME, EXTENSIONS, download(), load(con, args) and
batch(con, n, seed) -> [(kind, sql)].

    lake.py DATASET download
    lake.py DATASET load    --ext EXT --url URL
    lake.py DATASET run     --ext EXT --url URL [--readers N --reader-cpus 3-15 --pg-container C --pg-cpus 0-2]
    lake.py DATASET profile --ext EXT --url URL

run: READERS processes each run their own fixed batch PASSES times in
lockstep. Pass 1 is cold (new DuckDBs, after `just bench` restarted PostgreSQL
and evicted its files from the OS page cache), later passes warm, and a final
pass uses new random parameters (warm caches, unseen queries). One JSON line
per pass (with the round, when --round is given), then per-kind latencies.
profile: one warm query of each kind, split into planning (binding, DuckLake
catalog, footers) and operator time.
"""

import argparse
import importlib
import json
import multiprocessing as mp
import os
import statistics
import subprocess
import time
from urllib.parse import unquote, urlsplit

import duckdb


def connect(ds, args, threads: int | None = None) -> duckdb.DuckDBPyConnection:
    u = urlsplit(args.url)
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute("SET enable_progress_bar = false")
    if threads:  # before pgvfs connects: it sizes its I/O threads and pool from this
        con.execute(f"SET threads = {threads}")
    if args.memory_limit:
        con.execute(f"SET memory_limit = '{args.memory_limit}'")
    con.execute(f"LOAD '{args.ext}'")  # also turns on the Parquet footer cache
    for ext in ("postgres", "ducklake", *ds.EXTENSIONS):
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")
    con.execute(f"CREATE SECRET (TYPE postgres, HOST '{u.hostname}', PORT {u.port or 5432}, "
                f"USER '{unquote(u.username)}', PASSWORD '{unquote(u.password or '')}', "
                f"DATABASE '{u.path.lstrip('/')}')")
    con.execute(f"ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 'pgvfs://{ds.VOLUME}/')")
    return con


def load(ds, args) -> None:
    con = connect(ds, args)
    # Bounded, whatever the dataset: sorts that do not fit spill to disk.
    con.execute(f"SET memory_limit = '{args.load_memory}'")
    con.execute(f"SET threads = {min(8, os.cpu_count())}")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET temp_directory = '.tmp/duckdb-temp'")
    con.execute(f"CALL lake.set_option('parquet_row_group_size', {args.row_group_size})")
    con.execute(f"CALL lake.set_option('parquet_compression', '{args.compression}')")
    con.execute(f"CALL lake.set_option('parquet_version', {args.parquet_version})")
    con.execute(f"CALL lake.set_option('target_file_size', '{args.target_file_size}')")
    ds.load(con, args)


def layout(ds, args) -> str:
    """The lake's file layout, e.g. "zstd V2, 8192-row groups, 64MB files"."""
    con = connect(ds, args)
    opts = dict(con.execute("SELECT option_name, value FROM lake.options() WHERE option_name IN "
                            "('parquet_compression', 'parquet_version', 'parquet_row_group_size', "
                            "'target_file_size')").fetchall())
    con.close()
    size = opts.get("target_file_size", "512MB")
    size = f"{int(size) // 10**6}MB" if size.isdigit() else size  # stored in bytes
    return (f"{opts.get('parquet_compression', 'snappy')} {opts.get('parquet_version', 'V1')}, "
            f"{opts.get('parquet_row_group_size', '122880')}-row groups, {size} files")


def fetch(con, sql: str, arrow: bool = False) -> int:
    if arrow:
        return con.execute(sql).fetch_arrow_table().num_rows
    return len(con.execute(sql).fetchall())


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
        ds = importlib.import_module(args.dataset)  # modules do not pickle
        if args.reader_cpus:
            os.sched_setaffinity(0, cpus(args.reader_cpus))
        con = connect(ds, args, threads)
        seed = args.seed * 1000 + i
        queries = ds.batch(con, args.queries, seed)
        fresh = ds.batch(con, args.queries, seed + 500)  # the new-parameters pass
        passes = []
        for p in range(args.passes + 1):
            sync.wait()
            times, rows = [], 0
            before = stats(con)
            for kind, sql in queries if p < args.passes else fresh:
                t0 = time.perf_counter()
                rows += fetch(con, sql, args.arrow)
                times.append((kind, (time.perf_counter() - t0) * 1000))
            after = stats(con)
            passes.append((times, rows, {k: after[k] - before[k] for k in after}))
        sync.wait()
        out.put(passes)
    except BaseException:
        sync.abort()  # fail the run now, not at the barrier timeout
        raise


def stats(con) -> dict:
    """pgvfs's process-wide counters (cumulative)."""
    return json.loads(con.execute("SELECT pgvfs_stats()").fetchone()[0])


def pct(values: list[float], q: float) -> float:
    values = sorted(values)
    return round(values[int(q * (len(values) - 1))], 1)


def pgvfs_summary(deltas: list[dict], query_ms: float) -> dict:
    total = {k: sum(d[k] for d in deltas) for k in deltas[0]}
    return {"opens": total["opens"], "open_hits": total.get("open_hits", 0), "open_ms": round(total["open_ms"]),
            "reads": total["reads"], "read_mib": round(total["read_bytes"] / 2**20, 1),
            "read_ms": round(total["read_ms"]), "pieces": total["pieces"],
            # share of all query time spent inside pgvfs calls (reads can overlap
            # within a query, so this is an upper bound)
            "share": round((total["open_ms"] + total["read_ms"]) / query_ms, 2) if query_ms else 0}


def run(ds, args) -> None:
    lake_layout = layout(ds, args)
    reader_cores = len(cpus(args.reader_cpus)) or os.cpu_count()
    threads = max(1, reader_cores // args.readers)
    ctx = mp.get_context("spawn")
    # Lockstep: every reader starts each pass together.
    sync, out = ctx.Barrier(args.readers + 1, timeout=1800), ctx.Queue()
    procs = [ctx.Process(target=reader, args=(args, i, threads, sync, out)) for i in range(args.readers)]
    for proc in procs:
        proc.start()
    # Each barrier releases a pass (and ends the one before); one more ends the last.
    marks = []
    for _ in range(args.passes + 2):
        sync.wait()
        marks.append((time.perf_counter(), pg_cpu_s(args.pg_container)))
    results = [out.get(timeout=1800) for _ in procs]
    for proc in procs:
        proc.join()
    pg_cores = len(cpus(args.pg_cpus))
    kinds = {}
    for p in range(args.passes + 1):
        times = [t for r in results for _, t in r[p][0]]
        wall = marks[p + 1][0] - marks[p][0]
        label = "cold" if p == 0 else "new params" if p == args.passes else "warm"
        for r in results:
            for kind, t in r[p][0]:
                kinds.setdefault(kind, {}).setdefault(label, []).append(t)
        print(json.dumps({
            "pass": p + 1, **({"round": args.round} if args.round else {}),
            "kind": label, "readers": args.readers, "threads_each": threads,
            "queries": len(times), "wall_s": round(wall, 1), "qps": round(len(times) / wall, 1),
            "p50_ms": pct(times, .5), "p95_ms": pct(times, .95), "max_ms": pct(times, 1),
            "rows": sum(r[p][1] for r in results),
            "pgvfs": pgvfs_summary([r[p][2] for r in results], sum(times)),
            "pg_util": round((marks[p + 1][1] - marks[p][1]) / wall / pg_cores, 2) if pg_cores else None,
            "layout": lake_layout,
        }), flush=True)
    print(json.dumps({"p50_ms_by_kind": {k: {label: pct(v, .5) for label, v in by.items()}
                                         for k, by in kinds.items()}}), flush=True)


def profile(ds, args) -> None:
    con = connect(ds, args)
    queries = ds.batch(con, args.queries, args.seed)
    for _ in range(2):  # warm the caches
        for _, sql in queries:
            fetch(con, sql)
    out = f"/tmp/pgvfs-profile-{os.getpid()}.json"
    seen = set()
    for kind, sql in queries:
        if kind in seen:
            continue
        seen.add(kind)
        t0 = time.perf_counter()
        fetch(con, sql)
        wall = (time.perf_counter() - t0) * 1000
        con.execute(f"SET enable_profiling = 'json'; SET profiling_output = '{out}'")
        rows = fetch(con, sql)
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
            "kind": kind, "rows": rows, "wall_ms": round(wall, 1), "profiled_ms": round(latency, 1),
            # Outside operators: binding and planning, incl. DuckLake's catalog
            # queries. Operator time sums over threads, so it can exceed latency.
            "plan_ms": round(max(0.0, latency - sum(ops.values())), 1),
            "operators_ms": {k: round(v, 1) for k, v in sorted(ops.items(), key=lambda kv: -kv[1]) if v >= 0.1},
        }), flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("dataset", choices=["city", "hits"])
    ap.add_argument("command", choices=["download", "load", "run", "profile"])
    ap.add_argument("--ext")
    ap.add_argument("--url", help="PostgreSQL URL (becomes a postgres secret)")
    ap.add_argument("--queries", type=int, default=30, help="per reader batch")
    ap.add_argument("--passes", type=int, default=10, help="cold + warm passes (plus one new-params pass)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--readers", type=int, default=1)
    ap.add_argument("--memory-limit", help="per DuckDB, e.g. 2GiB")
    ap.add_argument("--compression", default="zstd", help="load: the lake's parquet_compression")
    ap.add_argument("--parquet-version", type=int, default=2, choices=[1, 2], help="load: the lake's parquet_version")
    ap.add_argument("--row-group-size", type=int, default=8192, help="load: the lake's parquet_row_group_size")
    ap.add_argument("--no-sort", dest="sort", action="store_false", help="load: keep source order")
    ap.add_argument("--target-file-size", default="64MB", help="load: the lake's target_file_size")
    ap.add_argument("--order", default="hilbert", choices=["hilbert", "x", "source"],
                    help="load (city): spatial row order")
    ap.add_argument("--variant", default="base",
                    choices=["base", "trickle", "compacted", "deleted", "rewritten", "split"],
                    help="load (city): layout variant")
    ap.add_argument("--load-memory", default="4GiB", help="load: DuckDB memory_limit (sorts spill beyond it)")
    ap.add_argument("--batch-rows", type=int, default=5_000_000, help="load: rows per sorted insert")
    ap.add_argument("--arrow", action="store_true", help="fetch Arrow tables, not Python tuples")
    ap.add_argument("--reader-cpus", help="pin readers, e.g. 3-15")
    ap.add_argument("--pg-cpus", help="Postgres's cores, for utilisation")
    ap.add_argument("--pg-container", help="Postgres container, for its CPU use")
    ap.add_argument("--round", type=int, help="run: which round of the benchmark this is")
    args = ap.parse_args()
    ds = importlib.import_module(args.dataset)
    if args.command == "download":
        ds.download()
    else:
        {"load": load, "run": run, "profile": profile}[args.command](ds, args)


if __name__ == "__main__":
    main()
