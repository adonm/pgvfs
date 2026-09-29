#!/usr/bin/env python3
"""Small DuckLake lookups on pgvfs: R reader processes each loop
`SELECT id, ST_AsWKB(geom) ... WHERE id BETWEEN x AND x + 999` (1000
polygons, ~530 KiB) for a fixed time.

    lookups.py --ext EXT --url URL --load              # 2M polygons, lake defaults
    lookups.py --ext EXT --url URL --readers 16 [--reader-cpus 0-11] [--no-cache]
               [--pg-container NAME --pg-cpus 12-15]

--no-cache disables DuckDB's external file cache, so every lookup reads
PostgreSQL (the storage-bound case); with it on, a long-lived reader
serves repeats from memory. With --pg-container, Postgres CPU use is read
from the container's cgroup. Prints one JSON line per run.
"""

import argparse
import json
import multiprocessing as mp
import os
import random
import resource
import subprocess
import time
from urllib.parse import unquote, urlsplit

import duckdb

ROWS, SPAN = 2_000_000, 1000
SQL = f"SELECT id, ST_AsWKB(geom) FROM lake.geo WHERE id BETWEEN $1 AND $1 + {SPAN - 1}"


def cpus(spec: str | None) -> list[int]:
    out = []
    for part in (spec or "").split(","):
        if part:
            lo, _, hi = part.partition("-")
            out += range(int(lo), int(hi or lo) + 1)
    return out


def connect(args, threads: int | None = None) -> duckdb.DuckDBPyConnection:
    u = urlsplit(args.url)
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute("SET enable_progress_bar = false")
    if threads:
        con.execute(f"SET threads = {threads}")
    if args.no_cache:
        con.execute("SET enable_external_file_cache = false")
    con.execute(f"LOAD '{args.ext}'")
    for ext in ("postgres", "ducklake", "spatial"):
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")
    con.execute(f"CREATE SECRET (TYPE postgres, HOST '{u.hostname}', PORT {u.port or 5432}, "
                f"USER '{unquote(u.username)}', PASSWORD '{unquote(u.password or '')}', "
                f"DATABASE '{u.path.lstrip('/')}')")
    con.execute(f"ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 'pgvfs://{args.volume}/')")
    return con


def load(args) -> None:
    con = connect(args)
    # The low-latency default (README): persisted in the lake's catalog.
    con.execute(f"CALL lake.set_option('parquet_row_group_size', {args.row_group_size})")
    con.execute("CALL lake.set_option('parquet_compression', 'lz4')")
    t0 = time.perf_counter()
    con.execute("DROP TABLE IF EXISTS lake.geo")
    con.execute(f"""CREATE TABLE lake.geo AS
        SELECT i AS id, ST_Buffer(ST_Point(random() * 360 - 180, random() * 170 - 85), 0.01) AS geom
        FROM range({ROWS}) t(i) ORDER BY i""")
    print(json.dumps({"load_s": round(time.perf_counter() - t0, 1), "rows": ROWS,
                      "row_group_size": args.row_group_size}), flush=True)


def reader(args, threads: int, seed: int, start, out) -> None:
    try:
        read(args, threads, seed, start, out)
    except BaseException:
        start.abort()  # fail the run now, not at the barrier timeout
        raise


def read(args, threads: int, seed: int, start, out) -> None:
    if args.reader_cpus:
        os.sched_setaffinity(0, cpus(args.reader_cpus))
    con = connect(args, threads)
    rnd = random.Random(seed)
    con.execute(SQL, [0]).fetchall()  # attach, catalog and pool warm
    start.wait()
    r0, lat = resource.getrusage(resource.RUSAGE_SELF), []
    deadline = time.perf_counter() + args.seconds
    while time.perf_counter() < deadline:
        t0 = time.perf_counter()
        rows = con.execute(SQL, [rnd.randrange(0, ROWS - SPAN)]).fetchall()
        lat.append(time.perf_counter() - t0)
        assert len(rows) == SPAN
    r1 = resource.getrusage(resource.RUSAGE_SELF)
    out.put((lat, r1.ru_utime + r1.ru_stime - r0.ru_utime - r0.ru_stime))


def pg_cpu_s(container: str | None) -> float:
    if not container:
        return 0.0
    stat = subprocess.run(["docker", "exec", container, "cat", "/sys/fs/cgroup/cpu.stat"],
                          capture_output=True, text=True, check=True).stdout
    return int(stat.split()[1]) / 1e6  # usage_usec


def run(args) -> dict:
    reader_cores = len(cpus(args.reader_cpus)) or os.cpu_count()
    threads = max(1, reader_cores // args.readers)
    # Per reader: as many pgvfs I/O threads and pooled connections as it needs.
    os.environ.update(PGVFS_IO_THREADS=str(threads), PGVFS_POOL_MIN="1",
                      PGVFS_POOL_MAX=str(max(2, 2 * threads)))
    ctx = mp.get_context("spawn")
    # A reader that dies before the barrier must fail the run, not hang it.
    start, out = ctx.Barrier(args.readers + 1, timeout=300), ctx.Queue()
    procs = [ctx.Process(target=reader, args=(args, threads, i, start, out)) for i in range(args.readers)]
    for p in procs:
        p.start()
    start.wait()
    pg0, t0 = pg_cpu_s(args.pg_container), time.perf_counter()
    results = [out.get(timeout=args.seconds + 300) for _ in procs]
    wall, pg1 = time.perf_counter() - t0, pg_cpu_s(args.pg_container)
    for p in procs:
        p.join()
        if p.exitcode:
            raise SystemExit(f"reader exited {p.exitcode}")
    lat = sorted(t for r, _ in results for t in r)
    q = lambda f: round(lat[int(f * (len(lat) - 1))] * 1000, 1)  # noqa: E731
    pg_cores = len(cpus(args.pg_cpus))
    return {
        "pg_cpus": args.pg_cpus, "reader_cpus": args.reader_cpus, "readers": args.readers,
        "threads_each": threads, "cache": not args.no_cache,
        "qps": round(len(lat) / args.seconds, 1), "p50_ms": q(0.5), "p95_ms": q(0.95), "p99_ms": q(0.99),
        # Busy fraction of the cores each side was given, over the measured window.
        "pg_util": round((pg1 - pg0) / wall / pg_cores, 2) if pg_cores and args.pg_container else None,
        "reader_util": round(sum(c for _, c in results) / wall / reader_cores, 2),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ext", required=True)
    ap.add_argument("--url", required=True, help="PostgreSQL URL (becomes a postgres secret)")
    ap.add_argument("--volume", default="bench")
    ap.add_argument("--load", action="store_true")
    ap.add_argument("--row-group-size", type=int, default=8192)
    ap.add_argument("--readers", type=int, default=1)
    ap.add_argument("--seconds", type=float, default=20)
    ap.add_argument("--reader-cpus", help="pin readers, e.g. 0-11")
    ap.add_argument("--pg-cpus", help="Postgres's cores, for utilisation")
    ap.add_argument("--pg-container", help="Postgres container, for its CPU use")
    ap.add_argument("--no-cache", action="store_true", help="disable DuckDB's external file cache")
    args = ap.parse_args()
    if args.load:
        load(args)
    else:
        print(json.dumps(run(args)), flush=True)


if __name__ == "__main__":
    main()
