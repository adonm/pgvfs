"""pgdb:// against the same DuckDB database in a local file.

    pgdb.py EXT URL [--rows N] [--readers N --reader-cpus A-B] [--pg-container C]

For each target: the writer loads an orders table, checkpoints, runs small
commits and a 10% update; then (pgdb) a reader moves to a new version with
and without DuckDB's external file cache; then a cold pass (PostgreSQL
restarted with its files evicted from the OS page cache, or the local file
evicted) in a fresh process; then READERS single-threaded reader processes run
the query mix concurrently, twice (the first pass also warms their caches).
One JSON line per target. `just bench-db` runs it on a disposable PostgreSQL.
"""

import argparse
import json
import multiprocessing as mp
import os
import random
import statistics
import subprocess
import tempfile
import time
from urllib.parse import unquote, urlsplit

import duckdb

QUERIES = {
    "point lookup": lambda r, n: f"SELECT * FROM orders WHERE id = {r.randrange(n)}",
    "customer orders": lambda r, n: f"SELECT count(*), sum(amount) FROM orders WHERE customer_id = {r.randrange(100_000)}",
    "id range": lambda r, n: (lambda a: f"SELECT sum(amount), avg(qty) FROM orders WHERE id BETWEEN {a} AND {a + 50_000}")(r.randrange(n)),
    "group by": lambda r, n: "SELECT qty, count(*), sum(amount) FROM orders GROUP BY qty ORDER BY qty",
    "scan": lambda r, n: "SELECT sum(h % 1000) FROM orders",
}


def connect(args, threads=None):
    url = urlsplit(args.url)
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute("SET enable_progress_bar = false")
    if threads:
        con.execute(f"SET threads = {threads}")
    con.execute(f"SET memory_limit = '{args.memory_limit}'")
    con.execute(f"LOAD '{args.ext}'")
    con.execute(
        f"CREATE SECRET (TYPE postgres, HOST '{url.hostname}', PORT {url.port or 5432}, "
        f"USER '{unquote(url.username)}', PASSWORD '{unquote(url.password)}', "
        f"DATABASE '{url.path.lstrip('/')}')"
    )
    con.execute("SET pgdb_refresh_interval = 0")
    return con


def timed(fn):
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


def stats(con):
    return json.loads(con.execute("SELECT pgvfs_stats()").fetchone()[0])


def mix(con, rng, n, rounds):
    """Each query kind once per round, shuffled: [(kind, seconds)]."""
    out = []
    for _ in range(rounds):
        kinds = list(QUERIES)
        rng.shuffle(kinds)
        for kind in kinds:
            sql = QUERIES[kind](rng, n)
            out.append((kind, timed(lambda: con.execute(sql).fetchall())))
    return out


def reader(args, path, n, seed, cpu, rounds, start, queue):
    try:
        if cpu is not None:
            os.sched_setaffinity(0, {cpu})
        con = connect(args, threads=1)
        con.execute(f"ATTACH '{path}' AS db (READ_ONLY)")
        con.execute("USE db")
        rng = random.Random(seed)
        start.wait(timeout=600)
        passes = []
        for _ in range(2):
            s0 = stats(con)
            t0 = time.perf_counter()
            times = mix(con, rng, n, rounds)
            wall = time.perf_counter() - t0
            s1 = stats(con)
            passes.append((wall, times, {k: s1[k] - s0[k] for k in s1}))
            start.wait(timeout=3600)
        queue.put(passes)
    except Exception as e:
        start.abort()
        queue.put(f"reader {seed}: {e!r}")


def concurrent(args, path, n):
    ctx = mp.get_context("spawn")
    cpus = list(range(*map(int, args.reader_cpus.split("-")))) + [int(args.reader_cpus.split("-")[1])]
    start = ctx.Barrier(args.readers)
    queue = ctx.Queue()
    procs = [
        ctx.Process(target=reader, args=(args, path, n, i, cpus[i % len(cpus)], args.rounds, start, queue))
        for i in range(args.readers)
    ]
    for p in procs:
        p.start()
    results = [queue.get(timeout=7200) for _ in procs]
    for p in procs:
        p.join()
    failed = [r for r in results if isinstance(r, str)]
    if failed:
        raise RuntimeError("; ".join(failed))
    out = {}
    for label, k in (("first", 0), ("warm", 1)):
        wall = max(r[k][0] for r in results)
        times = [t for r in results for _, t in r[k][1]]
        times.sort()
        out[label] = {
            "qps": round(len(times) / wall, 1),
            "p50_ms": round(1000 * times[len(times) // 2], 1),
            "p95_ms": round(1000 * times[int(len(times) * 0.95)], 1),
        }
        by_kind = {}
        for r in results:
            for kind, t in r[k][1]:
                by_kind.setdefault(kind, []).append(t)
        out[label]["p50_ms_by_kind"] = {k: round(1000 * statistics.median(v), 1) for k, v in by_kind.items()}
        totals = {}
        for r in results:
            for key, v in r[k][2].items():
                totals[key] = totals.get(key, 0) + v
        out[label]["pgvfs"] = {
            "db_reads": totals.get("db_reads", 0),
            "db_read_mb": round(totals.get("db_read_bytes", 0) / 1e6, 1),
            "db_cache_mb": round(totals.get("db_cache_bytes", 0) / 1e6, 1),
            "opens": totals.get("opens", 0),
        }
    return out


def evict_local(path):
    for p in (path, path + ".wal"):
        if os.path.exists(p):
            fd = os.open(p, os.O_RDONLY)
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            os.close(fd)


def restart_pg(container):
    subprocess.run(["docker", "restart", "-t", "60", container], check=True, capture_output=True)
    for _ in range(240):
        if subprocess.run(["docker", "exec", container, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                          capture_output=True).returncode == 0:
            break
        time.sleep(0.25)
    subprocess.run(["docker", "exec", container, "bash", "-c",
                    "find /var/lib/postgresql -type f -exec dd if={} iflag=nocache count=0 status=none \\;"],
                   check=True)


def cold_pass(args, path, n, queue):
    con = connect(args, threads=args.cold_threads)
    t0 = time.perf_counter()
    con.execute(f"ATTACH '{path}' AS db (READ_ONLY)")
    attach = time.perf_counter() - t0
    con.execute("USE db")
    rng = random.Random(7)
    times = {}
    for kind in QUERIES:
        times[kind] = round(1000 * timed(lambda: con.execute(QUERIES[kind](rng, n)).fetchall()), 1)
    queue.put({"attach_ms": round(1000 * attach, 1), "ms": times})


def run(args, label, path):
    out = {"target": label, "rows": args.rows, "block_size": args.block_size}
    n = args.rows
    w = connect(args)
    opts = f" (BLOCK_SIZE {args.block_size})" if args.block_size else ""
    w.execute(f"ATTACH '{path}' AS db{opts}")
    w.execute("USE db")
    out["load_s"] = round(timed(lambda: w.execute(
        f"CREATE TABLE orders AS SELECT i AS id, i % 100000 AS customer_id, "
        f"(i % 1000)::DECIMAL(12,2) AS amount, i % 7 AS qty, 'note ' || (i % 997) AS note, hash(i) AS h "
        f"FROM range({n}) t(i)")), 2)
    # The primary key first: DuckDB adds none to a table with an index.
    out["index_s"] = round(timed(lambda: w.execute(
        "ALTER TABLE orders ADD PRIMARY KEY (id); CREATE INDEX orders_by_customer ON orders(customer_id)")), 2)
    out["checkpoint_s"] = round(timed(lambda: w.execute("CHECKPOINT")), 2)
    commits = 300
    out["commit_ms"] = round(1000 * timed(lambda: [
        w.execute(f"INSERT INTO orders VALUES ({n + i}, 1, 1, 1, 'x', 0)") for i in range(commits)]) / commits, 2)
    out["update_10pct_s"] = round(timed(lambda: w.execute("UPDATE orders SET qty = qty + 1 WHERE id % 10 = 0")), 2)
    out["checkpoint2_s"] = round(timed(lambda: w.execute("CHECKPOINT")), 2)
    if label == "pgdb":
        r = connect(args)
        r.execute(f"ATTACH '{path}' AS db (READ_ONLY)")
        r.execute("USE db")
        mix(r, random.Random(1), n, 1)
        for cache in (True, False):
            r.execute(f"SET enable_external_file_cache = {str(cache).lower()}")
            w.execute(f"INSERT INTO orders VALUES ({n + 10_000 + cache}, 1, 1, 1, 'y', 0)")
            r.execute("USE memory")
            s0 = stats(r)
            attach = timed(lambda: r.execute(f"ATTACH OR REPLACE '{path}' AS db (READ_ONLY)"))
            r.execute("USE db")
            q = sum(t for _, t in mix(r, random.Random(2), n, 1))
            s1 = stats(r)
            out[f"refresh_cache_{'on' if cache else 'off'}"] = {
                "attach_ms": round(1000 * attach, 1),
                "mix_ms": round(1000 * q, 1),
                "pg_mb": round((s1["db_read_bytes"] - s0["db_read_bytes"]) / 1e6, 1),
            }
        r.close()
    w.execute("USE memory")
    w.execute("DETACH db")
    w.close()
    if label == "pgdb" and args.pg_container:
        restart_pg(args.pg_container)
    if label == "local":
        evict_local(path)
    ctx = mp.get_context("spawn")
    queue = ctx.Queue()
    p = ctx.Process(target=cold_pass, args=(args, path, n, queue))
    p.start()
    out["cold"] = queue.get()
    p.join()
    out[f"readers_{args.readers}"] = concurrent(args, path, n)
    print(json.dumps(out), flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ext")
    ap.add_argument("url")
    ap.add_argument("--rows", type=int, default=30_000_000)
    ap.add_argument("--readers", type=int, default=8)
    ap.add_argument("--reader-cpus", default=f"0-{os.cpu_count() - 1}")
    ap.add_argument("--rounds", type=int, default=10, help="query mixes per reader per pass")
    ap.add_argument("--cold-threads", type=int, default=4)
    ap.add_argument("--memory-limit", default="2GiB", help="per DuckDB (each reader is one)")
    ap.add_argument("--pg-container", help="restart and evict it before the cold pass")
    ap.add_argument("--targets", default="local,pgdb")
    ap.add_argument("--block-size", type=int, help="create the pgdb:// database with this block size")
    args = ap.parse_args()
    tmp = tempfile.mkdtemp()
    for target in args.targets.split(","):
        path = os.path.join(tmp, "bench.duckdb") if target == "local" else \
            f"pgdb://bench/b{os.getpid()}-{int(time.time())}.duckdb"
        run(args, target, path)


if __name__ == "__main__":
    main()
