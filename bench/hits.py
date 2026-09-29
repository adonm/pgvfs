"""Dataset for lake.py: ClickBench `hits` (99,997,497 web-analytics events, 105
columns), queried like a busy analytics dashboard: per-site traffic, top
pages and searches, a visitor's history, wide row fetches."""

import json
import os
import random
import subprocess
import time

VOLUME = "hits"
EXTENSIONS = ()
URL = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"
BYTES = 14_779_976_446
SRC = ".tmp/data/hits.parquet"
# ClickBench's duckdb/load: the queries expect typed dates and times.
TYPED = """* REPLACE (
    make_date(EventDate) AS EventDate,
    epoch_ms(EventTime * 1000) AS EventTime,
    epoch_ms(ClientEventTime * 1000) AS ClientEventTime,
    epoch_ms(LocalEventTime * 1000) AS LocalEventTime)"""


def download() -> None:
    if os.path.exists(SRC) and os.path.getsize(SRC) == BYTES:
        return
    os.makedirs(os.path.dirname(SRC), exist_ok=True)
    subprocess.run(["curl", "-fsSL", "-C", "-", "-o", SRC, URL], check=True)
    assert os.path.getsize(SRC) == BYTES, "truncated hits.parquet"


def load(con, args) -> None:
    src = f"SELECT {TYPED} FROM read_parquet('{SRC}', binary_as_string = true)"
    t0 = time.perf_counter()
    con.execute("DROP TABLE IF EXISTS lake.hits")
    if args.sort:
        # DuckLake sorts each insert by the declared key. Inserting disjoint
        # CounterID ranges in order keeps every sort small (bounded memory)
        # and still puts each site's rows in one run of files. The source's
        # row-group stats let each range read only its own row groups.
        con.execute(f"CREATE TABLE lake.hits AS {src} LIMIT 0")
        con.execute("ALTER TABLE lake.hits SET SORTED BY (CounterID, EventDate, UserID, EventTime)")
        total = con.execute(f"SELECT count(*) FROM read_parquet('{SRC}')").fetchone()[0]
        n = max(1, round(total / args.batch_rows))
        cuts = con.execute(f"SELECT approx_quantile(CounterID, {[i / n for i in range(1, n)]}) "
                           f"FROM read_parquet('{SRC}')").fetchone()[0] if n > 1 else []
        bounds = [None, *sorted(set(cuts)), None]
        for lo, hi in zip(bounds, bounds[1:]):
            where = " AND ".join(c for c in (lo is not None and f"CounterID >= {lo}",
                                             hi is not None and f"CounterID < {hi}") if c)
            con.execute(f"INSERT INTO lake.hits {src} WHERE {where or 'true'}")
    else:
        # Source order: nine concatenated sorted runs (ClickHouse parts).
        con.execute(f"CREATE TABLE lake.hits AS {src}")
    load_s = time.perf_counter() - t0
    # Query parameters from real rows, traffic-weighted: busy sites get
    # queried more. Stored once so readers need not scan 100M rows.
    con.execute("DROP TABLE IF EXISTS lake.hits_params")
    con.execute("CREATE TABLE lake.hits_params AS SELECT CounterID, UserID, EventDate "
                "FROM lake.hits USING SAMPLE 2000 ROWS (reservoir, 1)")
    rows = con.execute("SELECT count(*) FROM lake.hits").fetchone()[0]
    files = [f[0] for f in con.execute(
        "SELECT data_file FROM ducklake_list_files('lake', 'hits')").fetchall()]
    ranges = con.execute(f"""SELECT file_name, min(stats_min_value::BIGINT), max(stats_max_value::BIGINT),
        count(*) FROM parquet_metadata({files!r}) WHERE path_in_schema = 'CounterID' GROUP BY 1""").fetchall()
    sites = [r[0] for r in con.execute("SELECT DISTINCT CounterID FROM lake.hits_params").fetchall()]
    per_site = sorted(sum(lo <= c <= hi for _, lo, hi, _ in ranges) for c in sites)
    mib = con.execute("SELECT sum(data_file_size_bytes) // 1048576 FROM ducklake_list_files('lake', 'hits')").fetchone()[0]
    print(json.dumps({"table": "hits", "rows": rows, "sorted": args.sort, "load_s": round(load_s, 1),
                      "row_group_size": args.row_group_size, "files": len(files), "mib": mib,
                      "row_groups": sum(r[3] for r in ranges),
                      # how many files a sampled site's rows can be in (by min/max)
                      "files_per_site_p50": per_site[len(per_site) // 2], "files_per_site_max": per_site[-1]}),
          flush=True)


def batch(con, n: int, seed: int) -> list[tuple[str, str]]:
    rnd = random.Random(seed)
    params = con.execute("SELECT CounterID, UserID, EventDate FROM lake.hits_params ORDER BY ALL").fetchall()

    def pick():
        counter, user, day = rnd.choice(params)
        days = rnd.randint(0, 6)
        return counter, user, f"DATE '{day}'", f"DATE '{day}' + INTERVAL {days} DAY"

    def site(counter, d1, d2):
        return f"CounterID = {counter} AND EventDate BETWEEN {d1} AND {d2}"

    def daily():
        c, _, d1, d2 = pick()
        return ("SELECT EventDate, count(*), count(DISTINCT UserID) FROM lake.hits "
                f"WHERE {site(c, d1, d2)} GROUP BY 1 ORDER BY 1")

    def pages():
        c, _, d1, d2 = pick()
        return f"SELECT URL, count(*) AS n FROM lake.hits WHERE {site(c, d1, d2)} GROUP BY 1 ORDER BY 2 DESC LIMIT 10"

    def searches():
        c, _, d1, d2 = pick()
        return ("SELECT SearchPhrase, count(*) AS n FROM lake.hits "
                f"WHERE {site(c, d1, d2)} AND SearchPhrase <> '' GROUP BY 1 ORDER BY 2 DESC LIMIT 10")

    def regions():
        c, _, d1, d2 = pick()
        return ("SELECT RegionID, OS, count(*) AS n FROM lake.hits "
                f"WHERE {site(c, d1, d2)} GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20")

    def visitor():
        c, u, _, _ = pick()
        return ("SELECT EventTime, URL, Title, Referer FROM lake.hits "
                f"WHERE CounterID = {c} AND UserID = {u} ORDER BY EventTime DESC LIMIT 50")

    def wide():
        c, _, d1, _ = pick()
        return f"SELECT * FROM lake.hits WHERE CounterID = {c} AND EventDate = {d1} LIMIT 100"

    def hourly():
        c, _, d1, _ = pick()
        return ("SELECT extract(hour FROM EventTime) AS h, count(*) FROM lake.hits "
                f"WHERE CounterID = {c} AND EventDate = {d1} GROUP BY 1 ORDER BY 1")

    templates = [("site daily", daily), ("top pages", pages), ("top searches", searches),
                 ("regions x OS", regions), ("visitor history", visitor), ("wide rows", wide),
                 ("site hourly", hourly)]
    return [(templates[i % len(templates)][0], templates[i % len(templates)][1]()) for i in range(n)]
