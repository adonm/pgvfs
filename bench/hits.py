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


def load(con, _args) -> None:
    t0 = time.perf_counter()
    con.execute("DROP TABLE IF EXISTS lake.hits")
    # The source is in ClickHouse's key order (CounterID, EventDate, ...), so
    # site and date filters skip files and row groups without a re-sort.
    con.execute(f"CREATE TABLE lake.hits AS SELECT {TYPED} "
                f"FROM read_parquet('{SRC}', binary_as_string = true)")
    load_s = time.perf_counter() - t0
    # Query parameters from real rows, traffic-weighted: busy sites get
    # queried more. Stored once so readers need not scan 100M rows.
    con.execute("DROP TABLE IF EXISTS lake.hits_params")
    con.execute("CREATE TABLE lake.hits_params AS SELECT CounterID, UserID, EventDate "
                "FROM lake.hits USING SAMPLE 2000 ROWS (reservoir, 1)")
    rows = con.execute("SELECT count(*) FROM lake.hits").fetchone()[0]
    files = con.execute("SELECT count(*), sum(data_file_size_bytes) // 1048576 "
                        "FROM ducklake_list_files('lake', 'hits')").fetchone()
    print(json.dumps({"table": "hits", "rows": rows, "load_s": round(load_s, 1),
                      "files": files[0], "mib": files[1]}), flush=True)


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
