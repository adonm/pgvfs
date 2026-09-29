"""Dataset for lake.py: Overture buildings and places for central Houston
(835k + 229k rows, many attributes), queried by area of interest and
attribute filters."""

import json
import os
import random
import time

import duckdb

VOLUME = "city"
EXTENSIONS = ("spatial",)
RELEASE = "s3://overturemaps-us-west-2/release/2026-09-23.1"
THEMES = {"buildings": "buildings/type=building", "places": "places/type=place"}
XMIN, YMIN, XMAX, YMAX = -95.65, 29.60, -95.15, 29.95  # central Houston
BOX = f"{{'min_x': {XMIN}, 'min_y': {YMIN}, 'max_x': {XMAX}, 'max_y': {YMAX}}}::BOX_2D"
DATA = ".tmp/data/houston"


def within(x1, y1, x2, y2) -> str:
    return f"bbox.xmin <= {x2} AND bbox.xmax >= {x1} AND bbox.ymin <= {y2} AND bbox.ymax >= {y1}"


def download() -> None:
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


def load(con, _args) -> None:
    for name in THEMES:
        t0 = time.perf_counter()
        con.execute(f"DROP TABLE IF EXISTS lake.{name}")
        # Hilbert order: an area query touches few row groups.
        con.execute(f"CREATE TABLE lake.{name} AS SELECT * FROM '{DATA}/{name}.parquet' "
                    f"ORDER BY ST_Hilbert(geometry, {BOX})")
        rows = con.execute(f"SELECT count(*) FROM lake.{name}").fetchone()[0]
        print(json.dumps({"table": name, "rows": rows, "load_s": round(time.perf_counter() - t0, 1)}), flush=True)


def batch(con, n: int, seed: int) -> list[tuple[str, str]]:
    rnd = random.Random(seed)
    categories = [r[0] for r in con.execute(
        "SELECT taxonomy.primary FROM lake.places WHERE taxonomy.primary IS NOT NULL "
        "GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 20").fetchall()]

    def window(lo, hi):
        side = rnd.uniform(lo, hi)
        x, y = rnd.uniform(XMIN, XMAX - side), rnd.uniform(YMIN, YMAX - side)
        return within(x, y, x + side, y + side)

    templates = [
        ("buildings in view", lambda: "SELECT id, class, height, ST_AsWKB(geometry) FROM lake.buildings "
                                      f"WHERE {window(.005, .02)}"),
        ("places in view", lambda: "SELECT id, names.primary, taxonomy.primary, confidence, ST_AsWKB(geometry) "
                                   f"FROM lake.places WHERE {window(.005, .02)}"),
        ("tall buildings", lambda: "SELECT id, names.primary, height, num_floors, ST_AsWKB(geometry) "
                                   f"FROM lake.buildings WHERE height > {rnd.choice([20, 40, 80])} "
                                   f"AND {window(.05, .1)}"),
        ("places by category", lambda: "SELECT id, names.primary, confidence, ST_AsWKB(geometry) FROM lake.places "
                                       f"WHERE taxonomy.primary = '{rnd.choice(categories)}' AND confidence > 0.7"),
        ("area summary", lambda: "SELECT class, count(*), avg(height) FROM lake.buildings "
                                 f"WHERE {window(.02, .05)} GROUP BY class ORDER BY 2 DESC"),
    ]
    return [(templates[i % len(templates)][0], templates[i % len(templates)][1]()) for i in range(n)]
