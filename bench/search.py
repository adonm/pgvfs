#!/usr/bin/env python3
"""Tantivy search timings on a synthetic corpus, over local files or pgvfs.

    search.py --ext target/ext/release/pgvfs.duckdb_extension --dir /tmp/fts
    search.py --ext ... --url postgres://... --dir pgvfs://bench-fts   # splits on pgvfs

Builds SPLITS splits of DOCS/SPLITS documents each (reused if --dir has them,
so two builds of the extension can be compared on the same files), then times
the calls a search service makes over all of them, with DEAD of the documents
excluded by key. Features a build lacks print "n/a".

Documents: a text `body` in which "common" is in 7 of 10 documents, a fast
single-valued `group` (DOCS/8 distinct values) and a fast integer `id`.
--words adds that many words from a 20,000-word vocabulary to each body (and
stores it): wider documents, about 12 bytes of split per word. --merge also
times compacting all the splits into one, dropping the excluded documents,
against re-indexing the live documents from their source.
"""

import argparse
import json
import os
import statistics
import struct
import time
from urllib.parse import unquote, urlsplit

import duckdb


def schema(stored):
    return json.dumps(
        [
            {"name": "id", "type": "i64", "options": {"stored": True, "indexed": True, "fast": True}},
            {
                "name": "body",
                "type": "text",
                "options": {"stored": stored, "indexing": {"record": "position", "tokenizer": "default"}},
            },
            {
                "name": "group",
                "type": "text",
                "options": {"fast": True, "indexing": {"record": "basic", "tokenizer": "raw"}},
            },
        ]
    )


def documents(docs, words):
    """SQL for the source rows, `docs` of them."""
    wide = (
        f" || ' ' || array_to_string(list_transform(range({words}), lambda j: 'v' || (hash(i * 31 + j) % 20000)), ' ')"
        if words
        else ""
    )
    return f"""SELECT i AS id, 'term' || (i % 1009) || ' word' || (i % 53) || ' filler text'
                || CASE WHEN i % 10 < 7 THEN ' common' ELSE '' END{wide} AS body,
                'g' || (i % {docs // 8}) AS "group", i
         FROM range({docs}) r(i)"""


def is_dead(dead):
    """SQL for the excluded documents (as `ids` below picks them)."""
    return f"((i * 2654435761) >> 7) % 1000 < {int(dead * 1000)}"


def roaring(ids, total):
    """A portable 32-bit roaring bitmap of sorted ids below `total`."""
    containers = {}
    for i in ids:
        containers.setdefault(i >> 16, []).append(i & 0xFFFF)
    keys = sorted(containers)
    head = struct.pack("<II", 12346, len(keys))
    for k in keys:
        head += struct.pack("<HH", k, len(containers[k]) - 1)
    body, offsets, at = b"", b"", 8 + 8 * len(keys)
    for k in keys:
        lows = containers[k]
        if len(lows) <= 4096:
            data = struct.pack(f"<{len(lows)}H", *lows)
        else:
            words = [0] * 1024
            for low in lows:
                words[low >> 6] |= 1 << (low & 63)
            data = struct.pack("<1024Q", *words)
        offsets += struct.pack("<I", at)
        at += len(data)
        body += data
    return head + offsets + body


def timed(con, sql, args=(), runs=7):
    """(median ms, min ms, first row) of a query; None if the build lacks it."""
    times, row = [], None
    try:
        for _ in range(runs):
            t = time.perf_counter()
            row = con.execute(sql, list(args)).fetchone()
            times.append((time.perf_counter() - t) * 1000)
    except duckdb.Error as e:
        return None, None, str(e).splitlines()[0][:70]
    return statistics.median(times), min(times), row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ext", required=True)
    ap.add_argument("--dir", required=True, help="where splits live: a directory, or pgvfs://volume")
    ap.add_argument("--url", help="PostgreSQL URL, for pgvfs:// splits")
    ap.add_argument("--docs", type=int, default=4_000_000)
    ap.add_argument("--splits", type=int, default=4)
    ap.add_argument("--dead", type=float, default=0.6)
    ap.add_argument("--threads", type=int, default=0)
    ap.add_argument("--words", type=int, default=0)
    ap.add_argument("--merge", action="store_true")
    args = ap.parse_args()

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{args.ext}'")
    if args.threads:
        con.execute(f"SET threads = {args.threads}")
    if args.url:
        u = urlsplit(args.url)
        con.execute(
            f"CREATE SECRET (TYPE postgres, HOST '{u.hostname}', PORT {u.port or 5432}, USER '{unquote(u.username)}', "
            f"PASSWORD '{unquote(u.password)}', DATABASE '{u.path.lstrip('/')}')"
        )
    root = args.dir.rstrip("/")
    paths = [f"{root}/part-{i}.tantivy" for i in range(args.splits)]
    if not all(con.execute("SELECT count(*) FROM glob(?)", [p]).fetchone()[0] for p in paths):
        os.makedirs(root, exist_ok=True) if "://" not in root else None
        per = args.docs // args.splits
        t = time.perf_counter()
        con.execute(
            f"""SELECT i // {per} AS part, tantivy_index('{root}/part-' || (i // {per}) || '.tantivy', ?, to_json(t))
            FROM ({documents(args.docs, args.words)}) t GROUP BY part""",
            [schema(bool(args.words))],
        ).fetchall()
        print(f"built {args.splits} splits of {per} documents in {time.perf_counter() - t:.0f} s")
    ids = [i for i in range(args.docs) if (i * 2654435761 >> 7) % 1000 < int(args.dead * 1000)]
    dead = roaring(ids, args.docs)
    print(
        f"{args.docs} documents, {len(ids)} excluded ({len(dead) / 1e6:.1f} MB bitmap), threads={con.execute('SELECT current_setting(\'threads\')').fetchone()[0]}"
    )
    lst = "[" + ", ".join(f"'{p}'" for p in paths) + "]"
    ex = '\'{"exclude_field": "id"%s}\''
    cases = [
        ("count, no exclusion", f"SELECT tantivy_count({lst}, 'common')", ()),
        ("count, excluded", f"SELECT tantivy_count({lst}, 'common', {ex % ''}, ?::BLOB)", (dead,)),
        ("top 10, no exclusion", f"SELECT count(*) FROM tantivy_search({lst}, 'common', '{{\"top_k\": 10}}')", ()),
        (
            "top 10, excluded",
            f"SELECT count(*) FROM tantivy_search({lst}, 'common', {ex % ', \"top_k\": 10'}, ?::BLOB)",
            (dead,),
        ),
        (
            "top 10, one call per split (lateral)",
            f"SELECT count(*) FROM (SELECT * FROM (VALUES {', '.join(f'({chr(39)}{p}{chr(39)})' for p in paths)}) v(p) "
            "CROSS JOIN tantivy_search(v.p, 'common', '{\"top_k\": 10}') ORDER BY score DESC LIMIT 10)",
            (),
        ),
        (
            "page at 9000, stored doc",
            f"SELECT count(*) FROM tantivy_search({lst}, 'common', '{{\"top_k\": 10, \"offset\": 9000}}')",
            (),
        ),
        (
            "page at 9000, top_k 9010 + SQL offset (before offset)",
            f"SELECT count(*) FROM (SELECT * FROM tantivy_search({lst}, 'common', '{{\"top_k\": 9010}}') OFFSET 9000)",
            (),
        ),
        ("count, limit 100", f"SELECT tantivy_count({lst}, 'common', '{{\"limit\": 100}}')", ()),
        ("distinct groups, exact", f"SELECT tantivy_count({lst}, 'common', '{{\"distinct\": \"group\"}}')", ()),
        (
            "distinct groups, cardinality aggregation",
            f'SELECT tantivy_aggregate({lst}, \'common\', \'{{"n": {{"cardinality": {{"field": "group"}}}}}}\')',
            (),
        ),
        (
            "top 10 groups, collapse",
            f'SELECT count(*) FROM tantivy_search({lst}, \'common\', \'{{"top_k": 10, "collapse": "group", "fast": ["id"]}}\')',
            (),
        ),
    ]
    print(f"{'':58}{'median ms':>10}{'min ms':>9}  result")
    for name, sql, params in cases:
        median, low, row = timed(con, sql, params, runs=3 if "distinct" in name or "offset" in name else 7)
        if median is None:
            print(f"{name:58}{'n/a':>10}{'':>9}  {row}")
        else:
            print(f"{name:58}{median:10.1f}{low:9.1f}  {str(row[0])[:50]}")
    if args.merge:
        merge_timings(con, args, lst, root, dead)


def merge_timings(con, args, lst, root, dead):
    """Compact every split into one without the excluded documents, and the same
    from the source: the live documents indexed again."""
    t = time.perf_counter()
    kept = con.execute(
        f"SELECT tantivy_merge({lst}, '{root}/merged.tantivy', '{{\"exclude_field\": \"id\"}}', ?::BLOB)", [dead]
    ).fetchone()[0]
    merge = time.perf_counter() - t
    t = time.perf_counter()
    again = con.execute(
        f"SELECT tantivy_index('{root}/reindexed.tantivy', ?, to_json(t)) "
        f"FROM ({documents(args.docs, args.words)}) t WHERE NOT ({is_dead(args.dead)})",
        [schema(bool(args.words))],
    ).fetchone()[0]
    reindex = time.perf_counter() - t
    print(f"tantivy_merge of {args.splits} splits, {kept} documents kept: {merge:.1f} s")
    print(f"re-indexing the {again} live documents from source:    {reindex:.1f} s")
    for name in ("merged", "reindexed"):
        con.execute(f"SELECT tantivy_drop('{root}/{name}.tantivy')").fetchall()


if __name__ == "__main__":
    main()
