#!/usr/bin/env python3
"""Peak memory of one tantivy_index query: this process's, DuckDB included.

    index_memory.py --ext target/ext/release/pgvfs.duckdb_extension --docs 2000000 --words 30 --groups 8

Indexes DOCS synthetic documents (--words words of 20,000 each, stored), spread
over GROUPS splits that are built at once, by one query that streams its rows
from range(). Run it once per setting in a fresh process: the peak is the
process's. --no-index runs the same query with count(to_json(row)) instead,
which is what DuckDB itself uses, to subtract.
"""

import argparse
import json
import os
import resource
import tempfile
import threading
import time

import duckdb

SCHEMA = json.dumps(
    [
        {"name": "id", "type": "i64", "options": {"stored": True, "indexed": True, "fast": True}},
        {
            "name": "body",
            "type": "text",
            "options": {"stored": True, "indexing": {"record": "position", "tokenizer": "default"}},
        },
    ]
)


def sample(peaks, stop):
    """Peak heap (RssAnon), mapped-file (RssFile) and tmpfs (RssShmem) pages: files
    tantivy maps count in the process's RSS; the kernel can drop those of a disk
    but not of a tmpfs."""
    while not stop.is_set():
        with open("/proc/self/status") as f:
            for line in f:
                for key in ("RssAnon", "RssFile", "RssShmem"):
                    if line.startswith(key + ":"):
                        peaks[key] = max(peaks[key], int(line.split()[1]) / 1024)
        stop.wait(0.02)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ext", required=True)
    ap.add_argument("--docs", type=int, default=2_000_000)
    ap.add_argument("--words", type=int, default=30)
    ap.add_argument("--groups", type=int, default=1)
    ap.add_argument("--memory-budget", type=int, help="bytes; the default is tantivy_index's own")
    ap.add_argument("--no-index", action="store_true")
    args = ap.parse_args()

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{args.ext}'")
    rows = (
        f"SELECT i AS id, i % {args.groups} AS part, 'term' || (i % 1009) || ' '"
        f" || array_to_string(list_transform(range({args.words}), lambda j: 'v' || (hash(i * 31 + j) % 20000)), ' ') AS body"
        f" FROM range({args.docs}) r(i)"
    )
    with tempfile.TemporaryDirectory() as root:
        options = json.dumps({"memory_budget": args.memory_budget}) if args.memory_budget else None
        if args.no_index:
            sql = f"SELECT part, count(to_json(t)) FROM ({rows}) t GROUP BY part"
            params = []
        else:
            sql = (
                f"SELECT part, tantivy_index('{root}/p' || part || '.tantivy', ?, to_json(t){', ?' if options else ''}) "
                f"FROM ({rows}) t GROUP BY part"
            )
            params = [SCHEMA] + ([options] if options else [])
        before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peaks, stop = {"RssAnon": 0, "RssFile": 0, "RssShmem": 0}, threading.Event()
        sampler = threading.Thread(target=sample, args=(peaks, stop), daemon=True)
        sampler.start()
        t = time.perf_counter()
        con.execute(sql, params).fetchall()
        seconds = time.perf_counter() - t
        stop.set()
        sampler.join()
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        size = sum(os.path.getsize(os.path.join(root, f)) for f in os.listdir(root))
    budget = args.memory_budget or 256 << 20
    print(
        f"docs={args.docs} words={args.words} groups={args.groups} memory_budget={budget >> 20} MB"
        f"{' (no index)' if args.no_index else ''}: {seconds:.1f} s, heap peak {peaks['RssAnon']:.0f} MB, "
        f"mapped files {peaks['RssFile']:.0f} MB, tmpfs {peaks['RssShmem']:.0f} MB (RSS peak {peak / 1024:.0f} MB), splits {size / 1e6:.0f} MB"
    )


if __name__ == "__main__":
    main()
