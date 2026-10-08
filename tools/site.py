#!/usr/bin/env python3
"""Assemble the GitHub Pages site from downloaded release assets: the
mdbook docs (docs/) plus a DuckDB extension repository.

    site.py SITE_DIR [--releases DIR]

DIR is `just release-fetch DIR` output (or local builds laid out the
same way): DIR/<duckdb version>/{pgvfs.duckdb_extension, build.json} and
DIR/bench/history.jsonl. The site gets
<duckdb version>/linux_amd64/pgvfs.duckdb_extension.gz, which is what
`INSTALL pgvfs FROM 'https://pgvfs.adonm.dev'` fetches.
"""

import argparse
import gzip
import json
import os
import shutil
import subprocess

PLATFORM = "linux_amd64"
DOCS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "docs")


def builds(releases: str | None, site: str) -> list[dict]:
    found = []
    for version in sorted(os.listdir(releases)) if releases and os.path.isdir(releases) else []:
        meta = os.path.join(releases, version, "build.json")
        if not os.path.exists(meta):
            continue
        dest = os.path.join(site, version, PLATFORM)
        os.makedirs(dest)
        with open(os.path.join(releases, version, "pgvfs.duckdb_extension"), "rb") as src, \
                gzip.open(os.path.join(dest, "pgvfs.duckdb_extension.gz"), "wb", 9) as out:
            shutil.copyfileobj(src, out)
        found.append(json.load(open(meta)))
    return found


def fragments(found: list[dict], history: list[dict]) -> None:
    gen = os.path.join(DOCS, "generated")
    os.makedirs(gen, exist_ok=True)
    rows = ["| DuckDB version | Python wheel | Built | Commit | Direct install |", "| --- | --- | --- | --- | --- |"]
    for b in sorted(found, key=lambda b: b["date"], reverse=True):
        url = f"https://github.com/adonm/pgvfs/releases/download/duckdb-{b['duckdb_version']}/pgvfs.duckdb_extension"
        rows.append(f"| `{b['duckdb_version']}` | `duckdb=={b['wheel']}` | {b['date']} | `{b['commit']}` | [file]({url}) |")
    open(os.path.join(gen, "builds.md"), "w").write("\n".join(rows) + "\n" if found else "No builds published yet.\n")
    history = [r for r in history if "pass" in r]  # older formats: skipped
    if not history:
        open(os.path.join(gen, "bench.md"), "w").write("No benchmark runs yet.\n")
        return
    latest = [r for r in history if (r["date"], r["commit"]) == (history[-1]["date"], history[-1]["commit"])]
    first = latest[0]
    warm = sorted((r for r in latest if r["kind"] == "warm"), key=lambda r: r["qps"])
    shown = [latest[0], warm[len(warm) // 2], latest[-1]] if warm else latest
    lines = [
        f"Run {first['date']} at commit `{first['commit']}` (`just bench city`): Overture "
        f"Houston, {first['readers']} readers each running {first['queries'] // first['readers']} "
        f"area and attribute queries per pass, on a {first['cores']}-core GitHub runner"
        f"{'; lake layout ' + first['layout'] if first.get('layout') else ''}. Shared "
        "hardware, so read it as a trend, not a score. Full history: "
        "[history.jsonl](https://github.com/adonm/pgvfs/releases/download/bench/history.jsonl).",
        "",
        "| Pass | Queries/s | p50 | p95 | PostgreSQL busy |",
        "| --- | ---: | ---: | ---: | ---: |",
    ] + [f"| {r['kind']}{' (median)' if r['kind'] == 'warm' else ''} | {r['qps']} | {r['p50_ms']} ms | "
         f"{r['p95_ms']} ms | {round(100 * r['pg_util']) if r.get('pg_util') is not None else '-'}% |"
         for r in shown]
    open(os.path.join(gen, "bench.md"), "w").write("\n".join(lines) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("site", help="output directory (must not exist)")
    ap.add_argument("--releases", help="downloaded release assets")
    args = ap.parse_args()
    os.makedirs(args.site)
    found = builds(args.releases, args.site)
    history_path = os.path.join(args.releases or "", "bench", "history.jsonl")
    history = [json.loads(line) for line in open(history_path)] if os.path.exists(history_path) else []
    fragments(found, history)
    book = os.path.join(DOCS, "book")
    subprocess.run(["mdbook", "build", DOCS, "--dest-dir", book], check=True)
    shutil.copytree(book, args.site, dirs_exist_ok=True)
    print(f"site: {len(found)} builds, {len(history)} benchmark lines")


if __name__ == "__main__":
    main()
