#!/usr/bin/env python3
"""Update the GitHub Pages site: the mdbook docs (docs/) plus a DuckDB
extension repository and the weekly benchmark history.

    site.py SITE_DIR [--ext target/ext/release ...] [--bench bench.jsonl]

SITE_DIR is the gh-pages checkout. Its data survives each run:

    <duckdb version>/linux_amd64/pgvfs.duckdb_extension.gz   (INSTALL ... FROM)
    versions.json        version -> wheel, commit, date
    bench/history.jsonl  one line per reader count per run

Everything else is the book, rebuilt from docs/ each time. Every stable
build is kept; of the dev builds, the newest KEEP_DEV.
"""

import argparse
import datetime
import gzip
import json
import os
import re
import shutil
import subprocess

KEEP_DEV = 4
RELEASE = re.compile(r"^v\d+\.\d+\.\d+$")
VERSION_DIR = re.compile(r"^v\d+\.\d+\.\d+")
PLATFORM = "linux_amd64"
DOCS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "docs")


def publish(site: str, exts: list[str], commit: str, today: str) -> dict:
    path = os.path.join(site, "versions.json")
    manifest = json.load(open(path)) if os.path.exists(path) else {}
    for ext in exts:
        binary = os.path.join(ext, "pgvfs.duckdb_extension")
        if not os.path.exists(binary):
            print(f"skip {ext}: no build")
            continue
        version = open(os.path.join(ext, "DUCKDB_VERSION")).read().strip()
        wheel = open(os.path.join(ext, "DUCKDB_PY")).read().strip()
        dest = os.path.join(site, version, PLATFORM)
        os.makedirs(dest, exist_ok=True)
        with open(binary, "rb") as src, gzip.open(os.path.join(dest, "pgvfs.duckdb_extension.gz"), "wb", 9) as out:
            shutil.copyfileobj(src, out)
        manifest[version] = {"wheel": wheel, "commit": commit, "date": today}
        print(f"published {version} (duckdb=={wheel})")
    dev = sorted((v for v in manifest if not RELEASE.match(v)),
                 key=lambda v: [int(n) for n in re.findall(r"\d+", v)])
    for version in dev[:-KEEP_DEV]:
        shutil.rmtree(os.path.join(site, version), ignore_errors=True)
        del manifest[version]
        print(f"pruned {version}")
    json.dump(manifest, open(path, "w"), indent=1, sort_keys=True)
    return manifest


def record_bench(site: str, bench: str | None, commit: str, today: str) -> list[dict]:
    path = os.path.join(site, "bench", "history.jsonl")
    if bench and os.path.exists(bench):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a") as history:
            for line in open(bench):
                if line.startswith("{"):
                    record = json.loads(line)
                    record.update(date=today, commit=commit, cores=os.cpu_count())
                    history.write(json.dumps(record) + "\n")
    return [json.loads(line) for line in open(path)] if os.path.exists(path) else []


def write_fragments(manifest: dict, history: list[dict]) -> None:
    gen = os.path.join(DOCS, "generated")
    os.makedirs(gen, exist_ok=True)
    builds = ["| DuckDB version | Python wheel | Built | Commit |", "| --- | --- | --- | --- |"]
    for version, m in sorted(manifest.items(), key=lambda kv: kv[1]["date"], reverse=True):
        builds.append(f"| `{version}` | `duckdb=={m['wheel']}` | {m['date']} | `{m['commit']}` |")
    open(os.path.join(gen, "builds.md"), "w").write(
        "\n".join(builds) + "\n" if manifest else "No builds published yet.\n")
    if not history:
        open(os.path.join(gen, "bench.md"), "w").write("No benchmark runs yet.\n")
        return
    last = history[-1]["date"]
    latest = [r for r in history if r["date"] == last]
    first = latest[0]
    lines = [
        f"Run {last} at commit `{first['commit']}`: {first['queries'] // first['readers']} ClickBench "
        f"queries per reader, each reader a cold DuckDB with `{first['cores']} / readers` threads, on a "
        f"{first['cores']}-core GitHub runner. Shared hardware, so read it as a trend, not a score. "
        "Full history: [bench/history.jsonl](bench/history.jsonl).",
        "",
        "| Readers | Queries/s | p50 | p95 | Geomean |",
        "| ---: | ---: | ---: | ---: | ---: |",
    ]
    lines += [f"| {r['readers']} | {r['qps']} | {r['p50_ms']} ms | {r['p95_ms']} ms | {r['geomean_ms']} ms |"
              for r in latest]
    open(os.path.join(gen, "bench.md"), "w").write("\n".join(lines) + "\n")


def build_book(site: str) -> None:
    out = os.path.join(DOCS, "book")
    subprocess.run(["mdbook", "build", DOCS, "--dest-dir", out], check=True)
    keep = {".git", "versions.json", "bench"}
    for name in os.listdir(site):
        if name not in keep and not VERSION_DIR.match(name):
            path = os.path.join(site, name)
            shutil.rmtree(path) if os.path.isdir(path) else os.remove(path)
    shutil.copytree(out, site, dirs_exist_ok=True)
    open(os.path.join(site, ".nojekyll"), "w").close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("site")
    ap.add_argument("--ext", nargs="*", default=[], help="target/ext/<target> dirs")
    ap.add_argument("--bench", help="bench/readers.py output")
    ap.add_argument("--commit", default=os.environ.get("GITHUB_SHA", "local")[:12])
    args = ap.parse_args()
    today = datetime.date.today().isoformat()
    os.makedirs(args.site, exist_ok=True)
    manifest = publish(args.site, args.ext, args.commit, today)
    history = record_bench(args.site, args.bench, args.commit, today)
    write_fragments(manifest, history)
    build_book(args.site)


if __name__ == "__main__":
    main()
