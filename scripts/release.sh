#!/usr/bin/env bash
# GitHub pre-releases are the build store; the Pages site is assembled from
# them (scripts/site.py).
#
#   release.sh publish EXT_DIR     replace pre-release duckdb-<version> with a build
#   release.sh bench BENCH_JSONL   append a benchmark run to the `bench` pre-release
#   release.sh prune               keep every stable build and the newest KEEP_DEV dev builds
#   release.sh fetch DIR           download all builds (DIR/<version>/) and bench history
#
# Needs gh with a token that can write releases (GH_TOKEN in CI).
set -euo pipefail
cd "$(dirname "$0")/.."
export GH_HOST=github.com
KEEP_DEV=4
commit=$(git rev-parse --short=12 HEAD)
today=$(date -u +%F)

tags() { gh release list --limit 200 --json tagName -q '.[].tagName'; }

case "${1:?publish | bench | prune | fetch}" in
publish)
  ext=${2:?EXT_DIR}
  [ -f "$ext/pgvfs.duckdb_extension" ] || { echo "no build in $ext" >&2; exit 0; }
  version=$(cat "$ext/DUCKDB_VERSION")
  wheel=$(cat "$ext/DUCKDB_PY")
  tag="duckdb-$version"
  work=$(mktemp -d)
  cp "$ext/pgvfs.duckdb_extension" "$work/"
  printf '{"duckdb_version": "%s", "wheel": "%s", "commit": "%s", "date": "%s"}\n' \
    "$version" "$wheel" "$commit" "$today" >"$work/build.json"
  gh release delete "$tag" --cleanup-tag --yes 2>/dev/null || true
  gh release create "$tag" --prerelease --target "$(git rev-parse HEAD)" \
    --title "pgvfs for DuckDB $version" \
    --notes "Built from $commit on $today for \`duckdb==$wheel\` (linux_amd64).

\`\`\`sql
INSTALL pgvfs FROM 'https://pgvfs.adonm.dev';   -- picks this build for DuckDB $version
\`\`\`" \
    "$work/pgvfs.duckdb_extension" "$work/build.json"
  echo "published $tag"
  ;;
bench)
  bench=${2:?BENCH_JSONL}
  work=$(mktemp -d)
  if ! gh release view bench >/dev/null 2>&1; then
    gh release create bench --prerelease --title "Weekly benchmark history" \
      --notes "Concurrent-readers benchmark (bench/readers.py), one line per reader count per run."
  fi
  gh release download bench --pattern history.jsonl --dir "$work" 2>/dev/null || true
  grep '^{' "$bench" | python3 -c '
import json, os, sys
for line in sys.stdin:
    record = json.loads(line)
    record.update(date=sys.argv[1], commit=sys.argv[2], cores=os.cpu_count())
    print(json.dumps(record))' "$today" "$commit" >>"$work/history.jsonl"
  gh release upload bench "$work/history.jsonl" --clobber
  echo "bench history: $(wc -l <"$work/history.jsonl") lines"
  ;;
prune)
  tags | grep '^duckdb-' | sed 's/^duckdb-//' | grep -Ev '^v[0-9]+\.[0-9]+\.[0-9]+$' \
    | sort -V | head -n -"$KEEP_DEV" | while read -r version; do
      gh release delete "duckdb-$version" --cleanup-tag --yes
      echo "pruned duckdb-$version"
    done
  ;;
fetch)
  dir=${2:?DIR}
  mkdir -p "$dir"
  for tag in $(tags | grep '^duckdb-'); do
    gh release download "$tag" --dir "$dir/${tag#duckdb-}" --clobber
  done
  gh release download bench --pattern history.jsonl --dir "$dir/bench" --clobber 2>/dev/null || true
  ;;
*) echo "unknown command: $1" >&2; exit 2 ;;
esac
