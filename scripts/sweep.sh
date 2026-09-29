#!/usr/bin/env bash
# Split this host's cores between PostgreSQL and DuckDB readers and find the
# split that serves the most small lookups (bench/lookups.py, cache off, so
# every lookup reads PostgreSQL). Postgres gets the highest-numbered cores
# (on hybrid CPUs the efficiency cores), readers the rest, READERS_PER_CORE
# single-threaded readers per reader core. One load, re-pinned live.
#
#   PG_CORES="2 4 6 8" READERS_PER_CORE="1 2 3" SECONDS=20 scripts/sweep.sh
set -euo pipefail
cd "$(dirname "$0")/.."
cores=$(nproc)
pg_cores=${PG_CORES:-2 4 6 8}
per_core=${READERS_PER_CORE:-1 2 3}
ext=${EXT:-target/ext/release/pgvfs.duckdb_extension}
container="pgvfs-sweep-$$"
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT

docker run -d --name "$container" -e POSTGRES_PASSWORD=postgres --shm-size=1g \
  -p 127.0.0.1::5432 "${PG_IMAGE:-postgres:18}" \
  -c shared_buffers=2GB -c max_connections=1000 >/dev/null
for i in $(seq 1 60); do
  docker exec "$container" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && break
  [ "$i" = 60 ] && { docker logs "$container" >&2; exit 1; }
  sleep 0.5
done
docker exec "$container" psql -U postgres -qc 'CREATE DATABASE lake'
url="postgres://postgres:postgres@$(docker port "$container" 5432/tcp)/lake"
bench() {
  uv run --quiet --no-project --with "duckdb==${DUCKDB_PY:-1.5.6}" python bench/lookups.py \
    --ext "$ext" --url "$url" "$@"
}
echo "== $(lscpu | sed -n 's/^Model name: *//p'), $cores cores =="
bench --load
for p in $pg_cores; do
  first=$((cores - p))
  docker update --cpuset-cpus "$first-$((cores - 1))" "$container" >/dev/null
  for k in $per_core; do
    SECONDS_START=$(date +%s)
    bench --no-cache --seconds "${SECONDS:-20}" --readers $((k * first)) \
      --reader-cpus "0-$((first - 1))" --pg-cpus "$first-$((cores - 1))" --pg-container "$container"
    echo "   ($(( $(date +%s) - SECONDS_START ))s incl. reader startup)" >&2
  done
done
