#!/usr/bin/env bash
# Benchmark DuckLake on pgvfs against a disposable PostgreSQL (bench/lake.py).
#
#   scripts/bench.sh city [--passes 4]    Overture Houston, 1M rows (~1 min, 166 MB download)
#   scripts/bench.sh hits [--passes 4]    ClickBench, 100M rows (~4 min, 14 GB download)
#
# Downloads are cached in .tmp/data. The load uses every core; then
# PostgreSQL is pinned to PG_CPUS (default: the first ~1/5 of the cores) and
# restarted, so pass 1 starts with its buffers cold (the OS page cache stays
# warm). READERS readers (default one per remaining core) share READER_CPUS.
# MODE=profile times one warm query of each kind instead.
#
# Extra args go to `lake.py run`; LOAD_ARGS to `lake.py load` (e.g.
# "--variant trickle" for city, "--no-sort" for hits). Memory stays bounded:
# LOAD_MEMORY for the load (default 4GiB, spilling to disk), READER_MEMORY
# per reader (default 2GiB), PG_MEMORY for PostgreSQL (default 6g).
set -euo pipefail
cd "$(dirname "$0")/.."
dataset=${1:?city | hits}
shift
cores=$(nproc)
pg=$(( cores / 5 > 0 ? cores / 5 : 1 ))
pg_cpus=${PG_CPUS:-0-$((pg - 1))}
reader_cpus=${READER_CPUS:-$pg-$((cores - 1))}
first=${reader_cpus%-*}
last=${reader_cpus#*-}
readers=${READERS:-$((last - first + 1))}
ext=${EXT:-target/ext/release/pgvfs.duckdb_extension}
wheel=${DUCKDB_PY:-$(cat "$(dirname "$ext")/DUCKDB_PY" 2>/dev/null || echo 1.5.6)}
container="pgvfs-bench-$$"
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT
lake() {
  uv run --quiet --no-project --with "duckdb==$wheel" --with pyarrow python bench/lake.py "$dataset" "$@"
}
ready() {
  for _ in $(seq 1 120); do
    docker exec "$container" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && return
    sleep 0.5
  done
  docker logs "$container" >&2
  exit 1
}
url() { echo "postgres://postgres:postgres@$(docker port "$container" 5432/tcp)/lake"; }

lake download
docker run -d --name "$container" -e POSTGRES_PASSWORD=postgres --shm-size=2g \
  --memory "${PG_MEMORY:-6g}" -p 127.0.0.1::5432 "${PG_IMAGE:-postgres:18}" \
  -c shared_buffers=2GB -c max_connections=500 >/dev/null
ready
docker exec "$container" psql -U postgres -qc 'CREATE DATABASE lake'
# shellcheck disable=SC2086  # LOAD_ARGS is a list of flags
lake load --ext "$ext" --url "$(url)" --load-memory "${LOAD_MEMORY:-4GiB}" ${LOAD_ARGS:-}
docker update --cpuset-cpus "$pg_cpus" "$container" >/dev/null
docker restart "$container" >/dev/null  # also re-maps the host port
ready
if [ "${MODE:-run}" = profile ]; then
  echo "== profile: PostgreSQL on cores $pg_cpus, one reader on $reader_cpus =="
  taskset -c "$reader_cpus" \
    uv run --quiet --no-project --with "duckdb==$wheel" --with pyarrow python bench/lake.py "$dataset" \
    profile --ext "$ext" --url "$(url)" --memory-limit "${READER_MEMORY:-2GiB}" "$@"
  exit
fi
echo "== $dataset: PostgreSQL on cores $pg_cpus, $readers readers on $reader_cpus =="
lake run --ext "$ext" --url "$(url)" --readers "$readers" --reader-cpus "$reader_cpus" \
  --pg-cpus "$pg_cpus" --pg-container "$container" --memory-limit "${READER_MEMORY:-2GiB}" "$@"
