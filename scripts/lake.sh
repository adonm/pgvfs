#!/usr/bin/env bash
# bench/lake.py end to end for a dataset (city | hits) on a disposable
# PostgreSQL. The load uses every core; then PostgreSQL is pinned to PG_CPUS
# (default 0-2: three fast cores, ~1/5 of the CPU work per the core sweep)
# and restarted, so pass 1 starts with its buffers cold (the OS page cache
# stays warm). READERS single-threaded readers share READER_CPUS (default
# the rest). MODE=profile times one warm query of each kind instead.
# Extra args go to lake.py (e.g. --passes 5 --queries 21 --arrow).
#
#   scripts/lake.sh hits --passes 5
set -euo pipefail
cd "$(dirname "$0")/.."
dataset=${1:?city | hits}
shift
cores=$(nproc)
pg_cpus=${PG_CPUS:-0-2}
reader_cpus=${READER_CPUS:-3-$((cores - 1))}
ext=${EXT:-target/ext/release/pgvfs.duckdb_extension}
container="pgvfs-$dataset-$$"
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT
lake() {
  uv run --quiet --no-project --with "duckdb==${DUCKDB_PY:-1.5.6}" --with pyarrow \
    python bench/lake.py "$dataset" "$@"
}
ready() {
  for i in $(seq 1 120); do
    docker exec "$container" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && return
    sleep 0.5
  done
  docker logs "$container" >&2; exit 1
}
url() { echo "postgres://postgres:postgres@$(docker port "$container" 5432/tcp)/lake"; }

lake download
docker run -d --name "$container" -e POSTGRES_PASSWORD=postgres --shm-size=2g \
  -p 127.0.0.1::5432 "${PG_IMAGE:-postgres:18}" -c shared_buffers=2GB -c max_connections=500 >/dev/null
ready
docker exec "$container" psql -U postgres -qc 'CREATE DATABASE lake'
lake load --ext "$ext" --url "$(url)"
docker update --cpuset-cpus "$pg_cpus" "$container" >/dev/null
docker restart "$container" >/dev/null  # also re-maps the host port
ready
if [ "${MODE:-run}" = profile ]; then
  echo "== profile: PostgreSQL on cores $pg_cpus, one reader on $reader_cpus =="
  taskset -c "$reader_cpus" \
    uv run --quiet --no-project --with "duckdb==${DUCKDB_PY:-1.5.6}" --with pyarrow \
    python bench/lake.py "$dataset" profile --ext "$ext" --url "$(url)" "$@"
  exit
fi
echo "== PostgreSQL on cores $pg_cpus, ${READERS:-13} readers on $reader_cpus =="
lake run --ext "$ext" --url "$(url)" --readers "${READERS:-13}" --reader-cpus "$reader_cpus" \
  --pg-cpus "$pg_cpus" --pg-container "$container" "$@"
