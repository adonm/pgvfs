#!/usr/bin/env bash
# bench/city.py end to end. PostgreSQL gets PG_CPUS (default 0-2: three of
# the fast cores; the sweep put Postgres at ~1/5 of the CPU work), READERS
# single-threaded readers share READER_CPUS (default the rest). Postgres is
# restarted between load and run, so pass 1 starts with its buffers cold
# (the OS page cache stays warm). COMPRESSION sets the lake's Parquet codec;
# MODE=profile times where one warm query of each kind goes; extra args go
# to city.py (e.g. --metadata-cache --arrow).
set -euo pipefail
cd "$(dirname "$0")/.."
cores=$(nproc)
pg_cpus=${PG_CPUS:-0-2}
reader_cpus=${READER_CPUS:-3-$((cores - 1))}
container="pgvfs-city-$$"
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT
py() { uv run --quiet --no-project --with "duckdb==${DUCKDB_PY:-1.5.6}" --with pyarrow python bench/city.py "$@"; }
ready() {
  for i in $(seq 1 60); do
    docker exec "$container" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && return
    sleep 0.5
  done
  docker logs "$container" >&2; exit 1
}

py download
docker run -d --name "$container" -e POSTGRES_PASSWORD=postgres --shm-size=1g \
  --cpuset-cpus "$pg_cpus" -p 127.0.0.1::5432 "${PG_IMAGE:-postgres:18}" \
  -c shared_buffers=1GB >/dev/null
ready
docker exec "$container" psql -U postgres -qc 'CREATE DATABASE lake'
url="postgres://postgres:postgres@$(docker port "$container" 5432/tcp)/lake"
ext=${EXT:-target/ext/release/pgvfs.duckdb_extension}
py load --ext "$ext" --url "$url" ${COMPRESSION:+--compression "$COMPRESSION"}
docker restart "$container" >/dev/null
ready
url="postgres://postgres:postgres@$(docker port "$container" 5432/tcp)/lake"  # restart re-maps the port
if [ "${MODE:-run}" = profile ]; then
  echo "== profile: Postgres on cores $pg_cpus, one reader on $reader_cpus =="
  taskset -c "$reader_cpus" \
    uv run --quiet --no-project --with "duckdb==${DUCKDB_PY:-1.5.6}" --with pyarrow python bench/city.py profile \
    --ext "$ext" --url "$url" "$@"
  exit
fi
echo "== Postgres on cores $pg_cpus, ${READERS:-13} readers on $reader_cpus =="
py run --ext "$ext" --url "$url" --readers "${READERS:-13}" --reader-cpus "$reader_cpus" \
  --pg-cpus "$pg_cpus" --pg-container "$container" "$@"
