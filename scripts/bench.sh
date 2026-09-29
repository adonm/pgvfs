#!/usr/bin/env bash
# bench/readers.py against a disposable PostgreSQL; extra args pass through
# (e.g. --parts 100 --readers 1,4,16). PG_IMAGE picks the server version.
set -euo pipefail
cd "$(dirname "$0")/.."
image=${PG_IMAGE:-postgres:18}
container="pgvfs-bench-$$"
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT

docker run -d --name "$container" -e POSTGRES_PASSWORD=postgres --shm-size=1g \
  -p 127.0.0.1::5432 "$image" -c shared_buffers=1GB -c max_connections=400 >/dev/null
for i in $(seq 1 60); do
  docker exec "$container" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && break
  [ "$i" = 60 ] && { docker logs "$container" >&2; exit 1; }
  sleep 0.5
done
docker exec "$container" psql -U postgres -qc 'CREATE DATABASE lake'
addr=$(docker port "$container" 5432/tcp)
echo "== $image, $(nproc) cores =="
uv run --quiet --no-project --with "duckdb==${DUCKDB_PY:-1.5.6}" python bench/readers.py \
  --ext "${EXT:-target/ext/release/pgvfs.duckdb_extension}" --url "postgres://postgres:postgres@$addr/lake" "$@"
