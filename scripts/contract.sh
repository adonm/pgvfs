#!/usr/bin/env bash
# Storage contract (and, with an extension path, the DuckDB end-to-end test)
# against a disposable PostgreSQL. PG_IMAGE picks the server version.
#
#   scripts/contract.sh [path/to/pgvfs.duckdb_extension]
set -euo pipefail
cd "$(dirname "$0")/.."
image=${PG_IMAGE:-postgres:18}
container="pgvfs-contract-$$"
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT

docker run -d --name "$container" -e POSTGRES_PASSWORD=postgres \
  -p 127.0.0.1::5432 "$image" >/dev/null
for i in $(seq 1 60); do
  docker exec "$container" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && break
  [ "$i" = 60 ] && { docker logs "$container" >&2; exit 1; }
  sleep 0.5
done
psql() { docker exec -i "$container" psql -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
psql -c 'CREATE DATABASE pgvfs_t' -c 'CREATE DATABASE empty_t' -c 'CREATE DATABASE s3_t' \
  -c "CREATE ROLE reader LOGIN PASSWORD 'reader'"
psql -d s3_t -c 'CREATE SCHEMA s3p' -c 'CREATE TABLE s3p.chunks (x int)'
# SELECT-only, granted ahead of the schema the writer will create.
psql -d pgvfs_t -c 'ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT USAGE ON SCHEMAS TO reader' \
  -c 'ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT SELECT ON TABLES TO reader'

addr=$(docker port "$container" 5432/tcp)
base="postgres://postgres:postgres@$addr"
echo "== $(psql -tAc 'SHOW server_version') =="
PGVFS_TEST_DB_URL="$base/pgvfs_t" PGVFS_TEST_EMPTY_DB_URL="$base/empty_t" \
  PGVFS_TEST_S3_DB_URL="$base/s3_t" PGVFS_TEST_READER_URL="postgres://reader:reader@$addr/pgvfs_t" \
  PGVFS_POOL_MIN=1 PGVFS_POOL_MAX=8 \
  cargo test --locked --test store_contract -- --include-ignored --test-threads=1

if [ -n "${1:-}" ]; then
  PGVFS_TEST_URL="$base/pgvfs_t" uv run --quiet --with "duckdb==${DUCKDB_PY:-1.5.6}" \
    python extension/test/e2e.py "$1"
fi
