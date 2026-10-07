#!/usr/bin/env bash
# Storage contract (and, with an extension path, the DuckDB end-to-end test)
# against disposable PostgreSQL servers: a primary, a streaming standby of it,
# and a TLS-only server with a throwaway CA. PG_IMAGE picks the version.
#
#   scripts/contract.sh [path/to/pgvfs.duckdb_extension]
set -euo pipefail
cd "$(dirname "$0")/.."
image=${PG_IMAGE:-postgres:18}
name="pgvfs-contract-$$"
certs=$(mktemp -d)
cleanup() {
  docker rm -f "$name" "$name-standby" "$name-tls" >/dev/null 2>&1 || true
  docker network rm "$name" >/dev/null 2>&1 || true
  rm -rf "$certs"
}
trap cleanup EXIT

ready() {
  for i in $(seq 1 120); do
    docker exec "$1" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && return
    [ "$i" = 120 ] && { docker logs "$1" >&2; exit 1; }
    sleep 0.5
  done
}
psql() { docker exec -i "$name" psql -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

docker network create "$name" >/dev/null
docker run -d --name "$name" --network "$name" --network-alias primary \
  -e POSTGRES_PASSWORD=postgres -p 127.0.0.1::5432 "$image" >/dev/null
ready "$name"
psql -c 'CREATE DATABASE pgvfs_t' -c 'CREATE DATABASE empty_t' -c 'CREATE DATABASE s3_t' \
  -c "CREATE ROLE reader LOGIN PASSWORD 'reader'"
psql -d s3_t -c 'CREATE SCHEMA s3p' -c 'CREATE TABLE s3p.chunks (x int)'
# SELECT-only, granted ahead of the schema the writer will create.
psql -d pgvfs_t -c 'ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT USAGE ON SCHEMAS TO reader' \
  -c 'ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT SELECT ON TABLES TO reader'

# A streaming standby of the primary.
docker exec "$name" bash -c 'echo "host replication all all md5" >> "$PGDATA/pg_hba.conf"'
psql -c 'SELECT pg_reload_conf()' >/dev/null
docker run -d --name "$name-standby" --network "$name" -e PGPASSWORD=postgres \
  -p 127.0.0.1::5432 --entrypoint bash "$image" -c '
    set -e; d=/var/lib/postgresql/standby
    pg_basebackup -h primary -U postgres -D "$d" -R -X stream --checkpoint=fast
    chown -R postgres:postgres "$d"; chmod 700 "$d"
    exec gosu postgres postgres -D "$d"' >/dev/null
ready "$name-standby"

# A TLS-only server, its certificate signed by a throwaway CA.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=pgvfs-test-ca \
  -keyout "$certs/ca.key" -out "$certs/ca.crt" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj /CN=localhost \
  -keyout "$certs/server.key" -out "$certs/server.csr" 2>/dev/null
printf 'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n' >"$certs/ext"
openssl x509 -req -in "$certs/server.csr" -CA "$certs/ca.crt" -CAkey "$certs/ca.key" \
  -CAcreateserial -days 1 -extfile "$certs/ext" -out "$certs/server.crt" 2>/dev/null
chmod 644 "$certs"/*
docker run -d --name "$name-tls" -e POSTGRES_PASSWORD=postgres -v "$certs:/certs:ro,z" \
  -p 127.0.0.1::5432 --entrypoint bash "$image" -c '
    cp /certs/server.crt /certs/server.key /tmp/ && chown postgres /tmp/server.*
    chmod 600 /tmp/server.key
    exec docker-entrypoint.sh postgres -c ssl=on \
      -c ssl_cert_file=/tmp/server.crt -c ssl_key_file=/tmp/server.key' >/dev/null
ready "$name-tls"

addr=$(docker port "$name" 5432/tcp)
base="postgres://postgres:postgres@$addr"
echo "== $(psql -tAc 'SHOW server_version') =="
PGVFS_TEST_DB_URL="$base/pgvfs_t" PGVFS_TEST_EMPTY_DB_URL="$base/empty_t" \
  PGVFS_TEST_S3_DB_URL="$base/s3_t" PGVFS_TEST_READER_URL="postgres://reader:reader@$addr/pgvfs_t" \
  PGVFS_TEST_STANDBY_URL="postgres://postgres:postgres@$(docker port "$name-standby" 5432/tcp)/pgvfs_t" \
  PGVFS_TEST_TLS_URL="postgres://postgres:postgres@localhost:$(docker port "$name-tls" 5432/tcp | cut -d: -f2)/postgres?sslmode=require" \
  PGVFS_TEST_TLS_CA="$certs/ca.crt" PGVFS_POOL_MIN=1 PGVFS_POOL_MAX=8 \
  cargo test --locked --test store_contract -- --include-ignored --test-threads=1

if [ -n "${1:-}" ]; then
  PGVFS_TEST_URL="$base/pgvfs_t" \
    PGVFS_TEST_STANDBY_URL="postgres://postgres:postgres@$(docker port "$name-standby" 5432/tcp)/pgvfs_t" \
    uv run --quiet --no-project --with "duckdb==${DUCKDB_PY:-1.5.6}" \
    python extension/test/e2e.py "$1"
fi
