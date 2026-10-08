-- pgvfs layout v2 (store::LAYOUT_VERSION). Installed by the first writer, under
-- a transaction-scoped advisory lock (store::ensure_writable); any layout change
-- requires a fresh database. The functions are in functions.sql.
-- PostgreSQL 11+ (toast_tuple_target); no extensions.
--
-- Files are immutable: a write streams rows under a fresh file_id and
-- publishes (volume, path) -> file_id when the file closes, replacing any
-- previous file there. Readers address rows by file_id only, so a file_id
-- names the same bytes forever (DuckDB's cache version tag).
--
-- Chunk rows hold 8120-byte payloads kept INLINE: 8 + 4 + 4 + 8120 bytes plus
-- the 24-byte tuple header fit toast_tuple_target = 8160, so each row is one
-- 8160-byte tuple per 8 KB page with no TOAST indirection.
CREATE SCHEMA pgvfs;

CREATE TABLE pgvfs.layout (version int4 NOT NULL);

CREATE SEQUENCE pgvfs.file_ids;

-- path collates "C": prefix listings are primary-key range scans in byte order.
CREATE TABLE pgvfs.files (
  volume     text COLLATE "C" NOT NULL,
  path       text COLLATE "C" NOT NULL,
  file_id    int8             NOT NULL UNIQUE,
  size       int8             NOT NULL,
  created_at timestamptz      NOT NULL DEFAULT now(),
  PRIMARY KEY (volume, path),
  CONSTRAINT file_shape CHECK (
    volume ~ '^[a-z0-9][a-z0-9._-]{0,62}$' AND octet_length(path) BETWEEN 1 AND 1024 AND
    size BETWEEN 0 AND 2147483648::int8 * 8120)
);

-- One heap: a read is `file_id = $1 AND no BETWEEN ..`, a primary-key range.
-- (A relation holds up to 32 TiB, about 31.7 TiB of file data here.)
CREATE TABLE pgvfs.chunks (
  file_id int8  NOT NULL,
  no      int4  NOT NULL,
  data    bytea NOT NULL,
  PRIMARY KEY (file_id, no),
  CONSTRAINT chunk_shape CHECK (no >= 0 AND octet_length(data) BETWEEN 1 AND 8120)
) WITH (toast_tuple_target = 8160);

-- Never compress or move payloads out of line.
ALTER TABLE pgvfs.chunks ALTER COLUMN data SET STORAGE EXTERNAL;

-- Reaping deletes whole files' rows at once: vacuum early so their pages
-- are reused rather than growing the table (the default waits for 20% dead).
ALTER TABLE pgvfs.chunks SET (autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000);

-- Unpublished files. Readers take no snapshot across statements, so rows
-- outlive the unpublish by a grace period (PGVFS_REAP_GRACE_S) for reads that
-- are running. Any writer reaps (pgvfs.reap takes a try-lock).
CREATE TABLE pgvfs.garbage (
  file_id   int8 PRIMARY KEY,
  queued_at timestamptz NOT NULL DEFAULT now()
);
