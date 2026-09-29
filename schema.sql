-- pgvfs layout v1 (store::LAYOUT_VERSION). Installed by the writer under its
-- advisory lock; any layout change requires a fresh database.
-- PostgreSQL 11+ (hash partitions, toast_tuple_target); no extensions.
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

-- 32 hash partitions: each relation caps at 32 TiB, and every read is
-- `file_id = $1`, which prunes to one partition.
CREATE TABLE pgvfs.chunks (
  file_id int8  NOT NULL,
  no      int4  NOT NULL,
  data    bytea NOT NULL,
  PRIMARY KEY (file_id, no),
  CONSTRAINT chunk_shape CHECK (no >= 0 AND octet_length(data) BETWEEN 1 AND 8120)
) PARTITION BY HASH (file_id);

DO $$
BEGIN
  FOR i IN 0..31 LOOP
    EXECUTE format(
      'CREATE TABLE pgvfs.chunks_%s PARTITION OF pgvfs.chunks '
      'FOR VALUES WITH (MODULUS 32, REMAINDER %s) WITH (toast_tuple_target = 8160)',
      lpad(i::text, 2, '0'), i);
  END LOOP;
END $$;

-- Never compress or move payloads out of line (recurses to the partitions).
ALTER TABLE pgvfs.chunks ALTER COLUMN data SET STORAGE EXTERNAL;

-- Unpublished files. Readers take no snapshot across statements, so rows
-- outlive the unpublish by a grace period for queries that already opened
-- the file. Only the writer reaps.
CREATE TABLE pgvfs.garbage (
  file_id   int8 PRIMARY KEY,
  queued_at timestamptz NOT NULL DEFAULT now()
);

-- Delete up to p_max_rows chunk rows of files queued before the grace period.
CREATE FUNCTION pgvfs.reap(
  p_grace interval DEFAULT interval '10 minutes',
  p_max_rows int DEFAULT 65536
) RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  victim bigint;
  removed int := 0;
  n int;
BEGIN
  FOR victim IN SELECT file_id FROM pgvfs.garbage
      WHERE queued_at < now() - p_grace ORDER BY queued_at, file_id LOOP
    DELETE FROM pgvfs.chunks WHERE file_id = victim
      AND no IN (SELECT no FROM pgvfs.chunks WHERE file_id = victim
                 ORDER BY no LIMIT p_max_rows - removed);
    GET DIAGNOSTICS n = ROW_COUNT;
    removed := removed + n;
    EXIT WHEN removed >= p_max_rows;
    DELETE FROM pgvfs.garbage WHERE file_id = victim;
  END LOOP;
  RETURN removed;
END $$;
