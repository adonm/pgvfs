-- pgdb:// databases (src/db.rs): native DuckDB database files, beside layout v2.
-- Additive: the first database writer creates these, so the pgvfs layout
-- version does not change.
--
-- A database's files are chunks keyed by version (gen). The one writer
-- publishes a gen per fsync in one statement; a reader opens a gen and reads
-- the newest version of each chunk at or below it. A version is reaped once a
-- newer one is older than the reap grace (dbs.reap_horizon, checked by every
-- read in its own statement).
--
-- Chunk rows hold 8112-byte payloads INLINE, one row per page (the 24-byte
-- tuple header included in toast_tuple_target = 8160). An empty payload is a
-- tombstone (a truncated chunk): it reads as zeros.
CREATE SEQUENCE IF NOT EXISTS pgvfs.db_ids AS int4;

CREATE TABLE IF NOT EXISTS pgvfs.dbs (
  db_id          int4 PRIMARY KEY DEFAULT nextval('pgvfs.db_ids'),
  volume         text COLLATE "C" NOT NULL,
  name           text COLLATE "C" NOT NULL,
  committed_gen  int8 NOT NULL DEFAULT 0,
  -- Bumped by each writer attach; every write checks it (fencing).
  epoch          int8 NOT NULL DEFAULT 0,
  -- False while a writer is attached; false at attach means one died.
  clean          bool NOT NULL DEFAULT true,
  -- Versions superseded at or below this gen may be gone.
  reap_horizon   int8 NOT NULL DEFAULT 0,
  -- Gens whose superseded versions have been reaped.
  reaped_through int8 NOT NULL DEFAULT 0,
  UNIQUE (volume, name),
  CONSTRAINT db_shape CHECK (
    volume ~ '^[a-z0-9][a-z0-9._-]{0,62}$' AND octet_length(name) BETWEEN 1 AND 1024)
) WITH (fillfactor = 50);

-- Every file a database ever had, for reaping and writer recovery.
CREATE TABLE IF NOT EXISTS pgvfs.dbfileids (
  file_id     int8 PRIMARY KEY,
  db_id       int4 NOT NULL,
  retired_gen int8
);
CREATE INDEX IF NOT EXISTS dbfileids_db ON pgvfs.dbfileids (db_id);

-- Path (the database's suffix: '' or '.wal', ...) -> file and size, per gen.
CREATE TABLE IF NOT EXISTS pgvfs.dbfiles (
  db_id   int4 NOT NULL,
  path    text COLLATE "C" NOT NULL,
  gen     int8 NOT NULL,
  file_id int8,  -- NULL: removed at this gen
  size    int8,
  PRIMARY KEY (db_id, path, gen)
);

-- One row per published gen: when, and the chunks it wrote as (file_id, first,
-- end) triples. Readers key their cache by it; the reaper walks it.
CREATE TABLE IF NOT EXISTS pgvfs.dbgens (
  db_id        int4 NOT NULL,
  gen          int8 NOT NULL,
  committed_at timestamptz NOT NULL DEFAULT now(),
  changes      int8[] NOT NULL,
  PRIMARY KEY (db_id, gen)
);

CREATE TABLE IF NOT EXISTS pgvfs.dbchunks (
  file_id int8  NOT NULL,
  gen     int8  NOT NULL,
  no      int4  NOT NULL,
  data    bytea NOT NULL,
  PRIMARY KEY (file_id, no, gen),
  CONSTRAINT dbchunk_shape CHECK (no >= 0 AND octet_length(data) <= 8112)
) WITH (toast_tuple_target = 8160);

ALTER TABLE pgvfs.dbchunks ALTER COLUMN data SET STORAGE EXTERNAL;
ALTER TABLE pgvfs.dbchunks SET (autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000);
