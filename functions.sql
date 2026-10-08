-- pgvfs functions (store::FUNCTIONS). A writer whose role may replace them
-- refreshes these when the comment below differs from store::FUNCTIONS_MARKER,
-- so keep the two in step. CREATE OR REPLACE keeps the signature callers use.

-- Delete up to p_max_rows chunk rows of files unpublished more than p_grace
-- ago, and return how many rows were deleted. A file's rows go first to last,
-- so each delete is one range of its primary key. Only one reap runs at a time
-- database-wide (a try-lock); a concurrent call returns 0.
CREATE OR REPLACE FUNCTION pgvfs.reap(
  p_grace interval DEFAULT interval '10 minutes',
  p_max_rows int DEFAULT 65536
) RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  victim bigint;
  lo bigint;
  removed int := 0;
  n int;
BEGIN
  IF NOT pg_try_advisory_xact_lock(hashtext('pgvfs.reap')::bigint) THEN
    RETURN 0;
  END IF;
  FOR victim IN SELECT file_id FROM pgvfs.garbage
      WHERE queued_at < now() - p_grace ORDER BY queued_at, file_id LOOP
    SELECT min(no) INTO lo FROM pgvfs.chunks WHERE file_id = victim;
    IF lo IS NOT NULL THEN
      DELETE FROM pgvfs.chunks WHERE file_id = victim AND no < lo + (p_max_rows - removed);
      GET DIAGNOSTICS n = ROW_COUNT;
      removed := removed + n;
      EXIT WHEN removed >= p_max_rows;
    END IF;
    DELETE FROM pgvfs.garbage WHERE file_id = victim;
  END LOOP;
  RETURN removed;
END $$;

COMMENT ON FUNCTION pgvfs.reap(interval, int) IS 'pgvfs functions 3';
