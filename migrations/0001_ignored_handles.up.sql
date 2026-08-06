-- Upgrade from the baseline sql/schema.sql.
-- Migration 0001: ignored-handles support.
--
-- `ignored` excludes a handle from the S3 resolver's hot loop.
-- The ids to ignore are environment-specific data, so they are
-- deliberately NOT part of this migration: fill and run
-- sql/ignore_handles.sql manually once all handles have been indexed.
ALTER TABLE handles ADD COLUMN ignored BOOLEAN NOT NULL DEFAULT FALSE;

-- Rebuild the S3 hot-loop index to exclude ignored rows.
DROP INDEX IF EXISTS idx_handles_unresolved;
CREATE INDEX idx_handles_unresolved ON handles (block_timestamp)
  WHERE NOT processed_by_s3 AND NOT ignored;
