-- Migration 0001: grace-period + ignored-handles support.
-- Upgrades the production baseline (sql/schema.sql) to add the `ignored` column
-- and the indexes backing the unresolved-count query.

-- ignored: excludes a handle from observer metrics and from the S3 hot loop.
ALTER TABLE handles ADD COLUMN ignored BOOLEAN NOT NULL DEFAULT FALSE;

-- One-time backfill: ignore the pre-existing unresolved backlog.
UPDATE handles SET ignored = TRUE WHERE resolved_at IS NULL;

-- Rebuild the S3 hot-loop index to also exclude ignored rows.
DROP INDEX IF EXISTS idx_handles_unresolved;
CREATE INDEX idx_handles_unresolved ON handles (block_timestamp)
  WHERE NOT processed_by_s3 AND NOT ignored;

-- Serves the unresolved-count query (active, non-ignored rows) as an index-only scan.
CREATE INDEX idx_handles_active ON handles (chain_id, block_timestamp)
  INCLUDE (block_number) WHERE resolved_at IS NULL AND NOT ignored;
