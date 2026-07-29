-- Migration 0001: grace-period + ignored-handles support.
-- Upgrades the production baseline (sql/schema.sql) to add the `ignored` column
-- and the indexes backing the unresolved-count query.

-- ignored: excludes a handle from observer metrics and from the S3 hot loop.
ALTER TABLE handles ADD COLUMN ignored BOOLEAN NOT NULL DEFAULT FALSE;

-- One-time backfill of the pre-existing unresolved backlog. Marks an EXPLICIT,
-- operator-supplied list of handle_ids -- NOT every `resolved_at IS NULL` row --
-- so running this against a live system can never permanently ignore
-- freshly-ingested handles. Before running, capture the backlog and paste the
-- ids below:
--   SELECT handle_id FROM handles WHERE resolved_at IS NULL;
UPDATE handles SET ignored = TRUE
WHERE handle_id IN (
  '0x...',
  '0x...'
);

-- Rebuild the S3 hot-loop index to also exclude ignored rows.
DROP INDEX IF EXISTS idx_handles_unresolved;
CREATE INDEX idx_handles_unresolved ON handles (block_timestamp)
  WHERE NOT processed_by_s3 AND NOT ignored;

-- Serves the unresolved-count query (active, non-ignored rows) as an index-only scan.
CREATE INDEX idx_handles_active ON handles (chain_id, block_timestamp)
  INCLUDE (block_number) WHERE resolved_at IS NULL AND NOT ignored;
