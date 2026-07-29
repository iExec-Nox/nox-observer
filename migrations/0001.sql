-- Upgrades from the baseline sql/schema.sql to the v0.1.0 schema.
--
-- Migration 0001:
--    - grace-period for unresolved handles
--    - ignored-handles support.

-- =========================
-- ==== Table `handles` ====
-- =========================

-- New column: excludes a handle from observer metrics and the S3 hot loop.
ALTER TABLE handles ADD COLUMN ignored BOOLEAN NOT NULL DEFAULT FALSE;

-- One-time backfill: ignore the pre-existing unresolved handles.
UPDATE handles SET ignored = TRUE WHERE resolved_at IS NULL;

-- Rebuild the S3 hot-loop index to exclude ignored rows.
-- Serves the s3_resolver hot loop.
DROP INDEX IF EXISTS idx_handles_unresolved;
CREATE INDEX idx_handles_unresolved ON handles (block_timestamp)
  WHERE NOT processed_by_s3 AND NOT ignored;

-- Serves the unresolved/resolving buckets of the unresolved-count query as
-- an index-only scan over just the active backlog.
CREATE INDEX idx_handles_active ON handles (chain_id, block_timestamp)
  INCLUDE (block_number) WHERE resolved_at IS NULL AND NOT ignored;

-- Serves latest_seen_block = MAX(block_number) per chain in ~1 row.
CREATE INDEX idx_handles_latest_chain_block ON handles (chain_id, block_number);

-- Serves the resolved-but-not-seen-by-subgraph bucket (small, transient
-- population).
CREATE INDEX idx_handles_resolved_but_unseen_by_subgraph ON handles (chain_id)
  WHERE resolved_at IS NOT NULL AND NOT processed_by_subgraph AND NOT ignored;
