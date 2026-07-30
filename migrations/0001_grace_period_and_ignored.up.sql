-- Upgrade from the baseline sql/schema.sql.
-- Migration 0001:
--    - grace-period for unresolved handles
--    - ignored-handles support.
--    - new indexes backing the stats query


-- New column: excludes a handle from observer metrics and the S3 hot loop.
ALTER TABLE handles ADD COLUMN ignored BOOLEAN NOT NULL DEFAULT FALSE;

-- One-time backfill of the pre-existing unresolved handles.
-- Before running, paste the ids list it below:
UPDATE handles SET ignored = TRUE
WHERE handle_id IN (
  '0x0000066eee2301a9105e5da7d6be716294cbaf21bdfb1c2b8006300cbce6e6fa',
  '0x0000066eee230158b2b9696532102543ccb687971d8c6b8765174ccd912072d2',
  '0x0000066eee2300bababababababababababababababababababababababababa',
  '0x0000066eee2301b69660aba62301169484a6d10613a05a111c28fcf6beb96492'
);

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
