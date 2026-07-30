-- Migration 0001: grace-period + ignored-handles support.
-- Upgrades the production baseline (sql/schema.sql) to add the `ignored` column
-- and the indexes backing the unresolved-count query.

-- ignored: excludes a handle from observer metrics and from the S3 hot loop.
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

-- Rebuild the S3 hot-loop index to also exclude ignored rows.
DROP INDEX IF EXISTS idx_handles_unresolved;
CREATE INDEX idx_handles_unresolved ON handles (block_timestamp)
  WHERE NOT processed_by_s3 AND NOT ignored;

-- Serves the unresolved-count query (active, non-ignored rows) as an index-only scan.
CREATE INDEX idx_handles_active ON handles (chain_id, block_timestamp)
  INCLUDE (block_number) WHERE resolved_at IS NULL AND NOT ignored;
