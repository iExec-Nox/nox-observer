-- Down: revert migration 0001 (grace-period + ignored-handles support).
DROP INDEX IF EXISTS idx_handles_active;
DROP INDEX IF EXISTS idx_handles_latest_chain_block;
DROP INDEX IF EXISTS idx_handles_resolved_but_unseen_by_subgraph;

-- Restore the pre-0001 hot-loop index (without the NOT ignored predicate).
DROP INDEX IF EXISTS idx_handles_unresolved;
CREATE INDEX idx_handles_unresolved ON handles (block_timestamp) WHERE NOT processed_by_s3;

-- Drop the column last after related indexes.
ALTER TABLE handles DROP COLUMN IF EXISTS ignored;
