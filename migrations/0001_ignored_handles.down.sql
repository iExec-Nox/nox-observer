-- Down: revert migration 0001 (ignored-handles support).

-- Restore the pre-0001 hot-loop index (without the NOT ignored predicate).
DROP INDEX IF EXISTS idx_handles_unresolved;
CREATE INDEX idx_handles_unresolved ON handles (block_timestamp) WHERE NOT processed_by_s3;

-- Drop the column last, after the index that references it.
ALTER TABLE handles DROP COLUMN IF EXISTS ignored;
