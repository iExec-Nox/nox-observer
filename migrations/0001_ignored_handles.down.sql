-- Down: revert migration 0001, back to the sql/schema.sql baseline.
-- Mirrors the up migration in reverse order.


-- 3. Remove the sql function and index.
DROP FUNCTION IF EXISTS handles_unresolved_since(integer);
DROP INDEX IF EXISTS idx_handles_active;

-- 2. Restore the old table `subgraph_poller_state`. Data is unrecoverable.
DROP TABLE IF EXISTS subgraph_poller_state;
CREATE TABLE subgraph_poller_state (
    chain_id   INT         PRIMARY KEY,
    skip       BIGINT      NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chain_id_positive CHECK (chain_id > 0),
    CONSTRAINT skip_non_negative CHECK (skip >= 0)
);

-- 1. Remove `ignored` column and the old index.
DROP INDEX IF EXISTS idx_handles_unresolved;
CREATE INDEX idx_handles_unresolved ON handles (block_timestamp) WHERE NOT processed_by_s3;
-- Drop the column last, after the index that references it.
ALTER TABLE handles DROP COLUMN IF EXISTS ignored;
