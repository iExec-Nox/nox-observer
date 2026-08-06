-- Upgrade from the baseline sql/schema.sql.
-- Migration 0001:
--    - handles: ignored-handles support.
--    - subgraph_poller_state: block-based cursor instead of the old page offset


-- `ignored` excludes a handle from the S3 resolver's hot loop.
-- The ids to ignore are environment-specific data, so they are
-- deliberately NOT part of this migration: fill and run
-- sql/ignore_handles.sql manually once all handles have been indexed.
ALTER TABLE handles ADD COLUMN ignored BOOLEAN NOT NULL DEFAULT FALSE;

-- Rebuild the S3 hot-loop index to exclude ignored rows.
DROP INDEX IF EXISTS idx_handles_unresolved;
CREATE INDEX idx_handles_unresolved ON handles (block_timestamp)
  WHERE NOT processed_by_s3 AND NOT ignored;


-- Drop and recreate the table to use the new column `cursor_block`
-- and reset data.
DROP TABLE IF EXISTS subgraph_poller_state;
CREATE TABLE subgraph_poller_state (
    chain_id   INT         PRIMARY KEY,
    -- Block the poller has paginated up to (the composite cursor's block part).
    -- It may not be fully processed: a page can end mid-block, and the resume
    -- re-scans this block from its start (idempotent upserts).
    cursor_block BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chain_id_positive CHECK (chain_id > 0),
    CONSTRAINT cursor_block_non_negative CHECK (cursor_block >= 0)
);
