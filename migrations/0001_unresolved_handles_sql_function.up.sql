-- Upgrade from the baseline sql/schema.sql.
-- Migration 0001:
--    - subgraph_poller_state: block-based cursor instead of the old page offset
--    - handles: ignored-handles support.
--    - handles_unresolved_since: Hasura-tracked sql function + supporting index


-- 1. Drop and recreate the table `subgraph_poller_state` to use the new column
-- `cursor_block` and to reset data.
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

-- 2. Add `ignored` column and rebuild the S3 hot-loop index to exclude ignored rows.
-- Fill and run sql/ignore_handles.sql manually once all handles have been indexed.
ALTER TABLE handles ADD COLUMN ignored BOOLEAN NOT NULL DEFAULT FALSE;
DROP INDEX IF EXISTS idx_handles_unresolved;
CREATE INDEX idx_handles_unresolved ON handles (block_timestamp)
  WHERE NOT processed_by_s3 AND NOT ignored;

-- 3. Create function to expose unresolved handles after a grace period and add
-- supporting index.
CREATE OR REPLACE FUNCTION handles_unresolved_since(age_in_seconds integer)
RETURNS SETOF handles
LANGUAGE sql
STABLE
AS $$
  SELECT *
  FROM handles
  WHERE resolved_at IS NULL
    AND NOT ignored
    AND block_timestamp < now() - make_interval(secs => age_in_seconds)
$$;
CREATE INDEX idx_handles_active ON handles (chain_id, block_timestamp)
  INCLUDE (block_number) -- useful for Hasura's _aggregate (block range min/max)
  WHERE resolved_at IS NULL AND NOT ignored;
