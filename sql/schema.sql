-- TODO Move schema.sql to a migration step 0000_init.up/down.sql

CREATE TABLE handles (
    handle_id             TEXT        PRIMARY KEY,                  -- 0x... (66 chars)
    chain_id              INT         NOT NULL,
    operator              TEXT        NOT NULL,                     -- 'add', 'mul', 'transfer', ...
    caller                TEXT        NULL,
    tx_hash               TEXT        NULL,
    block_timestamp       TIMESTAMPTZ NULL,
    block_number          BIGINT      NULL,
    resolved_at           TIMESTAMPTZ NULL,                         -- set by s3_resolver when ciphertext is present
    processed_by_subgraph BOOLEAN     NOT NULL DEFAULT FALSE,
    processed_by_s3       BOOLEAN     NOT NULL DEFAULT FALSE,
    processed_by_nats     BOOLEAN     NOT NULL DEFAULT FALSE,

    CONSTRAINT handle_id_format        CHECK (handle_id ~ '^0x[a-fA-F0-9]{64}$'),
    CONSTRAINT tx_hash_format          CHECK (tx_hash IS NULL OR tx_hash ~ '^0x[a-fA-F0-9]{64}$'),
    CONSTRAINT caller_format           CHECK (caller IS NULL OR caller ~ '^0x[a-fA-F0-9]{40}$'),
    CONSTRAINT chain_id_positive       CHECK (chain_id > 0),
    CONSTRAINT block_number_positive   CHECK (block_number IS NULL OR block_number >= 0),
    CONSTRAINT resolved_after_emission CHECK (
        resolved_at IS NULL OR block_timestamp IS NULL OR resolved_at >= block_timestamp
    )
);

-- Every API query filters by chain_id
CREATE INDEX idx_handles_chain_id ON handles (chain_id);

-- Index for the s3_resolver's hot loop, which repeatedly runs:
--   WHERE NOT processed_by_s3 ORDER BY block_timestamp DESC NULLS FIRST LIMIT N
--
-- Two tricks make this cheap on every tick:
--  1. Partial (WHERE NOT processed_by_s3): the index only holds unresolved
--     handles, so it stays small. Handles drop out of it once resolved.
--  2. Ordered scan: this btree is stored block_timestamp ASC NULLS LAST, whose
--     exact reverse is DESC NULLS FIRST, so the query is served by a backward
--     index scan reading the first N rows directly, with no sort.
--
-- Ordering is newest-first by design. The order only matters when the unresolved
-- backlog exceeds one batch; below that, every unresolved row is HEAD-checked each
-- tick regardless of order. Newest-first keeps freshly-arrived handles flowing and
-- lets a backlog of stuck oldest handles sink to the tail instead of occupying the
-- batch and starving everything newer. Some result handles are observable before
-- their ciphertext is uploaded (compute runs after the on-chain request), so an
-- unresolved row is not necessarily resolvable yet; an unready handle simply
-- 404s and is retried on a later tick once its ciphertext lands.
--
-- Rows with a NULL block_timestamp arrived via the NATS path, whose messages carry
-- no block timestamp (the subgraph path fills it in once it indexes the block), so
-- NULL marks the most recent, not-yet-indexed handles. NULLS FIRST keeps that
-- recent activity at the front alongside the newest timestamped handles.
CREATE INDEX idx_handles_unresolved ON handles (block_timestamp) WHERE NOT processed_by_s3;

-- Junction table for parent-child relationships between handles.
-- Foreign keys to `handles` are intentionally NOT enforced: blockchain timing
-- (sub-second blocks, ties on blockTimestamp) and subgraph reindexing can produce
-- links before their endpoints are inserted. The link is kept and becomes valid
-- once the endpoint catches up. Queries that need handle metadata should INNER
-- JOIN with `handles` to filter out any temporarily-unresolved links.
CREATE TABLE handle_parents (
    child_handle_id  TEXT NOT NULL,
    parent_handle_id TEXT NOT NULL,

    PRIMARY KEY (child_handle_id, parent_handle_id),
    CONSTRAINT no_self_parent CHECK (child_handle_id <> parent_handle_id)
);

CREATE INDEX idx_handle_parents_parent ON handle_parents (parent_handle_id);

-- Pagination cursor of the subgraph poller. One row per configured chain, so
-- multichain deployments resume independently after a restart.
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
