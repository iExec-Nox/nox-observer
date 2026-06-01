CREATE TABLE handles (
    handle_id             TEXT        PRIMARY KEY,                  -- 0x... (66 chars)
    chain_id              INT         NOT NULL,
    operator              TEXT        NOT NULL,                     -- 'add', 'mul', 'transfer', ...
    caller                TEXT        NULL,
    tx_hash               TEXT        NULL,
    block_timestamp       TIMESTAMPTZ NULL,
    block_number          BIGINT      NULL,
    resolved_at           TIMESTAMPTZ NULL,                         -- set by s3_poller when ciphertext is present
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

-- Pagination cursor of the subgraph poller. Single-row table.
CREATE TABLE subgraph_poller_state (
    id         SMALLINT    PRIMARY KEY DEFAULT 1,
    skip       BIGINT      NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT singleton         CHECK (id = 1),
    CONSTRAINT skip_non_negative CHECK (skip >= 0)
);
