CREATE TABLE handles (
    handle_id             TEXT        PRIMARY KEY,                  -- 0x... (66 chars)
    chain_id              INT         NOT NULL,
    operator              TEXT        NOT NULL,                     -- 'add', 'mul', 'transfer', ...
    caller                TEXT        NULL,                          -- filled by nats_consumer, may be NULL when subgraph_syncer inserts first
    tx_hash               TEXT        NOT NULL,
    block_timestamp       TIMESTAMPTZ NOT NULL,
    block_number          BIGINT      NOT NULL,
    resolved_at           TIMESTAMPTZ NULL,                         -- set by s3_poller when ciphertext is present
    processed_by_subgraph BOOLEAN     NOT NULL DEFAULT FALSE,
    processed_by_s3       BOOLEAN     NOT NULL DEFAULT FALSE,
    processed_by_nats     BOOLEAN     NOT NULL DEFAULT FALSE,

    CONSTRAINT handle_id_format        CHECK (handle_id ~ '^0x[a-fA-F0-9]{64}$'),
    CONSTRAINT tx_hash_format          CHECK (tx_hash   ~ '^0x[a-fA-F0-9]{64}$'),
    CONSTRAINT caller_format           CHECK (caller IS NULL OR caller ~ '^0x[a-fA-F0-9]{40}$'),
    CONSTRAINT chain_id_positive       CHECK (chain_id      > 0),
    CONSTRAINT block_number_positive   CHECK (block_number >= 0),
    CONSTRAINT resolved_after_emission CHECK (resolved_at IS NULL OR resolved_at >= block_timestamp)
);

-- Every API query filters by chain_id
CREATE INDEX idx_handles_chain_id ON handles (chain_id);

-- Junction table for parent-child relationships between handles (filled by subgraph_syncer).
-- Foreign keys enforce referential integrity: a parent must exist in `handles` before being referenced.
CREATE TABLE handle_parents (
    child_handle_id  TEXT NOT NULL REFERENCES handles (handle_id) ON DELETE CASCADE,
    parent_handle_id TEXT NOT NULL REFERENCES handles (handle_id) ON DELETE RESTRICT,

    PRIMARY KEY (child_handle_id, parent_handle_id),
    CONSTRAINT no_self_parent CHECK (child_handle_id <> parent_handle_id)
);

CREATE INDEX idx_handle_parents_parent ON handle_parents (parent_handle_id);
