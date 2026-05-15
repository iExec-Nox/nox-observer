CREATE TABLE handles (
    handle_id             TEXT        PRIMARY KEY,                  -- 0x... (66 chars)
    chain_id              BIGINT      NOT NULL,
    operator              TEXT        NOT NULL,                     -- 'add', 'mul', 'transfer', ...
    caller                TEXT        NOT NULL,
    tx_hash               TEXT        NOT NULL,
    block_timestamp       TIMESTAMPTZ NOT NULL,
    block_number          BIGINT      NOT NULL,
    resolved_at           TIMESTAMPTZ NULL,                         -- set by s3_poller when ciphertext is present
    parent_handles        TEXT[]      NOT NULL DEFAULT '{}',        -- filled by subgraph_syncer
    processed_by_subgraph BOOLEAN     NOT NULL DEFAULT FALSE,
    processed_by_s3       BOOLEAN     NOT NULL DEFAULT FALSE,
    processed_by_nats     BOOLEAN     NOT NULL DEFAULT FALSE,

    CONSTRAINT handle_id_format        CHECK (handle_id ~ '^0x[a-fA-F0-9]{64}$'),
    CONSTRAINT tx_hash_format          CHECK (tx_hash   ~ '^0x[a-fA-F0-9]{64}$'),
    CONSTRAINT caller_format           CHECK (caller    ~ '^0x[a-fA-F0-9]{40}$'),
    CONSTRAINT chain_id_positive       CHECK (chain_id      > 0),
    CONSTRAINT block_number_positive   CHECK (block_number >= 0),
    CONSTRAINT resolved_after_emission CHECK (resolved_at IS NULL OR resolved_at >= block_timestamp)
);

-- Every API query filters by chain_id
CREATE INDEX idx_handles_chain_id ON handles (chain_id);
