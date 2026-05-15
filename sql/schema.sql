-- Initial schema for nox-observer. A single `handles` table aggregates
-- signals from the three independent sources (NATS JetStream, S3,
-- subgraph) into one row per handle. It is the source of truth queried by
-- the API and by operators when debugging a stuck handle.

CREATE TABLE handles (
    handle_id             TEXT        PRIMARY KEY,                  -- 0x... (66 chars)
    chain_id              INTEGER     NOT NULL,
    operator              TEXT        NOT NULL,                     -- 'add', 'mul', 'transfer', ...
    caller                TEXT        NOT NULL,
    tx_hash               TEXT        NOT NULL,
    block_timestamp       BIGINT      NOT NULL,
    block_number          BIGINT      NOT NULL,
    resolved_at           BIGINT      NULL,                         -- set by s3_poller when ciphertext is present
    parent_handles        TEXT[]      NOT NULL DEFAULT '{}',        -- filled by subgraph_syncer
    processed_by_subgraph BOOLEAN     NOT NULL DEFAULT FALSE,
    processed_by_s3       BOOLEAN     NOT NULL DEFAULT FALSE,
    processed_by_nats     BOOLEAN     NOT NULL DEFAULT FALSE
);
