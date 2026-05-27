-- Canonical upsert used by the three writers (nats_consumer, s3_poller,
-- subgraph_syncer). Each writer fills only the columns it owns; the ON
-- CONFLICT clause merges the new signal with the existing row without ever
-- losing information already captured by another writer.
--
-- Parent relationships live in the `handle_parents` junction table and are
-- written by `subgraph_syncer` in a separate INSERT ... ON CONFLICT DO NOTHING
-- statement (see `upsert_handle_parent.sql`).

INSERT INTO handles (
    handle_id,
    chain_id,
    operator,
    caller,
    tx_hash,
    block_timestamp,
    block_number,
    resolved_at,
    processed_by_subgraph,
    processed_by_s3,
    processed_by_nats
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
)
ON CONFLICT (handle_id) DO UPDATE SET
    caller          = COALESCE(handles.caller,          EXCLUDED.caller),
    -- TODO: drop COALESCE on tx_hash/block_* once subgraph indexes ValidateInputProof events.
    tx_hash         = COALESCE(handles.tx_hash,         EXCLUDED.tx_hash),
    block_timestamp = COALESCE(handles.block_timestamp, EXCLUDED.block_timestamp),
    block_number    = COALESCE(handles.block_number,    EXCLUDED.block_number),
    resolved_at     = COALESCE(handles.resolved_at,     EXCLUDED.resolved_at),
    processed_by_subgraph = handles.processed_by_subgraph OR EXCLUDED.processed_by_subgraph,
    processed_by_s3       = handles.processed_by_s3       OR EXCLUDED.processed_by_s3,
    processed_by_nats     = handles.processed_by_nats     OR EXCLUDED.processed_by_nats
-- Skip the UPDATE entirely when the incoming row brings no new information.
-- Avoids unnecessary Write-Ahead Log writes and row locks under heavy retry
-- traffic (NATS redeliveries, S3 polling, subgraph re-syncs).
WHERE
       (handles.caller          IS NULL AND EXCLUDED.caller          IS NOT NULL)
    OR (handles.tx_hash         IS NULL AND EXCLUDED.tx_hash         IS NOT NULL)
    OR (handles.block_timestamp IS NULL AND EXCLUDED.block_timestamp IS NOT NULL)
    OR (handles.block_number    IS NULL AND EXCLUDED.block_number    IS NOT NULL)
    OR (handles.resolved_at     IS NULL AND EXCLUDED.resolved_at     IS NOT NULL)
    OR (NOT handles.processed_by_subgraph AND EXCLUDED.processed_by_subgraph)
    OR (NOT handles.processed_by_s3       AND EXCLUDED.processed_by_s3)
    OR (NOT handles.processed_by_nats     AND EXCLUDED.processed_by_nats);
