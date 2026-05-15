-- Canonical upsert used by the three writers (nats_consumer, s3_poller,
-- subgraph_syncer). Each writer fills only the columns it owns; the ON
-- CONFLICT clause merges the new signal with the existing row without ever
-- losing information already captured by another writer.

INSERT INTO handles (
    handle_id,
    chain_id,
    operator,
    caller,
    tx_hash,
    block_timestamp,
    block_number,
    resolved_at,
    parent_handles,
    processed_by_subgraph,
    processed_by_s3,
    processed_by_nats
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
)
ON CONFLICT (handle_id) DO UPDATE SET
    resolved_at = COALESCE(handles.resolved_at, EXCLUDED.resolved_at),
    parent_handles = CASE
        WHEN handles.parent_handles = '{}' THEN EXCLUDED.parent_handles
        ELSE handles.parent_handles
    END,
    processed_by_subgraph = handles.processed_by_subgraph OR EXCLUDED.processed_by_subgraph,
    processed_by_s3       = handles.processed_by_s3       OR EXCLUDED.processed_by_s3,
    processed_by_nats     = handles.processed_by_nats     OR EXCLUDED.processed_by_nats
-- Skip the UPDATE entirely when the incoming row brings no new information.
-- Avoids unnecessary Write-Ahead Log writes and row locks under heavy retry
-- traffic (NATS redeliveries, S3 polling, subgraph re-syncs).
WHERE
       (handles.resolved_at    IS NULL AND EXCLUDED.resolved_at    IS NOT NULL)
    OR (handles.parent_handles = '{}'  AND EXCLUDED.parent_handles <> '{}')
    OR (NOT handles.processed_by_subgraph AND EXCLUDED.processed_by_subgraph)
    OR (NOT handles.processed_by_s3       AND EXCLUDED.processed_by_s3)
    OR (NOT handles.processed_by_nats     AND EXCLUDED.processed_by_nats);
