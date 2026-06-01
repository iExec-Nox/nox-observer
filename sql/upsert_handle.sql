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
    tx_hash         = COALESCE(handles.tx_hash,         EXCLUDED.tx_hash),
    block_timestamp = COALESCE(handles.block_timestamp, EXCLUDED.block_timestamp),
    block_number    = COALESCE(handles.block_number,    EXCLUDED.block_number),
    resolved_at     = COALESCE(handles.resolved_at,     EXCLUDED.resolved_at),
    processed_by_subgraph = handles.processed_by_subgraph OR EXCLUDED.processed_by_subgraph,
    processed_by_s3       = handles.processed_by_s3       OR EXCLUDED.processed_by_s3,
    processed_by_nats     = handles.processed_by_nats     OR EXCLUDED.processed_by_nats
WHERE
       (handles.caller          IS NULL AND EXCLUDED.caller          IS NOT NULL)
    OR (handles.tx_hash         IS NULL AND EXCLUDED.tx_hash         IS NOT NULL)
    OR (handles.block_timestamp IS NULL AND EXCLUDED.block_timestamp IS NOT NULL)
    OR (handles.block_number    IS NULL AND EXCLUDED.block_number    IS NOT NULL)
    OR (handles.resolved_at     IS NULL AND EXCLUDED.resolved_at     IS NOT NULL)
    OR (NOT handles.processed_by_subgraph AND EXCLUDED.processed_by_subgraph)
    OR (NOT handles.processed_by_s3       AND EXCLUDED.processed_by_s3)
    OR (NOT handles.processed_by_nats     AND EXCLUDED.processed_by_nats);
