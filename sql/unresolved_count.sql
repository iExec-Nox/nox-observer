-- Counts genuinely-stuck unresolved handles for a chain: resolved_at IS NULL,
-- not ignored, and older than the grace deadline ($2). Handles still within the
-- grace window (or with a NULL block_timestamp, i.e. fresh NATS arrivals) are not
-- counted. MIN/MAX(block_number) are NULL when no rows match (or when matching
-- rows have a NULL block_number, which is nullable).
-- $1 = chain_id
-- $2 = grace deadline timestamp (now - grace_period, computed in Rust)
SELECT COUNT(*) AS unresolved,
       MIN(block_number) AS oldest_block,
       MAX(block_number) AS newest_block
FROM handles
WHERE chain_id = $1
  AND resolved_at IS NULL
  AND NOT ignored
  AND block_timestamp < $2;
