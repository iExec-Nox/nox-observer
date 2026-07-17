-- Counts unresolved handles for a chain and reports the block range they span.
-- MIN/MAX(block_number) are NULL when no rows match (COUNT(*) is 0 in that case).
--
-- $1 = chain_id
-- $2 = grace deadline timestamp (now - grace_period, computed in Rust).
--
-- Partitions not-yet-resolved handles into:
--   unresolved: past the grace period (block_timestamp < deadline)
--   resolving:  within grace period (block_timestamp >= deadline) OR block_timestamp IS NULL
--               (NULL block_timestamp = freshest NATS-path arrivals, not yet indexed)
-- All metrics exclude ignored handle rows, except ignored_count itself.
-- latest_seen_block spans ALL handles for the chain (any state) as a reference watermark.

SELECT
  COUNT(*) FILTER (WHERE resolved_at IS NULL AND NOT ignored AND block_timestamp < $2) AS unresolved_count,
  MIN(block_number) FILTER (WHERE resolved_at IS NULL AND NOT ignored AND block_timestamp < $2) AS unresolved_oldest_block,
  MAX(block_number) FILTER (WHERE resolved_at IS NULL AND NOT ignored AND block_timestamp < $2) AS unresolved_newest_block,
  COUNT(*) FILTER (WHERE resolved_at IS NULL AND NOT ignored AND (block_timestamp >= $2 OR block_timestamp IS NULL)) AS resolving_count,
  MIN(block_number) FILTER (WHERE resolved_at IS NULL AND NOT ignored AND (block_timestamp >= $2 OR block_timestamp IS NULL)) AS resolving_oldest_block,
  MAX(block_number) FILTER (WHERE resolved_at IS NULL AND NOT ignored AND (block_timestamp >= $2 OR block_timestamp IS NULL)) AS resolving_newest_block,
  COUNT(*) FILTER (WHERE resolved_at IS NOT NULL AND NOT processed_by_subgraph AND NOT ignored) AS resolved_but_not_seen_by_subgraph,
  COUNT(*) FILTER (WHERE ignored) AS ignored_count,
  MAX(block_number) AS latest_seen_block
FROM handles
WHERE chain_id = $1;
