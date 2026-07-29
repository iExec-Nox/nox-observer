-- Counts unresolved handles for a chain and reports the block range they span.
--
-- $1 = chain_id
-- $2 = grace deadline timestamp (now - grace_period, computed in Rust).
--
-- Split into four per-bucket subqueries (rather than one aggregate over all
-- rows) so each bucket hits a targeted index instead of forcing a single
-- full per-chain scan:
--   a: unresolved/resolving buckets, served by idx_handles_active
--   r: resolved-but-not-seen-by-subgraph bucket, served by idx_handles_resolved_unseen
--   i: ignored count, still a full per-chain scan (small, cheap FILTER)
--   w: latest_seen_block watermark, served by idx_handles_chain_block
--
-- Partitions not-yet-resolved handles into:
--   unresolved: past the grace period (block_timestamp < deadline)
--   resolving:  within grace period (block_timestamp >= deadline) OR block_timestamp IS NULL
--               (NULL block_timestamp = freshest NATS-path arrivals, not yet indexed)
--
-- All metrics exclude ignored handle rows, except ignored_count itself.
-- latest_seen_block spans ALL handles for the chain (any state) as a reference watermark.


SELECT
  a.unresolved_count, a.unresolved_oldest_block, a.unresolved_newest_block,
  a.resolving_count, a.resolving_oldest_block, a.resolving_newest_block,
  r.resolved_but_not_seen_by_subgraph,
  i.ignored_count,
  w.latest_seen_block
FROM
  (SELECT
     COUNT(*) FILTER (WHERE block_timestamp < $2) AS unresolved_count,
     MIN(block_number) FILTER (WHERE block_timestamp < $2) AS unresolved_oldest_block,
     MAX(block_number) FILTER (WHERE block_timestamp < $2) AS unresolved_newest_block,
     COUNT(*) FILTER (WHERE block_timestamp >= $2 OR block_timestamp IS NULL) AS resolving_count,
     MIN(block_number) FILTER (WHERE block_timestamp >= $2 OR block_timestamp IS NULL) AS resolving_oldest_block,
     MAX(block_number) FILTER (WHERE block_timestamp >= $2 OR block_timestamp IS NULL) AS resolving_newest_block
   FROM handles
   WHERE chain_id = $1 AND resolved_at IS NULL AND NOT ignored) a,
  (SELECT COUNT(*) AS resolved_but_not_seen_by_subgraph
   FROM handles
   WHERE chain_id = $1 AND resolved_at IS NOT NULL AND NOT processed_by_subgraph AND NOT ignored) r,
  (SELECT COUNT(*) AS ignored_count
   FROM handles WHERE chain_id = $1 AND ignored) i,
  (SELECT MAX(block_number) AS latest_seen_block
   FROM handles WHERE chain_id = $1) w;
