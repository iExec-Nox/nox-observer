-- Counts unresolved handles for a chain and reports the block range they span.
-- MIN/MAX(block_number) are NULL when no rows match (COUNT(*) is 0), and also
-- when all matching rows have a NULL block_number (it is a nullable column).
-- $1 = chain_id
SELECT COUNT(*) AS unresolved,
       MIN(block_number) AS oldest_block,
       MAX(block_number) AS newest_block
FROM handles
WHERE resolved_at IS NULL
  AND chain_id = $1;
