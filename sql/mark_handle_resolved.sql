-- Mark handles resolved by the S3 resolver. resolved_at is the S3 object's
-- LastModified (when the ciphertext landed in S3); first-writer-wins via COALESCE.
-- GREATEST guards the resolved_after_emission CHECK against clock skew between the
-- chain and S3, and is a no-op whenever LastModified is at or after block_timestamp.
-- $1 = text[] handle_ids, $2 = timestamptz[] resolved_at, positionally paired.
UPDATE handles AS h
SET resolved_at     = COALESCE(h.resolved_at, GREATEST(v.resolved_at, h.block_timestamp)),
    processed_by_s3 = true
FROM (SELECT UNNEST($1::text[]) AS handle_id,
             UNNEST($2::timestamptz[]) AS resolved_at) AS v
WHERE h.handle_id = v.handle_id
  AND NOT h.processed_by_s3;
