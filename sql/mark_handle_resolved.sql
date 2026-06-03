-- Mark handles resolved by the S3 resolver: set resolved_at (first-writer-wins
-- via COALESCE) and processed_by_s3. The `AND NOT processed_by_s3` guard avoids
-- WAL churn on handles already resolved (mirrors the WHERE guard in
-- upsert_handle.sql). $1 is a text[] of handle_ids.
UPDATE handles
SET resolved_at     = COALESCE(resolved_at, now()),
    processed_by_s3 = true
WHERE handle_id = ANY($1)
  AND NOT processed_by_s3;
