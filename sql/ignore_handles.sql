-- Ignore-list backfill — one-off, operator-run. NOT a migration.
--
-- Marks specific handles as `ignored` so they are excluded from observer
-- metrics/alerting and from the S3 resolver's hot loop.
-- Run this MANUALLY against a populated database, once the handles
-- to be ignored have been indexed.
--
-- It is safe to extend the handles list without removing existing ids
-- because the operation is idempotent.
--
-- Usage example: `psql "$DATABASE_URL" -f sql/ignore_handles.sql`
--

UPDATE handles SET ignored = TRUE
WHERE handle_id IN (
  '0x0000066eee2301a9105e5da7d6be716294cbaf21bdfb1c2b8006300cbce6e6fa', -- Arbitrum sepolia
  '0x0000066eee230158b2b9696532102543ccb687971d8c6b8765174ccd912072d2', -- Arbitrum sepolia
  '0x0000066eee2301b69660aba62301169484a6d10613a05a111c28fcf6beb96492'  -- Arbitrum sepolia
);
