-- One-off backfill, run ONCE manually at rollout against a persistent DB that
-- already has a handle backlog. Adds the `ignored` column if missing and marks
-- all currently-unresolved handles as ignored, so the pre-existing backlog is
-- excluded from observer metrics. Handles inserted afterwards default to
-- ignored = FALSE and are counted normally.

ALTER TABLE handles ADD COLUMN IF NOT EXISTS ignored BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE handles SET ignored = TRUE WHERE resolved_at IS NULL;
