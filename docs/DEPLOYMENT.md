# Deployment runbook

Deploy notes per version to centralize **config breaking changes** and **manual steps**
required to move to a version. Schema migrations under `migrations/` apply automatically
on startup.

> [!NOTE]
> For user-facing breaking changes, check the changelog file.

## v0.1.0 - Unreleased

### Database

- Migration `0001_ignored_handles`:
  - adds `handles.ignored` and narrows the S3 hot-loop index to exclude those rows.
  - recreates `subgraph_poller_state` with `cursor_block` (block number) in place of
    `skip` (handle offset).

### Config

- No config changes.

### Manual steps

- Run `sql/ignore_handles.sql` against the populated DB **after** all handles have been indexed.

  ```bash
  psql "$DATABASE_URL" -f sql/ignore_handles.sql
  ```
