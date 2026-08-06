# Deployment runbook

Deploy notes per version to centralize **config breaking changes** and **manual steps**
required to move to a version. Schema migrations under `migrations/` apply automatically
on startup.

For user-facing breaking changes, check the changelog file.

## v0.1.0 - Unreleased

### Database

- Migration `0001_ignored_handles`.

### Config

- No config changes.

### Manual steps

- Run `sql/ignore_handles.sql` against the populated DB **after** all handles have been indexed.

  ```bash
  psql "$DATABASE_URL" -f sql/ignore_handles.sql
  ```
