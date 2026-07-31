# Deployment runbook

Deploy notes per version to centralize **config breaking changes** and **manual steps**
required to move to a version. Schema migrations under `migrations/` apply automatically
on startup.

For the use facing breaking changes checks the changelog file.

## v0.1.0 - Unreleased

### Database

- Migration `0001_grace_period_and_ignored_handles`.

### Config

- New config env var `NOX_OBSERVER_MONITORING__GRACE_PERIOD` (humantime string,
  e.g. `600s`, `10m`, `12h`). Default `10m`.

### Manual steps

- Run `sql/ignore_handles.sql` against the populated DB **after** all handles have been indexed.

  ```bash
  psql "$DATABASE_URL" -f sql/ignore_handles.sql
  ```
