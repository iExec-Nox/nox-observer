# nox-observer

Rust service that observes Nox protocol flows and materializes handle state in Postgres for debugging and alerting.

## Local development

### Prerequisites

- Docker and Docker Compose

### Build

#### Native

Install Rust via [rustup](https://rustup.rs/) if not already present.
On Linux these packages are needed:

- `pkg-config` lets the build script locate the system OpenSSL installation.

```bash
# Ubuntu / Debian
sudo apt install pkg-config
```

Then run:

```bash
cargo build
cargo test
```

#### Docker

```bash
docker compose build
```

### Run

The service requires Postgres, NATS, and an S3-compatible store. Start Postgres first, then run the service either natively or in Docker.

**1. Setup the env file**

```bash
cp .env.example .env
```

Fill in the required credentials for remote service (NATS, S3).

**2. Run dependency services**

```bash
docker compose up -d postgres pgadmin
```

`sql/schema.sql` should be loaded the first time the Postgres volume is created.
Pending migrations then apply automatically when the service starts — see [Database migrations](#database-migrations) below.

**3a. Run natively**

The `.env` defaults use docker-compose service names as hosts. For native `cargo run` some variables need to be overridden:

```bash
set -a && source .env && set +a && \
    NOX_OBSERVER_DATABASE__HOST=localhost \
    cargo run
```

**3b. Run in Docker**

```bash
docker compose up -d nox-observer
```

**4. Check the app is running**

All services reachable from the host (all bound to `127.0.0.1`):

| Service                 | URL                             | Notes                                        |
| ----------------------- | ------------------------------- | -------------------------------------------- |
| nox-observer `/`        | <http://localhost:9000/>        | `{"service":"nox-observer","timestamp":...}` |
| nox-observer `/health`  | <http://localhost:9000/health>  | `{"status":"ok"}`                            |
| nox-observer `/metrics` | <http://localhost:9000/metrics> | Prometheus metrics                           |
| pgAdmin                 | <http://localhost:5050>         | DB server config is auto-loaded              |
| Postgres                | `localhost:5432`                | `psql "$DATABASE_URL"`                       |

### Database migrations

`sql/schema.sql` is only the baseline. Incremental schema changes live in `migrations/<version>_<name>.up.sql` with a matching `.down.sql` to reverse them.

**Pending migrations are applied automatically on startup** using [`sqlx::migrate!`](https://docs.rs/sqlx/latest/sqlx/macro.migrate.html) before it begins serving, so a fresh DB and a normal deploy both converge to the latest schema with no extra step. sqlx embeds and validates the `migrations/` directory at compile time, tracks applied versions and their checksums in the `_sqlx_migrations` table, and holds a Postgres advisory lock during the run so multiple instances starting at once serialize instead of racing.

**Migrations are immutable once applied**, never edit a migration file after it has run anywhere (including in another environment). Sqlx verifies each applied migration's checksum on startup and will refuse to boot on a mismatch. If you need to change behavior, add a new migration instead.

**Reverting and adding new migrations** can be done using [`sqlx-cli`](https://github.com/launchbadge/sqlx/tree/main/sqlx-cli). Install the CLI using `cargo install sqlx-cli`:

```bash
sqlx migrate add -r <name>   # scaffold a new <version>_<name>.up.sql / .down.sql pair
sqlx migrate run             # apply pending migrations manually (same as startup does)
sqlx migrate revert          # roll back the most recently applied migration (runs its .down.sql)
```

`sqlx migrate revert` undoes a single migration; repeat it to step further back.

**Pointing sqlx-cli at a database**: `sqlx-cli` connects via a single `DATABASE_URL` (an env var, the `--database-url` flag, or a `.env` in the working directory).

```bash
# Local docker Postgres
export DATABASE_URL="postgres://nox_user:nox_password@localhost:5432/nox_observer"
sqlx migrate revert

# ...or pass it inline
sqlx migrate revert --database-url $DATABASE_URL
```

For a remote / production database, append `?sslmode=require` to the DB url (matches `NOX_OBSERVER_DATABASE__TLS_ENABLED=true`).

**To ignore an existing handle** fill the handle list in `sql/ignore_handles.sql` and run it manually against a populated database, once those handles have been indexed:

```bash
psql "$DATABASE_URL" -f sql/ignore_handles.sql
```

### Connect to Postgres

The compose stack ships a [pgAdmin](https://www.pgadmin.org/) container reachable at
[http://localhost:5050](http://localhost:5050) (default login: `admin@example.com` / `admin`).

Prefer another client? Point any Postgres GUI at the `DATABASE_URL` from your `.env`
(default: `postgres://nox_user:nox_password@localhost:5432/nox_observer`). A few good ones: [DBeaver](https://dbeaver.io/), [TablePlus](https://tableplus.com/), [Postico](https://eggerapps.at/postico2/).

## Database TLS

The Postgres client connects over TLS, toggled by a single env var:

- `NOX_OBSERVER_DATABASE__TLS_ENABLED` (default `false`)

When enabled the client uses `sslmode=require`: the connection is encrypted but the server certificate is not verified, which matches the trusted private network the managed database sits on. There is no client certificate; the client authenticates with its password. TLS defaults to off so the local docker-compose Postgres (plaintext) works out of the box. In production set `NOX_OBSERVER_DATABASE__TLS_ENABLED=true`.

## Subgraph schema

The file `generated/subgraph/schema.json` is the introspected schema of the upstream subgraph. The `graphql_client` derive macro reads it **at compile time** and generates Rust types for the queries in `src/subgraph/queries.graphql`.

> Despite the `generated/` folder name, this file **is committed** to Git. The folder name only reflects that the file is produced by a tool, not hand-written. See the rationale below.

### When to regenerate

Only when the upstream subgraph schema changes — e.g. after the subgraph team deploys a new version with added/removed/renamed fields.

### How to regenerate

```bash
# One-time install (skip if already installed):
cargo install graphql_client_cli

graphql-client introspect-schema \
  https://thegraph.arbitrum-sepolia-testnet.noxprotocol.io/api/subgraphs/id/BjQAX2HpmsSAzURJimKDhjZZnkSJtaczA8RPumggrStb \
  --output generated/subgraph/schema.json
```

After regenerating:

1. Run `cargo build` — if a query in `src/subgraph/queries.graphql` is no longer compatible with the new schema, the build fails with an explicit error. Fix the queries.
2. **Commit both files** (`schema.json` and any updated `.graphql` query) in the same PR so reviewers see the contract change.
