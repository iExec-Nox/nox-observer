# nox-observer

Rust service that observes Nox protocol flows and materializes handle state in Postgres for debugging and alerting.

## Local development

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

The service requires Postgres, NATS, and an S3-compatible store. Start them first, then run the service either natively or in Docker.

**1. Setup the env file**

```bash
cp .env.example .env
```

Fill in the required credentials.

**2. Run dependecy services**

```bash
docker compose up -d postgres pgadmin s3 s3-init
```
`sql/schema.sql` is loaded automatically the first time the Postgres volume is created.

**3a. Run natively**

The `.env` defaults use docker-compose service names as hosts. For native `cargo run` some variables need to be overwridden:

```bash
set -a && source .env && set +a && \
    NOX_OBSERVER_DATABASE__HOST=localhost \
    NOX_OBSERVER_S3__CHAINS__421614__ENDPOINT_URL=http://localhost:9100 \
    cargo run
```

**3b. Run in Docker**

```bash
docker compose up -d nox-observer
```

**4. Check the app is running**

```bash
curl http://localhost:9000/         # → {"service":"nox-observer","timestamp":...}
curl http://localhost:9000/health   # → {"status":"ok"}
curl http://localhost:9000/metrics  # → Prometheus text exposition
```

All services reachable from the host (all bound to `127.0.0.1`):

| Service | URL | Notes |
| --- | --- | --- |
| nox-observer API | <http://localhost:9000> | `/`, `/health`, `/metrics` |
| pgAdmin | <http://localhost:5050> | login `admin@example.com` / `admin` |
| Postgres | `localhost:5432` | `psql "$DATABASE_URL"` |
| MinIO (S3 API) | <http://localhost:9100> | endpoint used by the S3 resolver |
| MinIO console | <http://localhost:9101> | login `minioadmin` / `minioadmin` |


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
