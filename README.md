# nox-observer

Rust service that observes Nox protocol flows and materializes handle state in Postgres for debugging and alerting.

## Local development

### Prerequisites

- Docker and Docker Compose

### Start Postgres

```bash
cp .env.example .env
docker compose up -d
```

`sql/schema.sql` is loaded automatically the first time the volume is created (mounted into the Postgres init scripts directory).

To reload the schema from scratch:

```bash
docker compose down -v
docker compose up -d
```

### Connect

The compose stack ships a [pgAdmin](https://www.pgadmin.org/) container reachable at
[http://localhost:5050](http://localhost:5050) (default login: `admin@example.com` / `admin`).
Register the Postgres server inside pgAdmin with host `postgres`, port `5432`, and the credentials from your `.env`.

Prefer another client? Point any Postgres GUI at the `DATABASE_URL` from your `.env`
(default: `postgres://nox_user:nox_password@localhost:5432/nox_observer`). A few good ones: [DBeaver](https://dbeaver.io/), [TablePlus](https://tableplus.com/), [Postico](https://eggerapps.at/postico2/).

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
