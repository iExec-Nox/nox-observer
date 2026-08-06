# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
cargo build

# Run tests
cargo test

# Run a single test
cargo test <test_name>

# Run with logging
RUST_LOG=debug cargo run

# Lint
cargo clippy

# Format
cargo fmt

# Local dependencies (Postgres + pgAdmin)
cp .env.example .env
docker compose up -d postgres pgadmin

# Apply migrations on top of the baseline schema (see "Schema management")
psql "$DATABASE_URL" -f migrations/0001.sql

# Reset DB schema from scratch (re-runs initdb with sql/schema.sql)
docker compose down -v && docker compose up -d postgres pgadmin

# Run natively (the .env hosts point at compose service names)
set -a && source .env && set +a && NOX_OBSERVER_DATABASE__HOST=localhost cargo run
```

Running the service needs Postgres, NATS, and an S3-compatible store. NATS and S3 are remote; fill their credentials into `.env`. `S3Client::new` HEAD-checks every configured bucket at startup, so bad S3 config fails fast.

## Architecture

Rust/Axum service that observes Nox protocol flows and materializes handle state in Postgres for debugging and alerting. Three **writers** feed the `handles` table, each owning specific columns:

| Writer | Code | Writes |
|--------|------|--------|
| `subgraph_syncer` | `src/subgraph/poller.rs` | `operator`, `tx_hash`, `block_number`, `block_timestamp`, `processed_by_subgraph`, plus `handle_parents` |
| `nats_consumer` | `src/nats/consumer.rs` | `operator`, `caller`, `tx_hash`, `block_number`, `processed_by_nats` (no `block_timestamp` — NATS messages carry none) |
| `s3_poller` | `src/s3/resolver.rs` | `resolved_at`, `processed_by_s3` |

`Application::run` races all three plus the HTTP server in a single `tokio::select!`. **Any one exiting brings the whole process down** — the writers are meant to loop forever, so an exit is always fatal. Subgraph pollers (one task per configured chain) are owned by `SubgraphPollerSupervisor`, which wraps each in a cancel-vs-run race and drains them on shutdown.

Per-writer behavior worth knowing:

- **Subgraph poller** paginates with a composite `(blockNumber, id)` cursor (see `src/subgraph/queries.graphql`); only the block part is persisted in `subgraph_poller_state`, and the in-memory `id` resets to `0x` on restart, harmlessly re-scanning the resume block. `catch_up()` runs at full speed with exponential backoff until a page is non-full, then it enters interval-driven live mode where errors are swallowed (the next tick is the retry).
- **NATS consumer** uses a JetStream durable pull consumer with a 2-tier ack: ACK on success, ACK-discard on poison (deserialize failure, extract failure, non-configured chain), **no-ack** on DB error so JetStream redelivers after `ack_wait`. One PG transaction per message. `allowed_chains` is built at startup from `subgraph.chains ∪ s3.chains`.
- **S3 resolver** adapts its cadence: it loops immediately only when the DB page was full **and** at least one handle resolved; otherwise it waits for the tick. `resolved_at` comes from the S3 object's `LastModified`, clamped by `GREATEST(…, block_timestamp)` in SQL against clock skew.

### HTTP surface

| Route | Purpose |
|-------|---------|
| `GET /` | Service name + UTC timestamp |
| `GET /health` | Liveness probe |
| `GET /metrics` | Prometheus text format |

### Configuration

Loaded from env vars prefixed `NOX_OBSERVER_`. Nested keys use double-underscore (`NOX_OBSERVER_SERVER__HOST`, `NOX_OBSERVER_S3__CHAINS__421614__BUCKET`). Secret files: `NOX_OBSERVER_<SECTION>_FILE` points to a TOML/JSON/YAML file whose keys merge under that section (e.g. `NOX_OBSERVER_DATABASE_FILE=/run/secrets/db.toml`).

Sections: `server`, `subgraph`, `database`, `nats`, `s3`. Everything is validated with `validator` at startup (`Config::validate` in `main.rs`) — invalid config aborts before any connection is opened.

Notable defaults: `server.host=127.0.0.1`, `server.port=9000`, `subgraph.poll_interval_seconds=10`, `subgraph.batch_size=1000`, `s3.poll_interval_seconds=10`, `s3.batch_size=1000`, `s3.max_concurrent_requests=1000`, `database.tls_enabled=false`.

Two cross-cutting rules:

- `validate_chain_consistency`: every chain in `subgraph.chains` must also appear in `s3.chains`, otherwise its handles could never resolve. The reverse (S3 without subgraph) is allowed — NATS may populate those.
- Chain-ID map keys are `String`, not `i32`: the `config` crate produces string-typed map keys from env. Validators enforce they parse as `i32` (matching the `INT chain_id` column) and call sites re-parse.

`DatabaseConfig` takes discrete components rather than a DSN so passwords need no percent-encoding, and both it and `S3ChainConfig` hand-implement `Debug` to redact secrets. `S3Config`/`S3ChainConfig` must keep `Serialize` — `validator` 0.20's nested-`HashMap` derive requires it.

### Database schema

`handles` — one row per handle (primary key `handle_id`, a 66-char `0x…` hex). Tracks which writers have seen it (`processed_by_subgraph`, `processed_by_s3`, `processed_by_nats`) and when ciphertext was resolved (`resolved_at`). CHECK constraints enforce hex formats, positive `chain_id`, and `resolved_at >= block_timestamp`.

`ignored` (added by `migrations/0001.sql`) excludes a row from observer metrics; it defaults to `false` for new handles, and the migration backfills a hardcoded list of pre-existing stuck handle IDs to `true` at rollout. The S3 resolver's hot loop (`fetch_unresolved_handles` in `src/db.rs`) also skips `ignored` handles, so they're never re-fetched; the partial index `idx_handles_unresolved` excludes them too.

`handle_parents` — junction table for parent→child handle relationships, written only by `subgraph_syncer`. Foreign keys to `handles` are deliberately **not** enforced: subgraph reindexing and sub-second blocks can produce a link before its endpoints are inserted. Queries needing handle metadata should INNER JOIN.

`subgraph_poller_state` — one row per chain holding the poller's `cursor_block`, so multichain deployments resume independently.

**Upsert invariant** (`sql/upsert_handle.sql`): the `ON CONFLICT` clause only fires a write when the incoming row adds new information (fills a previously-NULL column or flips a `processed_by_*` flag from false to true). This avoids WAL churn under heavy NATS redelivery / S3 polling retries. `sql/mark_handle_resolved.sql` is similarly guarded by `AND NOT h.processed_by_s3`.

Queries live as `.sql` files under `sql/` and are pulled in with `include_str!` in `src/db.rs`. These are runtime-checked, not `sqlx::query!` macros — **a column rename in `sql/schema.sql` will not fail the build, it will fail at runtime.**

### Schema management

`sql/schema.sql` is the production baseline (mounted by docker-compose `initdb`, so it only runs on a fresh volume) and stays unchanged. Incremental changes live in `migrations/NNNN.sql`.

### Module layout

- `main.rs` — tracing setup, rustls provider install, config load+validate, hands off to `Application`
- `config.rs` — `Config` and its sections, with `validator` rules and defaults
- `application.rs` — builds the router and Prometheus layer, constructs every writer, races them in `run()`, owns graceful shutdown
- `handlers.rs` — thin route handlers
- `errors.rs` — one error enum per subsystem (`NatsError`, `SubgraphError`, `SubgraphPollerError`, `S3ResolverError`). Retry-driving variants expose `is_transient()`; extend this when adding domain errors
- `db.rs` — `Db` pool wrapper, `NewHandle`, and every query
- `events.rs` — serde types for the `nox_ingestor` NATS payload; `Operator` is a tagged enum whose `wire_tag()` is the canonical `handles.operator` string and whose `emitted_handles()` lists the handles one event produces
- `nats/` — `client.rs` (connection + state watch), `consumer.rs` (pull loop, ack policy, handle extraction)
- `s3/` — `client.rs` (per-chain buckets, shared semaphore, HEAD + error classification), `resolver.rs` (poll loop)
- `subgraph/` — `client.rs` (`graphql_client` codegen), `poller.rs` (cursor pagination), `supervisor.rs` (per-chain task fleet)
- `utils.rs` — PEM normalization for NATS TLS material passed via env

### Subgraph codegen

`generated/subgraph/schema.json` is the introspected upstream schema, read **at compile time** by the `GraphQLQuery` derive in `src/subgraph/client.rs`. It **is committed** despite the folder name. Regenerate only when the upstream subgraph changes (`graphql-client introspect-schema <url> --output generated/subgraph/schema.json`), then `cargo build` — incompatible queries fail the build — and commit the schema and any query change together.
