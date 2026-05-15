# nox-observer

Rust service that observes Nox protocol flows and materializes handle state in Postgres for debugging and alerting.

## Local development

### Prerequisites

- Docker and Docker Compose

### Start Postgres

```bash
cp .env.example .env
docker compose up -d postgres
```

`sql/schema.sql` is loaded automatically the first time the volume is created (mounted into the Postgres init scripts directory).

To reload the schema from scratch:

```bash
docker compose down -v
docker compose up -d postgres
```

### Connect

Use any Postgres client and point it at the `DATABASE_URL` from your `.env`
(default: `postgres://nox_user:nox_password@localhost:5432/nox_observer`). Suggested clients: [DBeaver](https://dbeaver.io/), [TablePlus](https://tableplus.com/), [Postico](https://eggerapps.at/postico2/).
